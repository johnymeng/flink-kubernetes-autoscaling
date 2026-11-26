/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.autoscaler.DelayedScaleDown;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingExecutor;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.autoscaler.utils.justin.ResourceProfileParser;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsUpdateHeaders;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

public final class LocalAdaptiveA4sSmoke {
    private static final AtomicBoolean SECOND_PHASE = new AtomicBoolean();
    private static final ConcurrentLinkedQueue<Tuple2<Long, Long>> OUTPUT =
            new ConcurrentLinkedQueue<>();

    public static final class DeterministicSource extends RichParallelSourceFunction<Long>
            implements CheckpointedFunction {
        private volatile boolean running = true;
        private transient ListState<Long> offsets;
        private long next = 1;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    long bound = SECOND_PHASE.get() ? 200 : 100;
                    while (next <= bound && running) {
                        ctx.collect(next++);
                    }
                }
                Thread.sleep(20);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            offsets =
                    ctx.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("offset", Long.class));
            if (ctx.isRestored()) {
                next = offsets.get().iterator().next();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            offsets.clear();
            offsets.add(next);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static final class CountByKey
            extends KeyedProcessFunction<Integer, Long, Tuple2<Long, Long>> {
        private transient ValueState<Long> counts;

        @Override
        public void open(Configuration config) throws Exception {
            counts = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(Long value, Context context, Collector<Tuple2<Long, Long>> out)
                throws Exception {
            Long before = counts.value();
            long count = before == null ? 1 : before + 1;
            counts.update(count);
            out.collect(Tuple2.of(value, count));
        }
    }

    public static final class CollectResults implements SinkFunction<Tuple2<Long, Long>> {
        @Override
        public void invoke(Tuple2<Long, Long> value, Context context) {
            OUTPUT.add(Tuple2.of(value.f0, value.f1));
        }
    }

    public static void main(String[] args) throws Exception {
        var conf = new Configuration();
        conf.set(RestOptions.BIND_ADDRESS, "127.0.0.1");
        conf.set(RestOptions.BIND_PORT, "0");
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        conf.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofSeconds(2));
        conf.set(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT, Duration.ofMillis(200));
        conf.set(TaskManagerOptions.CPU_CORES, 2.0);
        conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
        conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ZERO);
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(256));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(64));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(64));
        var cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(conf)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(4)
                                .setCommonBindAddress("127.0.0.1")
                                .build());
        try {
            cluster.start();
            URI address = cluster.getRestAddress().get(10, TimeUnit.SECONDS);
            var clientConfig = new Configuration(conf);
            clientConfig.set(RestOptions.ADDRESS, address.getHost());
            clientConfig.set(RestOptions.PORT, address.getPort());
            try (var client = new RestClusterClient<String>(clientConfig, "local-adaptive")) {
                var env =
                        StreamExecutionEnvironment.getExecutionEnvironment()
                                .setParallelism(1)
                                .setMaxParallelism(8);
                env.enableCheckpointing(250);
                env.getCheckpointConfig()
                        .setCheckpointStorage(Path.of(args[1], "checkpoints").toUri());
                env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
                env.addSource(new DeterministicSource())
                        .name("deterministic-source")
                        .slotSharingGroup("source")
                        .keyBy(value -> (int) (value % 4))
                        .process(new CountByKey())
                        .name("stateful-count")
                        .slotSharingGroup("stateful")
                        .addSink(new CollectResults())
                        .slotSharingGroup("stateful");
                var graph = env.getStreamGraph().getJobGraph();
                var id = graph.getJobID();
                cluster.submitJob(graph).get(15, TimeUnit.SECONDS);
                awaitOutput(100);
                var before = awaitRunning(client, id, 1);
                var source = find(before, "deterministic-source").getJobVertexID();
                var stateful = find(before, "stateful-count").getJobVertexID();
                var beforeProfiles = requirements(client, id).getJobVertexParallelisms();
                var sourceProfile = beforeProfiles.get(source).getAppliedResourceProfile();
                var statefulProfile = beforeProfiles.get(stateful).getAppliedResourceProfile();
                check(
                        sourceProfile != null && statefulProfile != null,
                        "Both live profiles are required");
                var checkpoint = cluster.triggerCheckpoint(id).get(20, TimeUnit.SECONDS);
                check(checkpoint != null, "Checkpoint path missing");
                System.out.println(
                        "Phase 1 observed 100 records and completed a checkpoint: " + checkpoint);

                var plannerConfig = new Configuration();
                plannerConfig.set(AutoScalerOptions.A4S_ENABLED, true);
                plannerConfig.set(AutoScalerOptions.JUSTIN_ENABLED, true);
                plannerConfig.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, false);
                plannerConfig.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1);
                plannerConfig.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 2);
                plannerConfig.set(
                        AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(source.toHexString()));
                plannerConfig.set(AutoScalerOptions.A4S_HIT_LATENCY_SEC, 0.0001);
                plannerConfig.set(AutoScalerOptions.A4S_MISS_LATENCY_SEC, 0.0002);
                plannerConfig.set(AutoScalerOptions.A4S_MANAGED_MEMORY_OVERRIDE_ENABLED, true);
                plannerConfig.set(
                        AutoScalerOptions.A4S_MANAGED_MEMORY_OVERRIDE_MB,
                        Math.toIntExact(
                                statefulProfile.getManagedMemory().getBytes() / (1024L * 1024L)));
                plannerConfig.set(AutoScalerOptions.A4S_RUNNER_URL, args[0]);
                plannerConfig.set(
                        AutoScalerOptions.A4S_SNAPSHOT_RECORD_FILE, "local-adaptive.jsonl");
                var ctx =
                        new JobAutoScalerContext<>(
                                id,
                                id,
                                JobStatus.RUNNING,
                                plannerConfig,
                                new UnregisteredMetricsGroup(),
                                () ->
                                        new RestClusterClient<String>(
                                                clientConfig, "local-adaptive"));
                var topology =
                        new JobTopology(
                                new VertexInfo(source, Map.of(), 1, 8),
                                new VertexInfo(stateful, Map.of(source, ShipStrategy.HASH), 1, 8));
                var now = Instant.now();
                var curve =
                        new MissRateCurve(
                                List.of(
                                        new MissRateCurve.Point(
                                                statefulProfile.getManagedMemory().getBytes(),
                                                0.05)));
                var evaluated =
                        new EvaluatedMetrics(
                                Map.of(source, metrics(100), stateful, metrics(15_000)),
                                Map.of(
                                        ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(0),
                                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                                EvaluatedScalingMetric.of(0)),
                                Map.of(stateful, curve),
                                now.minusMillis(5),
                                now,
                                Instant.ofEpochMilli(
                                        before.getTimestamps().get(JobStatus.RUNNING)));
                var state = new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>();
                var executor =
                        new ScalingExecutor<JobID, JobAutoScalerContext<JobID>>(
                                (context, type, reason, message, key, interval) ->
                                        System.out.println(reason + " " + message),
                                state);
                check(
                        executor.scaleResource(
                                ctx,
                                evaluated,
                                new HashMap<>(),
                                new ScalingTracking(),
                                now,
                                topology,
                                new DelayedScaleDown()),
                        "Remote A4S executor did not produce a plan");
                var parallelisms = state.getParallelismOverrides(ctx);
                var profiles = state.getResourceProfileOverrides(ctx);
                check(
                        parallelisms.equals(
                                Map.of(source.toHexString(), "1", stateful.toHexString(), "2")),
                        "Unexpected A4S parallelism: " + parallelisms);
                check(
                        ResourceProfileParser.parse(profiles.get(source.toHexString()))
                                .equals(sourceProfile),
                        "Source profile changed");
                check(
                        ResourceProfileParser.parse(profiles.get(stateful.toHexString()))
                                .equals(statefulProfile),
                        "Stateful profile changed");
                System.out.println(
                        "The executor stored the remote A4S plan with source parallelism 1 and stateful parallelism 2.");

                var builder = JustinResourceRequirements.newBuilder();
                for (var kv : parallelisms.entrySet()) {
                    int target = Integer.parseInt(kv.getValue());
                    builder.setParallelismForJobVertex(
                            JobVertexID.fromHexString(kv.getKey()),
                            target,
                            target,
                            ResourceProfileParser.parse(profiles.get(kv.getKey())));
                }
                var header = JustinResourceRequirementsUpdateHeaders.INSTANCE;
                var update = header.getUnresolvedMessageParameters();
                update.jobPathParameter.resolve(id);
                client.sendRequest(
                                header, update, new JustinResourceRequirementsBody(builder.build()))
                        .get(10, TimeUnit.SECONDS);

                var after = awaitRunning(client, id, 2);
                var afterProfiles = requirements(client, id).getJobVertexParallelisms();
                check(after.getJobId().equals(before.getJobId()), "Job ID changed");
                check(
                        after.getTimestamps().get(JobStatus.RUNNING)
                                > before.getTimestamps().get(JobStatus.RUNNING),
                        "Job did not enter a new running epoch");
                check(
                        sourceProfile.equals(afterProfiles.get(source).getAppliedResourceProfile()),
                        "Source profile differs after rescale");
                check(
                        statefulProfile.equals(
                                afterProfiles.get(stateful).getAppliedResourceProfile()),
                        "Stateful profile differs after rescale");
                System.out.println(
                        "Justin applied stateful parallelism 2 and preserved both exact slot profiles in a new running epoch.");

                SECOND_PHASE.set(true);
                awaitOutput(200);
                var seen = new HashMap<Long, Long>();
                for (var result : OUTPUT) {
                    check(
                            seen.putIfAbsent(result.f0, result.f1) == null,
                            "Duplicate ID " + result.f0);
                }
                for (long i = 1; i <= 200; i++) {
                    check(
                            Long.valueOf((i - 1) / 4 + 1).equals(seen.get(i)),
                            "Unexpected keyed count for ID " + i);
                }
                System.out.println(
                        "Verified all 200 unique IDs and keyed counts after the remote A4S-driven checkpoint rescale.");
                cluster.cancelJob(id).get(10, TimeUnit.SECONDS);
            }
        } finally {
            cluster.closeAsync().get(20, TimeUnit.SECONDS);
        }
    }

    private static Map<ScalingMetric, EvaluatedScalingMetric> metrics(double target) {
        var out = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        out.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(1));
        out.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(8));
        out.put(ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(0));
        out.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(target, target));
        out.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0));
        out.put(ScalingMetric.TRUE_PROCESSING_RATE, new EvaluatedScalingMetric(target, target));
        out.put(ScalingMetric.SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(target));
        out.put(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(0));
        return out;
    }

    private static JobDetailsInfo.JobVertexDetailsInfo find(JobDetailsInfo job, String name) {
        return job.getJobVertexInfos().stream()
                .filter(v -> v.getName().contains(name))
                .findFirst()
                .orElseThrow();
    }

    private static JobDetailsInfo awaitRunning(RestClusterClient<?> client, JobID id, int target)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() - deadline < 0) {
            var job = client.getJobDetails(id).get(5, TimeUnit.SECONDS);
            if (job.getJobStatus() == JobStatus.RUNNING
                    && job.getJobVertexInfos().size() == 2
                    && find(job, "stateful-count").getParallelism() == target
                    && find(job, "deterministic-source").getParallelism() == 1
                    && job.getJobVertexInfos().stream()
                            .allMatch(
                                    v ->
                                            v.getTasksPerState()
                                                            .getOrDefault(ExecutionState.RUNNING, 0)
                                                    == v.getParallelism())) {
                return job;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Job did not reach expected running parallelism " + target);
    }

    private static JustinResourceRequirements requirements(RestClusterClient<?> client, JobID jobId)
            throws Exception {
        var header = JustinResourceRequirementsHeaders.INSTANCE;
        var params = header.getUnresolvedMessageParameters();
        params.jobPathParameter.resolve(jobId);
        params.includeAppliedProfile.resolve(List.of(true));
        return client.sendRequest(header, params, EmptyRequestBody.getInstance())
                .get(5, TimeUnit.SECONDS)
                .asJobResourceRequirements()
                .orElseThrow();
    }

    private static void awaitOutput(int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (OUTPUT.size() < expected && System.nanoTime() - deadline < 0) {
            Thread.sleep(25);
        }
        check(
                OUTPUT.size() == expected,
                "Expected " + expected + " outputs but got " + OUTPUT.size());
    }

    private static void check(boolean valid, String message) {
        if (!valid) {
            throw new AssertionError(message);
        }
    }
}
