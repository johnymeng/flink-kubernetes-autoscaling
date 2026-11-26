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

package org.apache.flink.autoscaler.a4s;

import io.a4s.api.CachePoint;
import io.a4s.api.ClusterSnapshot;
import io.a4s.api.EvaluationRequest;
import io.a4s.api.OperatorConfiguration;
import io.a4s.api.OperatorMetrics;
import io.a4s.api.OperatorTelemetry;
import io.a4s.api.PlatformCapabilities;
import io.a4s.api.PlatformCapabilities.Action;
import io.a4s.api.PlatformCapabilities.Support;
import io.a4s.api.PolicyConfiguration;
import io.a4s.api.PolicyState;
import io.a4s.api.ScalingConstraints;
import io.a4s.api.ScalingPlan;
import io.a4s.core.A4sPolicy;
import io.a4s.http.DecisionClient;
import io.a4s.http.SnapshotRecorder;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.autoscaler.JobVertexScaler.KeyGroupOrPartitionsAdjustMode;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts the operator's execution context and aggregate telemetry to the A4S v1 contract. */
public final class Flink118A4sAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(Flink118A4sAdapter.class);
    private static final long BYTES_PER_MIB = 1024L * 1024L;

    private final A4sPolicy embedded = new A4sPolicy();

    public EvaluationRequest request(
            String jobId,
            String generation,
            Instant now,
            JobTopology topology,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, ScalingSummary> requested,
            Map<JobVertexID, ResourceProfile> profiles,
            Configuration conf,
            boolean statefulRescaleAvailable,
            Map<JobVertexID, Map<String, String>> taskPlacements) {
        var operators = new ArrayList<OperatorMetrics>();
        for (var id : topology.getVerticesInTopologicalOrder()) {
            var metrics = evaluated.getVertexMetrics().get(id);
            var vertex = topology.get(id);
            int current = vertex.getParallelism();
            var summary = requested.get(id);
            boolean frozen =
                    vertex.isFinished()
                            || conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS)
                                    .contains(id.toHexString());
            int start = frozen || summary == null ? current : summary.getNewParallelism();
            MissRateCurve curve = evaluated.getMissRateCurves().get(id);
            boolean useCache =
                    !frozen
                            && curve != null
                            && curve.isUsable()
                            && conf.getBoolean("state.backend.rocksdb.memory.managed", true)
                            && conf.getString("state.backend.rocksdb.memory.fixed-per-slot", null)
                                    == null;
            var points =
                    useCache
                            ? curve.getPoints().stream()
                                    .map(
                                            p ->
                                                    new CachePoint(
                                                            p.getCacheSizeBytes(), p.getMissRate()))
                                    .collect(Collectors.toList())
                            : List.<CachePoint>of();
            int min =
                    useCache
                            ? Math.min(current, conf.get(AutoScalerOptions.VERTEX_MIN_PARALLELISM))
                            : start;
            int max =
                    useCache
                            ? Math.min(
                                    vertex.getMaxParallelism(),
                                    Math.max(
                                            current,
                                            conf.get(AutoScalerOptions.VERTEX_MAX_PARALLELISM)))
                            : start;
            int divisions = vertex.getNumSourcePartitions();
            if (divisions > 0 && useCache) {
                max = Math.min(max, Math.max(current, divisions));
            }
            if (divisions <= 0 && vertex.getInputs().containsValue(ShipStrategy.HASH)) {
                divisions = vertex.getMaxParallelism();
            }
            var permitted = permittedParallelisms(min, max, current, start, divisions, conf);
            var profile = profiles.get(id);
            var allocation =
                    new OperatorConfiguration(
                            current,
                            null,
                            profile.getManagedMemory().getBytes(),
                            profile.getCpuCores().getValue().doubleValue());
            var upstream =
                    vertex.getInputs().keySet().stream()
                            .map(JobVertexID::toHexString)
                            .sorted()
                            .collect(Collectors.toList());
            operators.add(
                    new OperatorMetrics(
                            id.toHexString(),
                            topology.isSource(id) ? "source" : "vertex",
                            useCache ? Boolean.TRUE : null,
                            allocation,
                            min,
                            max,
                            permitted,
                            start,
                            useCache,
                            finite(metrics.get(ScalingMetric.TARGET_DATA_RATE)),
                            upstream,
                            useCache ? OperatorMetrics.CapacityAxis.LEGACY_AGGREGATE_BYTES : null,
                            points,
                            null,
                            telemetry(
                                    metrics,
                                    profile,
                                    current,
                                    evaluated.getObservedSourceBacklogs().get(id),
                                    taskPlacements.getOrDefault(id, Map.of()))));
        }
        double baseMiB = conf.get(AutoScalerOptions.A4S_MEMORY_BASE_MB);
        if (!Double.isFinite(baseMiB) || baseMiB < 0 || baseMiB > 600) {
            throw new IllegalArgumentException("A4S base memory must be in [0, 600] MiB");
        }
        var capabilities =
                new PlatformCapabilities(
                        Map.of(
                                Action.PARALLELISM,
                                Support.WITH_RESTART,
                                Action.MANAGED_MEMORY_PER_TASK,
                                Support.WITH_RESTART,
                                Action.STATEFUL_RECOVERY,
                                statefulRescaleAvailable
                                        ? Support.WITH_RESTART
                                        : Support.UNSUPPORTED));
        var snapshot =
                new ClusterSnapshot(
                        jobId,
                        UUID.randomUUID().toString(),
                        generation,
                        evaluated.getWindowEnd().toEpochMilli(),
                        evaluated.getWindowStart().toEpochMilli(),
                        evaluated.getWindowEnd().toEpochMilli(),
                        true,
                        operators,
                        List.of(),
                        conf.getString("state.backend", null),
                        null,
                        false,
                        capabilities);
        var constraints =
                new ScalingConstraints(
                        0,
                        Long.MAX_VALUE,
                        null,
                        600 * BYTES_PER_MIB,
                        exclusiveCpuBudget(conf, topology, operators),
                        0,
                        conf.get(AutoScalerOptions.A4S_SNAPSHOT_MAX_AGE).toMillis(),
                        null,
                        null,
                        null,
                        false);
        Long override =
                conf.get(AutoScalerOptions.A4S_MANAGED_MEMORY_OVERRIDE_ENABLED)
                        ? conf.get(AutoScalerOptions.A4S_MANAGED_MEMORY_OVERRIDE_MB) * BYTES_PER_MIB
                        : null;
        var policy =
                new PolicyConfiguration(
                        PolicyConfiguration.Mode.LEGACY_SLOT_RESERVATION,
                        conf.get(AutoScalerOptions.A4S_MAX_ATTEMPTS),
                        conf.get(AutoScalerOptions.A4S_HIT_LATENCY_SEC),
                        conf.get(AutoScalerOptions.A4S_MISS_LATENCY_SEC),
                        (long) Math.ceil(baseMiB * BYTES_PER_MIB),
                        override);
        return new EvaluationRequest(
                EvaluationRequest.SCHEMA_VERSION,
                snapshot,
                constraints,
                policy,
                new PolicyState(1, null),
                now.toEpochMilli());
    }

    private static Double exclusiveCpuBudget(
            Configuration conf, JobTopology topology, List<OperatorMetrics> operators) {
        var quota = conf.getOptional(AutoScalerOptions.CPU_QUOTA).orElse(null);
        if (quota == null
                || !Double.isFinite(quota)
                || quota < 0
                || topology.getSlotSharingGroupMapping().values().stream()
                        .anyMatch(group -> group.size() > 1)
                || !topology.getFinishedVertices().isEmpty()) {
            return null;
        }
        // Justin reports applied profiles only for exclusive running slots and creates
        // a group per vertex on update. The executor still checks whole TaskManagers.
        var currentCores =
                operators.stream()
                        .map(
                                operator ->
                                        BigDecimal.valueOf(operator.current.cpuCoresPerTask)
                                                .multiply(
                                                        BigDecimal.valueOf(
                                                                operator.current.parallelism)))
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
        return currentCores.compareTo(BigDecimal.valueOf(quota)) <= 0 ? quota : null;
    }

    private static List<Integer> permittedParallelisms(
            int min, int max, int current, int start, int divisions, Configuration conf) {
        if (divisions <= 0 || min == max) {
            return List.of();
        }
        boolean maximize =
                conf.get(AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE)
                        == KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION;
        return IntStream.rangeClosed(min, max)
                .filter(
                        p ->
                                p == current
                                        || p == start
                                        || divisions % p == 0
                                        || maximize && p > 1 && divisions / p < divisions / (p - 1))
                .boxed()
                .collect(Collectors.toList());
    }

    private static OperatorTelemetry telemetry(
            Map<ScalingMetric, EvaluatedScalingMetric> metrics,
            ResourceProfile profile,
            int parallelism,
            Long observedSourceBacklog,
            Map<String, String> taskPlacement) {
        Double busy = finite(metrics.get(ScalingMetric.LOAD));
        long bytesPerSlot = profile.getTotalMemory().getBytes();
        Long totalSlotBytes =
                bytesPerSlot <= Long.MAX_VALUE / parallelism ? bytesPerSlot * parallelism : null;
        return new OperatorTelemetry(
                finite(metrics.get(ScalingMetric.INPUT_RATE)),
                finite(metrics.get(ScalingMetric.OUTPUT_RATE)),
                finite(metrics.get(ScalingMetric.TRUE_PROCESSING_RATE)),
                busy != null && busy <= 1 ? busy : null,
                null,
                null,
                null,
                totalSlotBytes,
                bytesPerSlot,
                null,
                null,
                null,
                null,
                null,
                null,
                observedSourceBacklog,
                taskPlacement);
    }

    private static Double finite(EvaluatedScalingMetric metric) {
        if (metric == null) {
            return null;
        }
        double value = metric.getAverage();
        return Double.isFinite(value) && value >= 0 ? value : null;
    }

    public void record(Configuration conf, EvaluationRequest request, ScalingPlan recommendation)
            throws IOException {
        String filename = conf.get(AutoScalerOptions.A4S_SNAPSHOT_RECORD_FILE);
        if (filename == null) {
            return;
        }
        String directory = System.getenv("A4S_SNAPSHOT_DIR");
        if (directory == null || !filename.matches("[a-zA-Z0-9_-]+[.]jsonl")) {
            throw new IOException(
                    "Snapshot recording requires A4S_SNAPSHOT_DIR and a JSONL basename");
        }
        SnapshotRecorder.append(Path.of(directory).resolve(filename), request, recommendation);
    }

    public ScalingPlan evaluate(Configuration conf, EvaluationRequest request)
            throws IOException, InterruptedException {
        String remote = conf.get(AutoScalerOptions.A4S_RUNNER_URL);
        if (remote == null) {
            return embedded.evaluate(request);
        }
        var endpoint = URI.create(remote);
        var timeout = conf.get(AutoScalerOptions.A4S_RUNNER_TIMEOUT);
        if (conf.get(AutoScalerOptions.A4S_SNAPSHOT_RECORD_FILE) == null) {
            return Remote.CLIENT.evaluate(endpoint, timeout, request);
        }
        return Remote.CLIENT.evaluate(
                endpoint,
                timeout,
                request,
                timing -> LOG.info(
                        "A4S runner timing job={} snapshot={} requestBytes={} responseBytes={} "
                                + "encodeNanos={} exchangeNanos={} decodeNanos={}",
                        request.snapshot.jobId,
                        request.snapshot.snapshotId,
                        timing.requestBytes,
                        timing.responseBytes,
                        timing.encodeNanos,
                        timing.exchangeNanos,
                        timing.decodeNanos));
    }

    public boolean accepts(EvaluationRequest request, ScalingPlan plan, Instant now) {
        return embedded.isApplicable(request, Objects.requireNonNull(plan), now.toEpochMilli());
    }

    private static final class Remote {
        private static final DecisionClient CLIENT = new DecisionClient();
    }
}
