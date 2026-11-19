package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.LoggingEventHandler;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Evaluates and proposes with production A4S policy. A separate scope guard owns realization. */
public final class RyanQ8Policy {
    static final JobVertexID JOIN =
            JobVertexID.fromHexString("11ffd1a2f5905feab358b0af9a75fe4e");
    private static final Duration POLL = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    "Usage: RyanQ8Policy ARM JOB_ID DEPLOYMENT_JSON REST_URL_FILE STATE_FILE");
        }
        String arm = args[0];
        if (!"hardcoded".equals(arm) && !"calibrated".equals(arm)) {
            throw new IllegalArgumentException("Expected hardcoded or calibrated");
        }
        JobID jobId = JobID.fromHexString(args[1]);
        Path endpoint = Path.of(args[3]);
        Path handshake = Path.of(args[4]);
        var config = loadConfig(Path.of(args[2]), "calibrated".equals(arm));
        if (config.get(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_USE_FIXED_BASELINE)) {
            throw new IllegalArgumentException("The active online treatment requires its fitted baseline");
        }
        var tracing = new PolicyTrace(arm, jobId);
        var metricCollector = new RestApiMetricsCollector<String, JobAutoScalerContext<String>>();
        var eventHandler = new LoggingEventHandler<String, JobAutoScalerContext<String>>();
        var store = new InMemoryAutoScalerStateStore<String, JobAutoScalerContext<String>>();
        var realizer = new ProposalRealizer(tracing);
        var scaler =
                new JobAutoScalerImpl<>(
                        metricCollector, tracing, new PairedExecutor(eventHandler, store, tracing),
                        eventHandler, realizer, store);
        var metrics = new UnregisteredMetricsGroup();
        String acknowledgedTransaction = "";
        while (!Thread.currentThread().isInterrupted()) {
            long start = System.nanoTime();
            try {
                JsonNode gate = readJson(handshake);
                String state = gate.path("state").asText();
                if ("stopped".equals(state)) {
                    return;
                }
                String ack = gate.path("acknowledged_transaction").asText();
                if (!ack.isEmpty() && !ack.equals(acknowledgedTransaction)) {
                    ScalingExecutor.scalingTriggered(jobId);
                    acknowledgedTransaction = ack;
                }
                if ("ready".equals(state)) {
                    URI uri = localEndpoint(Files.readString(endpoint).strip());
                    var context =
                            new JobAutoScalerContext<>(
                                    arm, jobId, JobStatus.RUNNING, config, metrics,
                                    () -> new RestClusterClient<>(
                                            config, arm,
                                            (c, e) -> new StandaloneClientHAServices(uri.toString())));
                    try (var client = context.getRestClusterClient()) {
                        var details = client.getJobDetails(jobId).get(8, TimeUnit.SECONDS);
                        if (details.getJobStatus() == JobStatus.RUNNING) {
                            tracing.jobStartMillis = details.getStartTime();
                            scaler.scale(context);
                        }
                    }
                }
            } catch (Exception e) {
                var row = tracing.event("policy_error");
                row.put("message", e.toString());
                emit(row);
                e.printStackTrace(System.err);
            }
            long remaining = POLL.toNanos() - (System.nanoTime() - start);
            if (remaining > 0) {
                TimeUnit.NANOSECONDS.sleep(remaining);
            }
        }
    }

    private static Configuration loadConfig(Path path, boolean online) throws Exception {
        JsonNode spec = readJson(path).path("spec");
        var config = new Configuration();
        spec.path("flinkConfiguration").fields()
                .forEachRemaining(property -> config.setString(property.getKey(), property.getValue().asText()));
        config.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.parse(spec.path("taskManager").path("resource").path("memory").asText()));
        if (config.get(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED) != online) {
            throw new IllegalArgumentException("Arm and manifest cost modes disagree");
        }
        return config
                .set(AutoScalerOptions.AUTOSCALER_ENABLED, true)
                .set(AutoScalerOptions.SCALING_ENABLED, true)
                .set(AutoScalerOptions.JUSTIN_ENABLED, true)
                .set(AutoScalerOptions.A4S_ENABLED, true)
                // Collect identical native timing data on both arms. PolicyTrace selects the active model.
                .set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true)
                .set(AutoScalerOptions.MEMORY_TUNING_ENABLED, false);
    }

    private static URI localEndpoint(String text) {
        URI uri = URI.create(text);
        if (!"http".equals(uri.getScheme()) || !"127.0.0.1".equals(uri.getHost())
                || uri.getPort() < 1 || !uri.getPath().isEmpty()
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw new IllegalArgumentException("Expected the owned private loopback forward");
        }
        return uri;
    }

    private static JsonNode readJson(Path path) throws Exception {
        return RestMapperUtils.getStrictObjectMapper().readTree(path.toFile());
    }

    private static void emit(Map<String, Object> row) throws Exception {
        System.out.println(RestMapperUtils.getStrictObjectMapper().writeValueAsString(row));
        System.out.flush();
    }

    private static final class PolicyTrace extends ScalingMetricEvaluator {
        private final String arm;
        private final JobID job;
        private long jobStartMillis;
        private CollectedMetricHistory history;
        private Duration restartDuration;
        private Map<String, Object> snapshot;
        private Map<String, Object> comparison;

        private PolicyTrace(String arm, JobID job) {
            this.arm = arm;
            this.job = job;
        }

        private Map<String, Object> event(String type) {
            var row = new LinkedHashMap<String, Object>();
            row.put("type", type);
            row.put("arm", arm);
            row.put("job_id", job.toHexString());
            row.put("job_start_epoch_ms", jobStartMillis);
            row.put("epoch", Instant.now().toEpochMilli() / 1_000.0);
            return row;
        }

        @Override
        public EvaluatedMetrics evaluate(
                Configuration config, CollectedMetricHistory history, Duration restartTime) {
            boolean activeOnline = "calibrated".equals(arm);
            var fixed = new Configuration(config).set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, false);
            var onlineConfig = new Configuration(config).set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
            var result = super.evaluate(activeOnline ? onlineConfig : fixed, history, restartTime);
            this.history = history;
            this.restartDuration = restartTime;
            var samples = history.getMetricHistory();
            var latest = samples.get(samples.lastKey());
            int p = history.getJobTopology().get(JOIN).getParallelism();
            var estimate = selectedOnlineCacheCost(onlineConfig, samples, JOIN, p);
            var mrc = result.getMissRateCurves().get(JOIN);
            double baseline = config.get(AutoScalerOptions.A4S_HIT_LATENCY_SEC);
            double fixedSlope = config.get(AutoScalerOptions.A4S_MISS_LATENCY_SEC) - baseline;
            var row = event("selection");
            row.put("snapshot", samples.lastKey().toString());
            row.put("current_p", p);
            row.put("target_records_s", result.getVertexMetrics().get(JOIN).get(ScalingMetric.TARGET_DATA_RATE).getAverage());
            row.put("fixed_baseline_s", baseline);
            row.put("online_baseline_s", estimate.map(cost -> cost.getZeroMissSecondsPerRecord()).orElse(baseline));
            row.put("fixed_slope_s", fixedSlope);
            row.put("online_slope_s", estimate.map(cost -> cost.getMissPenaltySecondsPerRecord()).orElse(fixedSlope));
            row.put("online_fit_qualified", estimate.isPresent());
            row.put("fitted_baseline_s", estimate.map(cost -> cost.getZeroMissSecondsPerRecord()).orElse(null));
            row.put("mrc_points", mrc == null ? 0 : mrc.getPoints().size());
            try {
                emit(row);
            } catch (Exception e) {
                throw new IllegalStateException("Could not archive fit", e);
            }
            snapshot = new LinkedHashMap<>(row);
            snapshot.put("mrc", mrc == null ? null : mrc.getPoints());
            snapshot.put("subtasks", latest.getCacheCostSamples() == null ? null : latest.getCacheCostSamples().get(JOIN));
            return result;
        }
    }

    /** Repeat just the placement decision with the other fit, before cluster actuation. */
    private static final class PairedExecutor extends ScalingExecutor<String, JobAutoScalerContext<String>> {
        private final PolicyTrace trace;
        private final LoggingEventHandler<String, JobAutoScalerContext<String>> events;
        private Map<JobVertexID, ScalingSummary> trigger;

        PairedExecutor(LoggingEventHandler<String, JobAutoScalerContext<String>> events,
                InMemoryAutoScalerStateStore<String, JobAutoScalerContext<String>> store, PolicyTrace trace) {
            super(events, store);
            this.trace = trace;
            this.events = events;
        }

        @Override
        Map<JobVertexID, ScalingSummary> computeScalingSummary(JobAutoScalerContext<String> ctx,
                EvaluatedMetrics metrics, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> history,
                Duration restart, JobTopology topology, DelayedScaleDown delayed) {
            trigger = super.computeScalingSummary(ctx, metrics, history, restart, topology, delayed);
            return trigger;
        }

        @Override
        public boolean scaleResource(JobAutoScalerContext<String> ctx, EvaluatedMetrics metrics,
                Map<JobVertexID, SortedMap<Instant, ScalingSummary>> history, ScalingTracking tracking,
                Instant now, JobTopology topology, DelayedScaleDown delayed) throws Exception {
            trigger = Map.of();
            boolean applied = super.scaleResource(ctx, metrics, history, tracking, now, topology, delayed);
            if (trigger.isEmpty()) {
                return applied;
            }
            var comparison = new LinkedHashMap<>(trace.snapshot);
            comparison.put("trigger_parallelism", trigger.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                    e -> e.getKey().toHexString(), e -> e.getValue().getNewParallelism())));
            String other = trace.arm.equals("calibrated") ? "fixed" : "online";
            comparison.put(other, preview(other, ctx, now, topology));
            comparison.put("slope_only", preview("slope_only", ctx, now, topology));
            comparison.put("active_proposed", applied);
            comparison.put("type", "decision");
            emit(comparison);
            trace.comparison = comparison;
            return applied;
        }

        private Map<String, Object> preview(String mode, JobAutoScalerContext<String> ctx, Instant now,
                JobTopology topology) {
            var conf = new Configuration(ctx.getConfiguration())
                    .set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, !mode.equals("fixed"))
                    .set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_USE_FIXED_BASELINE, mode.equals("slope_only"));
            var shadowStore = new InMemoryAutoScalerStateStore<String, JobAutoScalerContext<String>>();
            var shadowExecutor = new ScalingExecutor<String, JobAutoScalerContext<String>>(events, shadowStore) {
                @Override
                Map<JobVertexID, ScalingSummary> computeScalingSummary(JobAutoScalerContext<String> context,
                        EvaluatedMetrics evaluated, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> ignoredHistory,
                        Duration restart, JobTopology jobTopology, DelayedScaleDown ignoredDelayed) {
                    return trigger;
                }
            };
            // Both planners receive the same standard scale trigger and still-live resource profiles.
            try {
                var metrics = new ScalingMetricEvaluator().evaluate(conf, trace.history, trace.restartDuration);
                boolean proposed = shadowExecutor.scaleResource(ctx, metrics, new HashMap<>(),
                        shadowStore.getScalingTracking(ctx), now, topology, shadowStore.getDelayedScaleDown(ctx));
                return Map.of("proposed", proposed, "parallelism", shadowStore.getParallelismOverrides(ctx),
                        "profiles", shadowStore.getResourceProfileOverrides(ctx));
            } catch (Exception error) {
                return Map.of("error", error.toString());
            }
        }
    }

    private static final class ProposalRealizer
            implements ScalingRealizer<String, JobAutoScalerContext<String>> {
        private final PolicyTrace trace;
        private Map<String, String> lastParallelism = Map.of();
        private Map<String, String> lastProfiles = Map.of();

        private ProposalRealizer(PolicyTrace trace) {
            this.trace = trace;
        }

        @Override
        public void realizeParallelismOverrides(
                JobAutoScalerContext<String> context, Map<String, String> parallelism) {
            throw new IllegalStateException("A4S must provide paired resource profiles");
        }

        @Override
        public void realizeParallelismOverrides(
                JobAutoScalerContext<String> context, Map<String, String> parallelism,
                Map<String, String> profiles) throws Exception {
            if (parallelism.isEmpty() || !parallelism.keySet().equals(profiles.keySet())) {
                throw new IllegalArgumentException("Outside Q8 paired profile scope");
            }
            if (parallelism.equals(lastParallelism) && profiles.equals(lastProfiles)) {
                return;
            }
            var row = trace.event("proposal");
            row.put("comparison", trace.comparison);
            row.put("parallelism", new TreeMap<>(parallelism));
            row.put("resource_profiles", new TreeMap<>(profiles));
            row.put("parallelism_config",
                    org.apache.flink.configuration.ConfigurationUtils.convertValue(
                            new TreeMap<>(parallelism), String.class));
            row.put("resource_profile_config",
                    org.apache.flink.configuration.ConfigurationUtils.convertValue(
                            new TreeMap<>(profiles), String.class));
            emit(row);
            lastParallelism = Map.copyOf(parallelism);
            lastProfiles = Map.copyOf(profiles);
        }

        @Override
        public void realizeConfigOverrides(JobAutoScalerContext<String> context, ConfigChanges changes) {
            throw new IllegalStateException("TaskManager configuration changes are outside scope");
        }
    }
}
