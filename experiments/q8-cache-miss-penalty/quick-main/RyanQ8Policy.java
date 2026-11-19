package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.LoggingEventHandler;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.a4s.core.MissRateCurve;
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
        if (!"hardcoded".equals(arm)) {
            throw new IllegalArgumentException("This driver requires the main/hardcoded arm");
        }
        JobID jobId = JobID.fromHexString(args[1]);
        Path endpoint = Path.of(args[3]);
        Path handshake = Path.of(args[4]);
        var config = loadConfig(Path.of(args[2]));
        var tracing = new PolicyTrace(arm, jobId);
        var metricCollector = new RestApiMetricsCollector<String, JobAutoScalerContext<String>>();
        var eventHandler = new LoggingEventHandler<String, JobAutoScalerContext<String>>();
        var store = new InMemoryAutoScalerStateStore<String, JobAutoScalerContext<String>>();
        var realizer = new ProposalRealizer(tracing);
        var scaler =
                new JobAutoScalerImpl<>(
                        metricCollector, tracing, new MainExecutor(eventHandler, store, tracing),
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

    private static Configuration loadConfig(Path path) throws Exception {
        JsonNode spec = readJson(path).path("spec");
        var config = new Configuration();
        spec.path("flinkConfiguration").fields()
                .forEachRemaining(property -> config.setString(property.getKey(), property.getValue().asText()));
        config.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.parse(spec.path("taskManager").path("resource").path("memory").asText()));
        return config
                .set(AutoScalerOptions.AUTOSCALER_ENABLED, true)
                .set(AutoScalerOptions.SCALING_ENABLED, true)
                .set(AutoScalerOptions.JUSTIN_ENABLED, true)
                .set(AutoScalerOptions.A4S_ENABLED, true)
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

        private final boolean literalMain;
        private boolean recordedFailureStack;
        private boolean emptyProofRecorded;
        private Instant lastFailedSnapshot;
        private Map<String, Object> snapshot;
        private Map<String, Object> comparison;

        private PolicyTrace(String arm, JobID job) {
            this.arm = arm;
            this.job = job;
            boolean markedLiteral = RyanQ8Policy.class.getResource("/a4s-quick-literal-main") != null;
            String mode = System.getProperty("a4s.quick.main-empty-curves",
                    markedLiteral ? "literal" : "omit-empty");
            if (!"literal".equals(mode) && !"omit-empty".equals(mode)) {
                throw new IllegalArgumentException("Unknown main empty-curve mode: " + mode);
            }
            if (markedLiteral && !"literal".equals(mode)) {
                throw new IllegalArgumentException("The literal driver requires literal main mode");
            }
            this.literalMain = "literal".equals(mode);
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
            var samples = history.getMetricHistory();
            var latest = samples.get(samples.lastKey());
            var curves = latest.getMissRateCurves();
            Map<JobVertexID, Integer> empty = new LinkedHashMap<>();
            if (curves != null) {
                curves.forEach((id, curve) -> {
                    if (curve != null && curve.getPoints().isEmpty()) {
                        empty.put(id, 0);
                    }
                });
            }
            EvaluatedMetrics result;
            if (literalMain) {
                // Always call unmodified main with the complete, original REST history. Reporting
                // an error must preserve the behavior of JobAutoScalerImpl on that same error.
                try {
                    result = super.evaluate(config, history, restartTime);
                } catch (RuntimeException error) {
                    Instant when = samples.lastKey();
                    if (!when.equals(lastFailedSnapshot)) {
                        var proof = event("MissRateCurve has no points".equals(error.getMessage())
                                ? "literal_main_empty_curve" : "literal_main_evaluation_failed");
                        proof.put("snapshot", when.toString());
                        proof.put("empty_curve_mode", "literal");
                        proof.put("curve_points_by_vertex", curves == null ? Map.of() : counts(curves));
                        // Archive ingress demand even when main fails before evaluating the join.
                        double sourceDemand = 0;
                        for (var source : history.getJobTopology().get(JOIN).getInputs().keySet()) {
                            double input = getRate(ScalingMetric.NUM_RECORDS_IN, source, samples);
                            double backlogGrowth = getRate(ScalingMetric.LAG, source, samples);
                            sourceDemand += Math.max(0, input + backlogGrowth);
                        }
                        if (Double.isFinite(sourceDemand)) {
                            proof.put("source_demand_records_s", sourceDemand);
                        }
                        proof.put("exception", error.toString());
                        if (!recordedFailureStack) {
                            proof.put("stack", java.util.Arrays.toString(error.getStackTrace()));
                            recordedFailureStack = true;
                        }
                        archive(proof);
                        lastFailedSnapshot = when;
                    }
                    throw error;
                }
            } else if (empty.isEmpty()) {
                result = super.evaluate(config, history, restartTime);
            } else {
                if (!emptyProofRecorded) {
                    // Preserve the outcome of a literal main evaluation against the actual REST data.
                    // The normalization below is allowed only after this specific production error.
                    try {
                        result = super.evaluate(config, history, restartTime);
                        snapshot = selection(config, history, result);
                        return result;
                    } catch (IllegalStateException error) {
                        if (!"MissRateCurve has no points".equals(error.getMessage())) {
                            throw error;
                        }
                        var proof = event("literal_main_empty_curve");
                        proof.put("snapshot", samples.lastKey().toString());
                        proof.put("empty_curve_mode", "omit-empty");
                        proof.put("curve_points_by_vertex", counts(curves));
                        proof.put("exception", error.toString());
                        proof.put("stack", java.util.Arrays.toString(error.getStackTrace()));
                        archive(proof);
                        emptyProofRecorded = true;
                    }
                }
                // main already accepts missing curves. Preserve all other metrics and the whole
                // history, representing only zero-point curves as unavailable (the online path skips them).
                var available = new HashMap<>(curves);
                empty.keySet().forEach(available::remove);
                var copy = new TreeMap<>(samples);
                copy.put(samples.lastKey(), new CollectedMetrics(
                        latest.getVertexMetrics(), latest.getGlobalMetrics(), available));
                var normalized = new CollectedMetricHistory(history.getJobTopology(), copy, history.getJobRunningTs());
                normalized.setFullyCollected(history.isFullyCollected());
                result = super.evaluate(config, normalized, restartTime);
            }
            snapshot = selection(config, history, result);
            return result;
        }

        private static Map<String, Integer> counts(Map<JobVertexID, MissRateCurve> curves) {
            Map<String, Integer> sizes = new TreeMap<>();
            curves.forEach((id, curve) -> sizes.put(id.toHexString(), curve == null ? -1 : curve.getPoints().size()));
            return sizes;
        }

        private Map<String, Object> selection(Configuration config, CollectedMetricHistory history, EvaluatedMetrics metrics) {
            var samples = history.getMetricHistory();
            var curve = metrics.getMissRateCurves().get(JOIN);
            var row = event("selection");
            row.put("snapshot", samples.lastKey().toString());
            row.put("current_p", history.getJobTopology().get(JOIN).getParallelism());
            row.put("target_records_s", metrics.getVertexMetrics().get(JOIN).get(ScalingMetric.TARGET_DATA_RATE).getAverage());
            double base = config.get(AutoScalerOptions.A4S_HIT_LATENCY_SEC);
            row.put("fixed_baseline_s", base);
            row.put("fixed_slope_s", config.get(AutoScalerOptions.A4S_MISS_LATENCY_SEC) - base);
            row.put("empty_curve_mode", literalMain ? "literal" : "omit-empty");
            row.put("controller_revision", "f8ce7fbbfae48869d8b1869748ad517801a9f726");
            row.put("mrc_points", curve == null ? 0 : curve.getPoints().size());
            archive(row);
            var decision = new LinkedHashMap<>(row);
            decision.put("mrc", curve == null ? null : curve.getPoints());
            return decision;
        }

        private static void archive(Map<String, Object> row) {
            try {
                emit(row);
            } catch (Exception e) {
                throw new IllegalStateException("Could not archive main decision data", e);
            }

        }
    }

    /** Observe the natural trigger and the proposal emitted by the main production executor. */
    private static final class MainExecutor extends ScalingExecutor<String, JobAutoScalerContext<String>> {
        private final PolicyTrace trace;
        private final LoggingEventHandler<String, JobAutoScalerContext<String>> events;
        private Map<JobVertexID, ScalingSummary> trigger;

        MainExecutor(LoggingEventHandler<String, JobAutoScalerContext<String>> events,
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
            comparison.put("active_proposed", applied);
            comparison.put("type", "decision");
            emit(comparison);
            trace.comparison = comparison;
            return applied;
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
