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

import io.a4s.api.EvaluationRequest;
import io.a4s.api.OperatorMetrics;
import io.a4s.api.PolicyConfiguration;
import io.a4s.api.ScalingPlan;
import io.a4s.core.A4sPolicy;
import io.a4s.core.FixedLatencyModel;
import io.a4s.http.DecisionClient;
import io.a4s.http.RecordedEvaluation;
import io.a4s.http.Wire;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.a4s.A4S;
import org.apache.flink.autoscaler.a4s.MemoryParallelismCurve;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/** Compare the pinned Flink policy to embedded evaluation and the independent runner. */
public final class CompareLegacyA4s {
    private static final long MIB = 1024 * 1024;
    private static final A4sPolicy EMBEDDED = new A4sPolicy();
    private static final DecisionClient CLIENT = new DecisionClient();

    public static void main(String[] args) throws Exception {
        var endpoint = URI.create(args[0]);
        var out = Path.of(args[2]);
        int cases = 0;
        try (var requests = Files.newBufferedReader(Path.of(args[1]));
                var recordings = Files.newOutputStream(out.resolve("recordings.jsonl"));
                var candidates = Files.newBufferedWriter(out.resolve("candidates.tsv"))) {
            candidates.write(
                    "snapshot\toperator\tparallelism\tlegacy_bytes\tembedded_bytes\tstatus\n");
            for (String line; (line = requests.readLine()) != null; ) {
                if (line.isBlank()) {
                    continue;
                }
                var request = Wire.decodeRecording(line.getBytes(StandardCharsets.UTF_8)).request;
                var embedded = EMBEDDED.evaluate(request);
                var remote = CLIENT.evaluate(endpoint, Duration.ofSeconds(3), request);
                require(
                        Arrays.equals(Wire.encode(embedded), Wire.encode(remote)),
                        request,
                        "Embedded and remote recommendations differ");
                var original = compareFrontiers(request, candidates);
                comparePlan(request, original, embedded);
                recordings.write(
                        Wire.encodeRecording(
                                new RecordedEvaluation(
                                        RecordedEvaluation.VERSION, request, remote)));
                recordings.write('\n');
                cases++;
            }
        }
        if (cases == 0) {
            throw new IllegalArgumentException("Supply at least one request");
        }
        System.out.println(
                "Verified identical embedded and remote plans on " + cases + " requests.");
    }

    private static Map<String, A4S.Decision> compareFrontiers(
            EvaluationRequest request, Writer evidence) throws Exception {
        require(
                request.policy.mode == PolicyConfiguration.Mode.LEGACY_SLOT_RESERVATION
                        && request.policy.managedMemoryOverrideBytes == null
                        && Long.valueOf(600 * MIB)
                                .equals(request.constraints.maxManagedMemoryPerTaskBytes)
                        && request.snapshot.workers.isEmpty()
                        && request.constraints.cpuBudgetCores == null
                        && request.constraints.totalCacheBudgetBytes == null,
                request,
                "Use the shared legacy 600 MiB limit with no worker, CPU, cache or reservation override");
        var ids = new LinkedHashMap<String, JobVertexID>();
        var vertices = new ArrayList<VertexInfo>();
        var metrics = new LinkedHashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>();
        var originalCurves = new LinkedHashMap<JobVertexID, MemoryParallelismCurve>();
        var summaries = new LinkedHashMap<JobVertexID, ScalingSummary>();
        var fixed = new FixedLatencyModel(request.policy);
        for (var operator : request.snapshot.operators) {
            var id = new JobVertexID(0, ids.size() + 1);
            ids.put(operator.operatorId, id);
            var upstream =
                    operator.upstreamOperators.stream()
                            .collect(Collectors.toMap(ids::get, name -> ShipStrategy.HASH));
            vertices.add(
                    new VertexInfo(
                            id, upstream, operator.current.parallelism, operator.maxParallelism));
            metrics.put(
                    id,
                    Map.of(
                            ScalingMetric.PARALLELISM,
                            EvaluatedScalingMetric.of(operator.current.parallelism)));
            summaries.put(
                    id,
                    new ScalingSummary(
                            operator.current.parallelism,
                            operator.startingParallelism,
                            metrics.get(id)));
            require(
                    operator.histogram == null && operator.permittedParallelisms.isEmpty(),
                    request,
                    "Use supplied MRCs and contiguous parallelism ranges for legacy comparisons");
            if (operator.cacheSensitive) {
                var baseline =
                        new MissRateCurve(
                                operator.missRateCurve.stream()
                                        .map(
                                                point ->
                                                        new MissRateCurve.Point(
                                                                point.capacityBytes,
                                                                point.missRate))
                                        .collect(Collectors.toList()));
                var nativeCurve =
                        MemoryParallelismCurve.fromMissRateCurve(
                                operator.targetRecordsPerSecond,
                                request.policy.missSeconds,
                                request.policy.hitSeconds,
                                operator.minParallelism,
                                operator.maxParallelism,
                                baseline);
                originalCurves.put(id, nativeCurve);
                var extracted = new io.a4s.core.MemoryParallelismCurve(operator, fixed);
                for (var point : nativeCurve.getPoints()) {
                    int p = point.getParallelism();
                    double oldBytes = point.getMemoryMB() * MIB;
                    var newBytes = extracted.minimumCacheBytes(p);
                    boolean measuredSupport = hasSupport(fixed, operator, p);
                    require(
                            newBytes.isPresent() == measuredSupport,
                            request,
                            "MPC disagrees with measured support");
                    require(
                            newBytes.isEmpty() || Math.abs(newBytes.getAsLong() - oldBytes) < 0.5,
                            request,
                            "Supported MPC point differs");
                    evidence.write(
                            request.snapshot.snapshotId
                                    + "\t"
                                    + operator.operatorId
                                    + "\t"
                                    + p
                                    + "\t"
                                    + (long) oldBytes
                                    + "\t"
                                    + (newBytes.isPresent() ? newBytes.getAsLong() : "")
                                    + "\t"
                                    + (measuredSupport ? "match" : "legacy plateau fallback")
                                    + "\n");
                }
            }
        }
        var config = new Configuration();
        config.set(
                AutoScalerOptions.A4S_MEMORY_BASE_MB,
                request.policy.managedMemoryBaseBytes / (double) MIB);
        config.set(AutoScalerOptions.A4S_MAX_ATTEMPTS, request.policy.maxAttempts);
        var evaluated = new EvaluatedMetrics(metrics, Map.of(), originalCurves);
        var placement =
                new A4S(new JobTopology(vertices), evaluated, summaries).makeDecision(config);
        var result = new LinkedHashMap<String, A4S.Decision>();
        ids.forEach(
                (operator, id) -> {
                    if (placement.containsKey(id)) {
                        result.put(operator, placement.get(id));
                    }
                });
        return result;
    }

    private static void comparePlan(
            EvaluationRequest request, Map<String, A4S.Decision> original, ScalingPlan plan) {
        var model = new FixedLatencyModel(request.policy);
        boolean usedFallback = false;
        var oldTargets = new LinkedHashMap<String, String>();
        var newTargets = new LinkedHashMap<String, String>();
        boolean allocationsMatch = true;
        if (original.isEmpty()
                && request.snapshot.operators.stream()
                        .anyMatch(operator -> operator.cacheSensitive)) {
            require(
                    plan.action == ScalingPlan.Action.INFEASIBLE_CONFIGURATION,
                    request,
                    "Expected the extracted policy to reject an exhausted legacy search");
        }
        for (OperatorMetrics operator : request.snapshot.operators) {
            var before = original.get(operator.operatorId);
            var after = plan.operators.get(operator.operatorId);
            if (before != null) {
                usedFallback |= !hasSupport(model, operator, before.getParallelism());
                long reservation =
                        (long)
                                Math.ceil(
                                        before.getMemoryMB()
                                                + request.policy.managedMemoryBaseBytes
                                                        / (double) MIB);
                oldTargets.put(operator.operatorId, before.getParallelism() + "/" + reservation);
                var target = after == null ? operator.current : after.target;
                allocationsMatch &=
                        target.parallelism == before.getParallelism()
                                && Long.valueOf(reservation * MIB)
                                        .equals(target.managedMemoryPerTaskBytes);
            } else if (operator.cacheSensitive) {
                allocationsMatch &= after == null;
            }
            if (after != null) {
                require(
                        !operator.cacheSensitive
                                || hasSupport(model, operator, after.target.parallelism),
                        request,
                        "Extracted policy selected a target without measured support");
                Long managed = after.target.managedMemoryPerTaskBytes;
                newTargets.put(
                        operator.operatorId,
                        after.target.parallelism
                                + "/"
                                + (managed == null ? "unknown" : managed / MIB));
            }
        }
        if (!original.isEmpty() && !usedFallback) {
            require(
                    plan.action == ScalingPlan.Action.RESCALE
                            || plan.action == ScalingPlan.Action.NO_CHANGE,
                    request,
                    "Expected the extracted policy to accept a supported legacy plan");
        }
        require(
                allocationsMatch || usedFallback,
                request,
                "Selected allocations differ without a legacy plateau fallback");
        System.out.printf(
                "%s legacy=%s extracted=%s %s comparison=%s%n",
                request.snapshot.snapshotId,
                oldTargets,
                plan.action,
                newTargets,
                usedFallback ? "legacy plateau fallback" : "match");
    }

    private static boolean hasSupport(
            FixedLatencyModel model, OperatorMetrics operator, int parallelism) {
        double maximum = model.maximumMissRate(parallelism, operator.targetRecordsPerSecond);
        return operator.missRateCurve.stream().anyMatch(point -> point.missRate <= maximum);
    }

    private static void require(boolean valid, EvaluationRequest request, String message) {
        if (!valid) {
            throw new AssertionError(request.snapshot.snapshotId + " " + message);
        }
    }
}
