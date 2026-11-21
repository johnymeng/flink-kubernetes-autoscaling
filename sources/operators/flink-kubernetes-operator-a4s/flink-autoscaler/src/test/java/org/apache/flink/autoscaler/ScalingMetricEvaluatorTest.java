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

package org.apache.flink.autoscaler;

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.autoscaler.a4s.A4S;
import org.apache.flink.autoscaler.a4s.A4SAlignedCacheWindow;
import org.apache.flink.autoscaler.a4s.CacheCostSample;
import org.apache.flink.autoscaler.a4s.OnlineCacheCost;
import org.apache.flink.autoscaler.a4s.QuickMrcOnlineCacheCostTest;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheMode;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.MetricAggregator;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SCacheCostSample;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.CATCH_UP_DURATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.PREFER_TRACKED_RESTART_TIME;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.RESTART_TIME;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Scaling evaluator test. */
public class ScalingMetricEvaluatorTest {

    private ScalingMetricEvaluator evaluator = new ScalingMetricEvaluator();

    @Test
    void fixedCostsSkipParallelismsBelowTheAllHitFloorAndPlaceAtTheFirstFeasiblePoint() {
        var vertex = new JobVertexID();
        var history = fixedDemandHistory(vertex, 1, 3);
        var config = fixedCostConfig();

        var evaluated = evaluator.evaluate(config, history, config.get(RESTART_TIME));
        var curve = evaluated.getMemoryParallelismCurves().get(vertex);
        assertTrue(evaluated.getCacheCostFailures().isEmpty());
        var metrics = evaluated.getVertexMetrics().get(vertex);
        assertEquals(150_000.0, metrics.get(ScalingMetric.TARGET_DATA_RATE).getAverage());
        assertEquals(239_286.0, curve.getTargetThroughput());
        assertTrue(curve.getMemoryMbForParallelism(1).isEmpty());
        assertEquals(200.0, curve.getMemoryMbForParallelism(2).orElseThrow());
        assertTrue(curve.hasSupportedCacheSize(2));
        assertEquals(5e-6, config.get(AutoScalerOptions.A4S_HIT_LATENCY_SEC));
        assertEquals(5e-3, config.get(AutoScalerOptions.A4S_MISS_LATENCY_SEC));

        var a4s = new A4S(history.getJobTopology(), evaluated, Map.of());
        var first = a4s.makeDecision(config).get(vertex);
        assertEquals(2, first.getParallelism());
        assertEquals(120.0, first.getMemoryMB());

        // The 100 MiB per-slot cap excludes p=2. Placement retries from p=2 to p=3.
        config.set(AutoScalerOptions.A4S_MEMORY_BASE_MB, A4S.MAX_MANAGED_MEMORY_MB - 100.0);
        var retry = a4s.makeDecision(config).get(vertex);
        assertEquals(3, retry.getParallelism());
        assertEquals(40.0, retry.getMemoryMB());
    }

    @Test
    void fixedCostsReportFailureWhenTheVertexLimitPrecludesEveryFeasibleCandidate() {
        var vertex = new JobVertexID();
        var config = fixedCostConfig();
        var evaluated = evaluator.evaluate(
                config, fixedDemandHistory(vertex, 1, 1), config.get(RESTART_TIME));

        assertEquals(Map.of(vertex, OnlineCacheCost.Reason.CAPACITY_UNSUPPORTED),
                evaluated.getCacheCostFailures());
        assertFalse(evaluated.getMemoryParallelismCurves().containsKey(vertex));
    }

    @Test
    void fixedCostsIncludeDeployedParallelismOutsideConfiguredBounds() {
        var vertex = new JobVertexID();
        var history = fixedDemandHistory(vertex, 2, 3);
        var config = fixedCostConfig()
                .set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 3)
                .set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 4);
        var belowMinimum = evaluator.evaluate(config, history, config.get(RESTART_TIME));
        assertEquals(2, belowMinimum.getMemoryParallelismCurves().get(vertex).getMinParallelism());
        assertEquals(3, belowMinimum.getMemoryParallelismCurves().get(vertex).getMaxParallelism());

        config.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1);
        config.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 1);
        var aboveMaximum = evaluator.evaluate(config, history, config.get(RESTART_TIME));
        assertEquals(2, aboveMaximum.getMemoryParallelismCurves().get(vertex).getMaxParallelism());
    }

    static Configuration fixedCostConfig() {
        return new Configuration()
                .set(AutoScalerOptions.JUSTIN_ENABLED, true)
                .set(AutoScalerOptions.A4S_ENABLED, true)
                .set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, false)
                .set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1)
                .set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 3)
                .set(TARGET_UTILIZATION, 0.7)
                .set(RESTART_TIME, Duration.ofSeconds(300))
                .set(CATCH_UP_DURATION, Duration.ofSeconds(1800));
    }

    static CollectedMetricHistory fixedDemandHistory(
            JobVertexID vertex, int parallelism, int maxParallelism) {
        var topology = new JobTopology(new VertexInfo(vertex, Map.of(), parallelism, maxParallelism));
        var mrc = new MissRateCurve(List.of(
                new MissRateCurve.Point(100L << 20, 0.001),
                new MissRateCurve.Point(200L << 20, 0.0)),
                MissRateCurve.CapacityAxis.ATTESTED_NATIVE_BYTES);
        var metrics = new TreeMap<Instant, CollectedMetrics>();
        for (int second = 0; second <= 1; second++) {
            metrics.put(Instant.ofEpochSecond(second), new CollectedMetrics(
                    Map.of(vertex, Map.of(
                            ScalingMetric.NUM_RECORDS_IN, 150_000.0 * second,
                            ScalingMetric.NUM_RECORDS_OUT, 150_000.0 * second,
                            ScalingMetric.LAG, 0.0,
                            ScalingMetric.LOAD, 0.8)),
                    Map.of(), Map.of(vertex, mrc)));
        }
        return new CollectedMetricHistory(topology, metrics, Instant.EPOCH);
    }

    @Test
    void usesCalibratedCostsForTheConfiguredVertex() {
        var vertex = new JobVertexID();
        var history = cacheCostHistory(vertex);
        var baseline = evaluator.evaluate(new Configuration(), history, Duration.ZERO);
        var config =
                Configuration.fromMap(
                        Map.of(
                                "job.autoscaler.a4s.calibrated-cache-costs",
                                vertex.toHexString() + ":0.0015/0.001"));
        var calibrated = evaluator.evaluate(config, history, Duration.ZERO);

        var legacyCurve = baseline.getMemoryParallelismCurves().get(vertex);
        var calibratedCurve = calibrated.getMemoryParallelismCurves().get(vertex);
        assertEquals(200.0, legacyCurve.getMemoryMbForParallelism(2).orElseThrow());
        assertTrue(calibratedCurve.getMemoryMbForParallelism(1).isEmpty());
        assertEquals(100.0, calibratedCurve.getMemoryMbForParallelism(2).orElseThrow());
        assertEquals(baseline.getVertexMetrics(), calibrated.getVertexMetrics());
        var fixed = baseline.getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheMode.LEGACY_FIXED, fixed.getSelectedCostPolicy());
        assertEquals(5e-6, fixed.getFixedCosts().getHitSecondsPerAccess());
        assertEquals(5e-3, fixed.getFixedCosts().getTotalMissSecondsPerAccess());
        assertEquals(4_995e-6, fixed.getFixedCosts().getIncrementalMissSecondsPerAccess(), 1e-12);
        assertEquals(CacheMode.OFFLINE_CALIBRATED,
                calibrated.getCacheEvaluation().getVertices().get(vertex).getSelectedCostPolicy());

        // The first feasible parallelism needs 100 MiB. With 50 MiB available, retry
        // starts there and finds parallelism 3, instead of looking up the omitted point at 1.
        config.set(AutoScalerOptions.A4S_MEMORY_BASE_MB, 550.0);
        var decision = new A4S(history.getJobTopology(), calibrated, Map.of()).makeDecision(config);
        assertEquals(3, decision.get(vertex).getParallelism());
    }

    @Test
    void legacyShortHistogramCannotQualifyLongTimingWindow() {
        var vertex = new JobVertexID();
        var history = cacheHistory(vertex, false);
        var config = new Configuration().set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
        var result = evaluator.evaluate(config, history, Duration.ZERO);
        assertFalse(result.getMemoryParallelismCurves().containsKey(vertex));
        assertEquals(OnlineCacheCost.Reason.UNBOUND_TIMING, result.getCacheCostFailures().get(vertex));
    }

    @Test
    void usesAQualifiedQuickMrcFitOnlyAtTheMeasuredParallelism() {
        var vertex = new JobVertexID();
        var history = compatibleQuickMrcHistory(vertex);
        var snapshots = history.getMetricHistory();

        var config = new Configuration().set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
        var online = evaluator.evaluate(config, history, Duration.ZERO);
        assertTrue(online.getCacheCostFailures().isEmpty());
        var projection = online.getMemoryParallelismCurves().get(vertex);
        assertEquals(8_192 / (1024.0 * 1024.0), projection.getMemoryMbForParallelism(1).orElseThrow());
        assertTrue(projection.getMemoryMbForParallelism(2).isEmpty());
        var decision = online.getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheMode.ONLINE_QUALIFIED, decision.getMode());
        assertFalse(online.getCacheEvaluation().isCachePlacementEnabled());
        assertNull(decision.getFallbackReason());
        assertEquals(8_192, decision.getRecommendation().getTotalNativeCacheBytes());
        assertEquals(60, decision.getFit().getAttemptedWindow().getStart().getEpochSecond());
        assertEquals(120, decision.getFit().getAttemptedWindow().getEnd().getEpochSecond());
        assertEquals(60_000_000_000L,
                decision.getFit().getAlignedCurve().subtasks.get(0).window.durationNanos());
        assertEquals(1, decision.getObservation().getReceivedSubtasks());
        assertEquals(3, decision.getObservation().getExpectedTimingAbi());
        assertEquals(3, decision.getObservation().getTimingEndpoints().get(0).getTimingAbi());
        assertEquals(1, decision.getFitStartObservation().getExpectedSubtasks());
        assertTrue(decision.toLogString().contains("meanHitServiceSecondsPerAccess="));
        assertTrue(decision.toLogString().contains("residualBusySecondsPerInput="));
        assertTrue(decision.toLogString().contains("slopeSecondsPerInputPerUnitMissFraction="));
        assertTrue(decision.toLogString().contains("uncertainty=unavailable"));

        // At 9,091/s the measured curve is too short, but the optimistic cache bound
        // still admits the demand. At 10,000/s, even that bound is insufficient.
        var unsupported = evaluator.evaluate(
                new Configuration(config).set(TARGET_UTILIZATION, .11), history, Duration.ZERO);
        assertEquals(OnlineCacheCost.Reason.CAPACITY_UNSUPPORTED,
                unsupported.getCacheCostFailures().get(vertex));
        var unsupportedDecision = unsupported.getCacheEvaluation().getVertices().get(vertex);
        assertEquals(OnlineCacheCost.Reason.CAPACITY_UNSUPPORTED, unsupportedDecision.getReason());
        assertNull(unsupportedDecision.getRecommendation());
        assertEquals(4096, unsupportedDecision.getProjection().getMinSupportedTotalBytes());
        assertEquals(12_288, unsupportedDecision.getProjection().getMaxSupportedTotalBytes());
        assertTrue(unsupportedDecision.getObservedCurveMaxBytes()
                > unsupportedDecision.getProjection().getMaxSupportedTotalBytes());
        var overloaded = evaluator.evaluate(
                new Configuration(config).set(TARGET_UTILIZATION, .10), history, Duration.ZERO);
        assertEquals("INSUFFICIENT_CURRENT_PARALLELISM",
                overloaded.getCacheCostFailures().get(vertex).name());
        assertFalse(overloaded.getMemoryParallelismCurves().containsKey(vertex));
        var overloadDecision = overloaded.getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheMode.ONLINE_OVERLOAD, overloadDecision.getMode());
        assertEquals(10_000, overloadDecision.getDemandRecordsPerSecond(), 1e-8);
        assertNotEquals(online.getCacheEvaluation().getId(), overloaded.getCacheEvaluation().getId());

        snapshots.get(snapshots.lastKey()).getCacheCostSamples().get(vertex).get(0)
                .setAttempt("restarted");
        var invalid = evaluator.evaluate(config, history, Duration.ZERO);
        assertEquals(OnlineCacheCost.Reason.UNBOUND_TIMING, invalid.getCacheCostFailures().get(vertex));
        assertFalse(invalid.getMemoryParallelismCurves().containsKey(vertex));
        var invalidDecision = invalid.getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheObservation.Status.ATTEMPT_MISMATCH,
                invalidDecision.getFit().getObservationStatus());
        assertEquals("restarted", invalidDecision.getObservation().getTimingEndpoints().get(0).getAttempt());
        assertNull(invalidDecision.getSelectedCostPolicy());

        var shadow = evaluator.evaluate(
                new Configuration(config).set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_SHADOW, true),
                history, Duration.ZERO);
        assertTrue(shadow.getCacheCostFailures().isEmpty());
        assertTrue(shadow.getMemoryParallelismCurves().get(vertex).getMemoryMbForParallelism(24).isPresent());
        var shadowDecision = shadow.getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheMode.ONLINE_SHADOW, shadowDecision.getMode());
        assertEquals(CacheMode.LEGACY_FIXED, shadowDecision.getSelectedCostPolicy());
        assertEquals(OnlineCacheCost.Reason.UNBOUND_TIMING, shadowDecision.getFallbackReason());
    }

    @Test
    void retainsObservationAndTimingRejectionsInTheDecisionRecord() {
        var vertex = new JobVertexID();
        var history = compatibleQuickMrcHistory(vertex);
        var snapshots = history.getMetricHistory();
        var latest = snapshots.get(snapshots.lastKey());
        var config = new Configuration().set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);

        var sample = latest.getCacheCostSamples().get(vertex).get(0);
        var originalMissCoverage = sample.getCoveredMisses();
        sample.setCoveredMisses(originalMissCoverage - 1_000);
        var coverage = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        assertEquals(OnlineCacheCost.Reason.TIMING_COVERAGE, coverage.getReason());
        assertTrue(coverage.getFit().getTimingCounts().getMissCoverage() < .90);
        assertNull(coverage.getSelectedCostPolicy());
        sample.setCoveredMisses(originalMissCoverage);

        sample.setSampledMisses(
                snapshots.get(Instant.ofEpochSecond(60)).getCacheCostSamples().get(vertex).get(0)
                        .getSampledMisses() + 63);
        latest.setCacheObservations(Map.of(vertex, pair(
                latest.getCacheObservations().get(vertex), latest.getCacheCostSamples().get(vertex))));
        var sparse = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        // The earlier 120-second candidate can fit. It has no independent prior window.
        assertEquals(OnlineCacheCost.Reason.MISSING_HOLDOUT, sparse.getReason());
        assertEquals(OnlineCacheCost.Reason.INSUFFICIENT_HISTORY,
                sparse.getFit().getHoldout().getPriorRejection());
        snapshots.remove(Instant.EPOCH);
        sparse = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        assertEquals(OnlineCacheCost.Reason.TOO_FEW_SAMPLES, sparse.getReason());
        assertEquals(63, sparse.getFit().getTimingCounts().getSampledMisses());

        sample.setSampledMisses(200);
        sample.setBusyMillis(601);
        var invalidDecomposition = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        assertEquals(OnlineCacheCost.Reason.NON_POSITIVE_BASELINE, invalidDecomposition.getReason());
        sample.setBusyMillis(1_200);
        sample.setCapacityBytes(16_384);
        var resized = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheObservation.Status.CACHE_CHANGED, resized.getFit().getObservationStatus());
        assertEquals(8192, resized.getObservation().getEndpoints().get(0).getCapacityBytes());
        assertEquals(16_384, resized.getObservation().getTimingEndpoints().get(0).getCapacityBytes());
        sample.setCapacityBytes(8192);
        sample.setTimingAbi(2);
        sample.setObservedAtNanos(null);
        var abi = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        assertEquals(CacheObservation.Status.UNSUPPORTED_NATIVE, abi.getFit().getObservationStatus());
        assertEquals(2, abi.getObservation().getTimingEndpoints().get(0).getTimingAbi());

        for (var status : List.of(CacheObservation.Status.INCOMPLETE, CacheObservation.Status.WARMUP,
                CacheObservation.Status.SHARED_CACHE)) {
            latest.setCacheObservations(Map.of(vertex, CacheObservationBatch.invalid(status)));
            var unqualified = evaluator.evaluate(config, history, Duration.ZERO)
                    .getCacheEvaluation().getVertices().get(vertex);
            assertEquals(status, unqualified.getObservation().getStatus());
            assertEquals(status, unqualified.getFit().getObservationStatus());
            assertNull(unqualified.getSelectedCostPolicy());
        }
        latest.setCacheObservations(Map.of());
        latest.setMissRateCurves(Map.of());
        var missing = evaluator.evaluate(config, history, Duration.ZERO)
                .getCacheEvaluation().getVertices().get(vertex);
        assertEquals(OnlineCacheCost.Reason.MISSING_OBSERVATIONS, missing.getReason());
        assertEquals(CacheObservation.Status.ABSENT, missing.getObservation().getStatus());
        assertNull(missing.getObservation().getReceivedSubtasks());
        assertNull(missing.getFit());
    }

    private static CollectedMetricHistory compatibleQuickMrcHistory(JobVertexID vertex) {
        return cacheHistory(vertex, true);
    }

    private static CollectedMetricHistory cacheHistory(JobVertexID vertex, boolean paired) {
        var history = cacheCostHistory(vertex);
        var snapshots = history.getMetricHistory();
        var curve = snapshots.get(snapshots.lastKey()).getMissRateCurves().get(vertex);
        snapshots.clear();
        for (int i = 0; i < 3; i++) {
            var metrics = new CollectedMetrics(
                    Map.of(vertex, Map.of(
                            ScalingMetric.NUM_RECORDS_IN, 60_000.0 * i,
                            ScalingMetric.NUM_RECORDS_OUT, 60_000.0 * i,
                            ScalingMetric.LAG, 0.0,
                            ScalingMetric.LOAD, 0.8)),
                    Map.of(), Map.of(vertex, curve));
            var samples = List.of(QuickMrcOnlineCacheCostTest.sample(i, 0, 0, 0));
            var batch = QuickMrcOnlineCacheCostTest.batch(i, 0);
            metrics.setCacheObservations(Map.of(vertex, paired ? pair(batch, samples) : batch));
            metrics.setCacheCostSamples(Map.of(vertex, samples));
            snapshots.put(Instant.ofEpochSecond(60L * i), metrics);
        }
        return history;
    }

    private static CacheObservationBatch pair(CacheObservationBatch batch, List<CacheCostSample> samples) {
        var rows = batch.subtasks.stream().map(observation -> {
            var timing = samples.get(observation.subtask);
            long scale = observation.window.sequence - 1;
            long nativeNanos = timing.getObservedAtNanos();
            var cumulative = new StackDistanceHistogram(
                    new long[] {10 * scale, 40 * scale, 25 * scale}, 25 * scale, 1, 1, 4096);
            String metric = new A4SCacheCostSample(
                    timing.getSubtask(), timing.getAttempt(), timing.getCacheId(),
                    timing.getCapacityBytes(), timing.getCacheEpoch(), timing.getLookups(),
                    timing.getMisses(), timing.getCoveredHits(), timing.getCoveredMisses(),
                    timing.getSampledHits(), timing.getSampledMisses(), timing.getHitNanos(),
                    timing.getMissNanos(), timing.getRecordsIn(), timing.getBusyMillis(),
                    timing.getObservedAtMillis(), timing.getTimingAbi(), timing.getCompactionHits(),
                    timing.getCompactionMisses(), timing.getProcess(), timing.getObservedAtNanos())
                    .toMetricString();
            return new CacheObservation(
                    CacheObservation.ALIGNED_VERSION, observation.job, observation.vertex,
                    observation.subtask, observation.attempt, observation.status,
                    observation.window, observation.histogram,
                    new CacheObservation.Endpoint(2, nativeNanos - 1, nativeNanos, nativeNanos + 1,
                            timing.getLookups(), timing.getMisses(), cumulative, metric));
        }).collect(java.util.stream.Collectors.toList());
        return new CacheObservationBatch(batch.version, batch.status, batch.expectedSubtasks,
                batch.wireBytes, batch.oldestAgeMillis, rows);
    }

    @Test
    void qualifiesAtTheNewParallelismOnlyAfterFreshCompatibleWindows() {
        var vertex = new JobVertexID();
        var config = new Configuration().set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
        var previous = compatibleQuickMrcHistory(vertex);
        var original = evaluator.evaluate(config, previous, Duration.ZERO);
        assertTrue(original.getMemoryParallelismCurves().get(vertex)
                .getMemoryMbForParallelism(1).isPresent());

        var snapshots = previous.getMetricHistory();
        var observed = new CollectedMetricHistory(new JobTopology(new VertexInfo(vertex, Map.of(), 4, 24)),
                snapshots, Instant.ofEpochSecond(120));
        var transition = evaluator.evaluate(config, observed, Duration.ZERO);
        assertEquals(OnlineCacheCost.Reason.MISSING_TIMING, transition.getCacheCostFailures().get(vertex));
        assertFalse(transition.getMemoryParallelismCurves().containsKey(vertex));

        var curve = snapshots.get(snapshots.lastKey()).getMissRateCurves().get(vertex);
        for (int poll = 2; poll <= 4; poll++) {
            var current = new CollectedMetrics(
                    Map.of(vertex, Map.of(
                            ScalingMetric.NUM_RECORDS_IN, 60_000.0 * poll,
                            ScalingMetric.NUM_RECORDS_OUT, 60_000.0 * poll,
                            ScalingMetric.LAG, 0.0, ScalingMetric.LOAD, 0.8)),
                    Map.of(), Map.of(vertex, curve));
            int window = poll;
            var samples = IntStream.range(0, 4)
                    .mapToObj(i -> QuickMrcOnlineCacheCostTest.sample(window, 1, 0, i))
                    .collect(java.util.stream.Collectors.toList());
            var one = QuickMrcOnlineCacheCostTest.batch(poll, 1).subtasks.get(0);
            var observations = samples.stream().map(sample -> new CacheObservation(
                    one.version, one.job, one.vertex, sample.getSubtask(), sample.getAttempt(), one.status,
                    new CacheObservation.Window(one.window.process, sample.getCacheId(), one.window.epoch,
                            one.window.sequence, one.window.owners, one.window.population,
                            one.window.chargePolicy, one.window.cachePolicy, one.window.samplingRate,
                            one.window.capacityBytes, one.window.startNanos, one.window.endNanos,
                            one.window.observedAtMillis, one.window.references, one.window.misses),
                    one.histogram)).collect(java.util.stream.Collectors.toList());
            current.setCacheCostSamples(Map.of(vertex, samples));
            current.setCacheObservations(Map.of(vertex, pair(new CacheObservationBatch(
                    CacheObservation.VERSION, CacheObservation.Status.VALID, 4, 256, 0, observations), samples)));
            snapshots.put(Instant.ofEpochSecond(60L * poll), current);
            var result = evaluator.evaluate(config, observed, Duration.ZERO);
            if (poll < 4) {
                assertFalse(result.getMemoryParallelismCurves().containsKey(vertex));
            } else {
                var p4 = result.getMemoryParallelismCurves().get(vertex);
                assertTrue(p4.getMemoryMbForParallelism(4).isPresent());
                assertTrue(p4.getMemoryMbForParallelism(1).isEmpty());
                assertEquals(4, result.getCacheEvaluation().getVertices().get(vertex)
                        .getFit().getFit().getIdentities().size());
            }
        }

        var interrupted = new CollectedMetrics();
        interrupted.setCacheObservations(Map.of(vertex, QuickMrcOnlineCacheCostTest.batch(2, 0)));
        snapshots.put(Instant.ofEpochSecond(150), interrupted);
        var invalid = ScalingMetricEvaluator.selectOnlineFit(snapshots, vertex, 4);
        assertEquals(OnlineCacheCost.Reason.MISSING_HOLDOUT, invalid.getReason());
        assertEquals(CacheObservation.Status.CACHE_CHANGED, invalid.getHoldout().getPriorObservationStatus());
    }

    @Test
    void alignedLocalWindowsReachTheFitterWithoutAnAggregateCurve() {
        var vertex = new JobVertexID();
        var history = compatibleQuickMrcHistory(vertex);
        history.getMetricHistory().values().forEach(snapshot -> snapshot.setMissRateCurves(Map.of()));
        var config = new Configuration().set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
        var evaluated = evaluator.evaluate(config, history, Duration.ZERO);
        var decision = evaluated.getCacheEvaluation().getVertices().get(vertex);
        assertTrue(decision.getFit() != null && decision.getFit().getFit() != null);
    }

    @Test
    void boundedEndpointsStayInMemoryOnlyAndMissingEndpointsHold() {
        var vertex = new JobVertexID();
        var history = compatibleQuickMrcHistory(vertex).getMetricHistory();
        var first = history.get(history.firstKey());
        var last = history.get(history.lastKey());
        var firstBatch = first.getCacheObservations().get(vertex);
        var lastBatch = last.getCacheObservations().get(vertex);
        var persisted = ScalingMetricCollector.persistedHistory(history);
        assertNull(persisted.get(history.lastKey()).getCacheObservations().get(vertex).subtasks.get(0).endpoint);
        assertTrue(lastBatch.endpointBytes() > 0);

        var delta = A4SAlignedCacheWindow.between(firstBatch, first.getCacheCostSamples().get(vertex),
                lastBatch, last.getCacheCostSamples().get(vertex));
        assertEquals(CacheObservation.Status.VALID, delta.status);
        assertEquals(120_000_000_000L, delta.batch.subtasks.get(0).window.durationNanos());
        assertEquals(80, delta.batch.subtasks.get(0).histogram.getFrequency(1));
        assertEquals(CacheObservation.Status.DUPLICATE,
                A4SAlignedCacheWindow.between(lastBatch, last.getCacheCostSamples().get(vertex),
                        lastBatch, last.getCacheCostSamples().get(vertex)).status);
        ScalingMetricCollector.boundEndpoints(history, lastBatch.endpointBytes());
        assertNull(first.getCacheObservations().get(vertex).subtasks.get(0).endpoint);
        assertEquals(CacheObservation.Status.INCOMPLETE,
                A4SAlignedCacheWindow.between(first.getCacheObservations().get(vertex),
                        first.getCacheCostSamples().get(vertex),
                        lastBatch, last.getCacheCostSamples().get(vertex)).status);
    }

    @Test
    void reportsAnInterveningCounterResetRatherThanAnUnexplainedContextChange() {
        var vertex = new JobVertexID();
        var history = new TreeMap<Instant, CollectedMetrics>();
        for (int i = 0; i < 2; i++) {
            var snapshot = new CollectedMetrics();
            snapshot.setCacheObservations(Map.of(vertex, QuickMrcOnlineCacheCostTest.batch(i, 0)));
            snapshot.setCacheCostSamples(Map.of(
                    vertex, List.of(QuickMrcOnlineCacheCostTest.sample(i, 0, 0, 0))));
            history.put(Instant.ofEpochSecond(60L * i), snapshot);
        }
        var reset = new CollectedMetrics();
        reset.setCacheObservations(Map.of(
                vertex, CacheObservationBatch.invalid(CacheObservation.Status.COUNTER_RESET)));
        history.put(Instant.ofEpochSecond(30), reset);

        var fit = ScalingMetricEvaluator.selectOnlineFit(history, vertex, 1);
        assertEquals(OnlineCacheCost.Reason.COUNTER_RESET, fit.getReason());
    }

    @Test
    void leavesVerticesWithoutUsableMissRateCurvesOutOfCachePlacement() {
        var vertex = new JobVertexID();
        var history = cacheCostHistory(vertex);
        var snapshots = history.getMetricHistory();
        snapshots.get(snapshots.lastKey())
                .setMissRateCurves(Map.of(vertex, new MissRateCurve(List.of())));
        var evaluated = evaluator.evaluate(new Configuration(), history, Duration.ZERO);
        assertTrue(evaluated.getMemoryParallelismCurves().isEmpty());
        assertTrue(evaluated.getVertexMetrics().containsKey(vertex));
    }

    @Test
    void abortsEvaluationWhenEveryCalibratedParallelismIsInfeasible() {
        var vertex = new JobVertexID();
        var config =
                Configuration.fromMap(
                        Map.of(
                                "job.autoscaler.a4s.calibrated-cache-costs",
                                vertex.toHexString() + ":1.0/0.001",
                                AutoScalerOptions.VERTEX_MAX_PARALLELISM.key(),
                                "2"));

        var history = cacheCostHistory(vertex);
        assertThrows(
                IllegalStateException.class,
                () -> evaluator.evaluate(config, history, Duration.ZERO));
    }

    @Test
    void rejectsInvalidCalibratedCosts() {
        var vertex = new JobVertexID();
        var history = cacheCostHistory(vertex);
        for (String invalid : List.of("0.0001/0", "NaN/0.001", "0.0001/Infinity", "0.0001")) {
            var config =
                    Configuration.fromMap(
                            Map.of(
                                    "job.autoscaler.a4s.calibrated-cache-costs",
                                    vertex.toHexString() + ":" + invalid));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> evaluator.evaluate(config, history, Duration.ZERO));
        }
    }

    private static CollectedMetricHistory cacheCostHistory(JobVertexID vertex) {
        var topology = new JobTopology(new VertexInfo(vertex, Map.of(), 1, 24, null));
        var mrc =
                new MissRateCurve(
                        List.of(
                                new MissRateCurve.Point(50L << 20, 1.0),
                                new MissRateCurve.Point(100L << 20, 0.5),
                                new MissRateCurve.Point(200L << 20, 0.0)),
                MissRateCurve.CapacityAxis.ATTESTED_NATIVE_BYTES);
        var metrics = new TreeMap<Instant, CollectedMetrics>();
        for (int second = 0; second <= 1; second++) {
            metrics.put(
                    Instant.ofEpochSecond(second),
                    new CollectedMetrics(
                            Map.of(
                                    vertex,
                                    Map.of(
                                            ScalingMetric.NUM_RECORDS_IN, 1000.0 * second,
                                            ScalingMetric.NUM_RECORDS_OUT, 1000.0 * second,
                                            ScalingMetric.LAG, 0.0,
                                            ScalingMetric.LOAD, 0.8)),
                            Map.of(),
                            Map.of(vertex, mrc)));
        }
        return new CollectedMetricHistory(topology, metrics, Instant.ofEpochSecond(1));
    }

    @Test
    public void testLagBasedSourceScaling() {
        var source = new JobVertexID();
        var sink = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptyMap(), 1, 1, null),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1, null));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        950.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        0.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        0.,
                                        ScalingMetric.LOAD,
                                        .8),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 0., ScalingMetric.LOAD, .4)),
                        Map.of()));

        // Lag 950 -> 1000
        // Input records 0 -> 100
        // -> Source Data rate = 150
        // Output ratio 2
        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        1000.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        100.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        200.,
                                        ScalingMetric.LOAD,
                                        .6),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 200., ScalingMetric.LOAD, .3)),
                        Map.of()));

        var conf = new Configuration();

        conf.set(CATCH_UP_DURATION, Duration.ofSeconds(2));
        var evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();

        assertEquals(
                EvaluatedScalingMetric.avg(.7),
                evaluatedMetrics.get(source).get(ScalingMetric.LOAD));

        assertEquals(
                EvaluatedScalingMetric.avg(.35),
                evaluatedMetrics.get(sink).get(ScalingMetric.LOAD));

        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(500.),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(source).get(ScalingMetric.LAG));
        assertFalse(evaluatedMetrics.get(sink).containsKey(ScalingMetric.LAG));

        conf.set(CATCH_UP_DURATION, Duration.ofSeconds(1));
        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(2000),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));

        // Restart time should not affect evaluated metrics
        conf.set(RESTART_TIME, Duration.ofSeconds(2));

        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(2000),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));

        // Turn off lag based scaling
        conf.set(CATCH_UP_DURATION, Duration.ZERO);
        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(0),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(0),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));

        // Test 0 lag
        metricHistory.clear();
        metricHistory.put(
                Instant.ofEpochMilli(3000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        0.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        100.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        200.,
                                        ScalingMetric.LOAD,
                                        .6),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 200., ScalingMetric.LOAD, .3)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(4000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        0.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        200.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        400.,
                                        ScalingMetric.LOAD,
                                        .6),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 400., ScalingMetric.LOAD, .3)),
                        Map.of()));

        conf.set(CATCH_UP_DURATION, Duration.ofMinutes(1));
        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(100),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(200),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
    }

    @Test
    public void testUtilizationBoundaryComputation() {

        var conf = new Configuration();
        conf.set(TARGET_UTILIZATION, 0.8);
        conf.set(TARGET_UTILIZATION_BOUNDARY, 0.1);
        conf.set(RESTART_TIME, Duration.ofSeconds(1));
        conf.set(CATCH_UP_DURATION, Duration.ZERO);

        // Default behaviour, restart time does not factor in
        assertEquals(Tuple2.of(778.0, 1000.0), getThresholds(700, 0, conf));

        conf.set(CATCH_UP_DURATION, Duration.ofSeconds(2));
        assertEquals(Tuple2.of(1128.0, 1700.0), getThresholds(700, 350, conf));
        assertEquals(Tuple2.of(778.0, 1350.0), getThresholds(700, 0, conf));

        // Test thresholds during catchup periods
        assertEquals(
                Tuple2.of(1050., Double.POSITIVE_INFINITY), getThresholds(700, 350, conf, true));
        assertEquals(Tuple2.of(700., Double.POSITIVE_INFINITY), getThresholds(700, 0, conf, true));
    }

    @Test
    public void testUtilizationBoundaryComputationWithRestartTimesTracking() {

        var conf = new Configuration();
        conf.set(TARGET_UTILIZATION, 0.8);
        conf.set(TARGET_UTILIZATION_BOUNDARY, 0.1);
        conf.set(RESTART_TIME, Duration.ofMinutes(10));
        conf.set(CATCH_UP_DURATION, Duration.ZERO);
        conf.set(PREFER_TRACKED_RESTART_TIME, true);

        var scalingTracking = new ScalingTracking();
        scalingTracking.addScalingRecord(
                Instant.parse("2023-11-15T16:00:00.00Z"), new ScalingRecord(Duration.ofMinutes(3)));
        scalingTracking.addScalingRecord(
                Instant.parse("2023-11-15T16:20:00.00Z"), new ScalingRecord(Duration.ofMinutes(5)));

        var restartTimeSec = scalingTracking.getMaxRestartTimeOrDefault(conf);
        // Restart time does not factor in
        assertEquals(Tuple2.of(778.0, 1000.0), getThresholds(700, 0, restartTimeSec, conf));

        conf.set(CATCH_UP_DURATION, Duration.ofMinutes(1));
        assertEquals(Tuple2.of(1128.0, 4850.0), getThresholds(700, 350, restartTimeSec, conf));
        assertEquals(Tuple2.of(778.0, 4500.0), getThresholds(700, 0, restartTimeSec, conf));

        // Test thresholds during catchup periods
        assertEquals(
                Tuple2.of(1050., Double.POSITIVE_INFINITY),
                getThresholds(700, 350, restartTimeSec, conf, true));
        assertEquals(
                Tuple2.of(700., Double.POSITIVE_INFINITY),
                getThresholds(700, 0, restartTimeSec, conf, true));
    }

    @Test
    public void testBacklogProcessingEvaluation() {
        var source = new JobVertexID();
        var sink = new JobVertexID();
        var conf = new Configuration();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptyMap(), 1, 1),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        // 0 lag
        metricHistory.put(
                Instant.ofEpochMilli(0),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(ScalingMetric.LAG, 0., ScalingMetric.NUM_RECORDS_IN, 0.),
                                sink,
                                Map.of()),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(ScalingMetric.LAG, 0., ScalingMetric.NUM_RECORDS_IN, 100.),
                                sink,
                                Map.of()),
                        Map.of()));
        assertFalse(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));

        // Missing lag
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.NUM_RECORDS_IN, 100.), sink, Map.of()),
                        Map.of()));

        assertFalse(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));

        // Catch up time is more than a minute at avg proc rate (200)
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        250.
                                                * conf.get(
                                                                AutoScalerOptions
                                                                        .BACKLOG_PROCESSING_LAG_THRESHOLD)
                                                        .toSeconds(),
                                        ScalingMetric.NUM_RECORDS_IN,
                                        200.),
                                sink,
                                Map.of()),
                        Map.of()));

        assertTrue(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));

        // Catch up time is less than a minute at avg proc rate (200)
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        180.
                                                * conf.get(
                                                                AutoScalerOptions
                                                                        .BACKLOG_PROCESSING_LAG_THRESHOLD)
                                                        .toSeconds(),
                                        ScalingMetric.NUM_RECORDS_IN,
                                        200.),
                                sink,
                                Map.of()),
                        Map.of()));
        assertFalse(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));
    }

    @Test
    public void testObservedTprEvaluation() {
        var source = new JobVertexID();
        var conf = new Configuration();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 200.)), Map.of()));
        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 400.)), Map.of()));

        // Observed TPR average : 300

        // Set diff threshold to 20% -> within threshold
        conf.set(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD, 0.2);

        // Test that we used busy time based TPR
        assertEquals(
                350,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));

        // Set diff threshold to 10% -> outside threshold
        conf.set(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD, 0.1);

        // Test that we used the observed TPR
        assertEquals(
                300,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));

        // Test that observed tpr min observations are respected. If less, use busy time
        conf.set(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS, 3);
        assertEquals(
                350,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));
    }

    @Test
    public void testMissingObservedTpr() {
        var source = new JobVertexID();
        var conf = new Configuration();
        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        // Test that we used busy time based TPR even if infinity
        assertEquals(
                350.,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));

        assertEquals(
                Double.POSITIVE_INFINITY,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        0, 100, metricHistory, source, conf));

        assertEquals(
                Double.POSITIVE_INFINITY,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        0, 0, metricHistory, source, conf));

        assertEquals(
                Double.NaN,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        Double.NaN, Double.NaN, metricHistory, source, conf));
    }

    @Test
    public void testMissingBusyTimeTpr() {
        var source = new JobVertexID();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 200.)), Map.of()));
        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 400.)), Map.of()));
        assertEquals(
                300.,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        Double.NaN, 1., metricHistory, source, new Configuration()));
    }

    @Test
    public void testGlobalMetricEvaluation() {
        var globalMetrics = new TreeMap<Instant, CollectedMetrics>();
        globalMetrics.put(Instant.now(), new CollectedMetrics(Map.of(), Map.of()));

        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.HEAP_MEMORY_USED,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(Double.NaN)),
                ScalingMetricEvaluator.evaluateGlobalMetrics(globalMetrics));

        globalMetrics.put(
                Instant.now(),
                new CollectedMetrics(
                        Map.of(),
                        Map.of(
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                0.5,
                                ScalingMetric.GC_PRESSURE,
                                0.6,
                                ScalingMetric.HEAP_MEMORY_USED,
                                512.,
                                ScalingMetric.MANAGED_MEMORY_USED,
                                420.,
                                ScalingMetric.METASPACE_MEMORY_USED,
                                110.)));
        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        new EvaluatedScalingMetric(0.5, 0.5),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(0.6),
                        ScalingMetric.HEAP_MEMORY_USED,
                        new EvaluatedScalingMetric(512, 512),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        new EvaluatedScalingMetric(420, 420),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        new EvaluatedScalingMetric(110, 110),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(Double.NaN)),
                ScalingMetricEvaluator.evaluateGlobalMetrics(globalMetrics));

        globalMetrics.put(
                Instant.now(),
                new CollectedMetrics(
                        Map.of(),
                        Map.of(
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                0.7,
                                ScalingMetric.GC_PRESSURE,
                                0.8,
                                ScalingMetric.HEAP_MEMORY_USED,
                                1024.,
                                ScalingMetric.MANAGED_MEMORY_USED,
                                840.,
                                ScalingMetric.METASPACE_MEMORY_USED,
                                220.,
                                ScalingMetric.NUM_TASK_SLOTS_USED,
                                42.)));
        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        new EvaluatedScalingMetric(0.7, 0.6),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(0.8),
                        ScalingMetric.HEAP_MEMORY_USED,
                        new EvaluatedScalingMetric(1024., 768.),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        new EvaluatedScalingMetric(840., 630.),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        new EvaluatedScalingMetric(220., 165.),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(42.)),
                ScalingMetricEvaluator.evaluateGlobalMetrics(globalMetrics));
    }

    @Test
    public void testZeroValuesForRatesOrBusyness() {
        assertInfiniteTpr(0, 0);
        assertInfiniteTpr(0, 1);
        assertInfiniteTpr(1, 0);
        assertInfiniteTpr(Double.NaN, 0);
    }

    private static void assertInfiniteTpr(double busyTime, long inputRate) {
        assertEquals(
                Double.POSITIVE_INFINITY,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        busyTime,
                        inputRate,
                        new TreeMap<>(),
                        new JobVertexID(),
                        new Configuration()));
    }

    @Test
    public void testBusyTimeEvaluation() {
        var v = new JobVertexID();
        var conf = new Configuration();
        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                v,
                                Map.of(
                                        ScalingMetric.LOAD, 0.2,
                                        ScalingMetric.ACCUMULATED_BUSY_TIME, 10000.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                v,
                                Map.of(
                                        ScalingMetric.LOAD, 0.3,
                                        ScalingMetric.ACCUMULATED_BUSY_TIME, 10200.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(3000),
                new CollectedMetrics(
                        Map.of(
                                v,
                                Map.of(
                                        ScalingMetric.LOAD, 0.4,
                                        ScalingMetric.ACCUMULATED_BUSY_TIME, 10400.)),
                        Map.of()));

        // With MAX or MIN we should compute from LOAD only, parallelism should not matter
        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MAX);
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 2));
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 0));

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MIN);
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 2));
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 0));

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.AVG);
        // With AVG we compute from accumulated busy time
        // Diff 400 over 2 seconds -> 200 / second (for the whole job -> we need to divide for
        // parallelism)
        assertEquals(100., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 2));
        assertEquals(200., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 1));
    }

    @Test
    public void testComputableOutputRatios() {
        var source1 = new JobVertexID();
        var source2 = new JobVertexID();

        var op1 = new JobVertexID();
        var sink1 = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source1, Collections.emptyMap(), 1, 1),
                        new VertexInfo(source2, Collections.emptyMap(), 1, 1),
                        new VertexInfo(op1, Map.of(source1, REBALANCE, source2, REBALANCE), 1, 1),
                        new VertexInfo(sink1, Map.of(op1, REBALANCE), 1, 1));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 100.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 100.),
                                op1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 100.),
                                sink1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 100.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 200.,
                                        ScalingMetric.NUM_RECORDS_OUT, 300.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 200.,
                                        ScalingMetric.NUM_RECORDS_OUT, 150.),
                                op1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 350.),
                                sink1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 150.)),
                        Map.of()));

        assertEquals(
                2.,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source1, op1, topology, metricHistory));
        assertEquals(
                0.5,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source2, op1, topology, metricHistory));
        assertEquals(
                0.2,
                ScalingMetricEvaluator.computeEdgeOutputRatio(op1, sink1, topology, metricHistory));
    }

    @Test
    public void testOutputRatioFallbackToOutPerSecond() {
        var source1 = new JobVertexID();
        var source2 = new JobVertexID();

        var op1 = new JobVertexID();
        var op2 = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source1, Collections.emptyMap(), 1, 1),
                        new VertexInfo(source2, Collections.emptyMap(), 1, 1),
                        new VertexInfo(op1, Map.of(source1, REBALANCE, source2, REBALANCE), 1, 1),
                        new VertexInfo(op2, Map.of(source1, REBALANCE, source2, REBALANCE), 1, 1));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 0.,
                                        ScalingMetric.NUM_RECORDS_OUT, 0.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 0.,
                                        ScalingMetric.NUM_RECORDS_OUT, 0.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 200.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 50.)),
                        Map.of()));

        assertEquals(
                2.,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source1, op1, topology, metricHistory));
        assertEquals(
                0.5,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source2, op1, topology, metricHistory));
        assertEquals(
                2.,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source1, op2, topology, metricHistory));
        assertEquals(
                0.5,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source2, op2, topology, metricHistory));
    }

    @Test
    public void getRateTest() {
        var m1 = ScalingMetric.NUM_RECORDS_IN;
        var m2 = ScalingMetric.NUM_RECORDS_OUT;

        var v1 = new JobVertexID();
        var v2 = new JobVertexID();

        var history = new TreeMap<Instant, CollectedMetrics>();
        history.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(Map.of(v1, Map.of(m1, 0.), v2, Map.of()), null));
        history.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(Map.of(v1, Map.of(m1, 0., m2, 10.), v2, Map.of()), null));
        history.put(
                Instant.ofEpochMilli(3000),
                new CollectedMetrics(
                        Map.of(v1, Map.of(m1, 4., m2, 20.), v2, Map.of(m1, 1.)), null));

        assertEquals(2, ScalingMetricEvaluator.getRate(m1, v1, history));
        assertEquals(10., ScalingMetricEvaluator.getRate(m2, v1, history));
        assertEquals(Double.NaN, ScalingMetricEvaluator.getRate(m1, v2, history));
        assertEquals(Double.NaN, ScalingMetricEvaluator.getRate(m2, v2, history));
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate, double catchUpRate, Configuration conf) {
        return getThresholds(inputTargetRate, catchUpRate, conf, false);
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate, double catchUpRate, Duration restartTime, Configuration conf) {
        return getThresholds(inputTargetRate, catchUpRate, restartTime, conf, false);
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate, double catchUpRate, Configuration conf, boolean catchingUp) {
        var restartTime = conf.get(AutoScalerOptions.RESTART_TIME);
        return getThresholds(inputTargetRate, catchUpRate, restartTime, conf, catchingUp);
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate,
            double catchUpRate,
            Duration restartTime,
            Configuration conf,
            boolean catchingUp) {
        var map = new HashMap<ScalingMetric, EvaluatedScalingMetric>();

        map.put(ScalingMetric.TARGET_DATA_RATE, EvaluatedScalingMetric.avg(inputTargetRate));
        map.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchUpRate));

        ScalingMetricEvaluator.computeProcessingRateThresholds(map, conf, catchingUp, restartTime);
        return Tuple2.of(
                map.get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent(),
                map.get(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD).getCurrent());
    }
}
