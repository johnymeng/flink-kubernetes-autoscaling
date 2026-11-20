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

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.a4s.core.StackDistanceHistogram;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuickMrcOnlineCacheCostTest {
    @Test
    void reportsActualForegroundPenaltyAndAppliesForegroundShareOnce() {
        var before = sample(0, 0, 0, 0);
        var after = sample(1, 0, 0, 0);
        var result = qualify(before, after, 0, 1);
        assertEquals(OnlineCacheCost.Reason.QUALIFIED, result.getReason());
        var fit = result.getFit();
        assertEquals(16e-6, fit.getCost().getMissPenaltySecondsPerRecord(), 1e-12);
        assertEquals(112e-6, fit.getCost().getZeroMissSecondsPerRecord(), 1e-12);
        assertEquals(20e-6, fit.getSubtasks().get(0).getExtraSecondsPerForegroundMiss(), 1e-12);
        assertEquals(.4, fit.getSubtasks().get(0).getForegroundMissShare(), 1e-12);
        assertEquals(2, fit.getSubtasks().get(0).getLookupsPerRecord(), 1e-12);
    }

    @Test
    void classifiesMissingCoverageResetAndNegativeBaseline() {
        var before = sample(0, 0, 0, 0);
        var after = sample(1, 0, 0, 0);
        after.setCoveredMisses(100);
        assertEquals(OnlineCacheCost.Reason.TIMING_COVERAGE, qualify(before, after, 0, 1).getReason());
        after = sample(1, 0, 0, 0);
        before.setHitNanos(after.getHitNanos() + 1);
        assertEquals(OnlineCacheCost.Reason.COUNTER_RESET, qualify(before, after, 0, 1).getReason());
        before = sample(0, 0, 0, 0);
        after.setBusyMillis(20);
        assertEquals(OnlineCacheCost.Reason.NON_POSITIVE_BASELINE, qualify(before, after, 0, 1).getReason());
        after.setBusyMillis(700);
        after.setMissNanos(20_000);
        after.setHitNanos(100_000);
        assertEquals(OnlineCacheCost.Reason.NON_POSITIVE_PENALTY, qualify(before, after, 0, 1).getReason());
    }

    @Test
    void checksMonotonicTimeAndRejectsCrossEpochFitEvenIfWallTimeLooksFine() {
        var before = sample(0, 0, 0, 0);
        var after = sample(1, 0, 0, 0);
        after.setObservedAtNanos(before.getObservedAtNanos());
        assertNull(qualify(before, after, 0, 1).getFit());
        after = sample(1, 1, 0, 0);
        assertEquals(OnlineCacheCost.Reason.CONTEXT_CHANGED, qualify(before, after, 0, 1).getReason());
        after = sample(1, 0, 0, 0);
        after.setProcess("another-worker");
        assertEquals(OnlineCacheCost.Reason.UNBOUND_TIMING, qualify(before, after, 0, 1).getReason());
    }

    @Test
    void detectsObservedDriftUsingASeparateLaterWindow() {
        var prior = qualify(sample(0, 0, 0, 0), sample(1, 0, 0, 0), 0, 1).getFit();
        var later = qualify(sample(1, 0, 0, 0), sample(2, 0, 2_000, 0), 1, 2).getFit();
        assertTrue(prior.holdoutRelativeError(later) > .75);
    }

    @Test
    void choosesOnlyAFeasibleMeasuredCapacityAndPreservesUnconstrainedBudget() {
        var fit = qualify(sample(0, 0, 0, 0), sample(1, 0, 0, 0), 0, 1).getFit();
        var observation = batch(1, 0);
        var supported = QuickMrcCostProjection.recommend(fit, observation, 8500, null).orElseThrow();
        assertEquals(12_288, supported.getTotalNativeCacheBytes());
        assertEquals(116e-6, supported.getPredictedSecondsPerRecord(), 1e-12);
        assertTrue(QuickMrcCostProjection.recommend(fit, observation, 10_000, null).isEmpty());
        var unconstrained = QuickMrcCostProjection.recommend(fit, observation, 5000, null).orElseThrow();
        assertEquals(8_192, unconstrained.getTotalNativeCacheBytes());
        assertTrue(unconstrained.isRetainedUnconstrainedBudget());
    }

    @Test
    void freezesCapacityForecastBeforeALaterActualMissDiagnostic() {
        var fit = qualify(sample(0, 0, 0, 0), sample(1, 0, 0, 0), 0, 1).getFit();
        var forecast = QuickMrcCostProjection.recommend(fit, batch(1, 0), 8500, null).orElseThrow();
        // Later trial fixture, deliberately with an MRC error of five percentage points.
        double laterBusySecondsPerRecord = 117e-6;
        double actualMissPlugIn = fit.getSubtasks().get(0).predict(.30);
        assertEquals(-.2e-6, actualMissPlugIn - laterBusySecondsPerRecord, 1e-12);
        assertEquals(-1e-6, forecast.getPredictedSecondsPerRecord() - laterBusySecondsPerRecord, 1e-12);
        assertEquals(4_096, forecast.getMinSupportedTotalBytes());
        assertEquals(12_288, forecast.getMaxSupportedTotalBytes());
    }

    @Test
    void idleOutcomesAndSparseTimingHaveAnExplicitReason() {
        var before = sample(0, 0, 0, 0);
        var after = sample(1, 0, 0, 0);
        after.setMisses(0);
        after.setCompactionMisses(0);
        assertEquals(OnlineCacheCost.Reason.NO_HIT_OR_MISS, qualify(before, after, 0, 1).getReason());
        after.setMisses(after.getLookups());
        after.setCompactionHits(0);
        assertEquals(OnlineCacheCost.Reason.NO_HIT_OR_MISS, qualify(before, after, 0, 1).getReason());
        after = sample(1, 0, 0, 0);
        after.setSampledMisses(63);
        assertEquals(OnlineCacheCost.Reason.TOO_FEW_SAMPLES, qualify(before, after, 0, 1).getReason());
    }

    @Test
    void avoidsCreditingSavingsInsideTheLiveCapacityBin() {
        var before = sample(0, 0, 0, 0);
        var after = sample(1, 0, 0, 0);
        before.setCapacityBytes(10_000);
        after.setCapacityBytes(10_000);
        var current = batch(1, 0, 10_000);
        var fit = OnlineCacheCost.fitQuickMrc(
                batch(0, 0, 10_000), List.of(before), current, List.of(after)).getFit();

        assertTrue(QuickMrcCostProjection.recommend(fit, current, 8500, null).isEmpty());
        assertTrue(QuickMrcCostProjection.recommend(fit, batch(2, 0, 10_000), 5000, null).isEmpty());
    }

    @Test
    void comparesMrcBiasAgainstTheSameHistogramWindow() {
        var current = batch(1, 0, 8192, 450);
        var fit = OnlineCacheCost.fitQuickMrc(
                batch(0, 0), List.of(sample(0, 0, 0, 0)),
                current, List.of(sample(1, 0, 0, 0))).getFit();

        var recommendation = QuickMrcCostProjection.recommend(fit, current, 8500, null).orElseThrow();
        assertEquals(.05, recommendation.getWorstAbsoluteAnchorMissRateError(), 1e-12);
    }

    private OnlineCacheCost.FitResult qualify(
            CacheCostSample start, CacheCostSample end, int startWindow, int endWindow) {
        return OnlineCacheCost.fitQuickMrc(
                batch(startWindow, start.getCacheEpoch()),
                List.of(start),
                batch(endWindow, end.getCacheEpoch()),
                List.of(end));
    }

    public static CacheCostSample sample(int window, long epoch, double extraBusyMillis, int subtask) {
        return new CacheCostSample(
                subtask, "original", 987 + subtask, 8192, epoch,
                10_000L * window, 5_000L * window, 4_000L * window, 2_000L * window,
                100L * window, 100L * window, 100_000L * window, 2_100_000L * window,
                5_000L * window, 600 * window + extraBusyMillis,
                1_000L + 60_000L * window, 3, 1_000L * window, 3_000L * window,
                "process", 1_000_000_000L + 60_000_000_000L * window);
    }

    public static CacheObservationBatch batch(int window, long epoch) {
        return batch(window, epoch, 8192);
    }

    private static CacheObservationBatch batch(int window, long epoch, long capacityBytes) {
        return batch(window, epoch, capacityBytes, 500);
    }

    private static CacheObservationBatch batch(
            int window, long epoch, long capacityBytes, long windowMisses) {
        long endNanos = 1_000_000_000L + 60_000_000_000L * window;
        var observation =
                new CacheObservation(
                        CacheObservation.VERSION, "job", "vertex", 0, "original",
                        CacheObservation.Status.VALID,
                        new CacheObservation.Window(
                                "process", 987, epoch, window + 1, List.of(1L),
                                CacheObservation.QUICK_MRC, CacheObservation.FIXED_4K,
                                CacheObservation.CACHE_POLICY, .02, capacityBytes,
                                endNanos - 10_000_000_000L, endNanos, 1_000L + 60_000L * window,
                                1000, windowMisses),
                        new StackDistanceHistogram(new long[] {10, 40, 25}, 25, 1, 1, 4096));
        return new CacheObservationBatch(
                CacheObservation.VERSION, CacheObservation.Status.VALID, 1, 128, 0,
                List.of(observation));
    }
}
