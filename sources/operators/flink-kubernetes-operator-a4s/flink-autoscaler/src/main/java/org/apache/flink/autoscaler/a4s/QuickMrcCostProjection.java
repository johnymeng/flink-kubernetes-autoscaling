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

import lombok.Value;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Evaluates a uniform native cache allocation using each local QuickMRC and its local timing. */
public final class QuickMrcCostProjection {
    private QuickMrcCostProjection() {}

    @Value
    public static class Recommendation {
        long totalNativeCacheBytes;
        double predictedSecondsPerRecord;
        long minSupportedTotalBytes;
        long maxSupportedTotalBytes;
        double worstAbsoluteAnchorMissRateError;
        boolean retainedUnconstrainedBudget;
    }

    @Value
    public static class Result {
        @Nullable Recommendation recommendation;
        @Nullable Long minSupportedTotalBytes;
        @Nullable Long maxSupportedTotalBytes;
    }

    public static Optional<Recommendation> recommend(
            OnlineCacheCost.Fit fit,
            CacheObservationBatch batch,
            double targetRecordsPerSecond,
            @Nullable Double fixedBaselineSecondsPerRecord) {
        return Optional.ofNullable(
                project(fit, batch, targetRecordsPerSecond, fixedBaselineSecondsPerRecord)
                        .getRecommendation());
    }

    public static Result project(
            OnlineCacheCost.Fit fit,
            CacheObservationBatch batch,
            double targetRecordsPerSecond,
            @Nullable Double fixedBaselineSecondsPerRecord) {
        if (!Double.isFinite(targetRecordsPerSecond)
                || targetRecordsPerSecond <= 0
                || (fixedBaselineSecondsPerRecord != null
                        && (!Double.isFinite(fixedBaselineSecondsPerRecord)
                                || fixedBaselineSecondsPerRecord <= 0))
                || batch.subtasks.isEmpty()
                || fit.getSubtasks().size() != batch.subtasks.size()
                || fit.getIdentities().size() != batch.subtasks.size()) {
            return unsupported();
        }

        try {
            return recommendWithinSupport(fit, batch, targetRecordsPerSecond, fixedBaselineSecondsPerRecord);
        } catch (ArithmeticException invalidGeometry) {
            return unsupported();
        }
    }

    private static Result unsupported() {
        return new Result(null, null, null);
    }

    /** True only if demand exceeds even the optimistic limit at the measured topology. */
    public static boolean cannotMeetDemandAtCurrentParallelism(
            OnlineCacheCost.Fit fit, double demand) {
        if (!Double.isFinite(demand) || demand <= 0) {
            return false;
        }
        var subtasks = fit.getSubtasks();
        double inputRate = subtasks.stream()
                .mapToDouble(task -> task.getRecords() / (double) task.getElapsedNanos())
                .sum();
        for (var task : subtasks) {
            double share = task.getRecords() / (double) task.getElapsedNanos() / inputRate;
            // The miss fraction can drop by at most one, regardless of histogram support.
            double workFloor = task.getBusySecondsPerRecord() - task.getMissPenaltySecondsPerRecord();
            if (workFloor > 0 && demand * share * workFloor > 1) {
                return true;
            }
        }
        return false;
    }

    private static Result recommendWithinSupport(
            OnlineCacheCost.Fit fit,
            CacheObservationBatch batch,
            double target,
            @Nullable Double fixedBaseline) {
        int parallelism = batch.subtasks.size();
        CacheObservation first = batch.subtasks.get(0);
        if (first.window == null || first.histogram == null) {
            return unsupported();
        }
        long capacityBytes = first.window.capacityBytes;
        long binBytes = Math.multiplyExact(
                first.histogram.getBucketSizeScaling(), first.histogram.getCacheItemSizeBytes());
        // Use the enclosing upper edge. A lower-edge miss rate would over-credit savings
        // when the live cache capacity falls between histogram bins.
        int anchorBin = Math.toIntExact((capacityBytes - 1) / binBytes);
        if (anchorBin < 0) {
            return unsupported();
        }

        List<StackDistanceHistogram> histograms = new ArrayList<>(parallelism);
        long[] totals = new long[parallelism];
        double[] anchorMissRates = new double[parallelism];
        double[] inputRates = new double[parallelism];
        double totalInputRate = 0;
        double worstBias = 0;
        int commonBins = Integer.MAX_VALUE;
        for (int i = 0; i < parallelism; i++) {
            CacheObservation observation = batch.subtasks.get(i);
            StackDistanceHistogram histogram = observation.histogram;
            var identity = fit.getIdentities().get(i);
            if (observation.window == null
                    || observation.status != CacheObservation.Status.VALID
                    || observation.subtask != i
                    || !observation.attempt.equals(identity.getAttempt())
                    || !observation.window.process.equals(identity.getProcess())
                    || observation.window.cacheId != identity.getCacheId()
                    || observation.window.epoch != identity.getEpoch()
                    || observation.window.sequence != identity.getEndSequence()
                    || observation.window.samplingRate != identity.getSamplingRate()
                    || observation.window.capacityBytes != identity.getCapacityBytes()
                    || observation.window.references <= 0
                    || observation.window.capacityBytes != capacityBytes
                    || histogram == null
                    || capacityBytes > Math.multiplyExact(histogram.getNumBuckets(), binBytes)
                    || histogram.getNumBuckets() <= anchorBin
                    || histogram.getBucketSizeScaling() != first.histogram.getBucketSizeScaling()
                    || histogram.getCacheItemSizeBytes() != first.histogram.getCacheItemSizeBytes()) {
                return unsupported();
            }
            histograms.add(histogram);
            totals[i] = histogram.getTotalFrequency();
            if (totals[i] <= 0) {
                return unsupported();
            }
            long hitsAtAnchor = 0;
            for (int bin = 0; bin <= anchorBin; bin++) {
                hitsAtAnchor = Math.addExact(hitsAtAnchor, histogram.getFrequency(bin));
            }
            OnlineCacheCost.SubtaskCost local = fit.getSubtasks().get(i);
            anchorMissRates[i] = 1.0 - hitsAtAnchor / (double) totals[i];
            inputRates[i] = local.getRecords() / (double) local.getElapsedNanos();
            totalInputRate += inputRates[i];
            double observedWindowMissRate =
                    observation.window.misses / (double) observation.window.references;
            worstBias = Math.max(worstBias, Math.abs(anchorMissRates[i] - observedWindowMissRate));
            commonBins = Math.min(commonBins, histogram.getNumBuckets());
        }

        long minSupportedTotalBytes = Math.multiplyExact(binBytes, parallelism);
        long maxSupportedTotalBytes =
                Math.multiplyExact(Math.multiplyExact(commonBins, binBytes), parallelism);
        long[] hits = new long[parallelism];
        for (int bin = 0; bin < commonBins; bin++) {
            double weightedSeconds = 0;
            boolean fits = true;
            for (int i = 0; i < parallelism; i++) {
                hits[i] = Math.addExact(hits[i], histograms.get(i).getFrequency(bin));
                double missRate = 1.0 - hits[i] / (double) totals[i];
                OnlineCacheCost.SubtaskCost local = fit.getSubtasks().get(i);
                double slope = local.getMissPenaltySecondsPerRecord();
                double seconds = fixedBaseline == null
                        ? local.getBusySecondsPerRecord() + slope * (missRate - anchorMissRates[i])
                        : fixedBaseline + slope * missRate;
                double recordShare = inputRates[i] / totalInputRate;
                if (!Double.isFinite(seconds) || seconds <= 0 || target * recordShare * seconds > 1) {
                    fits = false;
                }
                weightedSeconds += recordShare * seconds;
            }
            if (fits) {
                boolean unconstrained = bin == 0;
                long localBytes = unconstrained ? capacityBytes : Math.multiplyExact(bin + 1L, binBytes);
                if (unconstrained) {
                    weightedSeconds = 0;
                    for (int i = 0; i < parallelism; i++) {
                        OnlineCacheCost.SubtaskCost local = fit.getSubtasks().get(i);
                        double seconds = fixedBaseline == null
                                ? local.getBusySecondsPerRecord()
                                : fixedBaseline + local.getMissPenaltySecondsPerRecord() * anchorMissRates[i];
                        weightedSeconds += inputRates[i] / totalInputRate * seconds;
                    }
                }
                return new Result(
                        new Recommendation(
                                Math.multiplyExact(localBytes, parallelism),
                                weightedSeconds,
                                minSupportedTotalBytes,
                                maxSupportedTotalBytes,
                                worstBias,
                                unconstrained),
                        minSupportedTotalBytes, maxSupportedTotalBytes);
            }
        }
        return new Result(null, minSupportedTotalBytes, maxSupportedTotalBytes);
    }
}
