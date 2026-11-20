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

import lombok.Value;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Fits a per-record cache cost from a completed observation window. */
@Value
public class OnlineCacheCost {
    public static final long MIN_WINDOW_SECONDS = 60;
    public static final long MAX_WINDOW_SECONDS = 120;
    private static final int COMPACTION_TIMING_ABI = 3;
    private static final long MIN_TIMED_LOOKUPS_PER_CLASS = 64;
    private static final double MIN_COVERAGE = 0.90;
    // The histogram/ticker and resident-cache snapshots are separate native reads.
    private static final double MAX_COVERAGE = 1.02;

    double zeroMissSecondsPerRecord;
    double missPenaltySecondsPerRecord;

    public enum Reason {
        QUALIFIED,
        MISSING_TIMING,
        MISSING_OBSERVATIONS,
        UNBOUND_TIMING,
        CONTEXT_CHANGED,
        WINDOW_DURATION,
        WINDOW_SKEW,
        COUNTER_RESET,
        NO_HIT_OR_MISS,
        TIMING_COVERAGE,
        TOO_FEW_SAMPLES,
        NON_POSITIVE_PENALTY,
        NON_POSITIVE_BASELINE,
        RECORD_SKEW,
        MISSING_HOLDOUT,
        PHASE_DRIFT,
        CAPACITY_UNSUPPORTED,
        INSUFFICIENT_CURRENT_PARALLELISM
    }

    @Value
    public static class FitResult {
        Reason reason;
        @Nullable Fit fit;

        public static FitResult rejected(Reason reason) {
            return new FitResult(reason, null);
        }
    }

    @Value
    public static class SubtaskCost {
        long records;
        long lookups;
        long misses;
        long elapsedNanos;
        long sampledHits;
        long sampledMisses;
        double hitCoverage;
        double missCoverage;
        double meanHitSeconds;
        double meanMissSeconds;
        double foregroundMissShare;
        double busySecondsPerRecord;
        double zeroMissSecondsPerRecord;

        public double getExtraSecondsPerForegroundMiss() {
            return meanMissSeconds - meanHitSeconds;
        }

        public double getLookupsPerRecord() {
            return lookups / (double) records;
        }

        public double getMissPenaltySecondsPerRecord() {
            return getExtraSecondsPerForegroundMiss() * foregroundMissShare * getLookupsPerRecord();
        }

        public double getActualMissRate() {
            return misses / (double) lookups;
        }

        public double predict(double missRate) {
            return zeroMissSecondsPerRecord + getMissPenaltySecondsPerRecord() * missRate;
        }
    }

    /** The process/cache and attempt context that produced one projected subtask cost. */
    @Value
    public static class FitIdentity {
        String attempt;
        String process;
        long cacheId;
        long epoch;
        long capacityBytes;
        double samplingRate;
        long startSequence;
        long endSequence;
    }

    @Value
    public static class Fit {
        OnlineCacheCost cost;
        List<SubtaskCost> subtasks;
        List<FitIdentity> identities;
        long windowStartMillis;
        long windowEndMillis;

        /**
         * Retrospective timing diagnostic. Use independently observed miss ratios from the later
         * window and the coefficients frozen before it started.
         */
        public double holdoutRelativeError(Fit later) {
            if (subtasks.size() != later.subtasks.size()
                    || windowEndMillis != later.windowStartMillis
                    || !adjacentContexts(later)) {
                return Double.POSITIVE_INFINITY;
            }
            double worstRelativeError = 0;
            for (int i = 0; i < subtasks.size(); i++) {
                SubtaskCost observed = later.subtasks.get(i);
                double predicted = subtasks.get(i).predict(observed.getActualMissRate());
                worstRelativeError = Math.max(
                        worstRelativeError,
                        Math.abs(predicted - observed.busySecondsPerRecord) / observed.busySecondsPerRecord);
            }
            return worstRelativeError;
        }

        private boolean adjacentContexts(Fit later) {
            if (identities.size() != later.identities.size()) {
                return false;
            }
            for (int i = 0; i < identities.size(); i++) {
                FitIdentity before = identities.get(i);
                FitIdentity after = later.identities.get(i);
                if (!before.attempt.equals(after.attempt)
                        || !before.process.equals(after.process)
                        || before.cacheId != after.cacheId
                        || before.epoch != after.epoch
                        || before.capacityBytes != after.capacityBytes
                        || before.samplingRate != after.samplingRate
                        || before.endSequence != after.startSequence) {
                    return false;
                }
            }
            return true;
        }

        public boolean hasStableAccessMix(Fit later) {
            if (subtasks.size() != later.subtasks.size()) {
                return false;
            }
            for (int i = 0; i < subtasks.size(); i++) {
                SubtaskCost before = subtasks.get(i);
                SubtaskCost after = later.subtasks.get(i);
                if (Math.abs(before.foregroundMissShare - after.foregroundMissShare) > .10
                        || Math.abs(before.getLookupsPerRecord() / after.getLookupsPerRecord() - 1) > .25) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Historical ABI 2/3 diagnostic. The controller requires versioned observations. */
    public static Optional<OnlineCacheCost> from(List<CacheCostSample> start, List<CacheCostSample> end) {
        Fit fit = estimate(start, end, List.of()).fit;
        return fit == null ? Optional.empty() : Optional.of(fit.cost);
    }

    public static FitResult fitQuickMrc(
            @Nullable CacheObservationBatch startObservation,
            List<CacheCostSample> start,
            @Nullable CacheObservationBatch endObservation,
            List<CacheCostSample> end) {
        if (start.isEmpty() || start.size() != end.size()) {
            return FitResult.rejected(Reason.MISSING_TIMING);
        }
        if (!complete(startObservation, start.size()) || !complete(endObservation, end.size())) {
            return FitResult.rejected(Reason.MISSING_OBSERVATIONS);
        }
        List<FitIdentity> identities = new ArrayList<>(end.size());
        for (int i = 0; i < end.size(); i++) {
            CacheObservation before = startObservation.subtasks.get(i);
            CacheObservation after = endObservation.subtasks.get(i);
            if (!bound(before, start.get(i)) || !bound(after, end.get(i))) {
                return FitResult.rejected(Reason.UNBOUND_TIMING);
            }
            CacheObservation.Window left = before.window;
            CacheObservation.Window right = after.window;
            if (!before.job.equals(after.job)
                    || !before.vertex.equals(after.vertex)
                    || !left.process.equals(right.process)
                    || left.cacheId != right.cacheId
                    || left.epoch != right.epoch
                    || left.capacityBytes != right.capacityBytes
                    || left.samplingRate != right.samplingRate
                    || right.sequence <= left.sequence) {
                return FitResult.rejected(Reason.CONTEXT_CHANGED);
            }
            identities.add(new FitIdentity(
                    after.attempt, right.process, right.cacheId, right.epoch, right.capacityBytes,
                    right.samplingRate, left.sequence, right.sequence));
        }
        if (!aligned(start) || !aligned(end)) {
            return FitResult.rejected(Reason.WINDOW_SKEW);
        }
        return estimate(start, end, List.copyOf(identities));
    }

    private static boolean complete(@Nullable CacheObservationBatch batch, int size) {
        return batch != null
                && batch.version == CacheObservation.VERSION
                && batch.status == CacheObservation.Status.VALID
                && batch.expectedSubtasks == size
                && batch.subtasks.size() == size;
    }

    private static boolean bound(CacheObservation observation, CacheCostSample sample) {
        CacheObservation.Window window = observation.window;
        return sample != null
                && observation.version == CacheObservation.VERSION
                && observation.status == CacheObservation.Status.VALID
                && window != null
                && window.owners.equals(List.of(1L))
                && CacheObservation.QUICK_MRC.equals(window.population)
                && CacheObservation.CACHE_POLICY.equals(window.cachePolicy)
                && CacheObservation.FIXED_4K.equals(window.chargePolicy)
                && sample.getTimingAbi() == COMPACTION_TIMING_ABI
                && observation.subtask == sample.getSubtask()
                && observation.attempt.equals(sample.getAttempt())
                && window.process.equals(sample.getProcess())
                && window.cacheId == sample.getCacheId()
                && window.capacityBytes == sample.getCapacityBytes()
                && window.epoch == sample.getCacheEpoch()
                && sample.getObservedAtNanos() != null
                && Math.abs((double) (sample.getObservedAtNanos() - window.endNanos))
                        <= TimeUnit.SECONDS.toNanos(10);
    }

    private static boolean aligned(List<CacheCostSample> samples) {
        long oldest = Long.MAX_VALUE;
        long newest = Long.MIN_VALUE;
        for (CacheCostSample sample : samples) {
            oldest = Math.min(oldest, sample.getObservedAtMillis());
            newest = Math.max(newest, sample.getObservedAtMillis());
        }
        return newest - oldest <= CacheObservation.MAX_CLOCK_SKEW_MILLIS;
    }

    /** Reject an observed reset even if the counters recover before a fit's end. */
    public static boolean hasDiscontinuity(
            List<CacheCostSample> before, List<CacheCostSample> after) {
        if (before.size() != after.size()) {
            return true;
        }
        for (int subtask = 0; subtask < after.size(); subtask++) {
            CacheCostSample left = before.get(subtask);
            CacheCostSample right = after.get(subtask);
            if (!sameCache(left, right, subtask) || countersDecreased(left, right)) {
                return true;
            }
        }
        return false;
    }

    private static boolean countersDecreased(CacheCostSample before, CacheCostSample after) {
        return after.getRecordsIn() < before.getRecordsIn()
                || after.getBusyMillis() < before.getBusyMillis()
                || after.getLookups() < before.getLookups()
                || after.getMisses() < before.getMisses()
                || after.getCompactionHits() < before.getCompactionHits()
                || after.getCompactionMisses() < before.getCompactionMisses()
                || after.getCoveredHits() < before.getCoveredHits()
                || after.getCoveredMisses() < before.getCoveredMisses()
                || after.getSampledHits() < before.getSampledHits()
                || after.getSampledMisses() < before.getSampledMisses()
                || after.getHitNanos() < before.getHitNanos()
                || after.getMissNanos() < before.getMissNanos();
    }

    private static FitResult estimate(
            List<CacheCostSample> start, List<CacheCostSample> end, List<FitIdentity> identities) {
        if (start.isEmpty() || start.size() != end.size()) {
            return FitResult.rejected(Reason.MISSING_TIMING);
        }
        boolean monotonic = !identities.isEmpty();
        List<SubtaskCost> subtasks = new ArrayList<>(end.size());
        double totalRecords = 0;
        double busySecondsAtZeroMisses = 0;
        double secondsAtFullMissRate = 0;
        double minRecordRate = Double.POSITIVE_INFINITY;
        double maxRecordRate = 0;
        long oldestStart = Long.MAX_VALUE;
        long oldestEnd = Long.MAX_VALUE;
        for (int subtask = 0; subtask < end.size(); subtask++) {
            CacheCostSample before = start.get(subtask);
            CacheCostSample after = end.get(subtask);
            if (!sameCache(before, after, subtask)) {
                return FitResult.rejected(Reason.CONTEXT_CHANGED);
            }
            long elapsedNanos = monotonic
                    ? after.getObservedAtNanos() - before.getObservedAtNanos()
                    : TimeUnit.MILLISECONDS.toNanos(after.getObservedAtMillis() - before.getObservedAtMillis());
            if (elapsedNanos < TimeUnit.SECONDS.toNanos(MIN_WINDOW_SECONDS)
                    || elapsedNanos > TimeUnit.SECONDS.toNanos(MAX_WINDOW_SECONDS)
                    || before.getObservedAtMillis() <= 0
                    || after.getObservedAtMillis() <= before.getObservedAtMillis()) {
                return FitResult.rejected(Reason.WINDOW_DURATION);
            }

            if (countersDecreased(before, after)) {
                return FitResult.rejected(Reason.COUNTER_RESET);
            }
            long recordsIn = after.getRecordsIn() - before.getRecordsIn();
            double busySeconds = (after.getBusyMillis() - before.getBusyMillis()) / 1000;
            long lookups = after.getLookups() - before.getLookups();
            long misses = after.getMisses() - before.getMisses();
            long compactionHits = after.getCompactionHits() - before.getCompactionHits();
            long compactionMisses = after.getCompactionMisses() - before.getCompactionMisses();
            long coveredHits = after.getCoveredHits() - before.getCoveredHits();
            long coveredMisses = after.getCoveredMisses() - before.getCoveredMisses();
            long sampledHits = after.getSampledHits() - before.getSampledHits();
            long sampledMisses = after.getSampledMisses() - before.getSampledMisses();
            long hitNanos = after.getHitNanos() - before.getHitNanos();
            long missNanos = after.getMissNanos() - before.getMissNanos();
            if (misses > lookups || compactionHits > lookups - misses || compactionMisses > misses) {
                return FitResult.rejected(Reason.TIMING_COVERAGE);
            }
            long foregroundHits = lookups - misses - compactionHits;
            long foregroundMisses = misses - compactionMisses;
            if (foregroundHits == 0 || foregroundMisses == 0) {
                return FitResult.rejected(Reason.NO_HIT_OR_MISS);
            }
            if (!usableCoverage(coveredHits, foregroundHits)
                    || !usableCoverage(coveredMisses, foregroundMisses)
                    || sampledHits > coveredHits || sampledMisses > coveredMisses) {
                return FitResult.rejected(Reason.TIMING_COVERAGE);
            }
            if (recordsIn == 0 || sampledHits < MIN_TIMED_LOOKUPS_PER_CLASS
                    || sampledMisses < MIN_TIMED_LOOKUPS_PER_CLASS) {
                return FitResult.rejected(Reason.TOO_FEW_SAMPLES);
            }

            double meanHit = hitNanos / (double) sampledHits / 1e9;
            double meanMiss = missNanos / (double) sampledMisses / 1e9;
            double extraSecondsPerMiss = meanMiss - meanHit;
            if (hitNanos < sampledHits || missNanos < sampledMisses
                    || !positiveFinite(extraSecondsPerMiss)) {
                return FitResult.rejected(Reason.NON_POSITIVE_PENALTY);
            }
            double residual = busySeconds - extraSecondsPerMiss * foregroundMisses;
            if (!positiveFinite(residual)) {
                return FitResult.rejected(Reason.NON_POSITIVE_BASELINE);
            }

            var cost = new SubtaskCost(
                    recordsIn, lookups, misses, elapsedNanos, sampledHits, sampledMisses,
                    coveredHits / (double) foregroundHits, coveredMisses / (double) foregroundMisses,
                    meanHit, meanMiss, foregroundMisses / (double) misses,
                    busySeconds / recordsIn, residual / recordsIn);
            subtasks.add(cost);
            totalRecords += recordsIn;
            busySecondsAtZeroMisses += residual;
            secondsAtFullMissRate += cost.getMissPenaltySecondsPerRecord() * recordsIn;
            double recordRate = recordsIn / (double) elapsedNanos;
            minRecordRate = Math.min(minRecordRate, recordRate);
            maxRecordRate = Math.max(maxRecordRate, recordRate);
            oldestStart = Math.min(oldestStart, before.getObservedAtMillis());
            oldestEnd = Math.min(oldestEnd, after.getObservedAtMillis());
        }
        if (monotonic && maxRecordRate > minRecordRate * 1.25) {
            return FitResult.rejected(Reason.RECORD_SKEW);
        }
        return new FitResult(
                Reason.QUALIFIED,
                new Fit(
                        new OnlineCacheCost(
                                busySecondsAtZeroMisses / totalRecords,
                                secondsAtFullMissRate / totalRecords),
                        List.copyOf(subtasks), identities, oldestStart, oldestEnd));
    }

    private static boolean sameCache(CacheCostSample before, CacheCostSample after, int subtask) {
        return before != null
                && after != null
                && before.getSubtask() == subtask
                && after.getSubtask() == subtask
                && before.getAttempt() != null
                && !before.getAttempt().isEmpty()
                && before.getAttempt().equals(after.getAttempt())
                && Objects.equals(before.getProcess(), after.getProcess())
                && before.getCacheId() > 0
                && before.getCacheId() == after.getCacheId()
                && before.getCapacityBytes() > 0
                && before.getCapacityBytes() == after.getCapacityBytes()
                && before.getCacheEpoch() == after.getCacheEpoch()
                && before.getTimingAbi() == after.getTimingAbi()
                && validCounters(before)
                && validCounters(after);
    }

    private static boolean validCounters(CacheCostSample sample) {
        int abi = sample.getTimingAbi();
        boolean validCompactionCounts =
                abi == COMPACTION_TIMING_ABI
                        ? sample.getCompactionHits() >= 0 && sample.getCompactionMisses() >= 0
                        : (abi == 0 || abi == 2)
                                && sample.getCompactionHits() == 0
                                && sample.getCompactionMisses() == 0;
        return validCompactionCounts
                && sample.getCacheEpoch() >= 0
                && sample.getLookups() >= sample.getMisses()
                && sample.getMisses() >= 0
                && sample.getCoveredHits() >= 0
                && sample.getCoveredMisses() >= 0
                && sample.getSampledHits() >= 0
                && sample.getSampledMisses() >= 0
                && sample.getHitNanos() >= 0
                && sample.getMissNanos() >= 0
                && sample.getRecordsIn() >= 0
                && Double.isFinite(sample.getBusyMillis())
                && sample.getBusyMillis() >= 0;
    }

    private static boolean usableCoverage(long covered, long total) {
        return total > 0
                && covered >= MIN_COVERAGE * total
                && covered <= MAX_COVERAGE * total;
    }

    private static boolean positiveFinite(double value) {
        return Double.isFinite(value) && value > 0;
    }
}
