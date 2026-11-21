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
import org.apache.flink.a4s.core.CacheObservation.Status;
import org.apache.flink.a4s.core.CacheObservationBatch;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.With;

import javax.annotation.Nullable;

import java.time.Instant;
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
    public static final int COMPACTION_TIMING_ABI = 3;
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
        INSUFFICIENT_HISTORY,
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
        INSUFFICIENT_CURRENT_PARALLELISM,
        OVERLOAD_LIVE_PROFILE_UNVERIFIED,
        OVERLOAD_RESOURCE_QUOTA,
        OVERLOAD_SLOT_METRICS_UNAVAILABLE,
        OVERLOAD_WORKER_SNAPSHOT_UNAVAILABLE,
        OVERLOAD_RESOURCE_GEOMETRY_UNSUPPORTED,
        OVERLOAD_CLUSTER_RESOURCES_UNAVAILABLE,
        MEMORY_POLICY_UNVERIFIED,
        MEMORY_FLOOR_INVALID,
        MEMORY_OVERRIDE_BELOW_FLOOR,
        MEMORY_BUDGET_INFEASIBLE,
        FINAL_PARALLELISM_UNSUPPORTED,
        FINAL_PROFILE_UNVERIFIED,
        FINAL_RESOURCE_LIMIT,
        FINAL_CACHE_CAPACITY_UNSUPPORTED
    }

    @Value
    @With
    @AllArgsConstructor
    public static class FitResult {
        Reason reason;
        @Nullable Fit fit;
        @Nullable Status observationStatus;
        @Nullable Integer failingSubtask;
        @Nullable TimingCounts timingCounts;
        @Nullable FitWindow attemptedWindow;
        @Nullable Holdout holdout;
        @Nullable CacheObservationBatch alignedCurve;

        public FitResult(Reason reason, @Nullable Fit fit) {
            this(reason, fit, null, null, null, null, null, null);
        }

        public static FitResult rejected(Reason reason) {
            return new FitResult(reason, null);
        }
    }

    @Value
    public static class FitWindow {
        Instant start;
        Instant end;
    }

    @Value
    public static class Holdout {
        @Nullable FitWindow originalFitWindow;
        @Nullable Long fitEndMillis;
        @Nullable Double maxActualMissRelativeError;
        @Nullable Boolean stableAccessMix;
        @Nullable Reason priorRejection;
        @Nullable Status priorObservationStatus;
    }

    @Value
    public static class TimingCounts {
        @Nullable Long expectedForegroundHits;
        @Nullable Long expectedForegroundMisses;
        long coveredHits;
        long coveredMisses;
        long sampledHits;
        long sampledMisses;

        @Nullable
        public Double getHitCoverage() {
            return expectedForegroundHits != null && expectedForegroundHits > 0
                    ? coveredHits / (double) expectedForegroundHits : null;
        }

        @Nullable
        public Double getMissCoverage() {
            return expectedForegroundMisses != null && expectedForegroundMisses > 0
                    ? coveredMisses / (double) expectedForegroundMisses
                    : null;
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
            if (holdoutContextStatus(later) != Status.VALID) {
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

        public Status holdoutContextStatus(Fit later) {
            if (subtasks.size() != later.subtasks.size()
                    || identities.size() != later.identities.size()) {
                return Status.INCOMPLETE;
            }
            if (windowEndMillis != later.windowStartMillis) {
                return Status.SKEWED;
            }
            for (int i = 0; i < identities.size(); i++) {
                FitIdentity before = identities.get(i);
                FitIdentity after = later.identities.get(i);
                if (!before.attempt.equals(after.attempt)) {
                    return Status.ATTEMPT_MISMATCH;
                }
                if (!before.process.equals(after.process)
                        || before.cacheId != after.cacheId
                        || before.epoch != after.epoch
                        || before.capacityBytes != after.capacityBytes
                        || before.samplingRate != after.samplingRate) {
                    return Status.CACHE_CHANGED;
                }
                if (before.endSequence != after.startSequence) {
                    return Status.INCOMPATIBLE;
                }
            }
            return Status.VALID;
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
        Status startStatus = observationStatus(startObservation, start.size());
        Status endStatus = observationStatus(endObservation, end.size());
        if (!startStatus.supportsLocalObservations() || !endStatus.supportsLocalObservations()) {
            Status status = startStatus.supportsLocalObservations() ? endStatus : startStatus;
            return FitResult.rejected(
                            status == Status.COUNTER_RESET
                                    ? Reason.COUNTER_RESET : Reason.MISSING_OBSERVATIONS)
                    .withObservationStatus(status);
        }
        List<FitIdentity> identities = new ArrayList<>(end.size());
        for (int i = 0; i < end.size(); i++) {
            CacheObservation before = startObservation.subtasks.get(i);
            CacheObservation after = endObservation.subtasks.get(i);
            Status beforeBinding = bindingStatus(before, start.get(i));
            Status afterBinding = bindingStatus(after, end.get(i));
            if (beforeBinding != Status.VALID || afterBinding != Status.VALID) {
                return FitResult.rejected(Reason.UNBOUND_TIMING)
                        .withObservationStatus(
                                beforeBinding != Status.VALID ? beforeBinding : afterBinding)
                        .withFailingSubtask(i);
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
                return FitResult.rejected(Reason.CONTEXT_CHANGED)
                        .withObservationStatus(Status.CACHE_CHANGED)
                        .withFailingSubtask(i);
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

    public static Status observationStatus(@Nullable CacheObservationBatch batch, int size) {
        if (batch == null) {
            return Status.ABSENT;
        }
        if (batch.version != CacheObservation.VERSION) {
            return Status.UNSUPPORTED_VERSION;
        }
        if (batch.status.supportsLocalObservations()
                && (batch.expectedSubtasks != size || batch.subtasks.size() != size)) {
            return Status.INCOMPLETE;
        }
        if (batch.status == Status.AGGREGATE_RATE_SKEW && !batch.hasPairedLocalObservations()) {
            return Status.INCOMPLETE;
        }
        return batch.status;
    }

    private static Status bindingStatus(
            CacheObservation observation, @Nullable CacheCostSample sample) {
        CacheObservation.Window window = observation.window;
        if (!CacheObservation.supportsVersion(observation.version)) {
            return Status.UNSUPPORTED_VERSION;
        }
        if (observation.status != Status.VALID) {
            return observation.status;
        }
        if (sample == null || window == null) {
            return Status.INCOMPLETE;
        }
        if (!window.owners.equals(List.of(1L))) {
            return Status.SHARED_CACHE;
        }
        if (sample.getTimingAbi() != COMPACTION_TIMING_ABI) {
            return Status.UNSUPPORTED_NATIVE;
        }
        if (observation.subtask != sample.getSubtask()
                || !observation.attempt.equals(sample.getAttempt())) {
            return Status.ATTEMPT_MISMATCH;
        }
        if (!window.process.equals(sample.getProcess())
                || window.cacheId != sample.getCacheId()
                || window.capacityBytes != sample.getCapacityBytes()
                || window.epoch != sample.getCacheEpoch()) {
            return Status.CACHE_CHANGED;
        }
        if (!CacheObservation.QUICK_MRC.equals(window.population)
                || !CacheObservation.CACHE_POLICY.equals(window.cachePolicy)
                || (!CacheObservation.isNominalQuickMrcCharge(window.chargePolicy)
                        && !CacheObservation.ATTESTED_BYTE_CHARGE.equals(window.chargePolicy))) {
            return Status.INCOMPATIBLE;
        }
        if (sample.getObservedAtNanos() == null) {
            return Status.INCOMPLETE;
        }
        return Math.abs((double) (sample.getObservedAtNanos() - window.endNanos))
                        <= TimeUnit.SECONDS.toNanos(10)
                ? Status.VALID : Status.SKEWED;
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
        return findDiscontinuity(before, after).isPresent();
    }

    /** Reports the first incompatible subtask in a pair of timing observations. */
    public static Optional<FitResult> findDiscontinuity(
            List<CacheCostSample> before, List<CacheCostSample> after) {
        if (before.size() != after.size()) {
            return Optional.of(FitResult.rejected(Reason.MISSING_TIMING));
        }
        for (int subtask = 0; subtask < after.size(); subtask++) {
            CacheCostSample left = before.get(subtask);
            CacheCostSample right = after.get(subtask);
            var rejection = incompatibleSamples(left, right, subtask);
            if (rejection != null) {
                return Optional.of(rejection);
            }
            if (countersDecreased(left, right)) {
                return Optional.of(FitResult.rejected(Reason.COUNTER_RESET)
                        .withFailingSubtask(subtask));
            }
        }
        return Optional.empty();
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
        long oldestStart = Long.MAX_VALUE;
        long oldestEnd = Long.MAX_VALUE;
        for (int subtask = 0; subtask < end.size(); subtask++) {
            CacheCostSample before = start.get(subtask);
            CacheCostSample after = end.get(subtask);
            var incompatible = incompatibleSamples(before, after, subtask);
            if (incompatible != null) {
                return incompatible;
            }
            long elapsedNanos = monotonic
                    ? after.getObservedAtNanos() - before.getObservedAtNanos()
                    : TimeUnit.MILLISECONDS.toNanos(after.getObservedAtMillis() - before.getObservedAtMillis());
            if (elapsedNanos < TimeUnit.SECONDS.toNanos(MIN_WINDOW_SECONDS)
                    || elapsedNanos > TimeUnit.SECONDS.toNanos(MAX_WINDOW_SECONDS)
                    || before.getObservedAtMillis() <= 0
                    || after.getObservedAtMillis() <= before.getObservedAtMillis()) {
                return FitResult.rejected(Reason.WINDOW_DURATION).withFailingSubtask(subtask);
            }

            if (countersDecreased(before, after)) {
                return FitResult.rejected(Reason.COUNTER_RESET).withFailingSubtask(subtask);
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
                return rejectedTiming(Reason.TIMING_COVERAGE, subtask, null, null,
                        coveredHits, coveredMisses, sampledHits, sampledMisses);
            }
            if (lookups == 0) {
                return FitResult.rejected(Reason.NO_HIT_OR_MISS)
                        .withObservationStatus(Status.IDLE).withFailingSubtask(subtask);
            }
            long foregroundHits = lookups - misses - compactionHits;
            long foregroundMisses = misses - compactionMisses;
            if (foregroundHits == 0 || foregroundMisses == 0) {
                return rejectedTiming(Reason.NO_HIT_OR_MISS, subtask, foregroundHits,
                        foregroundMisses, coveredHits, coveredMisses, sampledHits, sampledMisses);
            }
            if (!usableCoverage(coveredHits, foregroundHits)
                    || !usableCoverage(coveredMisses, foregroundMisses)
                    || sampledHits > coveredHits || sampledMisses > coveredMisses) {
                return rejectedTiming(Reason.TIMING_COVERAGE, subtask, foregroundHits,
                        foregroundMisses, coveredHits, coveredMisses, sampledHits, sampledMisses);
            }
            if (recordsIn == 0 || sampledHits < MIN_TIMED_LOOKUPS_PER_CLASS
                    || sampledMisses < MIN_TIMED_LOOKUPS_PER_CLASS) {
                return rejectedTiming(Reason.TOO_FEW_SAMPLES, subtask, foregroundHits,
                        foregroundMisses, coveredHits, coveredMisses, sampledHits, sampledMisses);
            }

            double meanHit = hitNanos / (double) sampledHits / 1e9;
            double meanMiss = missNanos / (double) sampledMisses / 1e9;
            double extraSecondsPerMiss = meanMiss - meanHit;
            if (hitNanos < sampledHits || missNanos < sampledMisses
                    || !positiveFinite(extraSecondsPerMiss)) {
                return rejectedTiming(Reason.NON_POSITIVE_PENALTY, subtask, foregroundHits,
                        foregroundMisses, coveredHits, coveredMisses, sampledHits, sampledMisses);
            }
            double residual = busySeconds - extraSecondsPerMiss * foregroundMisses;
            if (!positiveFinite(residual)) {
                return rejectedTiming(Reason.NON_POSITIVE_BASELINE, subtask, foregroundHits,
                        foregroundMisses, coveredHits, coveredMisses, sampledHits, sampledMisses);
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
            oldestStart = Math.min(oldestStart, before.getObservedAtMillis());
            oldestEnd = Math.min(oldestEnd, after.getObservedAtMillis());
        }
        return new FitResult(
                Reason.QUALIFIED,
                new Fit(
                        new OnlineCacheCost(
                                busySecondsAtZeroMisses / totalRecords,
                                secondsAtFullMissRate / totalRecords),
                        List.copyOf(subtasks), identities, oldestStart, oldestEnd));
    }

    private static FitResult rejectedTiming(
            Reason reason, int subtask, @Nullable Long foregroundHits, @Nullable Long foregroundMisses,
            long coveredHits, long coveredMisses, long sampledHits, long sampledMisses) {
        return FitResult.rejected(reason).withFailingSubtask(subtask)
                .withTimingCounts(new TimingCounts(foregroundHits, foregroundMisses,
                        coveredHits, coveredMisses, sampledHits, sampledMisses));
    }

    @Nullable
    private static FitResult incompatibleSamples(
            @Nullable CacheCostSample before, @Nullable CacheCostSample after, int subtask) {
        Status status;
        if (before == null || after == null) {
            status = Status.INCOMPLETE;
        } else if (before.getSubtask() != subtask || after.getSubtask() != subtask
                || before.getAttempt() == null || before.getAttempt().isEmpty()
                || !before.getAttempt().equals(after.getAttempt())) {
            status = Status.ATTEMPT_MISMATCH;
        } else if (!Objects.equals(before.getProcess(), after.getProcess())
                || before.getCacheId() <= 0 || before.getCacheId() != after.getCacheId()
                || before.getCapacityBytes() <= 0 || before.getCapacityBytes() != after.getCapacityBytes()
                || before.getCacheEpoch() != after.getCacheEpoch()) {
            status = Status.CACHE_CHANGED;
        } else if (before.getTimingAbi() != after.getTimingAbi()) {
            status = Status.UNSUPPORTED_NATIVE;
        } else if (!validCounters(before) || !validCounters(after)) {
            status = Status.INCOMPATIBLE;
        } else {
            return null;
        }
        return FitResult.rejected(Reason.CONTEXT_CHANGED).withObservationStatus(status)
                .withFailingSubtask(subtask);
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
