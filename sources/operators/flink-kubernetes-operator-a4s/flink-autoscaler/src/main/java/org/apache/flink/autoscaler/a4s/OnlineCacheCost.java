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

import lombok.Value;

import java.util.List;
import java.util.Optional;

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

    /**
     * Both lists are ordered by subtask and contain all subtasks of one vertex. A single
     * unqualified subtask invalidates the estimate, since the MRC models the entire vertex.
     */
    public static Optional<OnlineCacheCost> from(
            List<CacheCostSample> start, List<CacheCostSample> end) {
        if (start.isEmpty() || start.size() != end.size()) {
            return Optional.empty();
        }

        double records = 0;
        double busySecondsAtZeroMisses = 0;
        double secondsAtFullMissRate = 0;
        for (int subtask = 0; subtask < end.size(); subtask++) {
            CacheCostSample before = start.get(subtask);
            CacheCostSample after = end.get(subtask);
            if (!sameCache(before, after, subtask) || !measuredWindow(before, after)) {
                return Optional.empty();
            }

            long recordsIn = after.getRecordsIn() - before.getRecordsIn();
            double busySeconds = (after.getBusyMillis() - before.getBusyMillis()) / 1000;
            long lookups = after.getLookups() - before.getLookups();
            long misses = after.getMisses() - before.getMisses();
            if (misses <= 0 || misses >= lookups) {
                return Optional.empty();
            }
            long hits = lookups - misses;
            long compactionHits = after.getCompactionHits() - before.getCompactionHits();
            long compactionMisses = after.getCompactionMisses() - before.getCompactionMisses();
            if (compactionHits < 0
                    || compactionMisses < 0
                    || compactionHits > hits
                    || compactionMisses > misses) {
                return Optional.empty();
            }
            long foregroundHits = hits - compactionHits;
            long foregroundMisses = misses - compactionMisses;
            long coveredHits = after.getCoveredHits() - before.getCoveredHits();
            long coveredMisses = after.getCoveredMisses() - before.getCoveredMisses();
            long sampledHits = after.getSampledHits() - before.getSampledHits();
            long sampledMisses = after.getSampledMisses() - before.getSampledMisses();
            long hitNanos = after.getHitNanos() - before.getHitNanos();
            long missNanos = after.getMissNanos() - before.getMissNanos();

            if (recordsIn <= 0
                    || !positiveFinite(busySeconds)
                    || !usableCoverage(coveredHits, foregroundHits)
                    || !usableCoverage(coveredMisses, foregroundMisses)
                    || sampledHits < MIN_TIMED_LOOKUPS_PER_CLASS
                    || sampledMisses < MIN_TIMED_LOOKUPS_PER_CLASS
                    || sampledHits > coveredHits
                    || sampledMisses > coveredMisses
                    || hitNanos <= 0
                    || missNanos <= 0) {
                return Optional.empty();
            }

            double extraSecondsPerMiss =
                    (missNanos / (double) sampledMisses - hitNanos / (double) sampledHits) / 1e9;
            if (!positiveFinite(extraSecondsPerMiss)) {
                return Optional.empty();
            }

            records += recordsIn;
            busySecondsAtZeroMisses += busySeconds - extraSecondsPerMiss * foregroundMisses;

            // QuickMRC predicts the fraction over all cache lookups. Estimate the cost
            // of that fraction using the observed foreground share of exact misses.
            double foregroundShare = foregroundMisses / (double) misses;
            secondsAtFullMissRate += extraSecondsPerMiss * foregroundShare * lookups;
        }

        double intercept = busySecondsAtZeroMisses / records;
        double slope = secondsAtFullMissRate / records;
        return positiveFinite(intercept) && positiveFinite(slope)
                ? Optional.of(new OnlineCacheCost(intercept, slope))
                : Optional.empty();
    }

    private static boolean sameCache(CacheCostSample before, CacheCostSample after, int subtask) {
        return before != null
                && after != null
                && before.getSubtask() == subtask
                && after.getSubtask() == subtask
                && before.getAttempt() != null
                && !before.getAttempt().isEmpty()
                && before.getAttempt().equals(after.getAttempt())
                && before.getCacheId() > 0
                && before.getCacheId() == after.getCacheId()
                && before.getCapacityBytes() > 0
                && before.getCapacityBytes() == after.getCapacityBytes()
                && before.getCacheEpoch() == after.getCacheEpoch()
                && before.getTimingAbi() == after.getTimingAbi()
                && validCounters(before)
                && validCounters(after);
    }

    private static boolean measuredWindow(CacheCostSample before, CacheCostSample after) {
        long elapsedMillis = after.getObservedAtMillis() - before.getObservedAtMillis();
        return before.getObservedAtMillis() > 0
                && after.getObservedAtMillis() >= before.getObservedAtMillis()
                && elapsedMillis >= MIN_WINDOW_SECONDS * 1000
                && elapsedMillis <= MAX_WINDOW_SECONDS * 1000;
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
