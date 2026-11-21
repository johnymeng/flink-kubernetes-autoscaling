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
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SCacheCostSample;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Build an interval from the same cache endpoints as the timing fit. */
public final class A4SAlignedCacheWindow {
    private A4SAlignedCacheWindow() {}

    public static Result between(
            CacheObservationBatch before, List<CacheCostSample> start,
            CacheObservationBatch after, List<CacheCostSample> end) {
        int expected = after.expectedSubtasks;
        if (expected <= 0 || before.expectedSubtasks != expected
                || before.subtasks.size() != expected || after.subtasks.size() != expected
                || start.size() != expected || end.size() != expected) {
            return new Result(Status.INCOMPLETE, null);
        }
        var slices = new ArrayList<CacheObservation>(expected);
        var caches = new HashSet<List<Object>>();
        for (int i = 0; i < expected; i++) {
            var a = before.subtasks.get(i);
            var b = after.subtasks.get(i);
            if (a.version != CacheObservation.ALIGNED_VERSION
                    || b.version != CacheObservation.ALIGNED_VERSION) {
                return new Result(Status.UNSUPPORTED_VERSION, null);
            }
            if (a.endpoint == null || b.endpoint == null || a.window == null || b.window == null) {
                return new Result(Status.INCOMPLETE, null);
            }
            var left = a.window;
            var right = b.window;
            if (a.subtask != i || b.subtask != i || !a.attempt.equals(b.attempt)) {
                return new Result(Status.ATTEMPT_MISMATCH, null);
            }
            if (!a.job.equals(b.job) || !a.vertex.equals(b.vertex)
                    || !left.process.equals(right.process) || left.cacheId != right.cacheId
                    || left.capacityBytes != right.capacityBytes || left.epoch != right.epoch
                    || a.endpoint.ownershipGeneration != b.endpoint.ownershipGeneration) {
                return new Result(Status.CACHE_CHANGED, null);
            }
            if (!left.owners.equals(List.of(1L)) || !right.owners.equals(List.of(1L))) {
                return new Result(Status.SHARED_CACHE, null);
            }
            if (!caches.add(List.of(right.process, right.cacheId)) || left.sequence == right.sequence) {
                return new Result(Status.DUPLICATE, null);
            }
            if (left.sequence > right.sequence
                    || a.endpoint.nativeNanos >= b.endpoint.nativeNanos
                    || a.endpoint.totalLookups > b.endpoint.totalLookups
                    || a.endpoint.totalMisses > b.endpoint.totalMisses) {
                return new Result(Status.COUNTER_RESET, null);
            }
            if (a.endpoint.totalLookups == b.endpoint.totalLookups) {
                return new Result(Status.IDLE, null);
            }
            if (!a.endpoint.withinAcquisitionBound() || !b.endpoint.withinAcquisitionBound()) {
                return new Result(Status.SKEWED, null);
            }
            if (!matches(a, start.get(i)) || !matches(b, end.get(i))
                    || left.samplingRate != right.samplingRate
                    || !left.chargePolicy.equals(right.chargePolicy)
                    || !left.cachePolicy.equals(right.cachePolicy)
                    || !left.population.equals(right.population)) {
                return new Result(Status.INCOMPATIBLE, null);
            }
            long references = b.endpoint.totalLookups - a.endpoint.totalLookups;
            long misses = b.endpoint.totalMisses - a.endpoint.totalMisses;
            var earliest = a.endpoint.cumulativeHistogram;
            var latest = b.endpoint.cumulativeHistogram;
            if (earliest.getBucketSizeScaling() != latest.getBucketSizeScaling()
                    || earliest.getCacheItemSizeBytes() != latest.getCacheItemSizeBytes()) {
                return new Result(Status.INCOMPATIBLE, null);
            }
            StackDistanceHistogram histogram;
            try {
                histogram = latest.deltaFrom(earliest);
            } catch (IllegalArgumentException reset) {
                return new Result(Status.COUNTER_RESET, null);
            }
            if (misses > references || histogram.getCompleteMisses() > misses) {
                return new Result(Status.INCOMPATIBLE, null);
            }
            if (histogram.getNumBuckets() == 0) {
                return new Result(Status.INCOMPLETE, null);
            }
            slices.add(new CacheObservation(
                    CacheObservation.ALIGNED_VERSION, b.job, b.vertex, b.subtask, b.attempt,
                    Status.VALID,
                    new CacheObservation.Window(
                            right.process, right.cacheId, right.epoch, right.sequence, right.owners,
                            right.population, right.chargePolicy, right.cachePolicy, right.samplingRate,
                            right.capacityBytes, a.endpoint.nativeNanos, b.endpoint.nativeNanos,
                            right.observedAtMillis, references, misses),
                    histogram));
        }
        return new Result(Status.VALID, new CacheObservationBatch(
                after.version, Status.VALID, expected, after.wireBytes, after.oldestAgeMillis, slices));
    }

    private static boolean matches(CacheObservation row, CacheCostSample timing) {
        var window = row.window;
        var endpoint = row.endpoint;
        A4SCacheCostSample captured;
        try {
            captured = A4SCacheCostSample.fromEndpoint(row, window.observedAtMillis);
        } catch (LinkageError incompatibleRuntime) {
            return false;
        }
        return captured != null && endpoint.ownershipGeneration > 0
                && timing.getTimingAbi() == OnlineCacheCost.COMPACTION_TIMING_ABI
                && timing.getSubtask() == row.subtask && row.attempt.equals(timing.getAttempt())
                && window.process.equals(timing.getProcess()) && window.cacheId == timing.getCacheId()
                && window.capacityBytes == timing.getCapacityBytes()
                && window.epoch == timing.getCacheEpoch()
                && window.observedAtMillis == timing.getObservedAtMillis()
                && Objects.equals(endpoint.nativeNanos, timing.getObservedAtNanos())
                && endpoint.totalLookups == timing.getLookups() && endpoint.totalMisses == timing.getMisses()
                && captured.coveredHits == timing.getCoveredHits()
                && captured.coveredMisses == timing.getCoveredMisses()
                && captured.sampledHits == timing.getSampledHits()
                && captured.sampledMisses == timing.getSampledMisses()
                && captured.hitNanos == timing.getHitNanos() && captured.missNanos == timing.getMissNanos()
                && captured.recordsIn == timing.getRecordsIn() && captured.busyMillis == timing.getBusyMillis()
                && Objects.equals(captured.compactionHits, timing.getCompactionHits())
                && Objects.equals(captured.compactionMisses, timing.getCompactionMisses())
                && endpoint.totalMisses >= 0 && endpoint.totalLookups >= endpoint.totalMisses
                && CacheObservation.QUICK_MRC.equals(window.population)
                && (CacheObservation.isNominalQuickMrcCharge(window.chargePolicy)
                        || CacheObservation.ATTESTED_BYTE_CHARGE.equals(window.chargePolicy))
                && CacheObservation.CACHE_POLICY.equals(window.cachePolicy)
                && endpoint.cumulativeHistogram.getCacheItemSizeBytes() == 4096;
    }

    public static final class Result {
        public final Status status;
        @Nullable public final CacheObservationBatch batch;

        private Result(Status status, @Nullable CacheObservationBatch batch) {
            this.status = status;
            this.batch = batch;
        }
    }
}
