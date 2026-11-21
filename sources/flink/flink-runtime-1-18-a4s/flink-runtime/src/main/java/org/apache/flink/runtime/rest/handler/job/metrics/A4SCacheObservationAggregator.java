/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservation.Status;
import org.apache.flink.a4s.core.CacheObservation.Window;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Validates current per-cache observations and projects an equal partition of total capacity. */
final class A4SCacheObservationAggregator {
    private static final long MIN_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(9);
    private static final long MAX_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_VERTEX_BUCKETS = 262_144;
    private static final double MAX_RATE_SKEW = 1.25;

    private A4SCacheObservationAggregator() {}

    static Result aggregate(
            String job,
            String vertex,
            Map<Integer, String> expected,
            List<CacheObservation> observations,
            long now) {
        if (observations.isEmpty()) {
            return new Result(Status.ABSENT);
        }
        Map<Integer, CacheObservation> bySubtask = new HashMap<>();
        Set<String> caches = new HashSet<>();
        long oldestEnd = Long.MAX_VALUE;
        long newestEnd = Long.MIN_VALUE;
        long earliestStart = Long.MAX_VALUE;
        long latestStart = Long.MIN_VALUE;
        double minRate = Double.POSITIVE_INFINITY;
        double maxRate = 0;
        double totalLookupRate = 0;
        int commonBins = Integer.MAX_VALUE;
        int totalBins = 0;
        long totalReferences = 0;
        long binBytes = 0;
        CacheObservation first = observations.get(0);
        List<Double> lookupRates = new ArrayList<>();
        List<Long> denominators = new ArrayList<>();
        try {
            for (CacheObservation observation : observations) {
                if (!job.equals(observation.job)
                        || !vertex.equals(observation.vertex)
                        || !observation.attempt.equals(expected.get(observation.subtask))) {
                    return new Result(Status.ATTEMPT_MISMATCH);
                }
                if (bySubtask.putIfAbsent(observation.subtask, observation) != null) {
                    return new Result(Status.DUPLICATE);
                }
                Status status = validate(observation, now);
                if (status != Status.VALID) {
                    return new Result(status);
                }
                Window window = observation.window;
                StackDistanceHistogram histogram = observation.histogram;
                if (!caches.add(window.process + "/" + window.cacheId)) {
                    return new Result(Status.DUPLICATE);
                }
                if (!compatible(first, observation)) {
                    return new Result(Status.INCOMPATIBLE);
                }
                oldestEnd = Math.min(oldestEnd, window.observedAtMillis);
                newestEnd = Math.max(newestEnd, window.observedAtMillis);
                long startMillis =
                        window.observedAtMillis - TimeUnit.NANOSECONDS.toMillis(window.durationNanos());
                earliestStart = Math.min(earliestStart, startMillis);
                latestStart = Math.max(latestStart, startMillis);
                double rate = (double) window.references / window.durationNanos();
                minRate = Math.min(minRate, rate);
                maxRate = Math.max(maxRate, rate);
                lookupRates.add(rate);
                totalLookupRate += rate;
                totalBins = Math.addExact(totalBins, histogram.getNumBuckets());
                if (totalBins > MAX_VERTEX_BUCKETS) {
                    return new Result(Status.OVERSIZE);
                }
                commonBins = Math.min(commonBins, histogram.getNumBuckets());
                binBytes =
                        Math.multiplyExact(
                                histogram.getBucketSizeScaling(),
                                histogram.getCacheItemSizeBytes());
                // Still reject overflow of the batch's exact reference count.
                totalReferences = Math.addExact(totalReferences, window.references);
                denominators.add(sampledReferences(histogram));
            }
            if (!bySubtask.keySet().equals(expected.keySet())) {
                return new Result(Status.INCOMPLETE);
            }
            if (newestEnd - oldestEnd > CacheObservation.MAX_CLOCK_SKEW_MILLIS
                    || latestStart - earliestStart > CacheObservation.MAX_CLOCK_SKEW_MILLIS) {
                return new Result(Status.SKEWED);
            }
            if (maxRate > MAX_RATE_SKEW * minRate) {
                return new Result(Status.SKEWED);
            }
            long[] cumulativeHits = new long[observations.size()];
            List<MissRateCurve.Point> points = new ArrayList<>(commonBins);
            for (int bin = 0; bin < commonBins; bin++) {
                double weightedMisses = 0;
                for (int index = 0; index < observations.size(); index++) {
                    CacheObservation observation = observations.get(index);
                    cumulativeHits[index] =
                            Math.addExact(cumulativeHits[index], observation.histogram.getFrequency(bin));
                    double localMissRate = 1 - (double) cumulativeHits[index] / denominators.get(index);
                    weightedMisses += lookupRates.get(index) * localMissRate;
                }
                long localBytes = Math.multiplyExact(bin + 1L, binBytes);
                long totalBytes = Math.multiplyExact(localBytes, expected.size());
                points.add(new MissRateCurve.Point(totalBytes, weightedMisses / totalLookupRate));
            }
            return new Result(
                    Status.VALID, new MissRateCurve(points, MissRateCurve.CapacityAxis.NOMINAL_ROUNDED_4K));
        } catch (ArithmeticException overflow) {
            return new Result(Status.MALFORMED);
        }
    }

    private static Status validate(CacheObservation observation, long now) {
        if (!CacheObservation.supportsVersion(observation.version)) {
            return Status.UNSUPPORTED_VERSION;
        }
        if (observation.status != Status.VALID) {
            return observation.status;
        }
        Window window = observation.window;
        StackDistanceHistogram histogram = observation.histogram;
        if (window == null
                || histogram == null
                || window.process.isEmpty()
                || window.cacheId <= 0
                || window.epoch < 0
                || window.sequence <= 0
                || window.capacityBytes <= 0
                || window.references <= 0
                || window.misses < 0
                || window.misses > window.references
                || !(window.samplingRate > 0 && window.samplingRate <= 1)
                || histogram.getNumPartitions() != 1
                || histogram.getNumBuckets() == 0) {
            return Status.MALFORMED;
        }
        if (window.owners.size() != 1 || window.owners.get(0) != 1L) {
            return Status.SHARED_CACHE;
        }
        if (!CacheObservation.isFresh(window.observedAtMillis, now)) {
            return Status.STALE;
        }
        if (window.durationNanos() < MIN_WINDOW_NANOS || window.durationNanos() > MAX_WINDOW_NANOS
                || !CacheObservation.CACHE_POLICY.equals(window.cachePolicy)) {
            return Status.INCOMPATIBLE;
        }
        if (!CacheObservation.QUICK_MRC.equals(window.population)
                || !CacheObservation.isNominalQuickMrcCharge(window.chargePolicy)
                || histogram.getCacheItemSizeBytes() != 4096) {
            return Status.INCOMPATIBLE;
        }
        return Status.VALID;
    }

    private static boolean compatible(CacheObservation first, CacheObservation other) {
        return first.window.population.equals(other.window.population)
                && first.window.chargePolicy.equals(other.window.chargePolicy)
                && first.window.capacityBytes == other.window.capacityBytes
                && first.window.samplingRate == other.window.samplingRate
                && first.histogram.getBucketSizeScaling() == other.histogram.getBucketSizeScaling()
                && first.histogram.getCacheItemSizeBytes() == other.histogram.getCacheItemSizeBytes();
    }

    private static long sampledReferences(StackDistanceHistogram histogram) {
        long count = histogram.getCompleteMisses();
        for (int i = 0; i < histogram.getNumBuckets(); i++) {
            count = Math.addExact(count, histogram.getFrequency(i));
        }
        if (count == 0) {
            throw new ArithmeticException("Histogram without sampled references");
        }
        return count;
    }

    static final class Result {
        final Status status;
        final MissRateCurve curve;

        Result(Status status) {
            this(status, new MissRateCurve(Collections.emptyList()));
        }

        Result(Status status, MissRateCurve curve) {
            this.status = status;
            this.curve = curve;
        }
    }
}
