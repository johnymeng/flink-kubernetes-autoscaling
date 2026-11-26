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

package io.a4s.core;

import io.a4s.api.CachePoint;
import io.a4s.api.StackDistanceHistogram;
import io.a4s.api.StackDistanceHistogram.Counters;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/** Byte-based MRC operations on measured support. */
public final class MissRateCurves {
    private MissRateCurves() {}

    public static StackDistanceHistogram merge(StackDistanceHistogram a, StackDistanceHistogram b) {
        if (!compatible(a, b)
                || a.counterKind != Counters.INTERVAL
                || b.counterKind != Counters.INTERVAL
                || a.startMillis != b.startMillis
                || a.endMillis != b.endMillis) {
            throw new IllegalArgumentException("Incompatible interval histograms");
        }
        // The caller must deduplicate physical cache samples before merging. Each input here
        // describes one partition, or a disjoint pool of partitions with the same scope.
        var frequencies = new ArrayList<Long>(a.frequencies.size());
        for (int i = 0; i < a.frequencies.size(); i++) {
            frequencies.add(Math.addExact(a.frequencies.get(i), b.frequencies.get(i)));
        }
        return new StackDistanceHistogram(
                a.estimator,
                Counters.INTERVAL,
                a.population,
                a.cacheScope,
                a.counterGeneration,
                a.sampling,
                a.startMillis,
                a.endMillis,
                Math.addExact(a.partitions, b.partitions),
                a.capacityBytesPerPartition,
                frequencies,
                Math.addExact(a.missesOutsideBuckets, b.missesOutsideBuckets));
    }

    public static StackDistanceHistogram delta(
            StackDistanceHistogram previous, StackDistanceHistogram current) {
        if (!compatible(previous, current)
                || previous.counterKind != Counters.CUMULATIVE
                || current.counterKind != Counters.CUMULATIVE
                || previous.partitions != current.partitions
                || previous.startMillis != current.startMillis
                || previous.endMillis >= current.endMillis) {
            throw new IllegalArgumentException("Incompatible cumulative snapshots");
        }
        var frequencies = new ArrayList<Long>(previous.frequencies.size());
        for (int i = 0; i < previous.frequencies.size(); i++) {
            frequencies.add(current.frequencies.get(i) - previous.frequencies.get(i));
        }
        return new StackDistanceHistogram(
                current.estimator,
                Counters.INTERVAL,
                current.population,
                current.cacheScope,
                current.counterGeneration,
                current.sampling,
                previous.endMillis,
                current.endMillis,
                current.partitions,
                current.capacityBytesPerPartition,
                frequencies,
                current.missesOutsideBuckets - previous.missesOutsideBuckets);
    }

    private static boolean compatible(StackDistanceHistogram a, StackDistanceHistogram b) {
        return a.estimator == b.estimator
                && a.population.equals(b.population)
                && a.cacheScope.equals(b.cacheScope)
                && a.counterGeneration.equals(b.counterGeneration)
                && a.sampling.equals(b.sampling)
                && a.capacityBytesPerPartition.equals(b.capacityBytesPerPartition);
    }

    public static List<CachePoint> fromHistogram(StackDistanceHistogram histogram) {
        if (histogram.counterKind != Counters.INTERVAL || histogram.frequencies.isEmpty()) {
            return List.of();
        }
        long total = histogram.missesOutsideBuckets;
        for (long frequency : histogram.frequencies) {
            total = Math.addExact(total, frequency);
        }
        if (total == 0) {
            return List.of();
        }
        var points = new ArrayList<CachePoint>(histogram.frequencies.size());
        long hits = 0;
        for (int i = 0; i < histogram.frequencies.size(); i++) {
            hits += histogram.frequencies.get(i);
            points.add(
                    new CachePoint(
                            Math.multiplyExact(
                                    histogram.capacityBytesPerPartition.get(i),
                                    histogram.partitions),
                            1 - (double) hits / total));
        }
        return List.copyOf(points);
    }

    public static OptionalLong supportedCapacity(List<CachePoint> points, double maximumMissRate) {
        int lower = 0;
        int upper = points.size();
        while (lower < upper) {
            int index = (lower + upper) >>> 1;
            if (points.get(index).missRate <= maximumMissRate) {
                upper = index;
            } else {
                lower = index + 1;
            }
        }
        return lower == points.size()
                ? OptionalLong.empty()
                : OptionalLong.of(points.get(lower).capacityBytes);
    }

    public static OptionalDouble interpolate(List<CachePoint> points, long capacityBytes) {
        if (points.isEmpty()
                || capacityBytes < points.get(0).capacityBytes
                || capacityBytes > points.get(points.size() - 1).capacityBytes) {
            return OptionalDouble.empty();
        }
        int low = 0;
        int high = points.size() - 1;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (points.get(middle).capacityBytes < capacityBytes) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        CachePoint right = points.get(low);
        if (right.capacityBytes == capacityBytes) {
            return OptionalDouble.of(right.missRate);
        }
        CachePoint left = points.get(low - 1);
        double fraction =
                (double) (capacityBytes - left.capacityBytes)
                        / (right.capacityBytes - left.capacityBytes);
        return OptionalDouble.of(left.missRate + fraction * (right.missRate - left.missRate));
    }
}
