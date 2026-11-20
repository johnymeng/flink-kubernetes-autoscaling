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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservation.Status;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.runtime.metrics.A4SHistogramMetricCodec;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Publishes the legacy QuickMRC stream once per cache interval. */
final class A4SCacheTelemetry {
    private static final Logger LOG = LoggerFactory.getLogger(A4SCacheTelemetry.class);
    private static final Map<Cache, A4SCacheTelemetry> LIVE = new IdentityHashMap<>();
    static final String PROCESS = UUID.randomUUID().toString();
    private static final long INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final int MAX_BUCKETS = 65_536;
    private static final Method LOOKUP_SNAPSHOT = lookupSnapshotMethod();

    @Nullable private final Supplier<long[]> readNative;
    private final Supplier<long[]> drainHistogram;
    private final double sampleRate;
    private final int binItems;
    private final A4SCacheSnapshots<Interval> snapshots = new A4SCacheSnapshots<>(INTERVAL_NANOS);

    @Nullable private long[] previous;

    A4SCacheTelemetry(
            @Nullable Supplier<long[]> readNative,
            Supplier<long[]> drainHistogram,
            double sampleRate,
            int binItems) {
        this.readNative = readNative;
        this.drainHistogram = drainHistogram;
        this.sampleRate = sampleRate;
        this.binItems = binItems;
    }

    static synchronized void register(Cache cache, double sampleRate, int binItems) {
        if (cache instanceof LRUCache && sampleRate > 0 && binItems > 0) {
            LRUCache lru = (LRUCache) cache;
            LIVE.put(
                    cache,
                    new A4SCacheTelemetry(
                            LOOKUP_SNAPSHOT == null ? null : () -> lookupSnapshot(lru),
                            () -> {
                                long[] counts = lru.getStackDistanceHistogram();
                                lru.resetQuickMRCStats();
                                return counts;
                            },
                            sampleRate,
                            binItems));
        }
    }

    @Nullable
    static synchronized A4SCacheTelemetry find(Cache cache) {
        return LIVE.get(cache);
    }

    static synchronized void release(Cache cache) {
        A4SCacheTelemetry telemetry = LIVE.remove(cache);
        if (telemetry != null) {
            telemetry.snapshots.close();
        }
    }

    long attach() {
        return snapshots.attach();
    }

    void detach(long owner) {
        snapshots.detach(owner);
    }

    boolean hasExclusiveOwner() {
        return snapshots.hasExclusiveOwner();
    }

    String metric(String job, String vertex, int subtask, String attempt) {
        return metric(job, vertex, subtask, attempt, System.nanoTime());
    }

    String metric(String job, String vertex, int subtask, String attempt, long nowNanos) {
        A4SCacheSnapshots.Publication<Interval> publication =
                snapshots.poll(nowNanos, this::readInterval);
        Interval interval = publication.value;
        Status status = !publication.exclusive ? Status.SHARED_CACHE : interval.status;
        CacheObservation.Window window = null;
        if (interval != null && interval.after != null) {
            long[] nativeState = interval.after;
            window = new CacheObservation.Window(
                    PROCESS,
                    nativeState[1],
                    nativeState[3],
                    publication.sequence,
                    publication.owners,
                    CacheObservation.QUICK_MRC,
                    CacheObservation.FIXED_4K,
                    CacheObservation.CACHE_POLICY,
                    sampleRate,
                    nativeState[2],
                    publication.startNanos,
                    publication.endNanos,
                    interval.wallTime,
                    interval.references,
                    interval.misses);
        }
        return A4SHistogramMetricCodec.encode(
                new CacheObservation(
                        CacheObservation.VERSION,
                        job,
                        vertex,
                        subtask,
                        attempt,
                        status,
                        window,
                        status == Status.VALID ? interval.histogram : null)
                        .toMetricString());
    }

    private Interval readInterval() {
        if (readNative == null) {
            return new Interval(Status.UNSUPPORTED_NATIVE);
        }
        try {
            long[] before = readNative.get();
            if (!valid(before)) {
                previous = null;
                return new Interval(Status.UNSUPPORTED_NATIVE);
            }
            long[] counts = drainHistogram.get();
            long[] after = readNative.get();
            long[] baseline = previous;
            previous = valid(after) ? after : null;
            if (previous == null || !sameGeneration(before, after)) {
                return new Interval(Status.CACHE_CHANGED);
            }
            if (baseline == null) {
                return new Interval(Status.WARMUP, after, 0, 0, null);
            }
            if (!sameGeneration(baseline, before)) {
                return new Interval(Status.CACHE_CHANGED, after, 0, 0, null);
            }
            long references = before[5] - baseline[5];
            long misses = before[6] - baseline[6];
            if (references < 0
                    || misses < 0
                    || misses > references
                    || after[5] < before[5]
                    || after[6] < before[6]) {
                return new Interval(Status.COUNTER_RESET, after, 0, 0, null);
            }
            // The old native ABI exposes separate calls for counters, histogram and reset.
            // Reject intervals whose publication boundary lost a material number of references.
            if (after[5] - before[5] > references / 50) {
                return new Interval(Status.INCOMPATIBLE, after, references, misses, null);
            }
            if (references == 0) {
                return new Interval(Status.IDLE, after, 0, 0, null);
            }
            if (counts == null || counts.length < 2) {
                return new Interval(Status.WARMUP, after, references, misses, null);
            }
            if (counts.length > MAX_BUCKETS + 1) {
                return new Interval(Status.OVERSIZE, after, references, misses, null);
            }
            StackDistanceHistogram histogram =
                    new StackDistanceHistogram(
                            Arrays.copyOf(counts, counts.length - 1),
                            counts[counts.length - 1],
                            1,
                            binItems,
                            4096);
            return new Interval(Status.VALID, after, references, misses, histogram);
        } catch (RuntimeException | LinkageError error) {
            previous = null;
            LOG.debug("Could not read cache telemetry", error);
            return new Interval(Status.MALFORMED);
        }
    }

    private static long[] lookupSnapshot(LRUCache cache) {
        try {
            return (long[]) LOOKUP_SNAPSHOT.invoke(cache);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Cannot read the native cache snapshot", error);
        }
    }

    @Nullable
    private static Method lookupSnapshotMethod() {
        try {
            return LRUCache.class.getMethod("getA4SCacheLookupSnapshot");
        } catch (NoSuchMethodException | LinkageError unavailable) {
            return null;
        }
    }

    private static boolean valid(@Nullable long[] sample) {
        return sample != null
                && sample.length == 7
                && sample[0] == 1
                && sample[1] > 0
                && sample[2] > 0
                && sample[3] >= 0
                && sample[4] == 1
                && sample[5] >= sample[6]
                && sample[6] >= 0;
    }

    private static boolean sameGeneration(long[] first, long[] second) {
        return first[1] == second[1] && first[2] == second[2] && first[3] == second[3];
    }

    private static final class Interval {
        private final Status status;
        @Nullable private final long[] after;
        private final long references;
        private final long misses;
        @Nullable private final StackDistanceHistogram histogram;
        private final long wallTime = System.currentTimeMillis();

        private Interval(Status status) {
            this(status, null, 0, 0, null);
        }

        private Interval(
                Status status,
                @Nullable long[] after,
                long references,
                long misses,
                @Nullable StackDistanceHistogram histogram) {
            this.status = status;
            this.after = after;
            this.references = references;
            this.misses = misses;
            this.histogram = histogram;
        }
    }
}
