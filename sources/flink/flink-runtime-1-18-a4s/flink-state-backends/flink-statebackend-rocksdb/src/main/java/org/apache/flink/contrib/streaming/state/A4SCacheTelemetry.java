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
import org.rocksdb.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** One publisher per process-local native cache ID. */
final class A4SCacheTelemetry {
    private static final Logger LOG = LoggerFactory.getLogger(A4SCacheTelemetry.class);
    private static final Registry LIVE = new Registry(UUID.randomUUID().toString());
    private static final long INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final int MAX_BUCKETS = 65_536;
    private static final int SNAPSHOT_HEADER = 10;
    private static final Method LOOKUP_SNAPSHOT = method(LRUCache.class, "getA4SCacheLookupSnapshot");
    private static final Method QUICK_MRC_SNAPSHOT = method(LRUCache.class, "getA4SQuickMrcSnapshot");
    private static final Method WRITE_BUFFER_BUDGET =
            method(WriteBufferManager.class, "getBufferSizeBytesV1");

    private final String process;
    private final long cacheId;
    private final double sampleRate;
    private final int binItems;
    private final A4SCacheSnapshots<Interval> snapshots = new A4SCacheSnapshots<>(INTERVAL_NANOS);
    private final Map<Source, Integer> sources = new LinkedHashMap<>();

    private boolean compatible = true;
    private boolean writeBufferCompatible = true;
    @Nullable private WriteBufferManager writeBufferManager;
    private boolean sampled;
    @Nullable private long[] previous;

    private A4SCacheTelemetry(String process, long cacheId, double sampleRate, int binItems) {
        this.process = process;
        this.cacheId = cacheId;
        this.sampleRate = sampleRate;
        this.binItems = binItems;
    }

    static void register(
            Cache cache, WriteBufferManager writeBufferManager, double sampleRate, int binItems) {
        if (sampleRate > 0 && binItems > 0) {
            Source source = nativeSource(cache);
            if (source != null) {
                LIVE.register(cache, source, sampleRate, binItems, writeBufferManager);
            }
        }
    }

    static void release(Cache cache) {
        LIVE.release(cache);
    }

    @Nullable
    static Lease attachBackend(@Nullable Cache cache) {
        return attach(cache, false);
    }

    @Nullable
    static Lease attachMonitor(@Nullable Cache cache) {
        return attach(cache, true);
    }

    @Nullable
    private static Lease attach(@Nullable Cache cache, boolean monitor) {
        Source source = nativeSource(cache);
        return source == null ? null : LIVE.attach(source, monitor);
    }

    @Nullable
    private static Source nativeSource(@Nullable Cache cache) {
        if (!(cache instanceof LRUCache) || LOOKUP_SNAPSHOT == null || QUICK_MRC_SNAPSHOT == null) {
            return null;
        }
        LRUCache lru = (LRUCache) cache;
        try {
            long[] identity = invoke(LOOKUP_SNAPSHOT, lru);
            return identity != null && identity.length == 7 && identity[0] == 1
                            && identity[1] > 0 && identity[4] == 1
                    ? new Source(identity[1], identity[5], () -> invoke(QUICK_MRC_SNAPSHOT, lru))
                    : null;
        } catch (RuntimeException | LinkageError unavailable) {
            LOG.debug("QuickMRC native cache identity is unavailable", unavailable);
            return null;
        }
    }

    private synchronized String metric(
            long monitor, String job, String vertex, int subtask, String attempt, long nowNanos,
            @Nullable TimingReader timing, @Nullable CacheObservation.AppliedSlot appliedSlot) {
        A4SCacheSnapshots.Publication<Interval> publication =
                snapshots.poll(monitor, nowNanos, () -> capture(monitor, timing, appliedSlot));
        Interval interval = publication.value;
        Status status = !compatible ? Status.INCOMPATIBLE
                : publication.ownershipChanged ? Status.SHARED_CACHE
                : publication.exclusive || interval.state == null
                        ? interval.status : Status.SHARED_CACHE;
        CacheObservation.Window window = null;
        // A lease transition invalidates eligibility immediately. Do not relabel the same
        // native sequence as a new interval with a different owner set.
        if (compatible && !publication.ownershipChanged && interval.state != null) {
            long[] state = interval.state;
            window = new CacheObservation.Window(
                    process,
                    cacheId,
                    state[3],
                    state[5],
                    publication.owners,
                    CacheObservation.QUICK_MRC,
                    CacheObservation.ROUNDED_4K_CHARGE,
                    CacheObservation.CACHE_POLICY,
                    sampleRate,
                    state[2],
                    publication.startNanos,
                    publication.endNanos,
                    interval.wallTime,
                    interval.references,
                    interval.misses);
        }
        CacheObservation.Endpoint endpoint =
                window != null && status == Status.VALID ? interval.endpoint : null;
        if (endpoint != null && !writeBufferCompatible) {
            endpoint = endpoint.withoutWriteBufferBudget();
        }
        String wire = A4SHistogramMetricCodec.encode(
                new CacheObservation(
                        endpoint == null ? CacheObservation.VERSION : CacheObservation.ALIGNED_VERSION,
                        job, vertex, subtask, attempt, status, window,
                        status == Status.VALID ? interval.histogram : null, endpoint).toMetricString());
        if (endpoint == null || !A4SHistogramMetricCodec.isOmitted(wire)) {
            return wire;
        }
        // Preserve a transportable interval for diagnostics and memory corroboration.
        // Consumers cannot qualify a learned fit without the paired endpoint.
        return A4SHistogramMetricCodec.encode(new CacheObservation(
                CacheObservation.VERSION, job, vertex, subtask, attempt, status, window,
                interval.histogram).toMetricString());
    }

    private Interval capture(
            long monitor, @Nullable TimingReader timing,
            @Nullable CacheObservation.AppliedSlot appliedSlot) {
        long start = System.nanoTime();
        Interval interval = readInterval();
        long nativeNanos = System.nanoTime();
        long ownerGeneration = snapshots.exclusiveGeneration(monitor);
        if (timing == null || ownerGeneration < 0 || interval.status != Status.VALID
                || interval.state.length - SNAPSHOT_HEADER > MAX_BUCKETS) {
            return interval;
        }
        long[] state = interval.state;
        CacheObservation.WriteBufferBudget writeBufferBudget = writeBufferCompatible
                ? readWriteBufferBudget(writeBufferManager) : null;
        String counters = timing.read(state, interval.wallTime, nativeNanos);
        long end = System.nanoTime();
        if (counters.isEmpty()) {
            return interval;
        }
        return interval.withEndpoint(new CacheObservation.Endpoint(
                    ownerGeneration, start, nativeNanos, end, state[6], state[7],
                    new StackDistanceHistogram(
                            Arrays.copyOfRange(state, SNAPSHOT_HEADER, state.length),
                            state[8], 1, binItems, 4096),
                    counters, appliedSlot, writeBufferBudget));
    }

    private Interval readInterval() {
        try {
            long[] state = null;
            for (Source source : sources.keySet()) {
                state = source.read.get();
                if (state != null) {
                    break;
                }
            }
            if (state == null) {
                previous = null;
                return new Interval(Status.UNSUPPORTED_NATIVE);
            }
            if (!valid(state)) {
                previous = null;
                return new Interval(Status.MALFORMED);
            }
            if (state[1] != cacheId) {
                previous = null;
                return new Interval(Status.CACHE_CHANGED);
            }
            // A surviving native alias may outlive all local registrations. A fresh registry
            // entry must not make a previously sampled physical cache exclusive again.
            if (!sampled) {
                if (state[5] > 1) {
                    snapshots.disqualifyPriorUse();
                }
                sampled = true;
            }
            long[] baseline = previous;
            previous = state.clone();
            if (!compatible) {
                return new Interval(Status.INCOMPATIBLE, previous, 0, 0, null);
            }
            if (baseline == null) {
                return new Interval(Status.WARMUP, previous, 0, 0, null);
            }
            if (baseline[2] != state[2] || baseline[3] != state[3]) {
                return new Interval(Status.CACHE_CHANGED, previous, 0, 0, null);
            }
            long references = state[6] - baseline[6];
            long misses = state[7] - baseline[7];
            long completeMisses = state[8] - baseline[8];
            if (state[5] <= baseline[5] || references < 0 || misses < 0 || misses > references
                    || completeMisses < 0 || completeMisses > misses || state.length < baseline.length) {
                return new Interval(Status.COUNTER_RESET, previous, 0, 0, null);
            }
            int populatedBins = 0;
            for (int i = SNAPSHOT_HEADER; i < state.length; i++) {
                long count = state[i] - (i < baseline.length ? baseline[i] : 0);
                if (count < 0) {
                    return new Interval(Status.COUNTER_RESET, previous, 0, 0, null);
                }
                if (count != 0) {
                    populatedBins = i - SNAPSHOT_HEADER + 1;
                }
            }
            if (references == 0) {
                return new Interval(Status.IDLE, previous, 0, 0, null);
            }
            if (populatedBins == 0) {
                return new Interval(Status.WARMUP, previous, references, misses, null);
            }
            if (populatedBins > MAX_BUCKETS) {
                return new Interval(Status.OVERSIZE, previous, references, misses, null);
            }
            long[] bins = Arrays.copyOfRange(state, SNAPSHOT_HEADER, SNAPSHOT_HEADER + populatedBins);
            for (int i = 0; i < bins.length && i + SNAPSHOT_HEADER < baseline.length; i++) {
                bins[i] -= baseline[i + SNAPSHOT_HEADER];
            }
            return new Interval(Status.VALID, previous, references, misses,
                    new StackDistanceHistogram(bins, completeMisses, 1, binItems, 4096));
        } catch (UnsupportedOperationException | LinkageError unavailable) {
            previous = null;
            return new Interval(Status.UNSUPPORTED_NATIVE);
        } catch (RuntimeException error) {
            previous = null;
            LOG.debug("Could not read cache telemetry", error);
            return new Interval(Status.MALFORMED);
        }
    }

    private void markRegistration(Source source) {
        if (source.initialLookups != 0) {
            snapshots.disqualifyPriorUse();
        }
        // The sequence also marks an allocation that used the cache without any
        // counted lookups or metric polls. The marker does not become a fit endpoint.
        try {
            long[] marker = source.read.get();
            if (marker != null && valid(marker) && marker[1] == cacheId) {
                if (marker[5] > 1) {
                    snapshots.disqualifyPriorUse();
                }
                sampled = true;
            }
        } catch (RuntimeException | LinkageError unavailable) {
            LOG.debug("Could not mark QuickMRC registration", unavailable);
        }
    }

    private static boolean valid(long[] sample) {
        if (sample.length < SNAPSHOT_HEADER || sample[0] != 1 || sample[1] <= 0
                || sample[2] <= 0 || sample[3] < 0 || sample[4] != 1 || sample[5] <= 0
                || sample[6] < sample[7] || sample[7] < sample[8] || sample[8] < 0
                || sample[9] != sample.length - SNAPSHOT_HEADER) {
            return false;
        }
        for (int i = SNAPSHOT_HEADER; i < sample.length; i++) {
            if (sample[i] < 0) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static long[] invoke(Method method, LRUCache cache) {
        try {
            return (long[]) method.invoke(cache);
        } catch (InvocationTargetException error) {
            if (error.getCause() instanceof UnsupportedOperationException
                    || error.getCause() instanceof LinkageError) {
                throw new UnsupportedOperationException("QuickMRC JNI snapshot unavailable", error.getCause());
            }
            throw new IllegalStateException("Cannot read the native cache snapshot", error.getCause());
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Cannot read the native cache snapshot", error);
        }
    }

    @Nullable
    private static Method method(Class<?> type, String name) {
        try {
            return type.getMethod(name);
        } catch (NoSuchMethodException | LinkageError unavailable) {
            return null;
        }
    }

    @Nullable
    static CacheObservation.WriteBufferBudget readWriteBufferBudget(
            @Nullable WriteBufferManager manager) {
        if (manager == null || WRITE_BUFFER_BUDGET == null) {
            return null;
        }
        try {
            long bytes = (Long) WRITE_BUFFER_BUDGET.invoke(manager);
            return bytes >= 0 ? new CacheObservation.WriteBufferBudget(
                    CacheObservation.WriteBufferBudget.VERSION, bytes) : null;
        } catch (ReflectiveOperationException | LinkageError unavailable) {
            // Closed managers and mismatched JNI bindings cannot attest a native budget.
            return null;
        }
    }

    static final class Source {
        final long cacheId;
        final long initialLookups;
        final Supplier<long[]> read;

        Source(long cacheId, long initialLookups, Supplier<long[]> read) {
            this.cacheId = cacheId;
            this.initialLookups = initialLookups;
            this.read = read;
        }
    }

    /** Lock order is registry, publisher, publication state. Readers never lock the registry. */
    static final class Registry {
        private final String process;
        private final Map<Long, A4SCacheTelemetry> live = new HashMap<>();
        // Wrapper tokens only pair allocation registration with release.
        private final Map<Object, Source> allocations = new IdentityHashMap<>();

        Registry(String process) {
            this.process = process;
        }

        synchronized void register(Object allocation, Source source, double sampleRate, int binItems) {
            register(allocation, source, sampleRate, binItems, null);
        }

        synchronized void register(
                Object allocation, Source source, double sampleRate, int binItems,
                @Nullable WriteBufferManager writeBufferManager) {
            if (allocations.putIfAbsent(allocation, source) != null) {
                return;
            }
            A4SCacheTelemetry publisher = live.computeIfAbsent(source.cacheId, id -> {
                A4SCacheTelemetry created = new A4SCacheTelemetry(process, id, sampleRate, binItems);
                created.markRegistration(source);
                created.writeBufferManager = writeBufferManager;
                return created;
            });
            synchronized (publisher) {
                publisher.compatible &= publisher.sampleRate == sampleRate && publisher.binItems == binItems;
                publisher.writeBufferCompatible &= publisher.writeBufferManager == writeBufferManager;
                publisher.sources.merge(source, 1, Integer::sum);
            }
        }

        @Nullable
        synchronized Lease attach(Source source, boolean monitor) {
            A4SCacheTelemetry publisher = live.get(source.cacheId);
            if (publisher == null) {
                return null;
            }
            synchronized (publisher) {
                publisher.sources.merge(source, 1, Integer::sum);
                return new Lease(this, publisher, source, publisher.snapshots.attach(monitor), monitor);
            }
        }

        synchronized void release(Object allocation) {
            Source source = allocations.remove(allocation);
            if (source != null) {
                A4SCacheTelemetry publisher = live.get(source.cacheId);
                synchronized (publisher) {
                    // The physical resource owner is disposing its native manager.
                    publisher.writeBufferCompatible = false;
                    removeSource(publisher, source);
                }
            }
        }

        synchronized void close(Lease lease) {
            if (lease.closed) {
                return;
            }
            lease.closed = true;
            synchronized (lease.publisher) {
                lease.publisher.snapshots.detach(lease.owner, lease.monitor);
                removeSource(lease.publisher, lease.source);
            }
        }

        private void removeSource(A4SCacheTelemetry publisher, Source source) {
            publisher.sources.computeIfPresent(source, (ignored, count) -> count == 1 ? null : count - 1);
            if (publisher.sources.isEmpty()) {
                publisher.snapshots.close();
                live.remove(publisher.cacheId);
            }
        }
    }

    static final class Lease implements AutoCloseable {
        private final Registry registry;
        private final A4SCacheTelemetry publisher;
        private final Source source;
        private final long owner;
        private final boolean monitor;
        private boolean closed;

        private Lease(Registry registry, A4SCacheTelemetry publisher, Source source, long owner, boolean monitor) {
            this.registry = registry;
            this.publisher = publisher;
            this.source = source;
            this.owner = owner;
            this.monitor = monitor;
        }

        long exclusiveGeneration() {
            synchronized (publisher) {
                return monitor && publisher.compatible && publisher.previous != null
                        ? publisher.snapshots.exclusiveGeneration(owner) : -1;
            }
        }

        String process() {
            return publisher.process;
        }

        long cacheId() {
            return publisher.cacheId;
        }

        String metric(String job, String vertex, int subtask, String attempt, long nowNanos) {
            return metric(job, vertex, subtask, attempt, nowNanos, null, null);
        }

        String metric(String job, String vertex, int subtask, String attempt, long nowNanos,
                @Nullable TimingReader timing, @Nullable CacheObservation.AppliedSlot appliedSlot) {
            return publisher.metric(owner, job, vertex, subtask, attempt, nowNanos, timing, appliedSlot);
        }

        @Override
        public void close() {
            registry.close(this);
        }
    }

    @FunctionalInterface
    interface TimingReader {
        String read(long[] state, long wallTimeMillis, long nativeNanos);
    }

    private static final class Interval {
        private final Status status;
        @Nullable private final long[] state;
        private final long references;
        private final long misses;
        @Nullable private final StackDistanceHistogram histogram;
        @Nullable private final CacheObservation.Endpoint endpoint;
        private final long wallTime;

        private Interval(Status status) {
            this(status, null, 0, 0, null);
        }

        private Interval(Status status, @Nullable long[] state, long references, long misses,
                @Nullable StackDistanceHistogram histogram) {
            this(status, state, references, misses, histogram, null, System.currentTimeMillis());
        }

        private Interval(Status status, @Nullable long[] state, long references, long misses,
                @Nullable StackDistanceHistogram histogram,
                @Nullable CacheObservation.Endpoint endpoint, long wallTime) {
            this.status = status;
            this.state = state;
            this.references = references;
            this.misses = misses;
            this.histogram = histogram;
            this.endpoint = endpoint;
            this.wallTime = wallTime;
        }

        private Interval withEndpoint(CacheObservation.Endpoint endpoint) {
            return new Interval(status, state, references, misses, histogram, endpoint, wallTime);
        }
    }
}
