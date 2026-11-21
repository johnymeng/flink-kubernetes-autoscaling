/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SCacheCostSample;

import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;

/**
 * A monitor which pulls {{@link RocksDB}} native metrics and forwards them to Flink's metric group.
 * All metrics are unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricMonitor.class);

    private final RocksDBNativeMetricOptions options;

    private final MetricGroup metricGroup;

    private final Object lock;

    static final String COLUMN_FAMILY_KEY = "column_family";

    @GuardedBy("lock")
    private RocksDB rocksDB;

    @Nullable
    @GuardedBy("lock")
    private Statistics statistics;

    @Nullable private final LRUCache lruCache;
    @Nullable private final A4SCacheTelemetry.Lease cacheMonitor;

    @Nullable private A4SBlockLatencyWindow a4sBlockLatency;

    public RocksDBNativeMetricMonitor(
            @Nonnull RocksDBNativeMetricOptions options,
            @Nonnull MetricGroup metricGroup,
            @Nonnull RocksDB rocksDB,
            @Nullable Statistics statistics,
            @Nullable Cache blockCache) {
        this.options = options;
        this.metricGroup = metricGroup;
        this.rocksDB = rocksDB;
        this.statistics = statistics;
        this.lruCache = (blockCache instanceof LRUCache) ? (LRUCache) blockCache : null;
        this.lock = new Object();
        this.cacheMonitor = A4SCacheTelemetry.attachMonitor(lruCache);
        try {
            registerStatistics();
            registerStackDistanceHistogram();
            registerA4SBlockLatency();
        } catch (RuntimeException | Error failure) {
            closeNativeMetrics();
            throw failure;
        }
    }

    /** Register gauges to pull native metrics for the database. */
    private void registerStatistics() {
        if (statistics != null) {
            for (TickerType tickerType : options.getMonitorTickerTypes()) {
                metricGroup.gauge(
                        String.format("rocksdb.%s", tickerType.name().toLowerCase()),
                        new RocksDBNativeStatisticsMetricView(tickerType));
            }
        }
    }

    private void registerA4SBlockLatency() {
        if (!options.isA4SBlockLatencyEnabled() || statistics == null) {
            return;
        }
        A4SBlockLatencyWindow.Kind[] kinds = A4SBlockLatencyWindow.KINDS;
        HistogramType[] histograms = new HistogramType[kinds.length];
        int abiVersion;
        try {
            // Reflective for compatibility with the stock FRocksDB jar. A Java enum by itself
            // is not sufficient: old JNI can silently map unknown histogram IDs to DB_GET.
            abiVersion =
                    (Integer) Statistics.class.getMethod("a4sBlockLatencyAbiVersion").invoke(null);
            if (abiVersion < 1 || abiVersion > 3) {
                LOG.warn("A4S block latency unavailable: unsupported RocksDB sampling ABI.");
                return;
            }
            for (A4SBlockLatencyWindow.Kind kind : kinds) {
                histograms[kind.ordinal()] =
                        HistogramType.valueOf("A4S_DATA_BLOCK_" + kind.name() + "_NANOS");
            }
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            LOG.warn("A4S block latency unavailable: RocksDB JNI lacks the sampling patch.", e);
            return;
        }

        A4SBlockLatencyWindow window = new A4SBlockLatencyWindow();
        a4sBlockLatency = window;
        metricGroup.gauge(
                "rocksdb.a4s_block_miss_penalty_seconds",
                new A4SBlockLatencyGauge(window, histograms));
        for (A4SBlockLatencyWindow.Kind kind : kinds) {
            String prefix = "rocksdb.a4s_block_" + kind.name().toLowerCase(Locale.ROOT);
            metricGroup.gauge(
                    prefix + "_samples", () -> window.latest(System.nanoTime()).count(kind));
            metricGroup.gauge(
                    prefix + "_seconds", () -> window.latest(System.nanoTime()).meanSeconds(kind));
        }
        if (abiVersion >= 2
                && lruCache != null
                && metricGroup instanceof InternalOperatorMetricGroup) {
            registerCacheCostSample(
                    (InternalOperatorMetricGroup) metricGroup, histograms, abiVersion);
        }
    }

    private void registerCacheCostSample(
            InternalOperatorMetricGroup operator, HistogramType[] histograms, int abiVersion) {
        try {
            Method readCache = LRUCache.class.getMethod("getA4SCacheLookupSnapshot");
            TickerType coveredHits = TickerType.valueOf("A4S_BLOCK_LATENCY_COVERED_HIT");
            TickerType coveredMisses = TickerType.valueOf("A4S_BLOCK_LATENCY_COVERED_MISS");
            TickerType compactionHits =
                    abiVersion >= 3 ? TickerType.valueOf("A4S_BLOCK_LATENCY_COMPACTION_HIT") : null;
            TickerType compactionMisses =
                    abiVersion >= 3 ? TickerType.valueOf("A4S_BLOCK_LATENCY_COMPACTION_MISS") : null;
            String attempt = operator.getAllVariables().get(ScopeFormat.SCOPE_TASK_ATTEMPT_ID);
            if (attempt != null) {
                metricGroup.addGroup("rocksdb")
                        .gauge(
                                A4SCacheCostSample.METRIC_NAME,
                                () ->
                                        readCacheCostSample(
                                                operator,
                                                attempt,
                                                histograms,
                                                readCache,
                                                coveredHits,
                                                coveredMisses,
                                                compactionHits,
                                                compactionMisses,
                                                abiVersion));
            }
        } catch (ReflectiveOperationException | IllegalArgumentException | LinkageError e) {
            LOG.warn("A4S online cache cost telemetry is unavailable.", e);
        }
    }

    private String readCacheCostSample(
            InternalOperatorMetricGroup operator,
            String attempt,
            HistogramType[] histograms,
            Method readCache,
            TickerType coveredHits,
            TickerType coveredMisses,
            @Nullable TickerType compactionHits,
            @Nullable TickerType compactionMisses,
            int abiVersion) {
        synchronized (lock) {
            if (statistics == null || lruCache == null || cacheMonitor == null) {
                return "";
            }
            long ownershipGeneration = cacheMonitor.exclusiveGeneration();
            if (ownershipGeneration < 0) {
                return "";
            }
            try {
                long observedAtMillis = System.currentTimeMillis();
                long observedAtNanos = System.nanoTime();
                long[] cache = (long[]) readCache.invoke(lruCache);
                if (cache == null || cache.length != 7 || cache[0] != 1 || cache[4] != 1
                        || cache[1] != cacheMonitor.cacheId()) {
                    return "";
                }
                IOMetrics task = operator.getTaskIOMetricGroup().createSnapshot();
                HistogramData hit = statistics.getHistogramData(histograms[0]);
                long missSamples = 0;
                long missNanos = 0;
                for (int index = 1; index < histograms.length; index++) {
                    HistogramData miss = statistics.getHistogramData(histograms[index]);
                    missSamples = Math.addExact(missSamples, miss.getCount());
                    missNanos = Math.addExact(missNanos, miss.getSum());
                }
                long hitCount = statistics.getTickerCount(coveredHits);
                long missCount = statistics.getTickerCount(coveredMisses);
                Long compactionHitCount =
                        compactionHits == null ? null : statistics.getTickerCount(compactionHits);
                Long compactionMissCount =
                        compactionMisses == null ? null : statistics.getTickerCount(compactionMisses);
                if (cacheMonitor.exclusiveGeneration() != ownershipGeneration
                        || System.nanoTime() - observedAtNanos > 100_000_000) {
                    return "";
                }
                return new A4SCacheCostSample(
                                0,
                                attempt,
                                cache[1],
                                cache[2],
                                cache[3],
                                cache[5],
                                cache[6],
                                hitCount,
                                missCount,
                                hit.getCount(),
                                missSamples,
                                hit.getSum(),
                                missNanos,
                                task.getNumRecordsIn(),
                                task.getAccumulateBusyTime(),
                                observedAtMillis,
                                abiVersion,
                                compactionHitCount,
                                compactionMissCount,
                                cacheMonitor.process(),
                                observedAtNanos)
                        .toMetricString();
            } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
                LOG.debug("Could not read the A4S online cache cost sample.", e);
                return "";
            }
        }
    }

    private class A4SBlockLatencyGauge implements View, Gauge<Double> {
        private final A4SBlockLatencyWindow window;
        private final HistogramType[] histograms;
        private boolean unavailable;

        private A4SBlockLatencyGauge(A4SBlockLatencyWindow window, HistogramType[] histograms) {
            this.window = window;
            this.histograms = histograms;
        }

        @Override
        public Double getValue() {
            return window.latest(System.nanoTime()).missPenaltySeconds();
        }

        @Override
        public void update() {
            synchronized (lock) {
                if (statistics == null || unavailable) {
                    return;
                }
                try {
                    long[] counts = new long[histograms.length];
                    long[] sumsNs = new long[histograms.length];
                    for (int i = 0; i < histograms.length; i++) {
                        HistogramData histogram = statistics.getHistogramData(histograms[i]);
                        counts[i] = histogram.getCount();
                        sumsNs[i] = histogram.getSum();
                    }
                    window.observe(System.nanoTime(), counts, sumsNs);
                } catch (RuntimeException | LinkageError e) {
                    window.clear();
                    unavailable = true;
                    LOG.warn("Disabling unavailable A4S block latency telemetry for this database.", e);
                }
            }
        }
    }

    /** Registers the stack distance histogram metric if enabled. */
    private void registerStackDistanceHistogram() {
        if (!options.isStackDistanceHistogramEnabled()
                || !(metricGroup instanceof InternalOperatorMetricGroup)) {
            return;
        }
        Map<String, String> variables = metricGroup.getAllVariables();
        String job = variables.get(ScopeFormat.SCOPE_JOB_ID);
        String vertex = variables.get(ScopeFormat.SCOPE_TASK_VERTEX_ID);
        String attempt = variables.get(ScopeFormat.SCOPE_TASK_ATTEMPT_ID);
        int subtask = Integer.parseInt(variables.get(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX));
        metricGroup.addGroup("rocksdb").gauge(
                CacheObservation.METRIC_NAME,
                () -> readCacheObservation(job, vertex, subtask, attempt));
    }

    private String readCacheObservation(String job, String vertex, int subtask, String attempt) {
        synchronized (lock) {
            CacheObservation.Status status;
            if (rocksDB == null) {
                status = CacheObservation.Status.CLOSED;
            } else if (cacheMonitor == null) {
                status = CacheObservation.Status.UNSUPPORTED_NATIVE;
            } else {
                try {
                    return cacheMonitor.metric(job, vertex, subtask, attempt, System.nanoTime());
                } catch (RuntimeException error) {
                    LOG.debug("Could not publish cache observation", error);
                    status = CacheObservation.Status.MALFORMED;
                }
            }
            return new CacheObservation(
                    CacheObservation.VERSION, job, vertex, subtask, attempt, status, null, null)
                    .toMetricString();
        }
    }

    /**
     * Register gauges to pull native metrics for the column family.
     *
     * @param columnFamilyName group name for the new gauges
     * @param handle native handle to the column family
     */
    void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {

        boolean columnFamilyAsVariable = options.isColumnFamilyAsVariable();
        MetricGroup group =
                columnFamilyAsVariable
                        ? metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName)
                        : metricGroup.addGroup(columnFamilyName);

        for (String property : options.getProperties()) {
            RocksDBNativePropertyMetricView gauge =
                    new RocksDBNativePropertyMetricView(handle, property);
            group.gauge(property, gauge);
        }
    }

    /** Updates the value of metricView if the reference is still valid. */
    private void setProperty(RocksDBNativePropertyMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        try {
            synchronized (lock) {
                if (rocksDB != null) {
                    long value = rocksDB.getLongProperty(metricView.handle, metricView.property);
                    metricView.setValue(value);
                }
            }
        } catch (RocksDBException e) {
            metricView.close();
            LOG.warn("Failed to read native metric {} from RocksDB.", metricView.property, e);
        }
    }

    private void setStatistics(RocksDBNativeStatisticsMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        if (statistics != null) {
            synchronized (lock) {
                metricView.setValue(statistics.getTickerCount(metricView.tickerType));
            }
        }
    }

    @Override
    public void close() {
        closeNativeMetrics();
    }

    private void closeNativeMetrics() {
        synchronized (lock) {
            rocksDB = null;
            statistics = null;
            if (a4sBlockLatency != null) {
                a4sBlockLatency.clear();
            }
            if (cacheMonitor != null) {
                cacheMonitor.close();
            }
        }
    }

    abstract static class RocksDBNativeView implements View {
        private boolean closed;

        RocksDBNativeView() {
            this.closed = false;
        }

        void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB property-based native metric for the specified
     * column family / metric pair.
     *
     * <p><strong>Note</strong>: As the returned property is of type {@code uint64_t} on C++ side
     * the returning value can be negative. Because java does not support unsigned long types, this
     * gauge wraps the result in a {@link BigInteger}.
     */
    class RocksDBNativePropertyMetricView extends RocksDBNativeView implements Gauge<BigInteger> {
        private final String property;

        private final ColumnFamilyHandle handle;

        private BigInteger bigInteger;

        private RocksDBNativePropertyMetricView(
                ColumnFamilyHandle handle, @Nonnull String property) {
            this.handle = handle;
            this.property = property;
            this.bigInteger = BigInteger.ZERO;
        }

        public void setValue(long value) {
            if (value >= 0L) {
                bigInteger = BigInteger.valueOf(value);
            } else {
                int upper = (int) (value >>> 32);
                int lower = (int) value;

                bigInteger =
                        BigInteger.valueOf(Integer.toUnsignedLong(upper))
                                .shiftLeft(32)
                                .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
            }
        }

        @Override
        public BigInteger getValue() {
            return bigInteger;
        }

        @Override
        public void update() {
            setProperty(this);
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB statistics-based native metric for the database.
     */
    class RocksDBNativeStatisticsMetricView extends RocksDBNativeView implements Gauge<Long> {
        private final TickerType tickerType;
        private long value;

        private RocksDBNativeStatisticsMetricView(TickerType tickerType) {
            this.tickerType = tickerType;
        }

        @Override
        public Long getValue() {
            return value;
        }

        void setValue(long value) {
            this.value = value;
        }

        @Override
        public void update() {
            setStatistics(this);
        }
    }

}
