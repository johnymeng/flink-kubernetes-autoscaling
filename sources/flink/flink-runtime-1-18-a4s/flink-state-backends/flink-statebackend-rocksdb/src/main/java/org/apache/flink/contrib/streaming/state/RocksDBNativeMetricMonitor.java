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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.a4s.logging.A4SMetricsFlowStep;

import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
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
import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;

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

    @GuardedBy("lock")
    private long cacheCounterSequence;

    @Nullable
    @GuardedBy("lock")
    private LiveStateSizeView liveStateSizeView;

    @Nullable private final LRUCache lruCache;
    private final boolean shardsEnabled;

    // Values indicate whether more than one monitor has used a cache. Once shared,
    // its cumulative histogram cannot be attributed to a single backend.
    @GuardedBy("SHARDS_CACHE_OWNERSHIP")
    private static final Map<LRUCache, Boolean> SHARDS_CACHE_OWNERSHIP = new WeakHashMap<>();

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
        this.shardsEnabled = lruCache != null && lruCache.getShardsMrcHistogram().length > 0;
        if (shardsEnabled) {
            synchronized (SHARDS_CACHE_OWNERSHIP) {
                SHARDS_CACHE_OWNERSHIP.put(
                        lruCache, SHARDS_CACHE_OWNERSHIP.containsKey(lruCache));
            }
        }
        registerStatistics();
        registerDatabaseObservations();
        registerStackDistanceHistogram();
    }

    private boolean isSharedShardsCache() {
        synchronized (SHARDS_CACHE_OWNERSHIP) {
            return Boolean.TRUE.equals(SHARDS_CACHE_OWNERSHIP.get(lruCache));
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

    private void registerDatabaseObservations() {
        final boolean reportCacheCounters =
                statistics != null
                        && options.getMonitorTickerTypes().contains(TickerType.BLOCK_CACHE_HIT)
                        && options.getMonitorTickerTypes().contains(TickerType.BLOCK_CACHE_MISS);
        final boolean reportLiveState =
                options.getProperties()
                        .contains(RocksDBProperty.EstimateLiveDataSize.getRocksDBProperty());
        if (!reportCacheCounters && !reportLiveState) {
            return;
        }
        final String databaseId = UUID.randomUUID().toString();
        if (reportCacheCounters) {
            metricGroup.gauge(
                    MetricNames.A4S_BLOCK_CACHE_COUNTERS, () -> sampleCacheCounters(databaseId));
        }
        if (reportLiveState) {
            liveStateSizeView = new LiveStateSizeView(databaseId);
            metricGroup.gauge(MetricNames.A4S_ROCKSDB_STATE_SIZE, liveStateSizeView);
        }
    }

    private String sampleCacheCounters(String databaseId) {
        synchronized (lock) {
            if (statistics == null || cacheCounterSequence == Long.MAX_VALUE) {
                return "";
            }
            long hits = statistics.getTickerCount(TickerType.BLOCK_CACHE_HIT);
            long misses = statistics.getTickerCount(TickerType.BLOCK_CACHE_MISS);
            return databaseId + ":" + (++cacheCounterSequence) + ":" + hits + ":" + misses;
        }
    }

    /** Registers the stack distance histogram metric if enabled. */
    private void registerStackDistanceHistogram() {
        if (options.isStackDistanceHistogramEnabled()) {
            LOG.info(
                    "Registering RocksDB stack distance histogram for {}.",
                    shardsEnabled ? "SHARDS" : "QuickMRC");
            RocksDBStackDistanceHistogramView view =
                    new RocksDBStackDistanceHistogramView(lruCache);
            metricGroup.gauge("rocksdb.stack-distance-histogram", view);
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
        synchronized (lock) {
            rocksDB = null;
            statistics = null;
            if (liveStateSizeView != null) {
                liveStateSizeView.observation = "";
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

    /** A periodically refreshed estimate across the database's column families. */
    private final class LiveStateSizeView implements Gauge<String>, View {
        private final String databaseId;
        private volatile String observation = "";

        @GuardedBy("lock")
        private long sequence;

        private LiveStateSizeView(String databaseId) {
            this.databaseId = databaseId;
        }

        @Override
        public String getValue() {
            return observation;
        }

        @Override
        public void update() {
            synchronized (lock) {
                if (rocksDB == null || sequence == Long.MAX_VALUE) {
                    observation = "";
                    return;
                }
                try {
                    long bytes =
                            rocksDB.getAggregatedLongProperty(
                                    RocksDBProperty.EstimateLiveDataSize.getRocksDBProperty());
                    observation = bytes < 0 ? "" : databaseId + ":" + (++sequence) + ":" + bytes;
                } catch (RocksDBException e) {
                    observation = "";
                    LOG.debug("Unable to observe RocksDB live state size", e);
                }
            }
        }
    }

    /** A stack distance histogram gauge updated by the shared {@link View} update cycle. */
    class RocksDBStackDistanceHistogramView extends RocksDBNativeView implements Gauge<String> {

        @Nullable private final LRUCache viewLruCache;
        private final long bucketSizeScaling;
        private final long cacheItemSizeBytes;
        private StackDistanceHistogram latestHistogram;

        RocksDBStackDistanceHistogramView(@Nullable LRUCache lruCache) {
            this.viewLruCache = lruCache;
            this.bucketSizeScaling = options.getStackDistanceHistogramBucketSizeScaling();
            this.cacheItemSizeBytes = options.getStackDistanceHistogramCacheItemSizeBytes();
            this.latestHistogram =
                    shardsEnabled
                            ? null
                            : new StackDistanceHistogram(
                                    new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
        }

        @Override
        public void update() {
            synchronized (lock) {
                // A queued update can outlive the monitor and its native cache handle.
                if (rocksDB == null || isClosed() || (shardsEnabled && isSharedShardsCache())) {
                    return;
                }
                if (viewLruCache == null) {
                    StackDistanceHistogram emptyHistogram =
                            new StackDistanceHistogram(
                                    new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
                    LOG.debug(
                            "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} reason=lru_cache_null",
                            A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATE_FAILED.name(),
                            emptyHistogram.getHistogramFingerprint(),
                            0,
                            1);
                    latestHistogram = emptyHistogram;
                    return;
                }
                try {
                    if (shardsEnabled) {
                        latestHistogram =
                                StackDistanceHistogram.fromShardsMrcArray(
                                        viewLruCache.getShardsMrcHistogram());
                        return;
                    }
                    long[] histogramCounts = viewLruCache.getStackDistanceHistogram();
                    viewLruCache.resetQuickMRCStats();
                    if (histogramCounts == null) {
                        StackDistanceHistogram emptyHistogram =
                                new StackDistanceHistogram(
                                        new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
                        LOG.warn(
                                "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} reason=histogram_null",
                                A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATE_FAILED.name(),
                                emptyHistogram.getHistogramFingerprint(),
                                0,
                                1);
                        latestHistogram = emptyHistogram;
                        return;
                    }

                    long completeMisses =
                            histogramCounts.length == 0 ? 0L : histogramCounts[histogramCounts.length - 1];
                    long[] finiteBucketCounts =
                            histogramCounts.length == 0
                                    ? new long[0]
                                    : java.util.Arrays.copyOf(histogramCounts, histogramCounts.length - 1);
                    latestHistogram =
                            new StackDistanceHistogram(
                                    finiteBucketCounts,
                                    completeMisses,
                                    1,
                                    bucketSizeScaling,
                                    cacheItemSizeBytes);
                    LOG.debug(
                            "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} totalFrequency={}",
                            A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATED.name(),
                            latestHistogram.getHistogramFingerprint(),
                            latestHistogram.getNumBuckets(),
                            latestHistogram.getNumPartitions(),
                            latestHistogram.getTotalFrequency());
                } catch (RuntimeException e) {
                    StackDistanceHistogram empty =
                            new StackDistanceHistogram(
                                    new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
                    LOG.warn(
                            "A4S [{}]: histogramFingerprint={} numBuckets={} numPartitions={} reason=exception message={}",
                            A4SMetricsFlowStep.ROCKSDB_HISTOGRAM_UPDATE_FAILED.name(),
                            empty.getHistogramFingerprint(),
                            0,
                            1,
                            e.getMessage(),
                            e);
                    latestHistogram = shardsEnabled ? null : empty;
                }
            }
        }

        @Override
        public String getValue() {
            StackDistanceHistogram histogram;
            synchronized (lock) {
                if (shardsEnabled
                        && (rocksDB == null || isClosed() || isSharedShardsCache()
                                || latestHistogram == null)) {
                    return "";
                }
                histogram = latestHistogram;
            }
            if (histogram == null) {
                histogram =
                        new StackDistanceHistogram(
                                new long[0], 0L, 1, bucketSizeScaling, cacheItemSizeBytes);
            }
            return histogram.toMetricString();
        }
    }
}
