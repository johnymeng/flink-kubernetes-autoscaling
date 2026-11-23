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

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies that a queued histogram update respects the monitor's lifetime. */
class RocksDBHistogramLifecycleTest {

    @Test
    void retiredViewStopsReadingEvenWhileAnotherMonitorUsesTheCache(@TempDir Path directory)
            throws Exception {
        RocksDB.loadLibrary();
        final RocksDBNativeMetricOptions metrics = new RocksDBNativeMetricOptions();
        metrics.enableStackDistanceHistogram();
        final UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, directory.resolve("db").toString());
                CountingCache cache = new CountingCache();
                RocksDBNativeMetricMonitor retired =
                        new RocksDBNativeMetricMonitor(metrics, metricGroup, db, null, cache);
                RocksDBNativeMetricMonitor active =
                        new RocksDBNativeMetricMonitor(metrics, metricGroup, db, null, cache)) {
            final RocksDBNativeMetricMonitor.RocksDBStackDistanceHistogramView retiredView =
                    retired.new RocksDBStackDistanceHistogramView(cache);
            final RocksDBNativeMetricMonitor.RocksDBStackDistanceHistogramView activeView =
                    active.new RocksDBStackDistanceHistogramView(cache);

            retiredView.update();
            assertEquals(1, cache.reads);
            assertEquals(1, cache.resets);

            retired.close();
            // Keep the cache live so a stale poll is detected without entering freed memory.
            retiredView.update();
            assertEquals(1, cache.reads, "A retired view accessed the cache");
            assertEquals(1, cache.resets, "A retired view reset the cache");

            activeView.update();
            assertEquals(2, cache.reads);
            assertEquals(2, cache.resets);
        }
    }

    private static final class CountingCache extends LRUCache {
        private int reads;
        private int resets;

        private CountingCache() {
            super(1 << 20, 0, false, 0.0, true, 64, 1, 1.0, 64);
        }

        @Override
        public long[] getStackDistanceHistogram() {
            reads++;
            return super.getStackDistanceHistogram();
        }

        @Override
        public void resetQuickMRCStats() {
            resets++;
            super.resetQuickMRCStats();
        }
    }
}
