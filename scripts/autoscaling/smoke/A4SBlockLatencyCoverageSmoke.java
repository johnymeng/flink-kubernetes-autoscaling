/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/** Checks cache lookup coverage by comparing each telemetry path with the exact physical LRU. */
public final class A4SBlockLatencyCoverageSmoke {
    private static final int KEYS = 8192;
    private static final int VALUE_BYTES = 8192;
    private static final HistogramType[] LATENCY_HISTOGRAMS = {
        HistogramType.A4S_DATA_BLOCK_HIT_NANOS,
        HistogramType.A4S_DATA_BLOCK_MISS_DIRECT_NANOS,
        HistogramType.A4S_DATA_BLOCK_MISS_NON_DIRECT_FILE_NANOS,
        HistogramType.A4S_DATA_BLOCK_MISS_OTHER_NANOS
    };

    public static void main(String[] args) throws Exception {
        require(args.length == 1, "Expected one new disposable directory");
        Path root = Files.createDirectory(Paths.get(args[0]));
        RocksDB.loadLibrary();
        require(Statistics.a4sBlockLatencyAbiVersion() == 3, "Unexpected RocksDB ABI");
        boolean enabled = "1".equals(System.getenv("FLINK_A4S_SAMPLE_BLOCK_LATENCY"));
        testForeground(root.resolve("foreground"), enabled);
        testCompaction(root.resolve("compaction"), enabled);
    }

    private static void testForeground(Path path, boolean enabled) throws Exception {
        try (LRUCache cache = newCache();
                Statistics stats = new Statistics();
                Options options = options(cache, stats, true);
                FlushOptions flush = new FlushOptions().setWaitForFlush(true);
                ReadOptions regular = new ReadOptions();
                ReadOptions noFill = new ReadOptions().setFillCache(false);
                ReadOptions cacheOnly = new ReadOptions().setReadTier(ReadTier.BLOCK_CACHE_TIER);
                RocksDB db = RocksDB.open(options, path.toString())) {
            byte[] value = new byte[VALUE_BYTES];
            for (int i = 0; i < KEYS; i++) {
                db.put(key(i), value);
            }
            db.flush(flush);
            Counters before = new Counters(cache, stats);
            seekIndividually(db, noFill, 64);
            Counters coldNoFill = new Counters(cache, stats);
            require(coldNoFill.misses > before.misses, "No cold no-fill misses");
            require(coldNoFill.coveredMisses == before.coveredMisses, "Covered a no-fill miss");
            require(coldNoFill.sampledMisses == before.sampledMisses, "Timed a no-fill miss");
            require(coldNoFill.compactionMisses == before.compactionMisses, "Tagged user no-fill as compaction");

            before = coldNoFill;
            long indexHitsBefore = stats.getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT);
            for (int pass = 0; pass < 3; pass++) {
                seekIndividually(db, regular, KEYS);
            }
            Counters after = new Counters(cache, stats);
            long hits = after.hits - before.hits;
            long misses = after.misses - before.misses;
            long indexHits = stats.getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT) - indexHitsBefore;
            require(hits > 0 && misses > 0 && indexHits >= 2 * KEYS, "Missing iterator data or index lookups");
            require(after.compactionHits == before.compactionHits && after.compactionMisses == before.compactionMisses,
                    "Tagged ordinary iterator as compaction");
            if (enabled) {
                require(after.coveredHits - before.coveredHits == hits, "Incomplete foreground hit coverage");
                require(after.coveredMisses - before.coveredMisses == misses, "Incomplete foreground miss coverage");
                require(after.sampledHits - before.sampledHits >= 16 && after.sampledMisses - before.sampledMisses >= 16,
                        "Missing foreground iterator timing samples");
            } else {
                after.requireDisabled();
            }
            Counters beforeCacheOnly = after;
            seekIndividually(db, cacheOnly, 64);
            after = new Counters(cache, stats);
            require(after.hits > beforeCacheOnly.hits, "No cache-only physical hits");
            require(after.coveredHits == beforeCacheOnly.coveredHits && after.sampledHits == beforeCacheOnly.sampledHits,
                    "Covered or timed a cache-only hit");
            require(after.compactionHits == beforeCacheOnly.compactionHits, "Tagged cache-only as compaction");
            System.out.printf("foreground enabled=%s physical_hits=%d physical_misses=%d cached_index_hits=%d%n",
                    enabled, hits, misses, indexHits);
        }
    }

    private static void testCompaction(Path path, boolean enabled) throws Exception {
        try (LRUCache cache = newCache();
                Statistics stats = new Statistics();
                Options options = options(cache, stats, false);
                FlushOptions flush = new FlushOptions().setWaitForFlush(true);
                RocksDB db = RocksDB.open(options, path.toString())) {
            byte[] value = new byte[VALUE_BYTES];
            // Interleaved ranges force a merge instead of a trivial level move.
            for (int parity = 0; parity < 2; parity++) {
                for (int i = parity; i < KEYS; i += 2) {
                    db.put(key(i), value);
                }
                db.flush(flush);
            }
            for (int i = 0; i < 512; i++) {
                require(db.get(key(i)).length == VALUE_BYTES, "Missing compaction fixture value");
            }
            Counters before = new Counters(cache, stats);
            db.compactRange();
            Counters after = new Counters(cache, stats);
            long hits = after.hits - before.hits;
            long misses = after.misses - before.misses;
            require(hits > 0 && misses > 0, "Compaction fixture did not access both warm and cold blocks");
            long compactionHits = after.compactionHits - before.compactionHits;
            long compactionMisses = after.compactionMisses - before.compactionMisses;
            if (enabled) {
                require(compactionHits == hits, "Incomplete compaction hit accounting");
                require(compactionMisses == misses, "Incomplete compaction miss accounting");
            } else {
                after.requireDisabled();
            }
            require(after.coveredHits == before.coveredHits && after.coveredMisses == before.coveredMisses,
                    "Included compaction in foreground coverage");
            require(after.sampledHits == before.sampledHits && after.sampledMisses == before.sampledMisses
                            && after.timedNanos == before.timedNanos,
                    "Included compaction in foreground timings");
            System.out.printf("compaction enabled=%s physical_hits=%d physical_misses=%d compaction_hits=%d compaction_misses=%d%n",
                    enabled, hits, misses, compactionHits, compactionMisses);
        }
    }

    private static LRUCache newCache() {
        return new LRUCache(160L * 1024 * 1024, 0, false, 0.0, true, 60, 128, 0.0, 1);
    }

    private static Options options(LRUCache cache, Statistics stats, boolean cacheMetadata) {
        return new Options().setCreateIfMissing(true)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setDisableAutoCompactions(true)
                .setWriteBufferSize(128L * 1024 * 1024)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockCache(cache).setCacheIndexAndFilterBlocks(cacheMetadata))
                .setStatistics(stats);
    }

    private static void seekIndividually(RocksDB db, ReadOptions options, int keys) {
        for (int i = 0; i < keys; i++) {
            try (RocksIterator iterator = db.newIterator(options)) {
                byte[] key = key(i);
                iterator.seek(key);
                require(iterator.isValid() && Arrays.equals(key, iterator.key())
                                && iterator.value().length == VALUE_BYTES,
                        "Missing or corrupt iterator value at " + i);
            }
        }
    }

    private static byte[] key(int index) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(index).array();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class Counters {
        private final long hits;
        private final long misses;
        private final long coveredHits;
        private final long coveredMisses;
        private final long compactionHits;
        private final long compactionMisses;
        private final long sampledHits;
        private final long sampledMisses;
        private final long timedNanos;

        private Counters(LRUCache cache, Statistics stats) {
            long[] physical = cache.getA4SCacheLookupSnapshot();
            require(physical != null && physical.length == 7, "Missing exact cache counters");
            hits = physical[5] - physical[6];
            misses = physical[6];
            coveredHits = stats.getTickerCount(TickerType.A4S_BLOCK_LATENCY_COVERED_HIT);
            coveredMisses = stats.getTickerCount(TickerType.A4S_BLOCK_LATENCY_COVERED_MISS);
            compactionHits = stats.getTickerCount(TickerType.A4S_BLOCK_LATENCY_COMPACTION_HIT);
            compactionMisses = stats.getTickerCount(TickerType.A4S_BLOCK_LATENCY_COMPACTION_MISS);
            HistogramData hit = stats.getHistogramData(LATENCY_HISTOGRAMS[0]);
            sampledHits = hit.getCount();
            long missSamples = 0;
            long nanos = hit.getSum();
            for (int index = 1; index < LATENCY_HISTOGRAMS.length; index++) {
                HistogramData miss = stats.getHistogramData(LATENCY_HISTOGRAMS[index]);
                missSamples += miss.getCount();
                nanos += miss.getSum();
            }
            sampledMisses = missSamples;
            timedNanos = nanos;
        }

        private void requireDisabled() {
            require(coveredHits == 0 && coveredMisses == 0
                            && compactionHits == 0 && compactionMisses == 0
                            && sampledHits == 0 && sampledMisses == 0 && timedNanos == 0,
                    "A4S latency telemetry ran without opt-in");
        }
    }
}
