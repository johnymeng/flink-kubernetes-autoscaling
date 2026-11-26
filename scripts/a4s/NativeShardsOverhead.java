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

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.SplittableRandom;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

/** Local comparison of the original and extracted estimators on the same warm RocksDB file. */
public final class NativeShardsOverhead {
    private static final int ROWS = 32_768;
    private static final int VALUE_BYTES = 1_000;
    private static final int CACHE_BYTES = 64 << 20;
    private static final int TRACE_LENGTH = 65_536;
    private static final int TRACE_REPEATS = 20;
    private static final TickerType CACHE_HIT = TickerType.BLOCK_CACHE_DATA_HIT;
    private static final TickerType CACHE_MISS = TickerType.BLOCK_CACHE_DATA_MISS;

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        boolean setup = "prepare".equals(args[1]);
        var path = Path.of(args[0]);
        try (var statistics = new Statistics();
                var cache = new LRUCache(CACHE_BYTES);
                var options = new Options()
                        .setCreateIfMissing(setup)
                        .setDisableAutoCompactions(true)
                        .setCompressionType(CompressionType.NO_COMPRESSION)
                        .setStatistics(statistics)
                        .setWriteBufferSize(64 << 20)
                        .setTableFormatConfig(new BlockBasedTableConfig()
                                .setBlockCache(cache)
                                .setBlockSize(1024))) {
            byte[][] keys = new byte[ROWS][];
            for (int i = 0; i < ROWS; i++) {
                keys[i] = ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
            }
            if (setup) {
                try (var db = RocksDB.open(options, path.toString());
                        var flush = new FlushOptions().setWaitForFlush(true)) {
                    var value = ByteBuffer.allocate(VALUE_BYTES);
                    for (int i = 0; i < ROWS; i++) {
                        value.putInt(0, i);
                        db.put(keys[i], value.array());
                    }
                    db.flush(flush);
                }
                return;
            }

            double rate = Double.parseDouble(args[1]);
            try (var db = RocksDB.open(options, path.toString())) {
                measure(db, cache, statistics, keys, rate);
            }
        }
    }

    private static void measure(RocksDB db, LRUCache cache, Statistics stats, byte[][] keys,
            double rate) throws Exception {
        var generator = new SplittableRandom(45611L);
        int[] requests = new int[TRACE_LENGTH];
        long expectedSum = 0;
        for (int i = 0; i < requests.length; i++) {
            requests[i] = generator.nextInt(ROWS);
            expectedSum += requests[i];
        }
        byte[] result = new byte[VALUE_BYTES];
        for (int i = 0; i < ROWS; i++) {
            read(db, keys[i], result, i);
        }
        run(db, keys, requests, result, 8);
        long beforeProfiler = anonymousRssKiB();
        if (rate > 0) {
            cache.enableShardsMrc(rate, 42L, 64L << 10, CACHE_BYTES);
        }
        run(db, keys, requests, result, 8);
        long[] initial = cache.getShardsMrcHistogram();
        long hitsBefore = stats.getTickerCount(CACHE_HIT);
        long missesBefore = stats.getTickerCount(CACHE_MISS);
        var cpu = ManagementFactory.getThreadMXBean();
        long wallStart = System.nanoTime();
        long cpuStart = cpu.getCurrentThreadCpuTime();
        long sum = run(db, keys, requests, result, TRACE_REPEATS);
        long cpuNs = cpu.getCurrentThreadCpuTime() - cpuStart;
        long wallNs = System.nanoTime() - wallStart;
        long afterProfiler = anonymousRssKiB();
        long[] end = cache.getShardsMrcHistogram();
        long hits = stats.getTickerCount(CACHE_HIT) - hitsBefore;
        long misses = stats.getTickerCount(CACHE_MISS) - missesBefore;
        long reads = (long) requests.length * TRACE_REPEATS;
        check(cpuNs > 0 && wallNs > 0 && sum == expectedSum * TRACE_REPEATS, "Invalid timed reads");
        if (hits != reads || misses != 0) {
            throw new IllegalStateException("Unexpected data-block cache counts " + hits + "/"
                    + misses + " at " + cache.getUsage() + " resident bytes");
        }
        long samples = 0;
        long tracked = 0;
        long trackedBytes = 0;
        if (rate > 0) {
            check(end[4] - initial[4] == reads, "Unexpected profiler reference count");
            samples = end[5] - initial[5];
            tracked = end[7];
            trackedBytes = end[8];
            check(samples > 0 && tracked > 0 && trackedBytes > 0, "Missing profiler observations");
        } else {
            check(end.length == 0, "Profiler was enabled in the control");
        }
        System.out.printf(Locale.ROOT, "%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                rate, reads, sum, wallNs, cpuNs, beforeProfiler, afterProfiler,
                afterProfiler - beforeProfiler, CACHE_BYTES, cache.getUsage(), hits,
                samples, tracked, trackedBytes);
    }

    private static long run(RocksDB db, byte[][] keys, int[] trace, byte[] result, int rounds)
            throws Exception {
        long sum = 0;
        for (int round = 0; round < rounds; round++) {
            for (int index : trace) {
                sum += read(db, keys[index], result, index);
            }
        }
        return sum;
    }

    private static int read(RocksDB db, byte[] key, byte[] result, int index) throws Exception {
        int bytes = db.get(key, result);
        int observed = ((result[2] & 0xff) << 8) | (result[3] & 0xff);
        check(bytes == VALUE_BYTES && result[0] == 0 && result[1] == 0 && observed == index,
                "Incorrect value");
        return observed;
    }

    private static long anonymousRssKiB() throws Exception {
        for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
            if (line.startsWith("RssAnon:")) {
                return Long.parseLong(line.split("\\s+")[1]);
            }
        }
        throw new IllegalStateException("RssAnon unavailable");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
