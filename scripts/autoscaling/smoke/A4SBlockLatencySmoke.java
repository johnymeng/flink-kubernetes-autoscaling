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
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Local JNI smoke: verifies cold and warm point reads reach different native histograms. */
public final class A4SBlockLatencySmoke {
    private static final int KEYS = 8_192;

    private static byte[] key(int index) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(index).array();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: A4SBlockLatencySmoke NEW_DB_PATH [direct]");
        }
        boolean direct = args.length == 2 && args[1].equals("direct");
        Path path = Paths.get(args[0]);
        if (Files.exists(path)) {
            throw new IllegalArgumentException("Use a new disposable DB path");
        }
        RocksDB.loadLibrary();
        if (Statistics.a4sBlockLatencyAbiVersion() != 3) {
            throw new AssertionError("unexpected RocksDB ABI");
        }

        try (LRUCache cache = new LRUCache(160L * 1024 * 1024);
                Statistics stats = new Statistics();
                Options options = new Options();
                FlushOptions flush = new FlushOptions()) {
            options.setCreateIfMissing(true)
                    .setCompressionType(CompressionType.NO_COMPRESSION)
                    .setUseDirectReads(direct)
                    .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache))
                    .setStatistics(stats);
            try (RocksDB db = RocksDB.open(options, path.toString())) {
                byte[] value = new byte[8_192];
                for (int i = 0; i < KEYS; i++) {
                    db.put(key(i), value);
                }
                db.flush(flush.setWaitForFlush(true));
                readAll(db);
                for (int i = 0; i < 3; i++) {
                    readAll(db);
                }
            }
            HistogramType hit = HistogramType.A4S_DATA_BLOCK_HIT_NANOS;
            HistogramType miss =
                    direct
                            ? HistogramType.A4S_DATA_BLOCK_MISS_DIRECT_NANOS
                            : HistogramType.A4S_DATA_BLOCK_MISS_NON_DIRECT_FILE_NANOS;
            HistogramType unexpected =
                    direct
                            ? HistogramType.A4S_DATA_BLOCK_MISS_NON_DIRECT_FILE_NANOS
                            : HistogramType.A4S_DATA_BLOCK_MISS_DIRECT_NANOS;
            HistogramData hitData = stats.getHistogramData(hit);
            HistogramData missData = stats.getHistogramData(miss);
            System.out.printf(
                    "%s count=%d sum_ns=%d; %s count=%d sum_ns=%d%n",
                    hit, hitData.getCount(), hitData.getSum(),
                    miss, missData.getCount(), missData.getSum());
            if (stats.getHistogramData(unexpected).getCount() != 0) {
                throw new AssertionError("mislabeled file read");
            }
            if ("1".equals(System.getenv("FLINK_A4S_SAMPLE_BLOCK_LATENCY"))) {
                if (hitData.getCount() < 16 || missData.getCount() < 16
                        || hitData.getSum() < hitData.getCount()
                        || missData.getSum() < missData.getCount()) {
                    throw new AssertionError("missing sampled hit or miss timings");
                }
            } else if (hitData.getCount() != 0 || missData.getCount() != 0) {
                throw new AssertionError("sampler ran without opt-in");
            }
        }
    }

    private static void readAll(RocksDB db) throws Exception {
        for (int i = 0; i < KEYS; i++) {
            byte[] value = db.get(key(i));
            if (value == null || value.length != 8_192) {
                throw new AssertionError("missing or corrupt value");
            }
        }
    }
}
