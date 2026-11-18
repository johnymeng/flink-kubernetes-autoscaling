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


import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;

/** Standalone check of v1 cache identity, geometry, lifetime, and opt-in gating. */
public final class A4SCacheLookupSmoke {
    private static final long CAPACITY = 4 * 1024 * 1024;

    public static void main(String[] args) {
        RocksDB.loadLibrary();
        boolean enabled = "1".equals(System.getenv("FLINK_A4S_COUNT_CACHE_LOOKUPS"));
        try (LRUCache one = newCache(0, true);
                LRUCache second = newCache(0, true);
                LRUCache multiple = newCache(1, true);
                LRUCache noQuickMrc = newCache(0, false)) {
            long[] a = one.getA4SCacheLookupSnapshot();
            long[] b = second.getA4SCacheLookupSnapshot();
            require(multiple.getA4SCacheLookupSnapshot() == null, "v1 accepted multiple shards");
            require(noQuickMrc.getA4SCacheLookupSnapshot() == null, "QuickMRC guard failed");
            if (enabled) {
                requireNewSnapshot(a);
                requireNewSnapshot(b);
                require(a[1] != b[1], "physical caches shared identity");
                require(one.getA4SCacheLookupSnapshot()[1] == a[1], "cache identity changed");
            } else {
                require(a == null && b == null, "counting was not disabled by default");
            }
            one.close();
            require(one.getA4SCacheLookupSnapshot() == null, "closed Java handle was used");
        }
        System.out.println("A4S cache lookup snapshot probe passed with enabled=" + enabled);
    }

    private static LRUCache newCache(int numShardBits, boolean quickMrc) {
        return new LRUCache(CAPACITY, numShardBits, true, 0.0, quickMrc, 60, 1, 0.0, 1);
    }

    private static void requireNewSnapshot(long[] snapshot) {
        require(snapshot != null && snapshot.length == 7, "missing or malformed snapshot");
        require(snapshot[0] == 1 && snapshot[1] > 0, "invalid schema or ID");
        require(snapshot[2] == CAPACITY && snapshot[3] > 0, "invalid capacity or epoch");
        require(snapshot[4] == 1, "invalid shard count");
        require(snapshot[5] == 0 && snapshot[6] == 0, "new cache had existing lookups");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
