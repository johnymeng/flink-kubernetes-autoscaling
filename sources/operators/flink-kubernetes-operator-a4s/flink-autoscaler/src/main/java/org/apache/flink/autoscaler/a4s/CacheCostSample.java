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

package org.apache.flink.autoscaler.a4s;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Cumulative same-attempt counters for one subtask and its RocksDB cache. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheCostSample {
    private int subtask;
    private String attempt;
    private long cacheId;
    private long capacityBytes;
    private long cacheEpoch;
    private long lookups;
    private long misses;
    private long coveredHits;
    private long coveredMisses;
    private long sampledHits;
    private long sampledMisses;
    private long hitNanos;
    private long missNanos;
    private long recordsIn;
    private double busyMillis;
    private long observedAtMillis;
    private int timingAbi;
    private long compactionHits;
    private long compactionMisses;
    private String process;
    private Long observedAtNanos;

    public CacheCostSample(
            int subtask, String attempt, long cacheId, long capacityBytes, long cacheEpoch,
            long lookups, long misses, long coveredHits, long coveredMisses, long sampledHits,
            long sampledMisses, long hitNanos, long missNanos, long recordsIn, double busyMillis,
            long observedAtMillis, int timingAbi, long compactionHits, long compactionMisses) {
        this(subtask, attempt, cacheId, capacityBytes, cacheEpoch, lookups, misses,
                coveredHits, coveredMisses, sampledHits, sampledMisses, hitNanos, missNanos,
                recordsIn, busyMillis, observedAtMillis, timingAbi, compactionHits, compactionMisses,
                null, null);
    }
}
