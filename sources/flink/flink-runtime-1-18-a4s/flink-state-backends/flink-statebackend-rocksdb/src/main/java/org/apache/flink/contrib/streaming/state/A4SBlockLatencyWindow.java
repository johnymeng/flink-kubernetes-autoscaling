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

import java.util.concurrent.TimeUnit;

/** Count-weighted, sampled latency measurements for one RocksDB instance; shadow-only. */
final class A4SBlockLatencyWindow {
    enum Kind {
        HIT,
        MISS_DIRECT,
        MISS_NON_DIRECT_FILE,
        MISS_OTHER
    }

    static final Kind[] KINDS = Kind.values();
    static final long WINDOW_NS = TimeUnit.SECONDS.toNanos(60);
    static final long MAX_GAP_NS = 2 * WINDOW_NS;
    static final long MIN_SAMPLES = 64;

    // Accessed by reporters as well as the metric updater.
    private volatile Sample latest;
    private long[] counts;
    private long[] sumsNs;
    private long[] lastCounts;
    private long[] lastSumsNs;
    private long startNs;

    void observe(long nowNs, long[] nextCounts, long[] nextSumsNs) {
        if (nextCounts.length != KINDS.length || nextSumsNs.length != KINDS.length) {
            clear();
            return;
        }
        for (int i = 0; i < KINDS.length; i++) {
            if (nextCounts[i] < 0 || nextSumsNs[i] < 0) {
                clear();
                return;
            }
        }
        if (counts == null) {
            rebase(nowNs, nextCounts, nextSumsNs);
            return;
        }

        long elapsedNs = nowNs - startNs;
        long[] deltaCounts = new long[KINDS.length];
        long[] deltaSumsNs = new long[KINDS.length];
        for (int i = 0; i < KINDS.length; i++) {
            deltaCounts[i] = nextCounts[i] - counts[i];
            deltaSumsNs[i] = nextSumsNs[i] - sumsNs[i];
            if (nextCounts[i] < lastCounts[i] || nextSumsNs[i] < lastSumsNs[i]) {
                latest = null;
                rebase(nowNs, nextCounts, nextSumsNs);
                return;
            }
        }
        if (elapsedNs < 0 || elapsedNs > MAX_GAP_NS) {
            latest = null;
            rebase(nowNs, nextCounts, nextSumsNs);
            return;
        }
        if (elapsedNs < WINDOW_NS) {
            lastCounts = nextCounts.clone();
            lastSumsNs = nextSumsNs.clone();
            return;
        }
        for (int i = 0; i < KINDS.length; i++) {
            // Reject inconsistent (possibly concurrently acquired) histogram snapshots.
            if ((deltaCounts[i] == 0) != (deltaSumsNs[i] == 0)
                    || deltaSumsNs[i] < deltaCounts[i]) {
                latest = null;
                rebase(nowNs, nextCounts, nextSumsNs);
                return;
            }
        }

        latest = new Sample(nowNs, deltaCounts, deltaSumsNs);
        rebase(nowNs, nextCounts, nextSumsNs);
    }

    private void rebase(long nowNs, long[] nextCounts, long[] nextSumsNs) {
        startNs = nowNs;
        counts = nextCounts.clone();
        sumsNs = nextSumsNs.clone();
        lastCounts = counts;
        lastSumsNs = sumsNs;
    }

    void clear() {
        counts = null;
        sumsNs = null;
        lastCounts = null;
        lastSumsNs = null;
        latest = null;
    }

    Sample latest(long nowNs) {
        Sample sample = latest;
        if (sample == null || nowNs - sample.endNs < 0 || nowNs - sample.endNs > MAX_GAP_NS) {
            return Sample.EMPTY;
        }
        return sample;
    }

    static final class Sample {
        private static final Sample EMPTY =
                new Sample(0, new long[KINDS.length], new long[KINDS.length]);
        private final long endNs;
        private final long[] counts;
        private final long[] sumsNs;

        private Sample(long endNs, long[] counts, long[] sumsNs) {
            this.endNs = endNs;
            this.counts = counts;
            this.sumsNs = sumsNs;
        }

        long count(Kind kind) {
            return counts[kind.ordinal()];
        }

        double meanSeconds(Kind kind) {
            return seconds(count(kind), sumsNs[kind.ordinal()]);
        }

        double missPenaltySeconds() {
            double misses = 0;
            double missNs = 0;
            for (Kind kind : KINDS) {
                if (kind != Kind.HIT) {
                    misses += count(kind);
                    missNs += sumsNs[kind.ordinal()];
                }
            }
            double penalty = seconds(misses, missNs) - meanSeconds(Kind.HIT);
            return Double.isFinite(penalty) && penalty > 0 ? penalty : Double.NaN;
        }

        private static double seconds(double count, double sumNs) {
            return count >= MIN_SAMPLES ? sumNs / count / 1_000_000_000.0 : Double.NaN;
        }
    }
}
