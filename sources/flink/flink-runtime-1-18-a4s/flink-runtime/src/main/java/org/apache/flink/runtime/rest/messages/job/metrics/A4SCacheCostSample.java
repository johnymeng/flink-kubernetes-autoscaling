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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Cumulative counters for one attempt and one exclusive physical RocksDB cache. */
public final class A4SCacheCostSample {
    public static final String METRIC_NAME = "a4s-cache-cost-sample";
    public static final String COMPACTION_QUERY_SELECTOR = "cacheCostCompaction";

    private static final long MAX_AGE_MILLIS = 30_000L;
    private static final long MAX_CLOCK_SKEW_MILLIS = 5_000L;

    public final int subtask;
    public final String attempt;
    public final long cacheId;
    public final long capacityBytes;
    public final long cacheEpoch;
    public final long lookups;
    public final long misses;
    public final long coveredHits;
    public final long coveredMisses;
    public final long sampledHits;
    public final long sampledMisses;
    public final long hitNanos;
    public final long missNanos;
    public final long recordsIn;
    public final double busyMillis;
    public final long observedAtMillis;

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final Integer timingAbi;

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final Long compactionHits;

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final Long compactionMisses;

    @JsonCreator
    public A4SCacheCostSample(
            @JsonProperty("subtask") int subtask,
            @JsonProperty("attempt") String attempt,
            @JsonProperty("cacheId") long cacheId,
            @JsonProperty("capacityBytes") long capacityBytes,
            @JsonProperty("cacheEpoch") long cacheEpoch,
            @JsonProperty("lookups") long lookups,
            @JsonProperty("misses") long misses,
            @JsonProperty("coveredHits") long coveredHits,
            @JsonProperty("coveredMisses") long coveredMisses,
            @JsonProperty("sampledHits") long sampledHits,
            @JsonProperty("sampledMisses") long sampledMisses,
            @JsonProperty("hitNanos") long hitNanos,
            @JsonProperty("missNanos") long missNanos,
            @JsonProperty("recordsIn") long recordsIn,
            @JsonProperty("busyMillis") double busyMillis,
            @JsonProperty("observedAtMillis") long observedAtMillis,
            @JsonProperty("timingAbi") @Nullable Integer timingAbi,
            @JsonProperty("compactionHits") @Nullable Long compactionHits,
            @JsonProperty("compactionMisses") @Nullable Long compactionMisses) {
        this.subtask = subtask;
        this.attempt = attempt;
        this.cacheId = cacheId;
        this.capacityBytes = capacityBytes;
        this.cacheEpoch = cacheEpoch;
        this.lookups = lookups;
        this.misses = misses;
        this.coveredHits = coveredHits;
        this.coveredMisses = coveredMisses;
        this.sampledHits = sampledHits;
        this.sampledMisses = sampledMisses;
        this.hitNanos = hitNanos;
        this.missNanos = missNanos;
        this.recordsIn = recordsIn;
        this.busyMillis = busyMillis;
        this.observedAtMillis = observedAtMillis;
        this.timingAbi = timingAbi;
        this.compactionHits = compactionHits;
        this.compactionMisses = compactionMisses;
    }

    public String toMetricString() {
        boolean includesClassification = compactionHits != null && compactionMisses != null;
        String snapshot = (includesClassification ? "2:" : "1:")
                + observedAtMillis
                + ":" + attempt
                + ":" + cacheId
                + ":" + capacityBytes
                + ":" + cacheEpoch
                + ":" + lookups
                + ":" + misses
                + ":" + coveredHits
                + ":" + coveredMisses
                + ":" + sampledHits
                + ":" + sampledMisses
                + ":" + hitNanos
                + ":" + missNanos
                + ":" + recordsIn
                + ":" + busyMillis;
        return includesClassification
                ? snapshot + ":" + compactionHits + ":" + compactionMisses
                : snapshot;
    }

    @Nullable
    public static A4SCacheCostSample fromMetricString(
            int subtask, String raw, long currentTimeMillis, boolean includeClassification) {
        String[] fields = raw.split(":", -1);
        boolean abi2 = fields.length == 16 && "1".equals(fields[0]);
        boolean abi3 = fields.length == 18 && "2".equals(fields[0]);
        if ((!abi2 && !abi3) || fields[2].isEmpty()) {
            return null;
        }
        try {
            long observedAtMillis = Long.parseLong(fields[1]);
            if (observedAtMillis < currentTimeMillis - MAX_AGE_MILLIS
                    || observedAtMillis > currentTimeMillis + MAX_CLOCK_SKEW_MILLIS) {
                return null;
            }
            Long compactionHits = abi3 ? Long.parseLong(fields[16]) : null;
            Long compactionMisses = abi3 ? Long.parseLong(fields[17]) : null;
            if (abi3 && (compactionHits < 0 || compactionMisses < 0)) {
                return null;
            }
            A4SCacheCostSample sample =
                    new A4SCacheCostSample(
                            subtask,
                            fields[2],
                            Long.parseLong(fields[3]),
                            Long.parseLong(fields[4]),
                            Long.parseLong(fields[5]),
                            Long.parseLong(fields[6]),
                            Long.parseLong(fields[7]),
                            Long.parseLong(fields[8]),
                            Long.parseLong(fields[9]),
                            Long.parseLong(fields[10]),
                            Long.parseLong(fields[11]),
                            Long.parseLong(fields[12]),
                            Long.parseLong(fields[13]),
                            Long.parseLong(fields[14]),
                            Double.parseDouble(fields[15]),
                            observedAtMillis,
                            includeClassification ? (abi3 ? 3 : 2) : null,
                            includeClassification ? compactionHits : null,
                            includeClassification ? compactionMisses : null);
            return sample.isValid() ? sample : null;
        } catch (NumberFormatException malformedMetric) {
            return null;
        }
    }

    private boolean isValid() {
        return cacheId > 0
                && capacityBytes > 0
                && cacheEpoch >= 0
                && misses >= 0
                && lookups >= misses
                && sampledHits >= 0
                && coveredHits >= sampledHits
                && sampledMisses >= 0
                && coveredMisses >= sampledMisses
                && hitNanos >= sampledHits
                && missNanos >= sampledMisses
                && recordsIn >= 0
                && Double.isFinite(busyMillis)
                && busyMillis >= 0;
    }
}
