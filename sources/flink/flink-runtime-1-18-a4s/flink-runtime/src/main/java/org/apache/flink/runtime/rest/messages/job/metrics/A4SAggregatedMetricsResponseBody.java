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

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.MissRateCurveObservation;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** REST response carrying a measured A4S curve or an availability status. */
public class A4SAggregatedMetricsResponseBody extends MissRateCurveObservation
        implements ResponseBody {

    public static final String FIELD_NAME_OBSERVED_BACKLOG = "observedSourceBacklogRecords";

    public static final String FIELD_NAME_CACHE_CAPACITY = "observedBlockCacheCapacityBytes";

    public static final String FIELD_NAME_CACHE_USAGE = "observedBlockCacheUsageBytes";

    public static final String FIELD_NAME_CACHE_COUNTERS = "observedBlockCacheCounters";

    public static final String FIELD_NAME_STATE_SIZE = "observedStateSizeBytes";

    public static final String FIELD_NAME_TASK_TIMES = "observedTaskTimes";

    @Nullable private final Long observedSourceBacklogRecords;
    @Nullable private final Long observedBlockCacheCapacityBytes;
    @Nullable private final Long observedBlockCacheUsageBytes;
    @Nullable private final CacheCounters observedBlockCacheCounters;
    @Nullable private final Long observedStateSizeBytes;
    @Nullable private final TaskTimeTotals observedTaskTimes;

    public A4SAggregatedMetricsResponseBody(@Nullable MissRateCurve scaledMrc) {
        this(null, scaledMrc);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status, @Nullable MissRateCurve scaledMrc) {
        this(status, scaledMrc, null);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status,
            @Nullable MissRateCurve scaledMrc,
            @Nullable Long observedSourceBacklogRecords) {
        this(status, scaledMrc, observedSourceBacklogRecords, null);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status,
            @Nullable MissRateCurve scaledMrc,
            @Nullable Long observedSourceBacklogRecords,
            @Nullable Long observedBlockCacheCapacityBytes) {
        this(
                status,
                scaledMrc,
                observedSourceBacklogRecords,
                observedBlockCacheCapacityBytes,
                null);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status,
            @Nullable MissRateCurve scaledMrc,
            @Nullable Long observedSourceBacklogRecords,
            @Nullable Long observedBlockCacheCapacityBytes,
            @Nullable Long observedBlockCacheUsageBytes) {
        this(
                status,
                scaledMrc,
                observedSourceBacklogRecords,
                observedBlockCacheCapacityBytes,
                observedBlockCacheUsageBytes,
                null);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status,
            @Nullable MissRateCurve scaledMrc,
            @Nullable Long observedSourceBacklogRecords,
            @Nullable Long observedBlockCacheCapacityBytes,
            @Nullable Long observedBlockCacheUsageBytes,
            @Nullable CacheCounters observedBlockCacheCounters) {
        this(
                status,
                scaledMrc,
                observedSourceBacklogRecords,
                observedBlockCacheCapacityBytes,
                observedBlockCacheUsageBytes,
                observedBlockCacheCounters,
                null);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status,
            @Nullable MissRateCurve scaledMrc,
            @Nullable Long observedSourceBacklogRecords,
            @Nullable Long observedBlockCacheCapacityBytes,
            @Nullable Long observedBlockCacheUsageBytes,
            @Nullable CacheCounters observedBlockCacheCounters,
            @Nullable Long observedStateSizeBytes) {
        this(
                status,
                scaledMrc,
                observedSourceBacklogRecords,
                observedBlockCacheCapacityBytes,
                observedBlockCacheUsageBytes,
                observedBlockCacheCounters,
                observedStateSizeBytes,
                null);
    }

    @JsonCreator
    public A4SAggregatedMetricsResponseBody(
            @JsonProperty(FIELD_NAME_STATUS) @Nullable Status status,
            @JsonProperty(FIELD_NAME_SCALED_MRC) @Nullable MissRateCurve scaledMrc,
            @JsonProperty(FIELD_NAME_OBSERVED_BACKLOG) @Nullable Long observedSourceBacklogRecords,
            @JsonProperty(FIELD_NAME_CACHE_CAPACITY) @Nullable Long observedBlockCacheCapacityBytes,
            @JsonProperty(FIELD_NAME_CACHE_USAGE) @Nullable Long observedBlockCacheUsageBytes,
            @JsonProperty(FIELD_NAME_CACHE_COUNTERS) @Nullable
                    CacheCounters observedBlockCacheCounters,
            @JsonProperty(FIELD_NAME_STATE_SIZE) @Nullable Long observedStateSizeBytes,
            @JsonProperty(FIELD_NAME_TASK_TIMES) @Nullable TaskTimeTotals observedTaskTimes) {
        super(status, scaledMrc);
        this.observedSourceBacklogRecords = observedSourceBacklogRecords;
        this.observedBlockCacheCapacityBytes = observedBlockCacheCapacityBytes;
        this.observedBlockCacheUsageBytes = observedBlockCacheUsageBytes;
        this.observedBlockCacheCounters = observedBlockCacheCounters;
        this.observedStateSizeBytes = observedStateSizeBytes;
        this.observedTaskTimes = observedTaskTimes;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_OBSERVED_BACKLOG)
    @Nullable
    public Long getObservedSourceBacklogRecords() {
        return observedSourceBacklogRecords;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_CACHE_CAPACITY)
    @Nullable
    public Long getObservedBlockCacheCapacityBytes() {
        return observedBlockCacheCapacityBytes;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_CACHE_USAGE)
    @Nullable
    public Long getObservedBlockCacheUsageBytes() {
        return observedBlockCacheUsageBytes;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_CACHE_COUNTERS)
    @Nullable
    public CacheCounters getObservedBlockCacheCounters() {
        return observedBlockCacheCounters;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_STATE_SIZE)
    @Nullable
    public Long getObservedStateSizeBytes() {
        return observedStateSizeBytes;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_TASK_TIMES)
    @Nullable
    public TaskTimeTotals getObservedTaskTimes() {
        return observedTaskTimes;
    }

    /** Cumulative time counters for a complete running task roster. */
    public static final class TaskTimeTotals {
        public final String rosterId;
        public final double busyMillis;
        public final long idleMillis;
        public final long backpressuredMillis;

        @JsonCreator
        public TaskTimeTotals(
                @JsonProperty(value = "rosterId", required = true) String rosterId,
                @JsonProperty(value = "busyMillis", required = true) double busyMillis,
                @JsonProperty(value = "idleMillis", required = true) long idleMillis,
                @JsonProperty(value = "backpressuredMillis", required = true)
                        long backpressuredMillis) {
            this.rosterId = rosterId;
            this.busyMillis = busyMillis;
            this.idleMillis = idleMillis;
            this.backpressuredMillis = backpressuredMillis;
        }
    }

    /** Cumulative database ticker counts in the current attempt roster. */
    public static final class CacheCounters {
        public final long hits;
        public final long misses;

        @JsonCreator
        public CacheCounters(
                @JsonProperty(value = "hits", required = true) long hits,
                @JsonProperty(value = "misses", required = true) long misses) {
            this.hits = hits;
            this.misses = misses;
        }
    }
}
