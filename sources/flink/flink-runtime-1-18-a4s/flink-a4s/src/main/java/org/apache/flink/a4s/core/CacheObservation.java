/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"). You may not use this file except in compliance
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

package org.apache.flink.a4s.core;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Versioned observation shared by the runtime and the autoscaler. */
public final class CacheObservation {
    public static final String METRIC_NAME = "a4s-cache-observation";
    public static final int VERSION = 1;
    public static final int ALIGNED_VERSION = 2;
    public static final long MAX_ACQUISITION_SKEW_NANOS = 100_000_000L;
    public static final long MAX_AGE_MILLIS = 30_000;
    public static final long MAX_CLOCK_SKEW_MILLIS = 5_000;
    public static final String QUICK_MRC = "quickmrc-resident-lookup-v1";
    /** Historical QuickMRC label. It did not attest actual fixed entry charges. */
    public static final String FIXED_4K = "fixed-4096-byte-item";
    public static final String ROUNDED_4K_CHARGE = "quickmrc-rounded-4k-charge-v1";
    /** Reserved for a byte-charge and eviction-policy attestation. QuickMRC does not emit it. */
    public static final String ATTESTED_BYTE_CHARGE = "attested-native-byte-charge-v1";
    public static final String CACHE_POLICY = "single-shard-rocksdb-lru";

    private static final ObjectMapper JSON =
            JacksonMapperFactory.createObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
                    .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    public static boolean isNominalQuickMrcCharge(String chargePolicy) {
        return ROUNDED_4K_CHARGE.equals(chargePolicy) || FIXED_4K.equals(chargePolicy);
    }

    /** Quality codes appear on the wire as enum names. */
    public enum Status {
        VALID,
        ABSENT,
        CLOSED,
        WARMUP,
        IDLE,
        SHARED_CACHE,
        UNSUPPORTED_NATIVE,
        UNSUPPORTED_VERSION,
        STALE,
        INCOMPLETE,
        ATTEMPT_MISMATCH,
        DUPLICATE,
        MALFORMED,
        OVERSIZE,
        INCOMPATIBLE,
        SKEWED,
        CACHE_CHANGED,
        COUNTER_RESET,
        REST_UNAVAILABLE
    }

    public final int version;
    public final String job;
    public final String vertex;
    public final int subtask;
    public final String attempt;
    public final Status status;
    @Nullable public final Window window;
    @Nullable public final StackDistanceHistogram histogram;
    @JsonProperty("endpoint")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable public final Endpoint endpoint;

    @JsonCreator
    public CacheObservation(
            @JsonProperty("version") int version,
            @JsonProperty("job") String job,
            @JsonProperty("vertex") String vertex,
            @JsonProperty("subtask") int subtask,
            @JsonProperty("attempt") String attempt,
            @JsonProperty("status") Status status,
            @JsonProperty("window") @Nullable Window window,
            @JsonProperty("histogram") @Nullable StackDistanceHistogram histogram) {
        this(version, job, vertex, subtask, attempt, status, window, histogram, null);
    }

    public CacheObservation(
            int version, String job, String vertex, int subtask, String attempt, Status status,
            @Nullable Window window, @Nullable StackDistanceHistogram histogram,
            @Nullable Endpoint endpoint) {
        this.version = version;
        this.job = Objects.requireNonNull(job);
        this.vertex = Objects.requireNonNull(vertex);
        this.subtask = subtask;
        this.attempt = Objects.requireNonNull(attempt);
        this.status = Objects.requireNonNull(status);
        this.window = window;
        this.histogram = histogram;
        this.endpoint = endpoint;
    }

    public static boolean supportsVersion(int version) {
        return version == VERSION || version == ALIGNED_VERSION;
    }

    public static boolean isFresh(long observedAtMillis, long nowMillis) {
        return observedAtMillis >= nowMillis - MAX_AGE_MILLIS
                && observedAtMillis <= nowMillis + MAX_CLOCK_SKEW_MILLIS;
    }

    public CacheObservation withoutHistogram() {
        return histogram == null
                ? this
                : new CacheObservation(version, job, vertex, subtask, attempt, status, window, null, endpoint);
    }

    public CacheObservation withoutEndpoint() {
        return endpoint == null ? this
                : new CacheObservation(version, job, vertex, subtask, attempt, status, window, histogram);
    }

    /** Cumulative cache data and the task/DB read acquired at the same boundary. */
    public static final class Endpoint {
        public final long ownershipGeneration;
        public final long acquisitionStartNanos;
        public final long nativeNanos;
        public final long acquisitionEndNanos;
        public final long totalLookups;
        public final long totalMisses;
        public final StackDistanceHistogram cumulativeHistogram;
        public final String timing;

        @JsonCreator
        public Endpoint(
                @JsonProperty("ownershipGeneration") long ownershipGeneration,
                @JsonProperty("acquisitionStartNanos") long acquisitionStartNanos,
                @JsonProperty("nativeNanos") long nativeNanos,
                @JsonProperty("acquisitionEndNanos") long acquisitionEndNanos,
                @JsonProperty("totalLookups") long totalLookups,
                @JsonProperty("totalMisses") long totalMisses,
                @JsonProperty("cumulativeHistogram") StackDistanceHistogram cumulativeHistogram,
                @JsonProperty("timing") String timing) {
            this.ownershipGeneration = ownershipGeneration;
            this.acquisitionStartNanos = acquisitionStartNanos;
            this.nativeNanos = nativeNanos;
            this.acquisitionEndNanos = acquisitionEndNanos;
            this.totalLookups = totalLookups;
            this.totalMisses = totalMisses;
            this.cumulativeHistogram = Objects.requireNonNull(cumulativeHistogram);
            this.timing = Objects.requireNonNull(timing);
        }

        public boolean withinAcquisitionBound() {
            long before = nativeNanos - acquisitionStartNanos;
            long after = acquisitionEndNanos - nativeNanos;
            return before >= 0 && after >= 0 && before <= MAX_ACQUISITION_SKEW_NANOS
                    && after <= MAX_ACQUISITION_SKEW_NANOS - before;
        }
    }

    public String toMetricString() {
        try {
            return JSON.writeValueAsString(this);
        } catch (Exception error) {
            throw new IllegalArgumentException("Could not encode cache observation", error);
        }
    }

    public static CacheObservation fromMetricString(String raw) {
        try {
            return Objects.requireNonNull(JSON.readValue(raw, CacheObservation.class));
        } catch (Exception error) {
            throw new IllegalArgumentException("Invalid cache observation", error);
        }
    }

    /** Monotonic times belong to one process. Wall time is used for freshness and skew only. */
    public static final class Window {
        public final String process;
        public final long cacheId;
        public final long epoch;
        public final long sequence;
        public final List<Long> owners;
        public final String population;
        public final String chargePolicy;
        public final String cachePolicy;
        public final double samplingRate;
        public final long capacityBytes;
        public final long startNanos;
        public final long endNanos;
        public final long observedAtMillis;
        public final long references;
        public final long misses;

        @JsonCreator
        public Window(
                @JsonProperty("process") String process,
                @JsonProperty("cacheId") long cacheId,
                @JsonProperty("epoch") long epoch,
                @JsonProperty("sequence") long sequence,
                @JsonProperty("owners") List<Long> owners,
                @JsonProperty("population") String population,
                @JsonProperty("chargePolicy") String chargePolicy,
                @JsonProperty("cachePolicy") String cachePolicy,
                @JsonProperty("samplingRate") double samplingRate,
                @JsonProperty("capacityBytes") long capacityBytes,
                @JsonProperty("startNanos") long startNanos,
                @JsonProperty("endNanos") long endNanos,
                @JsonProperty("observedAtMillis") long observedAtMillis,
                @JsonProperty("references") long references,
                @JsonProperty("misses") long misses) {
            this.process = Objects.requireNonNull(process);
            this.cacheId = cacheId;
            this.epoch = epoch;
            this.sequence = sequence;
            List<Long> copy = new ArrayList<>(owners);
            copy.forEach(Objects::requireNonNull);
            this.owners = Collections.unmodifiableList(copy);
            this.population = Objects.requireNonNull(population);
            this.chargePolicy = Objects.requireNonNull(chargePolicy);
            this.cachePolicy = Objects.requireNonNull(cachePolicy);
            this.samplingRate = samplingRate;
            this.capacityBytes = capacityBytes;
            this.startNanos = startNanos;
            this.endNanos = endNanos;
            this.observedAtMillis = observedAtMillis;
            this.references = references;
            this.misses = misses;
        }

        public long durationNanos() {
            return endNanos - startNanos;
        }
    }
}
