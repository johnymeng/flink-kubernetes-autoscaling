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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservation.Status;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.a4s.core.StackDistanceHistogram;

import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheObservationAggregatorTest {
    private static final long NOW = 500_000;

    @Test
    void matchesIndependentPythonFixture() throws Exception {
        ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        try (InputStream input = getClass().getResourceAsStream("/a4s/cache-observations-v1.json")) {
            JsonNode fixture = mapper.readTree(input);
            List<CacheObservation> observations = new ArrayList<>();
            for (JsonNode raw : fixture.get("observations")) {
                observations.add(mapper.treeToValue(raw, CacheObservation.class));
            }
            A4SCacheObservationAggregator.Result result = aggregate(observations);
            assertEquals(Status.VALID, result.status);
            assertEquals(fixture.get("curve").size(), result.curve.getPoints().size());
            for (int i = 0; i < result.curve.getPoints().size(); i++) {
                JsonNode expected = fixture.get("curve").get(i);
                MissRateCurve.Point actual = result.curve.getPoints().get(i);
                assertEquals(expected.get("cacheSizeBytes").asLong(), actual.getCacheSizeBytes());
                assertEquals(expected.get("missRate").asDouble(), actual.getMissRate(), 1e-12);
            }
        }
    }

    @Test
    void wireLabelsNominalGeometryAndWithholdsUnknownOrUnattestedAxes() throws Exception {
        CacheObservation a = observation(0, "a", 1, 100, NOW, new long[] {1, 1, 2}, 0);
        CacheObservation b = observation(1, "b", 2, 120, NOW, new long[] {3, 0}, 1);
        MissRateCurve curve = aggregate(Arrays.asList(a, b)).curve;
        assertEquals(MissRateCurve.CapacityAxis.NOMINAL_ROUNDED_4K, curve.getCapacityAxis());

        ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        ObjectNode wire = mapper.valueToTree(new A4SAggregatedMetricsResponseBody(curve));
        assertTrue(wire.get("scaledMrc").isArray());
        assertEquals("NOMINAL_ROUNDED_4K", wire.get(MissRateCurve.FIELD_NAME_CAPACITY_AXIS).asText());
        assertEquals(MissRateCurve.CapacityAxis.NOMINAL_ROUNDED_4K,
                mapper.treeToValue(wire, A4SAggregatedMetricsResponseBody.class)
                        .getScaledMrc().getCapacityAxis());
        wire.remove(MissRateCurve.FIELD_NAME_CAPACITY_AXIS);
        assertEquals(MissRateCurve.CapacityAxis.UNVERIFIED,
                mapper.treeToValue(wire, A4SAggregatedMetricsResponseBody.class)
                        .getScaledMrc().getCapacityAxis());
        wire.put(MissRateCurve.FIELD_NAME_CAPACITY_AXIS, "UNKNOWN_FUTURE_SCHEMA");
        assertEquals(MissRateCurve.CapacityAxis.UNVERIFIED,
                mapper.treeToValue(wire, A4SAggregatedMetricsResponseBody.class)
                        .getScaledMrc().getCapacityAxis());
        wire.put(MissRateCurve.FIELD_NAME_CAPACITY_AXIS, "ATTESTED_NATIVE_BYTES");
        assertEquals(MissRateCurve.CapacityAxis.UNVERIFIED,
                mapper.treeToValue(wire, A4SAggregatedMetricsResponseBody.class)
                        .getScaledMrc().getCapacityAxis());
    }

    @Test
    void equalDurationsMatchCountWeightsAndUseCommonByteSupport() {
        CacheObservation a = observation(0, "a", 1, 100, NOW, new long[] {1, 1, 2}, 0);
        CacheObservation b = observation(1, "b", 2, 120, NOW, new long[] {3, 0}, 1);
        A4SCacheObservationAggregator.Result result = aggregate(Arrays.asList(a, b));
        assertEquals(Status.VALID, result.status);
        assertEquals(2, result.curve.getPoints().size());
        assertEquals(8192, result.curve.getPoints().get(0).getCacheSizeBytes());
        assertEquals(16384, result.curve.getPoints().get(1).getCacheSizeBytes());
        assertEquals((100 * .75 + 120 * .25) / 220, result.curve.getPoints().get(0).getMissRate(), 1e-12);
        assertEquals((100 * .5 + 120 * .25) / 220, result.curve.getPoints().get(1).getMissRate(), 1e-12);
    }

    @ParameterizedTest
    @CsvSource({"10, 15", "15, 10"})
    void weightsEqualLookupRatesEquallyAcrossDifferentDurations(long durationA, long durationB) {
        CacheObservation a = exactObservation(0, NOW, durationA, 9 * durationA, durationA);
        CacheObservation b = exactObservation(1, NOW, durationB, durationB, 9 * durationB);
        A4SCacheObservationAggregator.Result result = aggregate(Arrays.asList(a, b));

        assertEquals(Status.VALID, result.status);
        assertEquals(1, result.curve.getPoints().size());
        MissRateCurve.Point point = result.curve.getPoints().get(0);
        assertEquals(8192, point.getCacheSizeBytes());
        assertEquals(0.5, point.getMissRate(), 1e-12);
        double countWeighted = (0.1 * durationA + 0.9 * durationB) / (durationA + durationB);
        assertNotEquals(countWeighted, point.getMissRate(), 1e-12);
    }

    @ParameterizedTest
    @CsvSource({"10, 16, 0", "16, 10, 6000"})
    void rejectsExcessiveStartOrEndSkew(long durationA, long durationB, long endSkewMillis) {
        // In the first case only the starts differ. In the second only the ends differ.
        CacheObservation a = exactObservation(0, NOW, durationA, 9 * durationA, durationA);
        CacheObservation b =
                exactObservation(1, NOW - endSkewMillis, durationB, durationB, 9 * durationB);
        A4SCacheObservationAggregator.Result result = aggregate(Arrays.asList(a, b));

        assertEquals(Status.SKEWED, result.status);
        assertTrue(result.curve.getPoints().isEmpty());
    }

    @Test
    void rejectsIncompleteDuplicateStaleAndReplacedAttempts() {
        CacheObservation first = observation(0, "a", 1, 100, NOW, new long[] {1}, 3);
        assertEquals(Status.INCOMPLETE, aggregate(Collections.singletonList(first)).status);
        CacheObservation duplicate = observation(1, "b", 1, 100, NOW, new long[] {2}, 3);
        assertEquals(Status.DUPLICATE, aggregate(Arrays.asList(first, duplicate)).status);
        CacheObservation stale = observation(1, "b", 2, 100, NOW - 31_000, new long[] {2}, 3);
        assertEquals(Status.STALE, aggregate(Arrays.asList(first, stale)).status);
        CacheObservation replaced = observation(1, "previous", 2, 100, NOW, new long[] {2}, 3);
        assertEquals(Status.ATTEMPT_MISMATCH, aggregate(Arrays.asList(first, replaced)).status);
        CacheObservation skewed = observation(1, "b", 2, 200, NOW, new long[] {2}, 3);
        A4SCacheObservationAggregator.Result skewedResult = aggregate(Arrays.asList(first, skewed));
        assertEquals(Status.SKEWED, skewedResult.status);
        assertTrue(skewedResult.curve.getPoints().isEmpty());
    }

    @Test
    void rejectsOverflowingExactReferenceTotals() {
        long references = Long.MAX_VALUE / 2 + 1;
        CacheObservation a = observation(0, "a", 1, references, NOW, new long[] {1}, 1);
        CacheObservation b = observation(1, "b", 2, references, NOW, new long[] {1}, 1);

        assertEquals(Status.MALFORMED, aggregate(Arrays.asList(a, b)).status);
    }

    private A4SCacheObservationAggregator.Result aggregate(List<CacheObservation> observations) {
        Map<Integer, String> expected = new HashMap<>();
        expected.put(0, "a");
        expected.put(1, "b");
        return A4SCacheObservationAggregator.aggregate(
                "job", "vertex", expected, observations, NOW);
    }

    private CacheObservation observation(
            int subtask,
            String attempt,
            long cacheId,
            long references,
            long observedAt,
            long[] bins,
            long completeMisses) {
        return new CacheObservation(
                CacheObservation.VERSION,
                "job",
                "vertex",
                subtask,
                attempt,
                Status.VALID,
                new CacheObservation.Window(
                        "process", cacheId, 0, 2, Collections.singletonList(1L), CacheObservation.QUICK_MRC,
                        CacheObservation.FIXED_4K, CacheObservation.CACHE_POLICY, .1, 8192,
                        100, 10_000_000_100L, observedAt, references, references / 4),
                new StackDistanceHistogram(bins, completeMisses, 1, 1, 4096));
    }

    private CacheObservation exactObservation(
            int subtask, long observedAt, long durationSeconds, long hits, long misses) {
        return new CacheObservation(
                CacheObservation.VERSION,
                "job",
                "vertex",
                subtask,
                subtask == 0 ? "a" : "b",
                Status.VALID,
                new CacheObservation.Window(
                        "process", subtask + 1L, 0, 2, Collections.singletonList(1L),
                        CacheObservation.QUICK_MRC, CacheObservation.FIXED_4K,
                        CacheObservation.CACHE_POLICY, 1, 8192, 100,
                        100 + TimeUnit.SECONDS.toNanos(durationSeconds), observedAt,
                        hits + misses, misses),
                new StackDistanceHistogram(new long[] {hits}, misses, 1, 1, 4096));
    }
}
