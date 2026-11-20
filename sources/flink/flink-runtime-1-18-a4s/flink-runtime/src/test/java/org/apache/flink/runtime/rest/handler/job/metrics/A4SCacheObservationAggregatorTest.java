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
import org.apache.flink.a4s.core.StackDistanceHistogram;

import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheObservationAggregatorTest {
    private static final long NOW = 500_000;

    @Test
    void matchesIndependentPythonFixture() throws Exception {
        var mapper = RestMapperUtils.getStrictObjectMapper();
        try (var input = getClass().getResourceAsStream("/a4s/cache-observations-v1.json")) {
            var fixture = mapper.readTree(input);
            List<CacheObservation> observations = new ArrayList<>();
            for (var raw : fixture.get("observations")) {
                observations.add(mapper.treeToValue(raw, CacheObservation.class));
            }
            var result = aggregate(observations);
            assertEquals(Status.VALID, result.status);
            assertEquals(fixture.get("curve").size(), result.curve.getPoints().size());
            for (int i = 0; i < result.curve.getPoints().size(); i++) {
                var expected = fixture.get("curve").get(i);
                var actual = result.curve.getPoints().get(i);
                assertEquals(expected.get("cacheSizeBytes").asLong(), actual.getCacheSizeBytes());
                assertEquals(expected.get("missRate").asDouble(), actual.getMissRate(), 1e-12);
            }
        }
    }

    @Test
    void weightsLocalRatiosByReferencesAndUsesCommonByteSupport() {
        var a = observation(0, "a", 1, 100, NOW, new long[] {1, 1, 2}, 0);
        var b = observation(1, "b", 2, 120, NOW, new long[] {3, 0}, 1);
        var result = aggregate(List.of(a, b));
        assertEquals(Status.VALID, result.status);
        assertEquals(2, result.curve.getPoints().size());
        assertEquals(8192, result.curve.getPoints().get(0).getCacheSizeBytes());
        assertEquals((100 * .75 + 120 * .25) / 220, result.curve.getPoints().get(0).getMissRate(), 1e-12);
        assertEquals((100 * .5 + 120 * .25) / 220, result.curve.getPoints().get(1).getMissRate(), 1e-12);
    }

    @Test
    void rejectsIncompleteDuplicateStaleAndReplacedAttempts() {
        var first = observation(0, "a", 1, 100, NOW, new long[] {1}, 3);
        assertEquals(Status.INCOMPLETE, aggregate(List.of(first)).status);
        var duplicate = observation(1, "b", 1, 100, NOW, new long[] {2}, 3);
        assertEquals(Status.DUPLICATE, aggregate(List.of(first, duplicate)).status);
        var stale = observation(1, "b", 2, 100, NOW - 31_000, new long[] {2}, 3);
        assertEquals(Status.STALE, aggregate(List.of(first, stale)).status);
        var replaced = observation(1, "previous", 2, 100, NOW, new long[] {2}, 3);
        assertEquals(Status.ATTEMPT_MISMATCH, aggregate(List.of(first, replaced)).status);
        var skewed = observation(1, "b", 2, 200, NOW, new long[] {2}, 3);
        var skewedResult = aggregate(List.of(first, skewed));
        assertEquals(Status.SKEWED, skewedResult.status);
        assertTrue(skewedResult.curve.getPoints().isEmpty());
    }

    private A4SCacheObservationAggregator.Result aggregate(List<CacheObservation> observations) {
        return A4SCacheObservationAggregator.aggregate(
                "job", "vertex", Map.of(0, "a", 1, "b"), observations, NOW);
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
                        "process", cacheId, 0, 2, List.of(1L), CacheObservation.QUICK_MRC,
                        CacheObservation.FIXED_4K, CacheObservation.CACHE_POLICY, .1, 8192,
                        100, 10_000_000_100L, observedAt, references, references / 4),
                new StackDistanceHistogram(bins, completeMisses, 1, 1, 4096));
    }
}
