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

package io.a4s.core;

import static org.junit.jupiter.api.Assertions.*;

import io.a4s.api.StackDistanceHistogram;
import io.a4s.api.StackDistanceHistogram.Counters;
import io.a4s.api.StackDistanceHistogram.Estimator;
import java.util.List;
import org.junit.jupiter.api.Test;

class MissRateCurvesTest {
    @Test
    void preservesColdMissesAndCapacitySupport() {
        var one = histogram(Counters.INTERVAL, "run", 10, List.of(2L, 4L), 2);
        var two = histogram(Counters.INTERVAL, "run", 10, List.of(2L, 2L), 4);
        var points = MissRateCurves.fromHistogram(MissRateCurves.merge(one, two));
        assertEquals(32, points.get(0).capacityBytes);
        assertEquals(64, points.get(1).capacityBytes);
        assertEquals(0.75, points.get(0).missRate);
        assertEquals(0.375, points.get(1).missRate);
        assertEquals(0.5625, MissRateCurves.interpolate(points, 48).orElseThrow());
        assertTrue(MissRateCurves.interpolate(points, 65).isEmpty());
        assertTrue(MissRateCurves.supportedCapacity(points, 0.37).isEmpty());
        assertEquals(64, MissRateCurves.supportedCapacity(points, 0.4).orElseThrow());
    }

    @Test
    void rejectsMismatchesResetsAndOverflow() {
        var prior = histogram(Counters.CUMULATIVE, "old", 10, List.of(5L, 4L), 2);
        var next = histogram(Counters.CUMULATIVE, "old", 20, List.of(6L, 4L), 4);
        var reset = histogram(Counters.CUMULATIVE, "new", 20, List.of(6L, 4L), 4);
        assertTrue(MissRateCurves.fromHistogram(prior).isEmpty());
        var interval = MissRateCurves.delta(prior, next);
        assertEquals(10, interval.startMillis);
        assertEquals(List.of(1L, 0L), interval.frequencies);
        assertEquals(2, interval.missesOutsideBuckets);
        assertThrows(IllegalArgumentException.class, () -> MissRateCurves.delta(prior, reset));
        assertThrows(IllegalArgumentException.class, () -> MissRateCurves.delta(next, prior));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MissRateCurves.delta(
                                prior,
                                histogram(Counters.CUMULATIVE, "old", 20, List.of(4L, 4L), 4)));
        assertThrows(IllegalArgumentException.class, () -> MissRateCurves.merge(prior, interval));
        assertThrows(
                ArithmeticException.class,
                () -> histogram(Counters.INTERVAL, "run", 10, List.of(Long.MAX_VALUE, 1L), 0));
        assertTrue(
                MissRateCurves.fromHistogram(
                                histogram(Counters.INTERVAL, "run", 10, List.of(0L, 0L), 0))
                        .isEmpty());
    }

    private static StackDistanceHistogram histogram(
            Counters type, String generation, long end, List<Long> buckets, long cold) {
        return new StackDistanceHistogram(
                Estimator.SHARDS,
                type,
                "user-data-blocks",
                "isolated",
                generation,
                "rate=1/4,seed=17",
                0,
                end,
                1,
                List.of(16L, 32L),
                buckets,
                cold);
    }
}
