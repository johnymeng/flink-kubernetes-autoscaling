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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OnlineCacheCostTest {

    @Test
    void convertsDifferenceInHitAndMissTimesToIncrementalCostPerRecord() {
        var costs = OnlineCacheCost.from(List.of(start()), List.of(end())).orElseThrow();

        assertEquals(0.000100, costs.getZeroMissSecondsPerRecord(), 1e-12);
        assertEquals(0.000040, costs.getMissPenaltySecondsPerRecord(), 1e-12);
    }

    @Test
    void accountsForCompactionWithoutChargingItsTimeToTaskRecords() {
        var previous = start();
        previous.setTimingAbi(3);
        var current = end();
        current.setTimingAbi(3);
        current.setCompactionHits(1_000);
        current.setCompactionMisses(3_000);
        current.setCoveredHits(4_100);
        current.setCoveredMisses(2_100);

        var cost = OnlineCacheCost.from(List.of(previous), List.of(current)).orElseThrow();

        // Foreground costs an extra 20 us, but only 2,000 of 5,000 misses were
        // foreground. QuickMRC models all lookups, including compaction.
        assertEquals(0.000112, cost.getZeroMissSecondsPerRecord(), 1e-12);
        assertEquals(0.000016, cost.getMissPenaltySecondsPerRecord(), 1e-12);

        current.setCompactionMisses(5_001);
        assertTrue(OnlineCacheCost.from(List.of(previous), List.of(current)).isEmpty());
        current.setCompactionMisses(1_000);
        assertTrue(OnlineCacheCost.from(List.of(previous), List.of(current)).isEmpty());
        current.setCompactionMisses(3_000);
        current.setTimingAbi(2);
        assertTrue(OnlineCacheCost.from(List.of(previous), List.of(current)).isEmpty());
    }

    @Test
    void rejectsIncompatibleGenerationsAndUncoveredMisses() {
        var previous = List.of(start());
        var current = end();
        current.setAttempt("restarted");
        assertTrue(OnlineCacheCost.from(previous, List.of(current)).isEmpty());
        current.setAttempt("original");
        current.setCacheEpoch(2);
        assertTrue(OnlineCacheCost.from(previous, List.of(current)).isEmpty());
        current.setCacheEpoch(1);
        current.setCoveredMisses(700);
        assertTrue(OnlineCacheCost.from(previous, List.of(current)).isEmpty());
    }

    @Test
    void requiresEnoughNewMissTimingsAfterTheWindowStarts() {
        var current = end();
        current.setSampledMisses(50);
        assertTrue(OnlineCacheCost.from(List.of(start()), List.of(current)).isEmpty());
    }

    @Test
    void usesTaskManagerObservationTimeToRejectStaleAndTooShortWindows() {
        var current = end();
        current.setObservedAtMillis(60_999);
        assertTrue(OnlineCacheCost.from(List.of(start()), List.of(current)).isEmpty());
        current.setObservedAtMillis(121_001);
        assertTrue(OnlineCacheCost.from(List.of(start()), List.of(current)).isEmpty());
    }

    private static CacheCostSample start() {
        return new CacheCostSample(
                0, "original", 987, 524288, 1, 200, 100, 100, 100,
                10, 10, 50_000, 100_000, 500, 10, 1_000, 2, 0, 0);
    }

    private static CacheCostSample end() {
        return new CacheCostSample(
                0, "original", 987, 524288, 1, 10_200, 5_100, 5_100, 5_100,
                110, 110, 150_000, 2_200_000, 5_500, 610, 61_000, 2, 0, 0);
    }
}
