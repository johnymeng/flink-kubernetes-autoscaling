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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JustinSlotMatching;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestSlotInfo;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests physical slot accounting with controlled inventories. */
class NativeRescalePreparationPlannerTest {

    private static final ResourceProfile SMALL =
            ResourceProfile.newBuilder().setCpuCores(1).setManagedMemoryMB(256).build();
    private static final ResourceProfile LARGE =
            ResourceProfile.newBuilder().setCpuCores(1).setManagedMemoryMB(512).build();

    @Test
    void matchesFreeSlotsWithoutAddingThemAgainToDeclaredDemand() {
        SlotInfo occupied = slot(SMALL);
        SlotInfo free = slot(SMALL);
        SlotInfo incompatible = slot(LARGE);
        ResourceCounter target = ResourceCounter.withResource(SMALL, 4);
        NativeRescalePreparationPlanner.Plan plan =
                NativeRescalePreparationPlanner.plan(
                        ResourceCounter.withResource(SMALL, 1),
                        target,
                        Map.of(occupied.getAllocationId(), SMALL),
                        List.of(occupied, free, incompatible, occupied),
                        new Configuration());

        assertThat(plan.fallback).isNull();
        assertThat(plan.reusable).containsExactly(occupied.getAllocationId());
        assertThat(plan.free).containsExactly(free.getAllocationId());
        assertThat(plan.missing).isEqualTo(ResourceCounter.withResource(SMALL, 2));
        assertThat(plan.demand).isEqualTo(target);
    }

    @Test
    void keepsExactMatchesForHeterogeneousTargetsBeforeWildcardsAndUnknown() {
        SlotInfo small = slot(SMALL);
        SlotInfo large = slot(LARGE);
        SlotInfo any = slot(ResourceProfile.ANY);
        ResourceCounter target =
                ResourceCounter.withResource(SMALL, 1)
                        .add(LARGE, 1)
                        .add(ResourceProfile.UNKNOWN, 1);

        JustinSlotMatching.Match match =
                JustinSlotMatching.match(target, List.of(any, large, small, small));

        assertThat(match.getMissing().isEmpty()).isTrue();
        assertThat(match.getAllocations())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                small.getAllocationId(), SMALL,
                                large.getAllocationId(), LARGE,
                                any.getAllocationId(), ResourceProfile.UNKNOWN));
    }

    @Test
    void rejectsTheCircularVerticalDependency() {
        SlotInfo first = slot(SMALL);
        SlotInfo second = slot(SMALL);
        NativeRescalePreparationPlanner.Plan plan =
                NativeRescalePreparationPlanner.plan(
                        ResourceCounter.withResource(SMALL, 2),
                        ResourceCounter.withResource(LARGE, 1),
                        Map.of(first.getAllocationId(), SMALL, second.getAllocationId(), SMALL),
                        List.of(first, second),
                        new Configuration());

        assertThat(plan.fallback).isEqualTo("occupied-allocation-profile-mismatch");
        assertThat(plan.incompatible)
                .containsExactlyInAnyOrder(first.getAllocationId(), second.getAllocationId());
        assertThat(plan.missing).isEqualTo(ResourceCounter.withResource(LARGE, 1));
    }

    @Test
    void rejectsDemandBeyondAConfiguredResourceLimit() {
        SlotInfo occupied = slot(SMALL);
        NativeRescalePreparationPlanner.Plan plan =
                NativeRescalePreparationPlanner.plan(
                        ResourceCounter.withResource(SMALL, 1),
                        ResourceCounter.withResource(SMALL, 2),
                        Map.of(occupied.getAllocationId(), SMALL),
                        List.of(occupied),
                        new Configuration().set(ResourceManagerOptions.MAX_SLOT_NUM, 1));

        assertThat(plan.fallback).isEqualTo("configured-resource-limit");
    }

    @Test
    void checksConfiguredCpuAndMemoryHeadroomSeparately() {
        final SlotInfo occupied = slot(SMALL);
        final ResourceCounter applied = ResourceCounter.withResource(SMALL, 1);
        final ResourceCounter target = ResourceCounter.withResource(SMALL, 2);
        final Map<AllocationID, ResourceProfile> occupants =
                Map.of(occupied.getAllocationId(), SMALL);

        for (Configuration limits :
                List.of(
                        new Configuration().set(ResourceManagerOptions.MAX_TOTAL_CPU, 1.5),
                        new Configuration()
                                .set(
                                        ResourceManagerOptions.MAX_TOTAL_MEM,
                                        MemorySize.ofMebiBytes(511)))) {
            assertThat(
                            NativeRescalePreparationPlanner.plan(
                                            applied, target, occupants, List.of(occupied), limits)
                                    .fallback)
                    .isEqualTo("configured-resource-limit");
        }
    }

    private static SlotInfo slot(ResourceProfile profile) {
        return new TestSlotInfo(profile);
    }
}
