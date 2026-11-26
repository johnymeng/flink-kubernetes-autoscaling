/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JustinSlotMatching;
import org.apache.flink.runtime.util.ResourceCounter;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Accounts for the two graphs during a compatible horizontal native rescale. */
final class NativeRescalePreparationPlanner {

    private NativeRescalePreparationPlanner() {}

    static Plan plan(
            ResourceCounter applied,
            ResourceCounter target,
            Map<AllocationID, ResourceProfile> occupied,
            Collection<? extends SlotInfo> inventory,
            Configuration configuration) {
        final List<SlotInfo> occupiedSlots = new ArrayList<>();
        final List<SlotInfo> freeSlots = new ArrayList<>();
        final Set<AllocationID> incompatible = new HashSet<>(occupied.keySet());
        ResourceCounter occupiedResources = ResourceCounter.empty();
        for (SlotInfo slot : inventory) {
            final AllocationID id = slot.getAllocationId();
            final ResourceProfile profile = slot.getResourceProfile();
            if (!occupied.containsKey(id)) {
                freeSlots.add(slot);
            } else if (profile.equals(occupied.get(id))
                    && !ResourceProfile.UNKNOWN.equals(profile)
                    && !ResourceProfile.ANY.equals(profile)
                    && incompatible.remove(id)) {
                occupiedSlots.add(slot);
                occupiedResources = occupiedResources.add(profile, 1);
            }
        }
        final JustinSlotMatching.Match reused = JustinSlotMatching.match(target, occupiedSlots);
        final Set<AllocationID> reusable = reused.getAllocations().keySet();
        for (SlotInfo slot : occupiedSlots) {
            if (!reusable.contains(slot.getAllocationId())) {
                incompatible.add(slot.getAllocationId());
            }
        }
        final JustinSlotMatching.Match free =
                JustinSlotMatching.match(reused.getMissing(), freeSlots);
        final ResourceCounter temporary = occupiedResources.add(reused.getMissing());
        final String fallback;
        if (!incompatible.isEmpty()
                || !occupiedResources.equals(applied)
                || !temporary.equals(target)
                || target.getResources().contains(ResourceProfile.UNKNOWN)
                || target.getResources().contains(ResourceProfile.ANY)) {
            fallback = "occupied-allocation-profile-mismatch";
        } else if (target.getTotalResourceCount() <= occupied.size()) {
            fallback = "no-additional-slots";
        } else if (exceedsConfiguredLimits(temporary, configuration)) {
            fallback = "configured-resource-limit";
        } else {
            fallback = null;
        }
        return new Plan(
                temporary,
                free.getMissing(),
                reusable,
                free.getAllocations().keySet(),
                incompatible,
                fallback);
    }

    private static boolean exceedsConfiguredLimits(
            ResourceCounter requirements, Configuration configuration) {
        if (requirements.getTotalResourceCount()
                > configuration.get(ResourceManagerOptions.MAX_SLOT_NUM)) {
            return true;
        }
        BigDecimal cpu = BigDecimal.ZERO;
        BigDecimal memory = BigDecimal.ZERO;
        for (Map.Entry<ResourceProfile, Integer> entry : requirements.getResourcesWithCount()) {
            final BigDecimal count = BigDecimal.valueOf(entry.getValue());
            cpu = cpu.add(entry.getKey().getCpuCores().getValue().multiply(count));
            memory =
                    memory.add(
                            BigDecimal.valueOf(entry.getKey().getTotalMemory().getBytes())
                                    .multiply(count));
        }
        final BigDecimal requiredCpu = cpu;
        final BigDecimal requiredMemory = memory;
        return configuration
                        .getOptional(ResourceManagerOptions.MAX_TOTAL_CPU)
                        .filter(limit -> requiredCpu.compareTo(BigDecimal.valueOf(limit)) > 0)
                        .isPresent()
                || configuration
                        .getOptional(ResourceManagerOptions.MAX_TOTAL_MEM)
                        .filter(
                                limit ->
                                        requiredMemory.compareTo(
                                                        BigDecimal.valueOf(limit.getBytes()))
                                                > 0)
                        .isPresent();
    }

    static final class Plan {
        final ResourceCounter demand;
        final ResourceCounter missing;
        final Set<AllocationID> reusable;
        final Set<AllocationID> free;
        final Set<AllocationID> incompatible;
        @Nullable final String fallback;

        Plan(
                ResourceCounter demand,
                ResourceCounter missing,
                Set<AllocationID> reusable,
                Set<AllocationID> free,
                Set<AllocationID> incompatible,
                @Nullable String fallback) {
            this.demand = demand;
            this.missing = missing;
            this.reusable = reusable;
            this.free = free;
            this.incompatible = incompatible;
            this.fallback = fallback;
        }
    }
}
