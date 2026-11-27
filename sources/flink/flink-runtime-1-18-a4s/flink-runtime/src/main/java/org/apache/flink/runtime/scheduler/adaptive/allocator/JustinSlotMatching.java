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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Matches physical allocations to the exact profiles used by the native scheduler. */
public final class JustinSlotMatching {

    private JustinSlotMatching() {}

    public static Match match(
            ResourceCounter requirements, Collection<? extends SlotInfo> candidates) {
        ResourceCounter remaining = requirements;
        final Map<AllocationID, ResourceProfile> matched = new LinkedHashMap<>();
        for (SlotInfo slot : candidates) {
            final ResourceProfile profile = slot.getResourceProfile();
            if (!ResourceProfile.ANY.equals(profile)
                    && !ResourceProfile.UNKNOWN.equals(profile)
                    && remaining.containsResource(profile)
                    && matched.putIfAbsent(slot.getAllocationId(), profile) == null) {
                remaining = remaining.subtract(profile, 1);
            }
        }

        // Match exact profiles globally before assigning any leftover concrete slot to UNKNOWN.
        for (SlotInfo slot : candidates) {
            if (!remaining.containsResource(ResourceProfile.UNKNOWN)) {
                break;
            }
            if (!ResourceProfile.ANY.equals(slot.getResourceProfile())
                    && !ResourceProfile.UNKNOWN.equals(slot.getResourceProfile())
                    && matched.putIfAbsent(slot.getAllocationId(), ResourceProfile.UNKNOWN)
                            == null) {
                remaining = remaining.subtract(ResourceProfile.UNKNOWN, 1);
            }
        }

        // Wildcard slots are valid for any remaining requirement.
        for (SlotInfo slot : candidates) {
            if (remaining.isEmpty()) {
                break;
            }
            if (ResourceProfile.ANY.equals(slot.getResourceProfile())
                    && !matched.containsKey(slot.getAllocationId())) {
                final ResourceProfile requirement = remaining.getResources().iterator().next();
                matched.put(slot.getAllocationId(), requirement);
                remaining = remaining.subtract(requirement, 1);
            }
        }
        return new Match(matched, remaining);
    }

    public static final class Match {
        private final Map<AllocationID, ResourceProfile> allocations;
        private final ResourceCounter missing;

        private Match(Map<AllocationID, ResourceProfile> allocations, ResourceCounter missing) {
            this.allocations = allocations;
            this.missing = missing;
        }

        public Map<AllocationID, ResourceProfile> getAllocations() {
            return allocations;
        }

        public ResourceCounter getMissing() {
            return missing;
        }
    }
}
