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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;

import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/** Assigns each Justin execution slot-sharing group to a matching physical allocation. */
public class JustinSlotAssigner implements SlotAssigner {

    static List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            VertexParallelism vertexParallelism, SlotSharingGroup slotSharingGroup) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();
        slotSharingGroup
                .getJobVertexIds()
                .forEach(
                        jobVertexId -> {
                            int parallelism = vertexParallelism.getParallelism(jobVertexId);
                            for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
                                sharedSlotToVertexAssignment
                                        .computeIfAbsent(subtaskIdx, ignored -> new HashSet<>())
                                        .add(new ExecutionVertexID(jobVertexId, subtaskIdx));
                            }
                        });
        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        final Map<ResourceProfile, Queue<ExecutionSlotSharingGroup>> groups = new HashMap<>();
        ResourceCounter requirements = ResourceCounter.empty();
        for (SlotSharingGroup group : jobInformation.getSlotSharingGroups()) {
            final List<ExecutionSlotSharingGroup> executionGroups =
                    createExecutionSlotSharingGroups(vertexParallelism, group);
            final ResourceProfile profile = group.getResourceProfile();
            groups.computeIfAbsent(profile, ignored -> new ArrayDeque<>()).addAll(executionGroups);
            requirements = requirements.add(profile, executionGroups.size());
        }

        final JustinSlotMatching.Match match = JustinSlotMatching.match(requirements, freeSlots);
        if (!match.getMissing().isEmpty()) {
            LOG.debug("Justin slot assignment is incomplete. Missing: {}", match.getMissing());
            return Collections.emptyList();
        }

        final Collection<SlotAssignment> assignments = new ArrayList<>();
        for (SlotInfo slot : freeSlots) {
            final ResourceProfile profile = match.getAllocations().remove(slot.getAllocationId());
            if (profile != null) {
                assignments.add(new SlotAssignment(slot, groups.get(profile).remove()));
            }
        }
        return assignments;
    }
}
