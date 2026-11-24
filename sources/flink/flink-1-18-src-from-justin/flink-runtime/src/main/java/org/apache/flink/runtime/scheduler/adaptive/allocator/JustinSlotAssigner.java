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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally. */
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
        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        // An unspecified group can use any slot. Assign specific groups first.
        jobInformation.getSlotSharingGroups().stream()
                .sorted(
                        Comparator.comparing(
                                group ->
                                        group.getResourceProfile().equals(ResourceProfile.UNKNOWN)))
                .forEach(
                        group ->
                                allGroups.addAll(
                                        createExecutionSlotSharingGroups(
                                                vertexParallelism, group)));

        final List<? extends SlotInfo> candidateSlots = new ArrayList<>(freeSlots);
        candidateSlots.sort(
                Comparator.comparing(
                        slot -> slot.getResourceProfile().equals(ResourceProfile.ANY)));
        Collection<SlotAssignment> assignments = new ArrayList<>();
        Set<AllocationID> used = new HashSet<>();
        for (ExecutionSlotSharingGroup group : allGroups) {
            for (SlotInfo slotInfo : candidateSlots) {
                if (used.contains(slotInfo.getAllocationId())) {
                    continue;
                }
                JobVertexID jobVertexID = group
                        .getContainedExecutionVertices()
                        .stream()
                        .findFirst()
                        .get()
                        .getJobVertexId();
                ResourceProfile resourceProfile = jobInformation
                        .getVertexInformation(jobVertexID)
                        .getSlotSharingGroup()
                        .getResourceProfile();
                LoggerFactory
                        .getLogger(JustinSlotAssigner.class)
                        .debug(jobVertexID + " -> " + resourceProfile);
                if (slotInfo.getResourceProfile().isMatching(resourceProfile)) {
                    used.add(slotInfo.getAllocationId());
                    assignments.add(new SlotAssignment(slotInfo, group));
                    break;
                }
            }
        }
        return assignments;
    }
}
