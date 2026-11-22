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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests resource preparation and allocation reuse during native scaling. */
class NativeRescaleSlotReuseTest {

    private static final ResourceProfile ORIGINAL_PROFILE = profileWithManagedMemory(32);
    private static final ResourceProfile CHANGED_PROFILE = profileWithManagedMemory(64);
    private static final Duration PREPARATION_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration RESOURCE_WAIT_TIMEOUT = Duration.ofMinutes(5);

    private final JobVertex vertex = new JobVertex("stateful operator");
    private final SlotOffer originalSlot = newSlot(ORIGINAL_PROFILE);
    private final List<AllocationID> deployed = new ArrayList<>();
    private final List<AllocationID> released = new ArrayList<>();
    private final LocalTaskManagerLocation location = new LocalTaskManagerLocation();
    private final SimpleAckingTaskManagerGateway gateway = new SimpleAckingTaskManagerGateway();
    private final ManuallyTriggeredComponentMainThreadExecutor executor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    private AdaptiveScheduler scheduler;
    private DefaultDeclarativeSlotPool slotPool;

    @BeforeEach
    void startJob() throws Exception {
        vertex.setParallelism(1);
        vertex.setInvokableClass(NoOpInvokable.class);
        final SlotSharingGroup group = new SlotSharingGroup();
        group.setResourceProfile(ORIGINAL_PROFILE);
        vertex.setSlotSharingGroup(group);
        final JobGraph jobGraph = streamingJobGraph(vertex);

        slotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Time.minutes(5),
                        Time.minutes(5));
        scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, executor, executor)
                        .setDeclarativeSlotPool(slotPool)
                        .setFatalErrorHandler(
                                error -> {
                                    throw new AssertionError(error);
                                })
                        .build();
        gateway.setSubmitConsumer(task -> deployed.add(task.getAllocationId()));
        gateway.setFreeSlotFunction(
                (allocation, cause) -> {
                    released.add(allocation);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        gateway.setCancelConsumer(
                attempt ->
                        executor.execute(
                                () ->
                                        scheduler.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        attempt, ExecutionState.CANCELED))));

        scheduler.startScheduling();
        assertThat(offerSlots(originalSlot)).containsExactly(originalSlot);
        drainImmediateTasks();
        assertThat(deployed).containsExactly(originalSlot.getAllocationId());
        deployed.clear();
    }

    @Test
    void keepsCurrentExecutionUntilAdditionalSlotsArriveAndReusesItsAllocation() {
        requestResize(ORIGINAL_PROFILE, 3);
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(offerSlots(newSlot(CHANGED_PROFILE))).isEmpty();
        assertThat(deployed).isEmpty();

        final SlotOffer additionalSlot = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(additionalSlot)).containsExactly(additionalSlot);
        drainImmediateTasks();

        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(), additionalSlot.getAllocationId());
        assertThat(released).isEmpty();
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
    }

    @Test
    void acceptsAWildcardSlotForTheRemainingConcreteRequirement() {
        requestResize(ORIGINAL_PROFILE, 2);
        final SlotOffer wildcard = newSlot(ResourceProfile.ANY);
        assertThat(offerSlots(wildcard)).containsExactly(wildcard);
        drainImmediateTasks();

        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(), wildcard.getAllocationId());
        assertThat(released).isEmpty();
    }

    @Test
    void releasesIncompatibleAllocationsBeforeWaitingForNewProfiles() {
        requestResize(CHANGED_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(released).containsExactly(originalSlot.getAllocationId());

        final SlotOffer first = newSlot(CHANGED_PROFILE);
        final SlotOffer second = newSlot(CHANGED_PROFILE);
        assertThat(offerSlots(first, second)).containsExactlyInAnyOrder(first, second);
        drainImmediateTasks();

        assertThat(deployed)
                .containsExactlyInAnyOrder(first.getAllocationId(), second.getAllocationId());
    }

    @Test
    void expiresPreparationWithoutTryingToDeployAnIncompleteTarget() {
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RUNNING);

        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isTrue();
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(scheduler.hasSufficientResources()).isFalse();
        assertThat(released).isEmpty();

        assertThat(triggerTimers(RESOURCE_WAIT_TIMEOUT)).isTrue();
        drainImmediateTasks();
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.FAILED);
        assertThat(scheduler.requestJob().getArchivedExecutionGraph().getFailureInfo())
                .isNotNull()
                .satisfies(
                        failure ->
                                assertThat(
                                                failure.getException()
                                                        .deserializeError(
                                                                getClass().getClassLoader()))
                                        .isInstanceOf(NoResourceAvailableException.class));
    }

    @Test
    void persistsNativeScaleTargetsWithTheirResourceProfiles() throws Exception {
        final JobGraph jobGraph = new JobGraph("native rescale");
        final JobVertexID vertexId = new JobVertexID();
        JustinResourceRequirements.writeToJobGraph(
                jobGraph,
                JustinResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(vertexId, 1, 2, CHANGED_PROFILE)
                        .build());
        final JustinResourceRequirements saved =
                JustinResourceRequirements.readFromJobGraph(jobGraph).orElseThrow();
        assertThat(saved.getParallelism(vertexId).getLowerBound()).isEqualTo(1);
        assertThat(saved.getParallelism(vertexId).getUpperBound()).isEqualTo(2);
        assertThat(saved.getResourceProfile(vertexId)).isEqualTo(CHANGED_PROFILE);
    }

    private void requestResize(ResourceProfile profile, int parallelism) {
        scheduler.updateJustinResourceRequirements(
                JustinResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(vertex.getID(), 1, parallelism, profile)
                        .build());
    }

    private Collection<SlotOffer> offerSlots(SlotOffer... offers) {
        return slotPool.offerSlots(Arrays.asList(offers), location, gateway, 0);
    }

    private void drainImmediateTasks() {
        do {
            executor.triggerAll();
        } while (triggerTimers(Duration.ZERO));
    }

    private boolean triggerTimers(Duration delay) {
        final List<ScheduledTask<?>> timers =
                executor.getActiveNonPeriodicScheduledTask().stream()
                        .filter(task -> !task.isDone())
                        .filter(task -> task.getDelay(TimeUnit.MILLISECONDS) == delay.toMillis())
                        .map(task -> (ScheduledTask<?>) task)
                        .collect(Collectors.toList());
        timers.forEach(ScheduledTask::execute);
        return !timers.isEmpty();
    }

    private static SlotOffer newSlot(ResourceProfile profile) {
        return new SlotOffer(new AllocationID(), -1, profile);
    }

    private static ResourceProfile profileWithManagedMemory(int memoryMiB) {
        return ResourceProfile.newBuilder().setCpuCores(1).setManagedMemoryMB(memoryMiB).build();
    }
}
