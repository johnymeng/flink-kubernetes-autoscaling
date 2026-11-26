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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.flip1.FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory;
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
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler.NATIVE_RESCALE_PREALLOCATION_ENABLED;
import static org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler.NATIVE_RESCALE_PREALLOCATION_TIMEOUT;
import static org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler.NATIVE_RESCALE_PREALLOCATION_TIMEOUT_ACTION;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests restart timing and allocation reuse during native scaling. */
class NativeRescaleSlotReuseTest {

    private static final ResourceProfile ORIGINAL_PROFILE = profileWithManagedMemory(32);
    private static final ResourceProfile CHANGED_PROFILE = profileWithManagedMemory(64);
    private static final Duration PREPARATION_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration RESOURCE_WAIT_TIMEOUT = Duration.ofMinutes(5);

    private final JobVertex vertex = new JobVertex("stateful operator");
    private final SlotOffer originalSlot = newSlot(ORIGINAL_PROFILE);
    private final List<AllocationID> deployed = new ArrayList<>();
    private final List<AllocationID> released = new ArrayList<>();
    private final List<ExecutionAttemptID> submitted = new ArrayList<>();
    private final List<ExecutionAttemptID> canceled = new ArrayList<>();
    private final List<Collection<ResourceRequirement>> declarations = new ArrayList<>();
    private final LocalTaskManagerLocation location = new LocalTaskManagerLocation();
    private final SimpleAckingTaskManagerGateway gateway = new SimpleAckingTaskManagerGateway();
    private final ManuallyTriggeredComponentMainThreadExecutor executor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    private AdaptiveScheduler scheduler;
    private DefaultDeclarativeSlotPool slotPool;

    private void startJob(Configuration configuration) throws Exception {
        startJob(configuration, false);
    }

    private void startJob(Configuration configuration, boolean recoverTarget) throws Exception {
        vertex.setParallelism(1);
        vertex.setInvokableClass(NoOpInvokable.class);
        final SlotSharingGroup group = new SlotSharingGroup();
        group.setResourceProfile(ORIGINAL_PROFILE);
        vertex.setSlotSharingGroup(group);
        final JobGraph jobGraph = streamingJobGraph(vertex);
        if (recoverTarget) {
            JustinResourceRequirements.writeToJobGraph(jobGraph, requirements(ORIGINAL_PROFILE, 2));
        }

        slotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        declarations::add,
                        Time.minutes(5),
                        Time.minutes(5));
        scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, executor, executor)
                        .setDeclarativeSlotPool(slotPool)
                        .setJobMasterConfiguration(configuration)
                        .setRestartBackoffTimeStrategy(
                                new FixedDelayRestartBackoffTimeStrategyFactory(1, 0).create())
                        .setFatalErrorHandler(
                                error -> {
                                    throw new AssertionError(error);
                                })
                        .build();
        gateway.setSubmitConsumer(
                task -> {
                    deployed.add(task.getAllocationId());
                    submitted.add(task.getExecutionAttemptId());
                });
        gateway.setFreeSlotFunction(
                (allocation, cause) -> {
                    released.add(allocation);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        gateway.setCancelConsumer(
                attempt -> {
                    canceled.add(attempt);
                    executor.execute(
                            () ->
                                    scheduler.updateTaskExecutionState(
                                            new TaskExecutionState(
                                                    attempt, ExecutionState.CANCELED)));
                });

        scheduler.startScheduling();
        assertThat(offerSlots(originalSlot)).containsExactly(originalSlot);
        drainImmediateTasks();
        if (!recoverTarget) {
            assertThat(deployed).containsExactly(originalSlot.getAllocationId());
            markSubmittedRunning();
            deployed.clear();
        }
    }

    @Test
    void restartsImmediatelyAndKeepsCompatibleAllocationsWhileWaiting() throws Exception {
        startJob(new Configuration());
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
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
    void waitsForTheLatestTargetWhenAScaleUpIsSuperseded() throws Exception {
        startJob(new Configuration());
        requestResize(ORIGINAL_PROFILE, 2);
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        requestResize(ORIGINAL_PROFILE, 3);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();

        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(second)).containsExactly(second);
        drainImmediateTasks();
        assertThat(deployed).isEmpty();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);

        final SlotOffer third = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(third)).containsExactly(third);
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(originalSlot.getAllocationId(), second.getAllocationId(),
                        third.getAllocationId());
        assertThat(released).isEmpty();
    }

    @Test
    void releasesRetainedSlotsIfThePendingTargetChangesProfile() throws Exception {
        startJob(new Configuration());
        requestResize(ORIGINAL_PROFILE, 2);
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        requestResize(CHANGED_PROFILE, 2);
        assertThat(released).containsExactly(originalSlot.getAllocationId());

        final SlotOffer first = newSlot(CHANGED_PROFILE);
        final SlotOffer second = newSlot(CHANGED_PROFILE);
        assertThat(offerSlots(first, second)).containsExactlyInAnyOrder(first, second);
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(first.getAllocationId(), second.getAllocationId());
    }

    @Test
    void acceptsAWildcardSlotForTheRemainingConcreteRequirement() throws Exception {
        startJob(new Configuration());
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
    void releasesIncompatibleAllocationsBeforeWaitingForNewProfiles() throws Exception {
        startJob(new Configuration());
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
    void startsTheNormalAllocationDeadlineWithoutAPreparationWait() throws Exception {
        startJob(new Configuration());
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
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

    @Test
    void keepsTheSameRunningGraphUntilTheFinalCompatibleOffer() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        State running = scheduler.getState();
        requestResize(ORIGINAL_PROFILE, 3);
        assertThat(scheduler.getState()).isSameAs(running);
        assertThat(canceled).isEmpty();
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 3));
        assertThat(offerSlots(newSlot(CHANGED_PROFILE))).isEmpty();
        assertThat(
                        scheduler
                                .requestJustinResourceRequirements()
                                .getJobVertexParallelisms()
                                .get(vertex.getID())
                                .getAppliedParallelism())
                .isEqualTo(1);
        assertThat(
                        scheduler
                                .requestJustinResourceRequirements()
                                .getJobVertexParallelisms()
                                .get(vertex.getID())
                                .getNativeRescaleStatus())
                .isEqualTo("PREPARING");
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(second)).containsExactly(second);
        drainImmediateTasks();
        assertThat(scheduler.getState()).isSameAs(running);
        assertThat(canceled).isEmpty();
        final SlotOffer third = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(third)).containsExactly(third);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(canceled).hasSize(1);
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(),
                        second.getAllocationId(),
                        third.getAllocationId());
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
        assertThat(
                        scheduler
                                .requestJustinResourceRequirements()
                                .getJobVertexParallelisms()
                                .get(vertex.getID())
                                .getNativeRescaleStatus())
                .isEqualTo("COMMITTING");
        markSubmittedRunning();
        assertThat(
                        scheduler
                                .requestJustinResourceRequirements()
                                .getJobVertexParallelisms()
                                .get(vertex.getID())
                                .getNativeRescaleStatus())
                .isEqualTo("APPLIED");
    }

    @Test
    void reusesAlreadyFreeSlotsWithoutWaitingOrIncreasingTheTotal() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        preRegister(second);
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 2));
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(), second.getAllocationId());
    }

    @Test
    void protectsAFreeAllocationThroughPreparationAndRestart() throws Exception {
        startJob(
                preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK)
                        .set(JobManagerOptions.SLOT_IDLE_TIMEOUT, 1_000L));
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        preRegister(second);
        requestResize(ORIGINAL_PROFILE, 3);
        assertThat(triggerTimers(Duration.ofSeconds(1))).isTrue();
        assertThat(slotPool.containsFreeSlot(second.getAllocationId())).isTrue();
        final SlotOffer third = newSlot(ORIGINAL_PROFILE);
        offerSlots(third);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        assertThat(triggerTimers(Duration.ofSeconds(1))).isTrue();
        assertThat(slotPool.containsFreeSlot(second.getAllocationId())).isTrue();
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(),
                        second.getAllocationId(),
                        third.getAllocationId());
    }

    @Test
    void occupiedIncompatibleProfileIsReleasedBeforeWaitingForNewProfile() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(CHANGED_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
        drainImmediateTasks();
        assertThat(released).containsExactly(originalSlot.getAllocationId());
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
    }

    @Test
    void bypassesPreparationForAPureScaleDown() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(offerSlots(newSlot(ORIGINAL_PROFILE))).hasSize(1);
        drainImmediateTasks();
        markSubmittedRunning();
        requestResize(ORIGINAL_PROFILE, 1);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
    }

    @Test
    void knownConfiguredLimitDefersWithoutDeclaringTheTarget() throws Exception {
        startJob(
                preallocation(AdaptiveScheduler.TimeoutAction.DEFER)
                        .set(ResourceManagerOptions.MAX_SLOT_NUM, 1));
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState()).isInstanceOf(Executing.class);
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 1));
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
        assertThat(canceled).isEmpty();
    }

    @Test
    void timeoutFallsBackOnceAndKeepsPartiallyPreparedResources() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 3);
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(second)).containsExactly(second);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isTrue();
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(canceled).hasSize(1);
        assertThat(released).isEmpty();
        final SlotOffer third = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(third)).containsExactly(third);
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(),
                        second.getAllocationId(),
                        third.getAllocationId());
    }

    @Test
    void deferRestoresDemandAndIgnoresADuplicate() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.DEFER));
        requestResize(ORIGINAL_PROFILE, 3);
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(second)).containsExactly(second);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isTrue();
        requestResize(ORIGINAL_PROFILE, 3);
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
        assertThat(scheduler.getState()).isInstanceOf(Executing.class);
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 1));
        assertThat(canceled).isEmpty();
        assertThat(
                        scheduler
                                .requestJustinResourceRequirements()
                                .getJobVertexParallelisms()
                                .get(vertex.getID())
                                .getNativeRescaleStatus())
                .isEqualTo("DEFERRED");
    }

    @Test
    void supersedingDownwardUsesTheLatestTargetOnly() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 3);
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(second)).containsExactly(second);
        requestResize(ORIGINAL_PROFILE, 2);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
        assertThat(canceled).hasSize(1);
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(), second.getAllocationId());
    }

    @Test
    void upwardSupersessionAndDuplicateRetainTheOriginalDeadline() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 2);
        final ScheduledTask<?> originalDeadline = preparationTimer();
        requestResize(ORIGINAL_PROFILE, 3);
        requestResize(ORIGINAL_PROFILE, 3);
        assertThat((Object) preparationTimer()).isSameAs(originalDeadline);
        assertThat(declarations).hasSize(3);
        originalDeadline.execute();
        drainImmediateTasks();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(canceled).hasSize(1);
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 3));
    }

    @Test
    void reversionCancelsTheWindowAndInvalidatesItsCallback() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 3);
        final ScheduledTask<?> oldDeadline = preparationTimer();
        requestResize(ORIGINAL_PROFILE, 1);
        assertThat(oldDeadline.isCancelled()).isTrue();
        assertThat(scheduler.getState()).isInstanceOf(Executing.class);
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 1));

        requestResize(ORIGINAL_PROFILE, 2);
        oldDeadline.execute();
        assertThat(scheduler.getState()).isInstanceOf(Executing.class);
        assertThat(canceled).isEmpty();
        assertThat(offerSlots(newSlot(ORIGINAL_PROFILE))).hasSize(1);
        drainImmediateTasks();
        assertThat(canceled).hasSize(1);
        assertThat(deployed).hasSize(2);
    }

    @Test
    void incompatibleReplacementLeavesPreparationAndUsesNativeFallback() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 3);
        final ScheduledTask<?> oldDeadline = preparationTimer();
        requestResize(CHANGED_PROFILE, 2);
        assertThat(oldDeadline.isCancelled()).isTrue();
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        drainImmediateTasks();
        assertThat(released).containsExactly(originalSlot.getAllocationId());
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(CHANGED_PROFILE, 2));
    }

    @Test
    void lossOfAPreparedSlotRequiresAnotherCompatibleOffer() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 3);
        final SlotOffer lost = newSlot(ORIGINAL_PROFILE);
        offerSlots(lost);
        slotPool.releaseSlot(lost.getAllocationId(), new RuntimeException("worker disconnected"));
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        offerSlots(second);
        assertThat(scheduler.getState()).isInstanceOf(Executing.class);
        assertThat(canceled).isEmpty();
        final SlotOffer third = newSlot(ORIGINAL_PROFILE);
        offerSlots(third);
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(),
                        second.getAllocationId(),
                        third.getAllocationId());
    }

    @Test
    void occupiedWorkerFailureUsesNormalRecoveryAndCancelsPreparation() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        ExecutionAttemptID oldAttempt =
                ((Executing) scheduler.getState())
                        .getExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator()
                        .next()
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        requestResize(ORIGINAL_PROFILE, 2);
        ScheduledTask<?> deadline = preparationTimer();
        slotPool.releaseSlot(originalSlot.getAllocationId(), new RuntimeException("worker lost"));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        oldAttempt, ExecutionState.FAILED, new RuntimeException("worker lost")));
        drainImmediateTasks();
        assertThat(deadline.isCancelled()).isTrue();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(deployed).isEmpty();
        final SlotOffer first = newSlot(ORIGINAL_PROFILE);
        final SlotOffer second = newSlot(ORIGINAL_PROFILE);
        assertThat(offerSlots(first, second)).containsExactlyInAnyOrder(first, second);
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(first.getAllocationId(), second.getAllocationId());
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
    }

    @Test
    void lossAfterReadinessPreventsIncompleteDeployment() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 2);
        final SlotOffer lost = newSlot(ORIGINAL_PROFILE);
        offerSlots(lost);
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.RESTARTING);
        slotPool.releaseSlot(lost.getAllocationId(), new RuntimeException("worker disconnected"));
        drainImmediateTasks();
        assertThat(deployed).isEmpty();
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        final SlotOffer replacement = newSlot(ORIGINAL_PROFILE);
        offerSlots(replacement);
        drainImmediateTasks();
        assertThat(deployed)
                .containsExactlyInAnyOrder(
                        originalSlot.getAllocationId(), replacement.getAllocationId());
    }

    @Test
    void cancellationWithdrawsTemporaryDemandAndCancelsTheDeadline() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK));
        requestResize(ORIGINAL_PROFILE, 3);
        final ScheduledTask<?> deadline = preparationTimer();
        scheduler.cancel();
        drainImmediateTasks();
        assertThat(deadline.isCancelled()).isTrue();
        assertThat(scheduler.getState().getJobStatus()).isEqualTo(JobStatus.CANCELED);
        assertThat(slotPool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(ORIGINAL_PROFILE, 1));
    }

    @Test
    void recoveryReadsPersistedTargetWithoutStartingAnotherPreparation() throws Exception {
        startJob(preallocation(AdaptiveScheduler.TimeoutAction.FALLBACK), true);
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(deployed).isEmpty();
        offerSlots(newSlot(ORIGINAL_PROFILE));
        drainImmediateTasks();
        assertThat(deployed).hasSize(2);
        assertThat(canceled).isEmpty();
        assertThat(triggerTimers(PREPARATION_TIMEOUT)).isFalse();
    }

    private ScheduledTask<?> preparationTimer() {
        return executor.getActiveNonPeriodicScheduledTask().stream()
                .filter(task -> !task.isDone())
                .filter(
                        task ->
                                task.getDelay(TimeUnit.MILLISECONDS)
                                        == PREPARATION_TIMEOUT.toMillis())
                .map(task -> (ScheduledTask<?>) task)
                .findFirst()
                .orElseThrow(AssertionError::new);
    }

    private static Configuration preallocation(AdaptiveScheduler.TimeoutAction action) {
        return new Configuration()
                .set(NATIVE_RESCALE_PREALLOCATION_ENABLED, true)
                .set(NATIVE_RESCALE_PREALLOCATION_TIMEOUT, PREPARATION_TIMEOUT)
                .set(NATIVE_RESCALE_PREALLOCATION_TIMEOUT_ACTION, action);
    }

    private void markSubmittedRunning() {
        for (ExecutionAttemptID attempt : submitted) {
            assertThat(
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attempt, ExecutionState.INITIALIZING)))
                    .isTrue();
            assertThat(
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attempt, ExecutionState.RUNNING)))
                    .isTrue();
        }
        submitted.clear();
    }

    private void preRegister(SlotOffer offer) {
        assertThat(slotPool.registerSlots(Collections.singleton(offer), location, gateway, 0))
                .containsExactly(offer);
    }

    private void requestResize(ResourceProfile profile, int parallelism) {
        scheduler.updateJustinResourceRequirements(requirements(profile, parallelism));
    }

    private JustinResourceRequirements requirements(ResourceProfile profile, int parallelism) {
        return JustinResourceRequirements.newBuilder()
                .setParallelismForJobVertex(vertex.getID(), 1, parallelism, profile)
                .build();
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
