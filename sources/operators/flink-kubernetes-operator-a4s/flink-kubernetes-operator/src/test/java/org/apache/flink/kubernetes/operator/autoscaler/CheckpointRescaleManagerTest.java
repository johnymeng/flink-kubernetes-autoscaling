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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.autoscaler.CheckpointRescaleTransaction;
import org.apache.flink.autoscaler.CheckpointRescaleTransaction.Phase;
import org.apache.flink.autoscaler.ScalingRecord;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.state.ConfigMapStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests the durable checkpoint-rescale transaction. */
@EnableKubernetesMockClient(crud = true)
class CheckpointRescaleManagerTest {

    private static final Map<String, String> PREVIOUS_PARALLELISM = Map.of("vertex", "1");
    private static final Map<String, String> PREVIOUS_PROFILES = Map.of("vertex", "profile-0");
    private static final Map<String, String> TARGET_PARALLELISM = Map.of("vertex", "2");
    private static final Map<String, String> TARGET_PROFILES = Map.of("vertex", "profile-1");
    private static final Instant DECISION_TIME = Instant.parse("2026-08-09T12:00:00Z");
    private static final Instant APPLY_TIME = DECISION_TIME.plusSeconds(90);
    private static final Instant RESTORED_TIME = APPLY_TIME.plusSeconds(20);

    KubernetesClient kubernetesClient;

    private TestingFlinkService flinkService;
    private KubernetesAutoScalerStateStore stateStore;
    private TestingCheckpointRescaleManager manager;
    private FlinkDeployment deployment;

    @BeforeEach
    void setup() throws Exception {
        flinkService = new TestingFlinkService(kubernetesClient);
        flinkService.setCheckpointInfo(Tuple2.of(Optional.empty(), Optional.empty()));
        deployment = createDeployment();
        stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(kubernetesClient));
        var scalingTracking = new ScalingTracking();
        scalingTracking.addScalingRecord(DECISION_TIME, new ScalingRecord());
        stateStore.storeScalingTracking(createContext(), scalingTracking);
        manager = new TestingCheckpointRescaleManager(stateStore);
        manager.setClock(Clock.fixed(DECISION_TIME, ZoneOffset.UTC));
        manager.runningTimestamp = DECISION_TIME.minusSeconds(60).toEpochMilli();
    }

    @Test
    void jobWithoutCheckpointSupportCanResizeByDefault() throws Exception {
        flinkService = spy(flinkService);
        doThrow(new IllegalStateException("Checkpointing is disabled for this job"))
                .when(flinkService)
                .getCheckpointInfo(any(), any());
        var config = deployment.getSpec().getFlinkConfiguration();
        config.remove(AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED.key());
        var context = createContext();
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES))
                .isPresent();
        verify(flinkService, never()).getCheckpointInfo(any(), any());
        assertThat(stateStore.getCheckpointRescaleTransaction(context)).isEmpty();

        config.put(AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED.key(), "true");
        var gated = createContext();
        assertThat(manager.prepareScaling(gated, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(gated, Phase.FAILED);
        assertThat(manager.blocksNewDecision(gated)).isTrue();
        assertThat(manager.blocksScalingApplication(gated)).isTrue();
    }

    @Test
    void successfulPollMaterializesTargetInTheSameRealization() throws Exception {
        var context = createContext();
        var realizer = new KubernetesScalingRealizer(manager);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);
        assertThat(manager.blocksScalingApplication(context)).isTrue();
        assertThat(
                        Configuration.fromMap(deployment.getSpec().getFlinkConfiguration())
                                .get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(PREVIOUS_PARALLELISM);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        assertPhase(context, Phase.APPLYING);
        assertThat(transaction(context).getCompletedCheckpointId()).isNotNull();
        var desired = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        assertThat(desired.get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(TARGET_PARALLELISM);
        assertThat(desired.get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES))
                .isEqualTo(TARGET_PROFILES);
        assertThat(manager.blocksScalingApplication(context)).isFalse();
    }

    @Test
    void failedPollDoesNotApplyTheTarget() throws Exception {
        var context = createContext();
        var realizer = new KubernetesScalingRealizer(manager);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        var triggered = transaction(context);
        triggered.setCheckpointTriggerId("unknown-to-flink");
        stateStore.storeCheckpointRescaleTransaction(context, triggered);
        stateStore.flush(context);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        assertPhase(context, Phase.FAILED);
        assertThat(manager.blocksScalingApplication(context)).isTrue();
        assertThat(transaction(context).isTargetApplied()).isFalse();
        assertThat(
                        Configuration.fromMap(deployment.getSpec().getFlinkConfiguration())
                                .get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(PREVIOUS_PARALLELISM);
    }

    @Test
    void restartFromPersistedReadyStateStillApplies() throws Exception {
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        var ready = transaction(context);
        ready.setCompletedCheckpointId(16L);
        ready.setCheckpointCompletedTimestamp(DECISION_TIME);
        ready.setPhase(Phase.READY_TO_APPLY);
        stateStore.storeCheckpointRescaleTransaction(context, ready);
        stateStore.flush(context);
        stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(kubernetesClient));
        manager = new TestingCheckpointRescaleManager(stateStore);
        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        var recovered = createContext();
        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(recovered, TARGET_PARALLELISM, TARGET_PROFILES);
        assertPhase(recovered, Phase.APPLYING);
        assertThat(transaction(recovered).getCompletedCheckpointId()).isEqualTo(16L);
        assertThat(manager.blocksScalingApplication(recovered)).isFalse();
    }

    @Test
    void testCheckpointCompletesBeforeTargetIsApplied() throws Exception {
        var context = createContext();

        assertThat(manager.blocksScalingApplication(context)).isFalse();

        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);
        assertThat(manager.blocksScalingApplication(context)).isTrue();

        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);

        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isPresent();
        assertPhase(context, Phase.APPLYING);
        assertThat(manager.blocksScalingApplication(context)).isTrue();

        // Reload the recorded apply intent before the realizer has received the target.
        stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(kubernetesClient));
        manager = new TestingCheckpointRescaleManager(stateStore);
        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        manager.runningTimestamp = DECISION_TIME.minusSeconds(60).toEpochMilli();
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES))
                .isPresent();
        var applying = transaction(context);
        assertThat(applying.getPhase()).isEqualTo(Phase.APPLYING);
        assertThat(applying.isTargetApplied()).isTrue();
        assertThat(applying.getCompletedCheckpointId()).isNotNull();
        assertThat(manager.blocksScalingApplication(context)).isTrue();

        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        assertThat(manager.blocksScalingApplication(context)).isFalse();

        applyTargetToObservedSpec(TARGET_PROFILES);
        manager.runningTimestamp = RESTORED_TIME.toEpochMilli();
        manager.setClock(Clock.fixed(RESTORED_TIME, ZoneOffset.UTC));
        var restoredContext = createContext();
        assertThat(manager.prepareScaling(restoredContext, TARGET_PARALLELISM, TARGET_PROFILES))
                .isEmpty();
        assertPhase(restoredContext, Phase.COMPLETED);
        var completed = transaction(restoredContext);
        assertThat(completed.getRestoredRunningTimestamp()).isEqualTo(RESTORED_TIME.toEpochMilli());
        assertThat(completed.getObservedRestartDurationMillis())
                .isEqualTo(Duration.ofSeconds(20).toMillis());
        assertThat(
                        stateStore
                                .getScalingTracking(restoredContext)
                                .getLatestScalingRecordEntry()
                                .orElseThrow()
                                .getValue()
                                .getRestartDuration())
                .isEqualTo(Duration.ofSeconds(20));
        assertThat(manager.blocksNewDecision(restoredContext)).isFalse();
        assertThat(manager.blocksScalingApplication(restoredContext)).isFalse();

        // A fresh Kubernetes reconcile starts from the raw user manifest. Keep it gated until the
        // autoscaler replays its completed target into the in-memory deployment spec.
        var rawDesired = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        rawDesired.set(PipelineOptions.PARALLELISM_OVERRIDES, PREVIOUS_PARALLELISM);
        rawDesired.removeConfig(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES);
        deployment.getSpec().setFlinkConfiguration(rawDesired.toMap());
        assertThat(manager.blocksScalingApplication(restoredContext)).isTrue();

        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(restoredContext, TARGET_PARALLELISM, TARGET_PROFILES);
        assertThat(manager.blocksScalingApplication(restoredContext)).isFalse();
    }

    @Test
    void testEnablingGatePreservesAlreadyAppliedOverrides() throws Exception {
        var config = deployment.getSpec().getFlinkConfiguration();
        config.remove(PipelineOptions.PARALLELISM_OVERRIDES.key());
        config.remove(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES.key());
        var context = createContext();

        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(context, PREVIOUS_PARALLELISM, PREVIOUS_PROFILES);

        assertThat(Configuration.fromMap(config).get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(PREVIOUS_PARALLELISM);
        assertThat(
                        Configuration.fromMap(config)
                                .get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES))
                .isEqualTo(PREVIOUS_PROFILES);
        assertThat(stateStore.getCheckpointRescaleTransaction(context)).isEmpty();
    }

    @Test
    void testA4sMemoryOnlyChangeWaitsForCheckpoint() throws Exception {
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.A4S_ENABLED.key(), "true");
        var context = createContext();
        var realizer = new KubernetesScalingRealizer(manager);

        realizer.realizeParallelismOverrides(context, PREVIOUS_PARALLELISM, TARGET_PROFILES);
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);
        assertThat(
                        Configuration.fromMap(deployment.getSpec().getFlinkConfiguration())
                                .get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES))
                .isEqualTo(PREVIOUS_PROFILES);
        realizer.realizeParallelismOverrides(context, PREVIOUS_PARALLELISM, TARGET_PROFILES);
        realizer.realizeParallelismOverrides(context, PREVIOUS_PARALLELISM, TARGET_PROFILES);
        assertPhase(context, Phase.APPLYING);
        var applied = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        assertThat(applied.get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(PREVIOUS_PARALLELISM);
        assertThat(applied.get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES))
                .isEqualTo(TARGET_PROFILES);
    }

    @Test
    void testProducerIsPausedBeforeCheckpointAndResumedAfterRestore() throws Exception {
        enableProducerPause();
        var context = createContext();

        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        var waitingForPause = transaction(context);
        assertThat(waitingForPause.getPhase()).isEqualTo(Phase.WAITING_PRODUCER_PAUSE);
        assertThat(waitingForPause.getTransactionId()).isNotBlank();
        assertThat(waitingForPause.getCheckpointTriggerId()).isNull();

        acknowledge(
                CheckpointRescaleManager.PRODUCER_PAUSE_ACK_ANNOTATION,
                waitingForPause.getTransactionId());
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(context, Phase.WAITING_CHECKPOINT);
        assertThat(transaction(context).getCheckpointTriggerId()).isNull();

        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);
        assertThat(transaction(context).getCheckpointTriggerId()).isNotBlank();

        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);

        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES))
                .isPresent();
        applyTargetToObservedSpec(TARGET_PROFILES);
        manager.runningTimestamp = RESTORED_TIME.toEpochMilli();
        manager.setClock(Clock.fixed(RESTORED_TIME, ZoneOffset.UTC));
        var restoredContext = createContext();
        assertThat(manager.prepareScaling(restoredContext, TARGET_PARALLELISM, TARGET_PROFILES))
                .isEmpty();
        assertPhase(restoredContext, Phase.WAITING_PRODUCER_RESUME);
        assertThat(manager.blocksNewDecision(restoredContext)).isTrue();

        var transactionId = transaction(restoredContext).getTransactionId();
        acknowledge(CheckpointRescaleManager.PRODUCER_RESUME_ACK_ANNOTATION, transactionId);
        assertThat(manager.prepareScaling(restoredContext, TARGET_PARALLELISM, TARGET_PROFILES))
                .isEmpty();
        assertPhase(restoredContext, Phase.COMPLETED);
        assertThat(manager.blocksNewDecision(restoredContext)).isFalse();
    }

    @Test
    void testProducerPauseTimeoutFailsBeforeCheckpoint() throws Exception {
        enableProducerPause();
        var context = createContext();

        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        manager.setClock(Clock.fixed(DECISION_TIME.plusSeconds(61), ZoneOffset.UTC));
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);

        var failed = transaction(context);
        assertThat(failed.getPhase()).isEqualTo(Phase.FAILED);
        assertThat(failed.getCheckpointTriggerId()).isNull();
        assertThat(failed.getError()).contains("WAITING_PRODUCER_PAUSE");
    }

    @Test
    void testScalingApplicationIsGatedOutsideApplyAndCompletedPhases() throws Exception {
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        var transaction = transaction(context);

        for (var phase :
                new Phase[] {
                    Phase.WAITING_PRODUCER_PAUSE,
                    Phase.WAITING_CHECKPOINT,
                    Phase.CHECKPOINT_TRIGGERED,
                    Phase.READY_TO_APPLY,
                    Phase.RESTORING,
                    Phase.VERIFYING,
                    Phase.WAITING_PRODUCER_RESUME,
                    Phase.FAILED
                }) {
            transaction.setPhase(phase);
            stateStore.storeCheckpointRescaleTransaction(context, transaction);
            assertThat(manager.blocksScalingApplication(context)).as("phase %s", phase).isTrue();
        }

        transaction.setPhase(Phase.APPLYING);
        stateStore.storeCheckpointRescaleTransaction(context, transaction);
        assertThat(manager.blocksScalingApplication(context)).isTrue();

        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(context, TARGET_PARALLELISM, TARGET_PROFILES);
        assertThat(manager.blocksScalingApplication(context)).isFalse();
    }

    @Test
    void testApplyingTargetIsReemittedAfterOperatorRestart() throws Exception {
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);

        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES))
                .isPresent();
        assertPhase(context, Phase.APPLYING);

        // Simulate a crash after APPLYING was persisted but before the realizer mutated the spec.
        stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(kubernetesClient));
        manager = new TestingCheckpointRescaleManager(stateStore);
        manager.setClock(Clock.fixed(APPLY_TIME.plusSeconds(1), ZoneOffset.UTC));
        manager.runningTimestamp = DECISION_TIME.minusSeconds(60).toEpochMilli();
        var restartedContext = createContext();

        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(
                        restartedContext, Map.of("vertex", "3"), Map.of("vertex", "later-profile"));

        var desired = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        assertThat(desired.get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(TARGET_PARALLELISM);
        assertThat(desired.get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES))
                .isEqualTo(TARGET_PROFILES);
        assertPhase(restartedContext, Phase.APPLYING);
        assertThat(transaction(restartedContext).getAppliedTimestamp()).isEqualTo(APPLY_TIME);
    }

    @Test
    void testAbortRestoresPreviousOverridesBeforeApply() throws Exception {
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);

        deployment
                .getMetadata()
                .getAnnotations()
                .put(CheckpointRescaleManager.ABORT_NONCE_ANNOTATION, "1");
        assertThat(manager.blocksNewDecision(context)).isTrue();
        assertThat(stateStore.getCheckpointRescaleTransaction(context)).isEmpty();
        assertThat(stateStore.getParallelismOverrides(context)).isEqualTo(PREVIOUS_PARALLELISM);
        assertThat(stateStore.getResourceProfileOverrides(context)).isEqualTo(PREVIOUS_PROFILES);
    }

    @Test
    void testDs2DecisionUsesCheckpointGate() throws Exception {
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.JUSTIN_ENABLED.key(), "false");
        var context = createContext();

        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, Map.of())).isEmpty();
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);
        assertThat(manager.blocksNewDecision(context)).isTrue();
        assertThat(transaction(context).getTargetResourceProfileOverrides()).isEmpty();
    }

    @Test
    void testDs2CheckpointTransactionCompletes() throws Exception {
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.JUSTIN_ENABLED.key(), "false");
        var context = createContext();

        var realizer = new KubernetesScalingRealizer(manager);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM);
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM);
        assertPhase(context, Phase.CHECKPOINT_TRIGGERED);

        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        realizer.realizeParallelismOverrides(context, TARGET_PARALLELISM);
        var target = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        assertThat(target.get(PipelineOptions.PARALLELISM_OVERRIDES)).isEqualTo(TARGET_PARALLELISM);
        assertThat(target.get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES))
                .isEqualTo(PREVIOUS_PROFILES);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        manager.runningTimestamp = RESTORED_TIME.toEpochMilli();
        manager.setClock(Clock.fixed(RESTORED_TIME, ZoneOffset.UTC));
        var restoredContext = createContext();
        realizer.realizeParallelismOverrides(restoredContext, TARGET_PARALLELISM);
        assertPhase(restoredContext, Phase.COMPLETED);
        assertThat(transaction(restoredContext).getObservedRestartDurationMillis())
                .isEqualTo(Duration.ofSeconds(20).toMillis());
    }

    @Test
    void testTimeoutLatchesFailureAndRetryKeepsFrozenTarget() throws Exception {
        var initialTime = Instant.parse("2026-08-05T12:00:00Z");
        manager.setClock(Clock.fixed(initialTime, ZoneOffset.UTC));
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);

        manager.setClock(Clock.fixed(initialTime.plusSeconds(301), ZoneOffset.UTC));
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES)).isEmpty();
        var failed = transaction(context);
        assertThat(failed.getPhase()).isEqualTo(Phase.FAILED);
        assertThat(failed.getTargetParallelismOverrides()).isEqualTo(TARGET_PARALLELISM);
        assertThat(
                        stateStore
                                .getScalingTracking(context)
                                .getLatestScalingRecordEntry()
                                .orElseThrow()
                                .getValue()
                                .getRestartDuration())
                .isNull();

        deployment
                .getMetadata()
                .getAnnotations()
                .put(CheckpointRescaleManager.RETRY_NONCE_ANNOTATION, "1");
        assertThat(manager.blocksNewDecision(context)).isTrue();
        var retrying = transaction(context);
        assertThat(retrying.getPhase()).isEqualTo(Phase.WAITING_CHECKPOINT);
        assertThat(retrying.getTargetParallelismOverrides()).isEqualTo(TARGET_PARALLELISM);
        assertThat(retrying.getError()).isNull();
    }

    @Test
    void testApplyFailureIsDurableAndRetryReusesFrozenTarget() throws Exception {
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES);
        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, TARGET_PROFILES))
                .isPresent();
        assertPhase(context, Phase.APPLYING);

        manager.handleScalingFailure(
                context,
                new RuntimeException(
                        "REST request failed", new IllegalStateException("requirements rejected")));

        var failed = transaction(context);
        assertThat(failed.getPhase()).isEqualTo(Phase.FAILED);
        assertThat(failed.getError())
                .isEqualTo(
                        "Failed to apply checkpoint rescale target: "
                                + "IllegalStateException: requirements rejected");
        assertThat(failed.getTargetParallelismOverrides()).isEqualTo(TARGET_PARALLELISM);
        assertThat(failed.getTargetResourceProfileOverrides()).isEqualTo(TARGET_PROFILES);
        assertThat(manager.blocksNewDecision(context)).isTrue();
        assertThat(manager.blocksScalingApplication(context)).isTrue();

        // Verify that an operator restart observes the latched failure from the ConfigMap.
        stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(kubernetesClient));
        manager = new TestingCheckpointRescaleManager(stateStore);
        var restartedContext = createContext();
        assertPhase(restartedContext, Phase.FAILED);
        assertThat(manager.blocksNewDecision(restartedContext)).isTrue();

        deployment
                .getMetadata()
                .getAnnotations()
                .put(CheckpointRescaleManager.RETRY_NONCE_ANNOTATION, "1");
        assertThat(manager.blocksNewDecision(restartedContext)).isTrue();
        var retrying = transaction(restartedContext);
        assertThat(retrying.getPhase()).isEqualTo(Phase.APPLYING);
        assertThat(manager.blocksScalingApplication(restartedContext)).isTrue();
        assertThat(retrying.getError()).isNull();
        assertThat(retrying.getTargetParallelismOverrides()).isEqualTo(TARGET_PARALLELISM);
        assertThat(retrying.getTargetResourceProfileOverrides()).isEqualTo(TARGET_PROFILES);

        new KubernetesScalingRealizer(manager)
                .realizeParallelismOverrides(restartedContext, TARGET_PARALLELISM, TARGET_PROFILES);
        assertThat(manager.blocksScalingApplication(restartedContext)).isFalse();
    }

    @Test
    void testDs2ApplyFailureLatchesTransaction() throws Exception {
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.JUSTIN_ENABLED.key(), "false");
        var context = createContext();
        manager.prepareScaling(context, TARGET_PARALLELISM, Map.of());
        manager.prepareScaling(context, TARGET_PARALLELISM, Map.of());
        manager.prepareScaling(context, TARGET_PARALLELISM, Map.of());
        manager.setClock(Clock.fixed(APPLY_TIME, ZoneOffset.UTC));
        assertThat(manager.prepareScaling(context, TARGET_PARALLELISM, Map.of())).isPresent();

        manager.handleScalingFailure(context, new IllegalStateException("requirements rejected"));

        var failed = transaction(context);
        assertThat(failed.getPhase()).isEqualTo(Phase.FAILED);
        assertThat(failed.getTargetParallelismOverrides()).isEqualTo(TARGET_PARALLELISM);
        assertThat(failed.getTargetResourceProfileOverrides()).isEmpty();
    }

    private FlinkDeployment createDeployment() {
        var resource = TestUtils.buildApplicationCluster();
        resource.getMetadata().setName("checkpoint-test");
        resource.getMetadata().setAnnotations(new HashMap<>());
        resource.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        resource.getStatus().getJobStatus().setState(JobStatus.RUNNING);

        var observed = Configuration.fromMap(resource.getSpec().getFlinkConfiguration());
        observed.set(PipelineOptions.PARALLELISM_OVERRIDES, PREVIOUS_PARALLELISM);
        observed.set(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES, PREVIOUS_PROFILES);
        resource.getSpec().setFlinkConfiguration(observed.toMap());
        resource.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(resource.getSpec(), resource);

        var desired = Configuration.fromMap(resource.getSpec().getFlinkConfiguration());
        desired.set(AutoScalerOptions.JUSTIN_ENABLED, true);
        desired.set(AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED, true);
        resource.getSpec().setFlinkConfiguration(desired.toMap());
        return resource;
    }

    private KubernetesJobAutoScalerContext createContext() {
        var configManager = new FlinkConfigManager(new Configuration());
        return new FlinkDeploymentContext(
                        deployment,
                        flinkService.getContext(),
                        null,
                        configManager,
                        ignored -> flinkService)
                .getJobAutoScalerContext();
    }

    private void enableProducerPause() {
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.CHECKPOINT_RESCALE_PRODUCER_PAUSE_ENABLED.key(), "true");
    }

    private void acknowledge(String annotation, String transactionId) {
        deployment.getMetadata().getAnnotations().put(annotation, transactionId);
    }

    private void applyTargetToObservedSpec(Map<String, String> resourceProfiles) {
        var target = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        target.set(PipelineOptions.PARALLELISM_OVERRIDES, TARGET_PARALLELISM);
        target.set(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES, resourceProfiles);
        deployment.getSpec().setFlinkConfiguration(target.toMap());
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
    }

    private void assertPhase(KubernetesJobAutoScalerContext context, Phase phase) throws Exception {
        assertThat(transaction(context).getPhase()).isEqualTo(phase);
    }

    private CheckpointRescaleTransaction transaction(KubernetesJobAutoScalerContext context)
            throws Exception {
        return stateStore.getCheckpointRescaleTransaction(context).orElseThrow();
    }

    private static class TestingCheckpointRescaleManager extends CheckpointRescaleManager {

        private long runningTimestamp = 100L;

        private TestingCheckpointRescaleManager(KubernetesAutoScalerStateStore stateStore) {
            super(stateStore);
        }

        @Override
        long getRunningTimestamp(KubernetesJobAutoScalerContext context) {
            return runningTimestamp;
        }
    }
}
