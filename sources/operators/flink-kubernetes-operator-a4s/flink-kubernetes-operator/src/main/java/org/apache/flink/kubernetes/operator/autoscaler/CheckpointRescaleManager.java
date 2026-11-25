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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.CheckpointRescaleTransaction;
import org.apache.flink.autoscaler.CheckpointRescaleTransaction.Phase;
import org.apache.flink.autoscaler.ScalingDecisionGate;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_CHECKPOINT_TIMEOUT;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_PRODUCER_CONTROL_TIMEOUT;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_PRODUCER_PAUSE_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_RESTORE_TIMEOUT;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_CLIENT_TIMEOUT;

/** Coordinates a durable checkpoint before an autoscaler in-place rescale. */
public class CheckpointRescaleManager
        implements ScalingDecisionGate<KubernetesJobAutoScalerContext> {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRescaleManager.class);

    public static final String PHASE_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-phase";
    public static final String RETRY_NONCE_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-retry-nonce";
    public static final String ABORT_NONCE_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-abort-nonce";
    public static final String PRODUCER_PAUSE_ACK_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-producer-pause-ack";
    public static final String PRODUCER_RESUME_ACK_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-producer-resume-ack";

    private final KubernetesAutoScalerStateStore stateStore;
    private Clock clock = Clock.systemUTC();

    public CheckpointRescaleManager(KubernetesAutoScalerStateStore stateStore) {
        this.stateStore = stateStore;
    }

    @Override
    public boolean blocksNewDecision(KubernetesJobAutoScalerContext context) throws Exception {
        if (!enabled(context)) {
            return false;
        }
        var transaction = stateStore.getCheckpointRescaleTransaction(context);
        if (transaction.isEmpty()) {
            return false;
        }
        if (handleAction(context, transaction.get())) {
            // Do not make a fresh scaling decision in the same reconcile cycle as a manual
            // transaction action. A retry can still advance its frozen target in the realizer.
            return true;
        }
        return transaction.isPresent() && transaction.get().getPhase() != Phase.COMPLETED;
    }

    @Override
    public void handleScalingFailure(KubernetesJobAutoScalerContext context, Throwable failure)
            throws Exception {
        if (!enabled(context)) {
            return;
        }

        var transaction = stateStore.getCheckpointRescaleTransaction(context);
        if (transaction.isEmpty() || transaction.get().getPhase() != Phase.APPLYING) {
            LOG.debug("Ignoring a scaling failure without an applying checkpoint transaction");
            return;
        }

        fail(context, transaction.get(), "Failed to apply checkpoint rescale target", failure);
    }

    @Override
    public boolean blocksScalingApplication(KubernetesJobAutoScalerContext context)
            throws Exception {
        if (!enabled(context)) {
            return false;
        }
        var transaction = stateStore.getCheckpointRescaleTransaction(context);
        if (transaction.isEmpty()) {
            return false;
        }

        var checkpointTransaction = transaction.get();
        switch (checkpointTransaction.getPhase()) {
            case WAITING_PRODUCER_PAUSE:
            case WAITING_CHECKPOINT:
            case CHECKPOINT_TRIGGERED:
            case READY_TO_APPLY:
            case RESTORING:
            case VERIFYING:
            case WAITING_PRODUCER_RESUME:
            case FAILED:
                // The realizer intentionally leaves the raw deployment spec untouched in these
                // phases. Do not let regular reconciliation interpret that as removal of the
                // autoscaler-owned overrides while a checkpoint transaction is still advancing.
                return true;
            case APPLYING:
            case COMPLETED:
                // READY_TO_APPLY and VERIFYING can transition into these phases after the
                // realizer has already decided not to mutate the spec in the current cycle. Only
                // continue once a realizer invocation has materialized the complete frozen target.
                return !desiredSpecContainsTarget(context, checkpointTransaction);
            default:
                throw new IllegalStateException(
                        "Unsupported checkpoint rescale phase: "
                                + checkpointTransaction.getPhase());
        }
    }

    @Value
    static class ScalingTarget {
        Map<String, String> parallelism;
        Map<String, String> resourceProfiles;
    }

    /** Returns the exact overrides the realizer may write in this reconcile cycle. */
    Optional<ScalingTarget> prepareScaling(
            KubernetesJobAutoScalerContext context,
            Map<String, String> targetParallelism,
            Map<String, String> targetResourceProfiles)
            throws Exception {
        var requested = new ScalingTarget(targetParallelism, targetResourceProfiles);
        if (!enabled(context)) {
            return Optional.of(requested);
        }

        var transactionOpt = stateStore.getCheckpointRescaleTransaction(context);
        if (transactionOpt.isEmpty()
                || (transactionOpt.get().getPhase() == Phase.COMPLETED
                        && !sameTarget(
                                transactionOpt.get(), targetParallelism, targetResourceProfiles))) {
            // When enabling the gate on an already-scaled job, keep materializing its
            // observed overrides even though no new checkpoint or restart is needed.
            if (matchesOverrides(context.getResourceContext().getObserveConfig(), requested)) {
                if (transactionOpt.isPresent()) {
                    stateStore.removeCheckpointRescaleTransaction(context);
                    setPhaseAnnotation(context, null);
                    stateStore.flush(context);
                }
                return Optional.of(requested);
            }
            transactionOpt = startTransaction(context, targetParallelism, targetResourceProfiles);
        }
        if (transactionOpt.isEmpty()) {
            return Optional.empty();
        }

        var transaction = transactionOpt.get();
        if (handleAction(context, transaction)) {
            return Optional.empty();
        }
        if (transaction.getPhase() == Phase.FAILED) {
            LOG.error("Checkpoint rescale is blocked: {}", transaction.getError());
            return Optional.empty();
        }
        var frozen = frozenTarget(transaction);
        if (transaction.getPhase() == Phase.COMPLETED) {
            return Optional.of(frozen);
        }
        if (!sameTarget(transaction, targetParallelism, targetResourceProfiles)) {
            LOG.warn("Ignoring a changed target while checkpoint rescale transaction is active");
        }

        try {
            switch (transaction.getPhase()) {
                case WAITING_PRODUCER_PAUSE:
                    waitForProducerPause(context, transaction);
                    return Optional.empty();
                case WAITING_CHECKPOINT:
                    waitForCheckpointSlot(context, transaction);
                    return Optional.empty();
                case CHECKPOINT_TRIGGERED:
                    pollCheckpoint(context, transaction);
                    return Optional.empty();
                case READY_TO_APPLY:
                    transaction.setAppliedTimestamp(clock.instant());
                    transaction.setTargetApplied(true);
                    transition(context, transaction, Phase.APPLYING);
                    return Optional.of(frozen);
                case APPLYING:
                    if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_RESTORE_TIMEOUT)) {
                        return Optional.empty();
                    }
                    if (!currentJobMatches(context, transaction)) {
                        fail(context, transaction, "Job ID changed during in-place rescale", null);
                        return Optional.empty();
                    }
                    if (!observesTarget(context, transaction)) {
                        return Optional.of(frozen);
                    }
                    verifyRestoredTarget(context, transaction);
                    return Optional.empty();
                case RESTORING:
                case VERIFYING:
                    verifyRestoredTarget(context, transaction);
                    return Optional.empty();
                case WAITING_PRODUCER_RESUME:
                    waitForProducerResume(context, transaction);
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            fail(
                    context,
                    transaction,
                    "Checkpoint rescale phase " + transaction.getPhase() + " failed",
                    e);
            return Optional.empty();
        }
    }

    private Optional<CheckpointRescaleTransaction> startTransaction(
            KubernetesJobAutoScalerContext context,
            Map<String, String> targetParallelism,
            Map<String, String> targetResourceProfiles)
            throws Exception {
        if (context.getJobID() == null || context.getJobStatus() != JobStatus.RUNNING) {
            LOG.info("Waiting for a running job before starting checkpoint rescale");
            return Optional.empty();
        }

        var observeConfig = context.getResourceContext().getObserveConfig();
        var previousParallelism =
                observeConfig == null
                        ? Map.<String, String>of()
                        : observeConfig
                                .getOptional(PipelineOptions.PARALLELISM_OVERRIDES)
                                .orElse(Map.of());
        var previousProfiles =
                observeConfig == null
                        ? Map.<String, String>of()
                        : observeConfig
                                .getOptional(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES)
                                .orElse(Map.of());

        var transaction = new CheckpointRescaleTransaction();
        transaction.setPhase(
                producerPauseEnabled(context)
                        ? Phase.WAITING_PRODUCER_PAUSE
                        : Phase.WAITING_CHECKPOINT);
        transaction.setTransactionId(UUID.randomUUID().toString());
        transaction.setJobId(context.getJobID().toHexString());
        transaction.setPreviousParallelismOverrides(new HashMap<>(previousParallelism));
        transaction.setPreviousResourceProfileOverrides(new HashMap<>(previousProfiles));
        transaction.setTargetParallelismOverrides(new HashMap<>(targetParallelism));
        transaction.setTargetResourceProfileOverrides(new HashMap<>(targetResourceProfiles));
        transaction.setDecisionTimestamp(clock.instant());
        transaction.setPhaseTimestamp(clock.instant());
        persist(context, transaction);
        try {
            transaction.setInitialRunningTimestamp(getRunningTimestamp(context));
            persist(context, transaction);
        } catch (Exception e) {
            fail(context, transaction, "Cannot read initial RUNNING timestamp", e);
        }
        if (transaction.getPhase() != Phase.FAILED) {
            LOG.info("Started checkpoint rescale transaction for job {}", transaction.getJobId());
        }
        return Optional.of(transaction);
    }

    private void waitForProducerPause(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_PRODUCER_CONTROL_TIMEOUT)) {
            return;
        }
        if (!transactionAcknowledged(
                context, PRODUCER_PAUSE_ACK_ANNOTATION, transaction.getTransactionId())) {
            LOG.info(
                    "Waiting for producer pause acknowledgement for transaction {}",
                    transaction.getTransactionId());
            return;
        }
        transition(context, transaction, Phase.WAITING_CHECKPOINT);
        LOG.info("Producer pause acknowledged for transaction {}", transaction.getTransactionId());
    }

    private void waitForCheckpointSlot(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_CHECKPOINT_TIMEOUT)) {
            return;
        }
        var checkpointInfo =
                context.getResourceContext()
                        .getFlinkService()
                        .getCheckpointInfo(context.getJobID(), runtimeConfig(context));
        if (checkpointInfo.f1.isPresent()) {
            LOG.info(
                    "Waiting for checkpoint {} before triggering the rescale checkpoint",
                    checkpointInfo.f1.get().getId());
            return;
        }

        try {
            transaction.setCheckpointTriggerId(
                    context.getResourceContext()
                            .getFlinkService()
                            .triggerCheckpoint(
                                    transaction.getJobId(),
                                    CheckpointType.CONFIGURED,
                                    runtimeConfig(context)));
            transition(context, transaction, Phase.CHECKPOINT_TRIGGERED);
        } catch (Exception e) {
            fail(context, transaction, "Checkpoint trigger failed", e);
        }
    }

    private void pollCheckpoint(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_CHECKPOINT_TIMEOUT)) {
            return;
        }
        CheckpointFetchResult result =
                context.getResourceContext()
                        .getFlinkService()
                        .fetchCheckpointInfo(
                                transaction.getCheckpointTriggerId(),
                                transaction.getJobId(),
                                runtimeConfig(context));
        if (result.isPending()) {
            return;
        }
        if (result.getError() != null) {
            fail(context, transaction, "Checkpoint failed: " + result.getError(), null);
            return;
        }
        transaction.setCompletedCheckpointId(result.getCheckpointId());
        transaction.setCheckpointCompletedTimestamp(clock.instant());
        transition(context, transaction, Phase.READY_TO_APPLY);
    }

    private void verifyRestoredTarget(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_RESTORE_TIMEOUT)) {
            return;
        }
        if (!currentJobMatches(context, transaction)) {
            fail(context, transaction, "Job ID changed during in-place rescale", null);
            return;
        }
        if (context.getJobStatus() != JobStatus.RUNNING) {
            transition(context, transaction, Phase.RESTORING);
            return;
        }

        long runningTimestamp = getRunningTimestamp(context);
        if (runningTimestamp <= transaction.getInitialRunningTimestamp()) {
            return;
        }
        transition(context, transaction, Phase.VERIFYING);

        if (!observesTarget(context, transaction)) {
            return;
        }

        recordObservedRestartDuration(context, transaction, runningTimestamp);
        if (producerPauseEnabled(context)) {
            transition(context, transaction, Phase.WAITING_PRODUCER_RESUME);
            LOG.info(
                    "Waiting for producer resume acknowledgement for transaction {}",
                    transaction.getTransactionId());
            return;
        }
        complete(context, transaction);
    }

    private void waitForProducerResume(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_PRODUCER_CONTROL_TIMEOUT)) {
            return;
        }
        if (!transactionAcknowledged(
                context, PRODUCER_RESUME_ACK_ANNOTATION, transaction.getTransactionId())) {
            LOG.info(
                    "Waiting for producer resume acknowledgement for transaction {}",
                    transaction.getTransactionId());
            return;
        }
        complete(context, transaction);
    }

    private void complete(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        transition(context, transaction, Phase.COMPLETED);
        LOG.info(
                "Checkpoint rescale completed after checkpoint {} with a {} ms restart",
                transaction.getCompletedCheckpointId(),
                transaction.getObservedRestartDurationMillis());
    }

    private void recordObservedRestartDuration(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            long runningTimestamp)
            throws Exception {
        var appliedTimestamp = transaction.getAppliedTimestamp();
        if (appliedTimestamp == null) {
            throw new IllegalStateException("Applied timestamp is unavailable");
        }

        var restoredAt = Instant.ofEpochMilli(runningTimestamp);
        var restartDuration = Duration.between(appliedTimestamp, restoredAt);
        if (restartDuration.isNegative()) {
            throw new IllegalStateException(
                    String.format(
                            "Restored RUNNING timestamp %s precedes apply timestamp %s",
                            restoredAt, appliedTimestamp));
        }

        transaction.setRestoredRunningTimestamp(runningTimestamp);
        transaction.setObservedRestartDurationMillis(restartDuration.toMillis());

        var scalingTracking = stateStore.getScalingTracking(context);
        scalingTracking.recordLatestRestartDuration(restartDuration);
        stateStore.storeScalingTracking(context, scalingTracking);
    }

    private boolean handleAction(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        long retryNonce = annotationNonce(context, RETRY_NONCE_ANNOTATION);
        long abortNonce = annotationNonce(context, ABORT_NONCE_ANNOTATION);
        if (abortNonce > transaction.getHandledAbortNonce()) {
            transaction.setHandledAbortNonce(abortNonce);
            if (transaction.wasApplied()) {
                persist(context, transaction);
                LOG.warn("Ignoring abort because target overrides were already applied");
            } else {
                stateStore.storeParallelismOverrides(
                        context, transaction.getPreviousParallelismOverrides());
                stateStore.storeResourceProfileOverrides(
                        context, transaction.getPreviousResourceProfileOverrides());
                stateStore.removeCheckpointRescaleTransaction(context);
                setPhaseAnnotation(context, null);
                stateStore.flush(context);
                LOG.warn("Aborted checkpoint rescale before applying target overrides");
            }
            return true;
        }
        if (retryNonce > transaction.getHandledRetryNonce()) {
            transaction.setHandledRetryNonce(retryNonce);
            if (transaction.getPhase() == Phase.FAILED) {
                transaction.setError(null);
                if (transaction.wasApplied()) {
                    transition(context, transaction, Phase.APPLYING);
                } else {
                    transaction.setCheckpointTriggerId(null);
                    transaction.setCompletedCheckpointId(null);
                    transaction.setCheckpointCompletedTimestamp(null);
                    transition(
                            context,
                            transaction,
                            producerPauseEnabled(context)
                                    ? Phase.WAITING_PRODUCER_PAUSE
                                    : Phase.WAITING_CHECKPOINT);
                }
                LOG.warn("Retrying frozen checkpoint rescale target");
            } else {
                persist(context, transaction);
                LOG.warn("Ignoring retry because checkpoint rescale has not failed");
            }
            return true;
        }
        return false;
    }

    private boolean checkTimeout(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            ConfigOption<Duration> timeoutOption)
            throws Exception {
        var deadline =
                transaction.getPhaseTimestamp().plus(context.getConfiguration().get(timeoutOption));
        if (clock.instant().isAfter(deadline)) {
            fail(
                    context,
                    transaction,
                    "Timed out in checkpoint rescale phase " + transaction.getPhase(),
                    null);
            return true;
        }
        return false;
    }

    long getRunningTimestamp(KubernetesJobAutoScalerContext context) throws Exception {
        try (var client = context.getRestClusterClient()) {
            JobDetailsInfo details =
                    client.getJobDetails(context.getJobID())
                            .get(
                                    context.getConfiguration()
                                            .get(FLINK_CLIENT_TIMEOUT)
                                            .toSeconds(),
                                    TimeUnit.SECONDS);
            Long runningTimestamp = details.getTimestamps().get(JobStatus.RUNNING);
            if (runningTimestamp == null) {
                throw new IllegalStateException("RUNNING timestamp is unavailable");
            }
            return runningTimestamp;
        }
    }

    private Configuration runtimeConfig(KubernetesJobAutoScalerContext context) {
        var observeConfig = context.getResourceContext().getObserveConfig();
        return observeConfig == null ? context.getConfiguration() : observeConfig;
    }

    private void transition(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            Phase phase)
            throws Exception {
        if (transaction.getPhase() == phase) {
            return;
        }
        transaction.setPhase(phase);
        transaction.setPhaseTimestamp(clock.instant());
        persist(context, transaction);
    }

    private void fail(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            String message,
            Throwable cause)
            throws Exception {
        transaction.setError(cause == null ? message : message + ": " + rootCauseMessage(cause));
        if (transaction.getPhase() == Phase.FAILED) {
            persist(context, transaction);
        } else {
            transition(context, transaction, Phase.FAILED);
        }
        LOG.error(transaction.getError(), cause);
    }

    private String rootCauseMessage(Throwable failure) {
        var rootCause = failure;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        var message = rootCause.getMessage();
        return rootCause.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
    }

    private void persist(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        stateStore.storeCheckpointRescaleTransaction(context, transaction);
        setPhaseAnnotation(context, transaction.getPhase());
        stateStore.flush(context);
    }

    private void setPhaseAnnotation(KubernetesJobAutoScalerContext context, Phase phase) {
        var annotations = context.getResource().getMetadata().getAnnotations();
        if (annotations == null) {
            annotations = new HashMap<>();
            context.getResource().getMetadata().setAnnotations(annotations);
        }
        if (phase == null) {
            annotations.remove(PHASE_ANNOTATION);
        } else {
            annotations.put(PHASE_ANNOTATION, phase.name());
        }
    }

    private long annotationNonce(KubernetesJobAutoScalerContext context, String annotation) {
        var value =
                Optional.ofNullable(context.getResource().getMetadata().getAnnotations())
                        .map(values -> values.get(annotation))
                        .orElse(null);
        if (value == null) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOG.warn("Ignoring invalid {} annotation value: {}", annotation, value);
            return 0L;
        }
    }

    private boolean transactionAcknowledged(
            KubernetesJobAutoScalerContext context, String annotation, String transactionId) {
        if (transactionId == null || transactionId.isBlank()) {
            return false;
        }
        return Optional.ofNullable(context.getResource().getMetadata().getAnnotations())
                .map(values -> values.get(annotation))
                .map(transactionId::equals)
                .orElse(false);
    }

    private boolean producerPauseEnabled(KubernetesJobAutoScalerContext context) {
        return context.getConfiguration().get(CHECKPOINT_RESCALE_PRODUCER_PAUSE_ENABLED);
    }

    private boolean enabled(KubernetesJobAutoScalerContext context) {
        return context.getConfiguration().get(CHECKPOINT_RESCALE_ENABLED);
    }

    private boolean sameTarget(
            CheckpointRescaleTransaction transaction,
            Map<String, String> parallelism,
            Map<String, String> resourceProfiles) {
        return transaction.getTargetParallelismOverrides().equals(parallelism)
                && transaction.getTargetResourceProfileOverrides().equals(resourceProfiles);
    }

    private boolean currentJobMatches(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction) {
        return context.getJobID() != null
                && transaction.getJobId().equals(context.getJobID().toHexString());
    }

    private boolean observesTarget(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction) {
        return matchesOverrides(
                context.getResourceContext().getObserveConfig(), frozenTarget(transaction));
    }

    private boolean desiredSpecContainsTarget(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction) {
        return matchesOverrides(
                Configuration.fromMap(context.getResource().getSpec().getFlinkConfiguration()),
                frozenTarget(transaction));
    }

    private static ScalingTarget frozenTarget(CheckpointRescaleTransaction transaction) {
        return new ScalingTarget(
                transaction.getTargetParallelismOverrides(),
                transaction.getTargetResourceProfileOverrides());
    }

    private static boolean matchesOverrides(Configuration config, ScalingTarget target) {
        return config != null
                && config.get(PipelineOptions.PARALLELISM_OVERRIDES).equals(target.getParallelism())
                && config.get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES)
                        .equals(target.getResourceProfiles());
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }
}
