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

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.exceptions.NotReadyException;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.realizer.ScalingRealizer.A4sIntentResult;
import org.apache.flink.autoscaler.state.A4sScalingIntent;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.A4S_CONDITIONAL_JUSTIN_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.A4S_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.JUSTIN_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.initRecommendedParallelism;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.resetRecommendedParallelism;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingHistory;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingTracking;

/** The default implementation of {@link JobAutoScaler}. */
public class JobAutoScalerImpl<KEY, Context extends JobAutoScalerContext<KEY>>
        implements JobAutoScaler<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    @VisibleForTesting protected static final String AUTOSCALER_ERROR = "AutoscalerError";

    private final ScalingMetricCollector<KEY, Context> metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor<KEY, Context> scalingExecutor;
    private final AutoScalerEventHandler<KEY, Context> eventHandler;
    private ScalingRealizer<KEY, Context> scalingRealizer;
    private final AutoScalerStateStore<KEY, Context> stateStore;

    private Clock clock = Clock.systemDefaultZone();

    @VisibleForTesting
    final Map<KEY, EvaluatedMetrics> lastEvaluatedMetrics = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<KEY, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    public JobAutoScalerImpl(
            ScalingMetricCollector<KEY, Context> metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor<KEY, Context> scalingExecutor,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            ScalingRealizer<KEY, Context> scalingRealizer,
            AutoScalerStateStore<KEY, Context> stateStore) {
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.eventHandler = eventHandler;
        this.scalingRealizer = scalingRealizer;
        this.stateStore = stateStore;
    }

    @Override
    public void scale(Context ctx) throws Exception {
        var autoscalerMetrics = getOrInitAutoscalerFlinkMetrics(ctx);
        var conf = ctx.getConfiguration();
        boolean a4sEnabled = false;
        boolean reconcileConditional = false;
        boolean realizeOverrides = false;
        try {
            a4sEnabled = conf.get(JUSTIN_ENABLED) && conf.get(A4S_ENABLED);
            boolean autoscalerEnabled = conf.getBoolean(AUTOSCALER_ENABLED);
            boolean allowNewIntent =
                    autoscalerEnabled && a4sEnabled && conf.get(A4S_CONDITIONAL_JUSTIN_ENABLED);
            reconcileConditional = allowNewIntent;
            var pending = stateStore.getA4sScalingIntent(ctx);
            reconcileConditional |= pending.isPresent();
            realizeOverrides = !reconcileConditional;
            if (reconcileConditional) {
                scaleConditionally(ctx, autoscalerMetrics, pending, allowNewIntent);
                return;
            }

            if (!autoscalerEnabled) {
                LOG.debug("Autoscaler is disabled");
                stateStore.clearAll(ctx);
                stateStore.flush(ctx);
                return;
            }

            if (ctx.getJobStatus() != JobStatus.RUNNING) {
                LOG.debug("Autoscaler is waiting for stable, running state");
                lastEvaluatedMetrics.remove(ctx.getJobKey());
                return;
            }

            runScalingLogic(ctx, autoscalerMetrics);
            stateStore.flush(ctx);
        } catch (NotReadyException e) {
            LOG.debug("Not ready for scaling", e);
        } catch (Throwable e) {
            if (a4sEnabled || reconcileConditional) {
                // A failed reconciliation can leave one half of an A4S decision in the
                // state-store cache. Discard it before any subsequent reconciliation.
                realizeOverrides = false;
                stateStore.removeInfoFromCache(ctx.getJobKey());
            }
            onError(ctx, autoscalerMetrics, e);
            if (reconcileConditional) {
                // Let the controller retry without interpreting an incomplete status overlay
                // as a user-initiated spec change.
                throw e;
            }
        } finally {
            if (realizeOverrides) {
                try {
                    if (conf.getBoolean(JUSTIN_ENABLED)) {
                        applyResourceProfileOverrides(ctx);
                    } else {
                        applyParallelismOverrides(ctx);
                    }
                    applyConfigOverrides(ctx);
                } catch (Exception e) {
                    LOG.error("Error applying overrides.", e);
                    onError(ctx, autoscalerMetrics, e);
                }
            }
        }
    }

    private void scaleConditionally(
            Context ctx,
            AutoscalerFlinkMetrics autoscalerMetrics,
            Optional<A4sScalingIntent> pending,
            boolean allowNewIntent)
            throws Exception {
        var jobId = Objects.toString(ctx.getJobID(), "");
        if (pending.isPresent() && pending.get().getJobId().equals(jobId)) {
            scalingRealizer.restoreA4sOverrides(
                    ctx, pending.get().getParallelism(), pending.get().getResourceProfiles());
        }
        scalingRealizer.restoreA4sOverrides(
                ctx, getParallelismOverrides(ctx), getResourceProfileOverrides(ctx));

        if (ctx.getJobStatus() != JobStatus.RUNNING) {
            lastEvaluatedMetrics.remove(ctx.getJobKey());
            return;
        }
        if (pending.isEmpty()) {
            runScalingLogic(ctx, autoscalerMetrics);
            stateStore.flush(ctx);
            pending = stateStore.getA4sScalingIntent(ctx);
        }
        if (pending.isEmpty()) {
            return;
        }
        var intent = pending.get();
        var conf = ctx.getConfiguration();
        boolean allowSubmission =
                allowNewIntent
                        && conf.get(SCALING_ENABLED)
                        && intent.matchesConfiguration(conf)
                        && !CalendarUtils.inExcludedPeriods(conf, clock.instant());
        var result =
                intent.getJobId().equals(jobId)
                        ? scalingRealizer.reconcileA4sIntent(ctx, intent, clock, allowSubmission)
                        : A4sIntentResult.DISCARDED;
        if (result == A4sIntentResult.PENDING) {
            return;
        }
        if (result == A4sIntentResult.APPLIED) {
            stateStore.storeParallelismOverrides(ctx, intent.getParallelism());
            stateStore.storeResourceProfileOverrides(ctx, intent.getResourceProfiles());
        }
        stateStore.removeA4sScalingIntent(ctx);
        stateStore.flush(ctx);
    }

    @Override
    public void cleanup(KEY jobKey) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(jobKey);
        lastEvaluatedMetrics.remove(jobKey);
        flinkMetrics.remove(jobKey);
        stateStore.removeInfoFromCache(jobKey);
    }

    @VisibleForTesting
    protected Map<String, String> getParallelismOverrides(Context ctx) throws Exception {
        return stateStore.getParallelismOverrides(ctx);
    }

    @VisibleForTesting
    protected Map<String, String> getResourceProfileOverrides(Context ctx) throws Exception {
        return stateStore.getResourceProfileOverrides(ctx);
    }

    /**
     * If there are any parallelism overrides by the {@link JobAutoScaler} apply them to the
     * scalingRealizer.
     *
     * @param ctx Job context
     */
    @VisibleForTesting
    protected void applyParallelismOverrides(Context ctx) throws Exception {
        var overrides = getParallelismOverrides(ctx);
        if (overrides.isEmpty()) {
            return;
        }
        LOG.debug("Applying parallelism overrides: {}", overrides);

        var conf = ctx.getConfiguration();
        var userOverrides = new HashMap<>(conf.get(PipelineOptions.PARALLELISM_OVERRIDES));
        var exclusions = conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);

        overrides.forEach(
                (k, v) -> {
                    // Respect user override for excluded vertices
                    if (exclusions.contains(k)) {
                        userOverrides.putIfAbsent(k, v);
                    } else {
                        userOverrides.put(k, v);
                    }
                });
        scalingRealizer.realizeParallelismOverrides(ctx, userOverrides);
    }

    /**
     * If there are any parallelism overrides by the {@link JobAutoScaler} apply them to the
     * scalingRealizer.
     *
     * @param ctx Job context
     */
    @VisibleForTesting
    protected void applyResourceProfileOverrides(Context ctx) throws Exception {
        var parallelismOverrides = getParallelismOverrides(ctx);
        var resourceProfileOverrides = getResourceProfileOverrides(ctx);
        if (parallelismOverrides.isEmpty() && resourceProfileOverrides.isEmpty()) {
            return;
        }
        if (ctx.getConfiguration().get(A4S_ENABLED)
                && !parallelismOverrides.keySet().equals(resourceProfileOverrides.keySet())) {
            throw new IllegalStateException("Incomplete A4S parallelism and resource-profile overrides");
        }
        LOG.debug("Applying parallelism overrides: {}, {}", parallelismOverrides, resourceProfileOverrides);

        scalingRealizer.realizeParallelismOverrides(ctx, parallelismOverrides, resourceProfileOverrides);
    }

    @VisibleForTesting
    void applyConfigOverrides(Context ctx) throws Exception {
        var conf = ctx.getConfiguration();
        if (!conf.get(AutoScalerOptions.MEMORY_TUNING_ENABLED)
                || (conf.get(JUSTIN_ENABLED) && conf.get(A4S_ENABLED))) {
            return;
        }

        ConfigChanges configChanges = stateStore.getConfigChanges(ctx);
        LOG.debug("Applying config overrides: {}", configChanges);
        scalingRealizer.realizeConfigOverrides(ctx, configChanges);
    }

    private void runScalingLogic(Context ctx, AutoscalerFlinkMetrics autoscalerMetrics)
            throws Exception {

        var justinEnabled = ctx.getConfiguration().getBoolean(JUSTIN_ENABLED);

        var collectedMetrics = metricsCollector.updateMetrics(ctx, stateStore);
        var jobTopology = collectedMetrics.getJobTopology();

        var now = clock.instant();
        var scalingTracking = getTrimmedScalingTracking(stateStore, ctx, now);
        var scalingHistory = getTrimmedScalingHistory(stateStore, ctx, now);
        // A scaling tracking without an end time gets created whenever a scaling decision is
        // applied. Here, we record the end time for it (runScalingLogic is only called when the job
        // transitions back into the RUNNING state).
        if (scalingTracking.recordRestartDurationIfTrackedAndParallelismMatches(
                collectedMetrics.getJobRunningTs(), jobTopology, scalingHistory)) {
            stateStore.storeScalingTracking(ctx, scalingTracking);
        }

        // We require at least 2 metric collections for evaluation as required by rate computations
        // from accumulated metrics
        if (collectedMetrics.getMetricHistory().size() < 2) {
            return;
        }
        LOG.debug("Collected metrics: {}", collectedMetrics);

        // Scaling tracking data contains previous restart times that are taken into account
        var restartTime = scalingTracking.getMaxRestartTimeOrDefault(ctx.getConfiguration());
        var evaluatedMetrics =
                evaluator.evaluate(ctx.getConfiguration(), collectedMetrics, restartTime);
        LOG.debug("Evaluated metrics: {}", evaluatedMetrics);
        lastEvaluatedMetrics.put(ctx.getJobKey(), evaluatedMetrics);

        initRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
        autoscalerMetrics.registerScalingMetrics(
                jobTopology.getVerticesInTopologicalOrder(),
                () -> lastEvaluatedMetrics.get(ctx.getJobKey()));

        if (!collectedMetrics.isFullyCollected()) {
            // We have done an upfront evaluation, but we are not ready for scaling.
            resetRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
            return;
        }

        var delayedScaleDown = stateStore.getDelayedScaleDown(ctx);
        var parallelismChanged =
                scalingExecutor.scaleResource(
                        ctx,
                        evaluatedMetrics,
                        scalingHistory,
                        scalingTracking,
                        now,
                        jobTopology,
                        delayedScaleDown);

        if (delayedScaleDown.isUpdated()) {
            stateStore.storeDelayedScaleDown(ctx, delayedScaleDown);
        }

        if (parallelismChanged) {
            autoscalerMetrics.incrementScaling();
        } else {
            autoscalerMetrics.incrementBalanced();
        }
    }

    private void onError(Context ctx, AutoscalerFlinkMetrics autoscalerMetrics, Throwable e) {
        LOG.error("Error while scaling job", e);
        autoscalerMetrics.incrementError();
        eventHandler.handleException(ctx, AUTOSCALER_ERROR, e);
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(Context ctx) {
        return this.flinkMetrics.computeIfAbsent(
                ctx.getJobKey(),
                id -> new AutoscalerFlinkMetrics(ctx.getMetricGroup().addGroup("AutoScaler")));
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }
}
