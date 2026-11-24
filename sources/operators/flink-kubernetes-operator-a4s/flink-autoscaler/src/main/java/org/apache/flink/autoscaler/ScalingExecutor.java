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
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.a4s.A4S;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.resources.NoopResourceCheck;
import org.apache.flink.autoscaler.resources.ResourceCheck;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.tuning.MemoryTuning;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.autoscaler.utils.justin.JustinVertexResourceRequirements;
import org.apache.flink.autoscaler.utils.justin.ResourceProfileParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.*;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_EXECUTION_DISABLED_REASON;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.*;

/** Class responsible for executing scaling decisions. */
public class ScalingExecutor<KEY, Context extends JobAutoScalerContext<KEY>> {

    public static final String GC_PRESSURE_MESSAGE =
            "GC Pressure %s is above the allowed limit for scaling operations. Please adjust the available memory manually.";

    public static final String HEAP_USAGE_MESSAGE =
            "Heap Usage %s is above the allowed limit for scaling operations. Please adjust the available memory manually.";

    public static final String RESOURCE_QUOTA_REACHED_MESSAGE =
            "Resource usage is above the allowed limit for scaling operations. Please adjust the resource quota manually.";

    private static final Logger LOG = LoggerFactory.getLogger(ScalingExecutor.class);

    private final JobVertexScaler<KEY, Context> jobVertexScaler;
    private final AutoScalerEventHandler<KEY, Context> autoScalerEventHandler;
    private final AutoScalerStateStore<KEY, Context> autoScalerStateStore;
    private final ResourceCheck resourceCheck;

    private final ScalingConfigurations scalingConfigurations;

    private static final HashMap<JobID, Integer> periods = new HashMap<>();

    public ScalingExecutor(
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore) {
        this(autoScalerEventHandler, autoScalerStateStore, null);
    }

    public ScalingExecutor(
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore,
            @Nullable ResourceCheck resourceCheck) {
        this.jobVertexScaler = new JobVertexScaler<>(autoScalerEventHandler);
        this.autoScalerEventHandler = autoScalerEventHandler;
        this.autoScalerStateStore = autoScalerStateStore;
        this.resourceCheck = resourceCheck != null ? resourceCheck : new NoopResourceCheck();
        this.scalingConfigurations = new ScalingConfigurations();
    }

    public boolean scaleResource(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown)
            throws Exception {
        var conf = context.getConfiguration();
        if (isJobUnderMemoryPressure(context, evaluatedMetrics.getGlobalMetrics())) {
            LOG.info("Skipping scaling due to memory pressure");
            return false;
        }

        var restartTime = scalingTracking.getMaxRestartTimeOrDefault(conf);
        var scalingSummaries =
                new HashMap<>(
                        computeScalingSummary(
                                context,
                                evaluatedMetrics,
                                scalingHistory,
                                restartTime,
                                jobTopology,
                                delayedScaleDown));

        Map<JobVertexID, ResourceProfile> a4sProfiles = new HashMap<>();
        boolean memoryChanged = false;
        boolean justinEnabled = conf.get(JUSTIN_ENABLED);
        boolean a4sEnabled = justinEnabled && conf.get(A4S_ENABLED);
        if (a4sEnabled) {
            var decisions =
                    new A4S(jobTopology, evaluatedMetrics, scalingSummaries).makeDecision(conf);
            if (!decisions.isEmpty() || !scalingSummaries.isEmpty()) {
                var currentRequirements = readResourceRequirements(context);
                var excluded = conf.get(VERTEX_EXCLUDE_IDS);
                for (var entry : evaluatedMetrics.getVertexMetrics().entrySet()) {
                    var id = entry.getKey();
                    var metrics = entry.getValue();
                    var currentProfile = appliedProfile(currentRequirements.get(id));
                    if (currentProfile == null) {
                        LOG.warn(
                                "A4S: Deferring rescale because the applied resource profile for vertex {} cannot be preserved",
                                id);
                        return false;
                    }
                    var profile = currentProfile;
                    var decision = decisions.get(id);
                    if (decision != null && !excluded.contains(id.toHexString())) {
                        int currentParallelism = (int) metrics.get(PARALLELISM).getCurrent();
                        int targetParallelism = decision.getParallelism();
                        if (currentParallelism == targetParallelism) {
                            scalingSummaries.remove(id);
                        } else {
                            metrics.put(
                                    EXPECTED_PROCESSING_RATE,
                                    EvaluatedScalingMetric.of(
                                            evaluatedMetrics
                                                    .getMemoryParallelismCurves()
                                                    .get(id)
                                                    .getTargetThroughput()));
                            scalingSummaries.put(
                                    id,
                                    new ScalingSummary(
                                            currentParallelism, targetParallelism, metrics));
                        }
                        double memoryMb =
                                conf.get(A4S_MANAGED_MEMORY_OVERRIDE_ENABLED)
                                        ? conf.get(A4S_MANAGED_MEMORY_OVERRIDE_MB)
                                        : decision.getMemoryMB() + conf.get(A4S_MEMORY_BASE_MB);
                        if (!Double.isFinite(memoryMb) || memoryMb <= 0) {
                            throw new IllegalArgumentException(
                                    "Invalid A4S managed memory: " + memoryMb);
                        }
                        profile =
                                ResourceProfile.newBuilder(currentProfile)
                                        .setManagedMemory(
                                                MemorySize.ofMebiBytes((long) Math.ceil(memoryMb)))
                                        .build();
                    }
                    // Vertices without a usable A4S decision retain their deployed resources.
                    a4sProfiles.put(id, profile);
                    memoryChanged |= !profile.equals(currentProfile);
                }
            }
        }

        if (scalingSummaries.isEmpty() && !memoryChanged) {
            LOG.info("Job resources already match the scaling recommendation.");
            return false;
        }

        updateRecommendedParallelism(evaluatedMetrics.getVertexMetrics(), scalingSummaries);

        if (checkIfBlockedAndTriggerScalingEvent(context, scalingSummaries, conf, now)) {
            return false;
        }

        var configOverrides =
                MemoryTuning.tuneTaskManagerMemory(
                        context,
                        evaluatedMetrics,
                        jobTopology,
                        scalingSummaries,
                        autoScalerEventHandler);

        var memoryTuningEnabled = conf.get(AutoScalerOptions.MEMORY_TUNING_ENABLED);
        if (scalingWouldExceedMaxResources(
                memoryTuningEnabled ? configOverrides.newConfigWithOverrides(conf) : conf,
                jobTopology,
                evaluatedMetrics,
                scalingSummaries,
                context)) {
            return false;
        }

        var parallelismOverrides =
                getVertexParallelismOverrides(evaluatedMetrics.getVertexMetrics(), scalingSummaries);
        Map<String, String> profileOverrides = Map.of();
        if (a4sEnabled) {
            profileOverrides =
                    a4sProfiles.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            e -> e.getKey().toString(),
                                            e -> e.getValue().toString()));
        } else if (justinEnabled) {
            var currentScalingConf =
                    scalingConfigurations.setCurrentConfiguration(
                            context.getJobID(),
                            evaluatedMetrics.getVertexMetrics(),
                            scalingSummaries,
                            periods.getOrDefault(context.getJobID(), 0));

            policy(context, currentScalingConf, conf);

            LOG.info(scalingConfigurations.toString());

            var profiles = getJustinResourceProfiles(context, currentScalingConf);
            if (profiles.isEmpty()) {
                return false;
            }
            profileOverrides = profiles.get();
            parallelismOverrides = getVertexParallelismOverrides(currentScalingConf);
        }

        addToScalingHistoryAndStore(
                autoScalerStateStore, context, scalingHistory, now, scalingSummaries);
        scalingTracking.addScalingRecord(now, new ScalingRecord());
        autoScalerStateStore.storeScalingTracking(context, scalingTracking);
        autoScalerStateStore.storeParallelismOverrides(context, parallelismOverrides);
        if (justinEnabled) {
            autoScalerStateStore.storeResourceProfileOverrides(context, profileOverrides);
        }
        autoScalerStateStore.storeConfigChanges(context, configOverrides);

        // Try to clear all delayed scale down requests after scaling.
        delayedScaleDown.clearAll();

        return true;
    }

    @VisibleForTesting
    Map<JobVertexID, JustinVertexResourceRequirements> readResourceRequirements(Context context)
            throws Exception {
        return readResourceRequirements(context, false);
    }

    private Map<JobVertexID, JustinVertexResourceRequirements> readResourceRequirements(
            Context context, boolean includeDefaultSlot) throws Exception {
        var headers = JustinResourceRequirementsHeaders.INSTANCE;
        var parameters = headers.getUnresolvedMessageParameters();
        parameters.jobPathParameter.resolve(context.getJobID());
        parameters.includeAppliedProfile.resolve(Collections.singletonList(true));
        if (includeDefaultSlot) {
            parameters.includeDefaultSlot.resolve(Collections.singletonList(true));
        }
        try (var client = context.getRestClusterClient()) {
            return client.sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                    .get(
                            context.getConfiguration().get(FLINK_CLIENT_TIMEOUT).toMillis(),
                            TimeUnit.MILLISECONDS)
                    .asJustinResourceRequirements()
                    .orElseThrow()
                    .getJobVertexParallelisms();
        }
    }

    private void updateRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {
        scalingSummaries.forEach(
                (jobVertexID, scalingSummary) ->
                        evaluatedMetrics
                                .get(jobVertexID)
                                .put(
                                        ScalingMetric.RECOMMENDED_PARALLELISM,
                                        EvaluatedScalingMetric.of(
                                                scalingSummary.getNewParallelism())));
    }

    @VisibleForTesting
    static boolean allChangedVerticesWithinUtilizationTarget(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Set<JobVertexID> changedVertices) {
        // No vertices with changed parallelism.
        if (changedVertices.isEmpty()) {
            return true;
        }

        for (JobVertexID vertex : changedVertices) {
            var metrics = evaluatedMetrics.get(vertex);

            double trueProcessingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
            double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
            double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

            if (trueProcessingRate < scaleUpRateThreshold
                    || trueProcessingRate > scaleDownRateThreshold) {
                LOG.debug(
                        "Vertex {} processing rate {} is outside ({}, {})",
                        vertex,
                        trueProcessingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
                return false;
            } else {
                LOG.debug(
                        "Vertex {} processing rate {} is within target ({}, {})",
                        vertex,
                        trueProcessingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
            }
        }
        LOG.info("All vertex processing rates are within target.");
        return true;
    }

    @VisibleForTesting
    Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Duration restartTime,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown) {
        LOG.debug("Restart time used in scaling summary computation: {}", restartTime);

        var out = new HashMap<JobVertexID, ScalingSummary>();

        var excludeVertexIdList =
                context.getConfiguration().get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);
        evaluatedMetrics
                .getVertexMetrics()
                .forEach(
                        (v, metrics) -> {
                            if (excludeVertexIdList.contains(v.toHexString())) {
                                LOG.debug(
                                        "Vertex {} is part of `vertex.exclude.ids` config, Ignoring it for scaling",
                                        v);
                            } else {
                                var currentParallelism =
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();

                                var parallelismChange =
                                        jobVertexScaler.computeScaleTargetParallelism(
                                                context,
                                                v,
                                                jobTopology.get(v).getInputs().values(),
                                                metrics,
                                                scalingHistory.getOrDefault(
                                                        v, Collections.emptySortedMap()),
                                                restartTime,
                                                delayedScaleDown);
                                if (parallelismChange.isNoChange()) {
                                    return;
                                }
                                out.put(
                                        v,
                                        new ScalingSummary(
                                                currentParallelism,
                                                parallelismChange.getNewParallelism(),
                                                metrics));
                            }
                        });

        // If the Utilization of all tasks is within range, we can skip scaling.
        if (allChangedVerticesWithinUtilizationTarget(
                evaluatedMetrics.getVertexMetrics(), out.keySet())) {
            return Map.of();
        }

        return out;
    }

    private boolean isJobUnderMemoryPressure(
            Context ctx, Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics) {

        var gcPressure = evaluatedMetrics.get(ScalingMetric.GC_PRESSURE).getCurrent();
        var conf = ctx.getConfiguration();
        if (gcPressure > conf.get(AutoScalerOptions.GC_PRESSURE_THRESHOLD)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Normal,
                    "MemoryPressure",
                    String.format(GC_PRESSURE_MESSAGE, gcPressure),
                    "gcPressure",
                    conf.get(SCALING_EVENT_INTERVAL));
            return true;
        }

        var heapUsage = evaluatedMetrics.get(ScalingMetric.HEAP_MAX_USAGE_RATIO).getAverage();
        if (heapUsage > conf.get(AutoScalerOptions.HEAP_USAGE_THRESHOLD)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Normal,
                    "MemoryPressure",
                    String.format(HEAP_USAGE_MESSAGE, heapUsage),
                    "heapUsage",
                    conf.get(SCALING_EVENT_INTERVAL));
            return true;
        }

        return false;
    }

    @VisibleForTesting
    protected boolean scalingWouldExceedMaxResources(
            Configuration tunedConfig,
            JobTopology jobTopology,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Context ctx) {
        if (scalingWouldExceedClusterResources(
                tunedConfig, evaluatedMetrics, scalingSummaries, ctx)) {
            return true;
        }
        if (scalingWouldExceedResourceQuota(tunedConfig, jobTopology, scalingSummaries, ctx)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Warning,
                    "ResourceQuotaReached",
                    RESOURCE_QUOTA_REACHED_MESSAGE,
                    null,
                    tunedConfig.get(SCALING_EVENT_INTERVAL));
            return true;
        }
        return false;
    }

    private boolean scalingWouldExceedClusterResources(
            Configuration tunedConfig,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            JobAutoScalerContext<?> ctx) {

        final double taskManagerCpu = ctx.getTaskManagerCpu().orElse(0.);
        final MemorySize taskManagerMemory = MemoryTuning.getTotalMemory(tunedConfig, ctx);

        if (taskManagerCpu <= 0 || taskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            // We can't extract the requirements, we can't make any assumptions
            return false;
        }

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        if (!globalMetrics.containsKey(ScalingMetric.NUM_TASK_SLOTS_USED)) {
            LOG.info("JM metrics not ready yet");
            return true;
        }

        int numTaskSlotsUsed =
                (int) globalMetrics.get(ScalingMetric.NUM_TASK_SLOTS_USED).getCurrent();
        final int numTaskSlotsAfterRescale =
                ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                        evaluatedMetrics.getVertexMetrics(), scalingSummaries, numTaskSlotsUsed);

        int taskSlotsPerTm = tunedConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);

        int currentNumTms = (int) Math.ceil(numTaskSlotsUsed / (double) taskSlotsPerTm);
        int newNumTms = (int) Math.ceil(numTaskSlotsAfterRescale / (double) taskSlotsPerTm);

        return !resourceCheck.trySchedule(
                currentNumTms, newNumTms, taskManagerCpu, taskManagerMemory);
    }

    protected static boolean scalingWouldExceedResourceQuota(
            Configuration tunedConfig,
            JobTopology jobTopology,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            JobAutoScalerContext<?> ctx) {

        if (jobTopology == null || jobTopology.getSlotSharingGroupMapping().isEmpty()) {
            return false;
        }

        var cpuQuota = tunedConfig.getOptional(AutoScalerOptions.CPU_QUOTA);
        var memoryQuota = tunedConfig.getOptional(AutoScalerOptions.MEMORY_QUOTA);
        var tmMemory = MemoryTuning.getTotalMemory(tunedConfig, ctx);
        var tmCpu = ctx.getTaskManagerCpu().orElse(0.);

        if (cpuQuota.isPresent() || memoryQuota.isPresent()) {
            var currentSlotSharingGroupMaxParallelisms = new HashMap<SlotSharingGroupId, Integer>();
            var newSlotSharingGroupMaxParallelisms = new HashMap<SlotSharingGroupId, Integer>();
            for (var e : jobTopology.getSlotSharingGroupMapping().entrySet()) {
                int currentMaxParallelism =
                        e.getValue().stream()
                                .filter(scalingSummaries::containsKey)
                                .mapToInt(v -> scalingSummaries.get(v).getCurrentParallelism())
                                .max()
                                .orElse(0);
                currentSlotSharingGroupMaxParallelisms.put(e.getKey(), currentMaxParallelism);
                int newMaxParallelism =
                        e.getValue().stream()
                                .filter(scalingSummaries::containsKey)
                                .mapToInt(v -> scalingSummaries.get(v).getNewParallelism())
                                .max()
                                .orElse(0);
                newSlotSharingGroupMaxParallelisms.put(e.getKey(), newMaxParallelism);
            }

            var numSlotsPerTm = tunedConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);
            var currentTotalSlots =
                    currentSlotSharingGroupMaxParallelisms.values().stream()
                            .mapToInt(Integer::intValue)
                            .sum();
            var currentNumTms = currentTotalSlots / numSlotsPerTm;
            var newTotalSlots =
                    newSlotSharingGroupMaxParallelisms.values().stream()
                            .mapToInt(Integer::intValue)
                            .sum();
            var newNumTms = newTotalSlots / numSlotsPerTm;

            if (newNumTms <= currentNumTms) {
                LOG.debug(
                        "Skipping quota check due to new resource allocation is less or equals than the current");
                return false;
            }

            if (cpuQuota.isPresent()) {
                LOG.debug("CPU resource quota is {}, checking limits", cpuQuota.get());
                double totalCPU = tmCpu * newNumTms;
                if (totalCPU > cpuQuota.get()) {
                    LOG.info("CPU resource quota reached with value: {}", totalCPU);
                    return true;
                }
            }

            if (memoryQuota.isPresent()) {
                LOG.debug("Memory resource quota is {}, checking limits", memoryQuota.get());
                long totalMemory = tmMemory.getBytes() * newNumTms;
                if (totalMemory > memoryQuota.get().getBytes()) {
                    LOG.info(
                            "Memory resource quota reached with value: {}",
                            new MemorySize(totalMemory));
                    return true;
                }
            }
        }

        return false;
    }

    private static Map<String, String> getVertexParallelismOverrides(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    if (summaries.containsKey(id)) {
                        overrides.put(
                                id.toString(),
                                String.valueOf(summaries.get(id).getNewParallelism()));
                    } else {
                        overrides.put(
                                id.toString(),
                                String.valueOf(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent()));
                    }
                });
        return overrides;
    }

    private static Map<String, String> getVertexParallelismOverrides(
            ScalingConfigurations.ScalingConfiguration scalingConfiguration) {
        var overrides = new HashMap<String, String>();
        scalingConfiguration.getScaling().forEach((
                (jobVertexID, scalingInformation) -> {
                    overrides.put(
                            jobVertexID.toString(),
                            String.valueOf(scalingInformation.getParallelism()));
        }));

        return overrides;
    }

    @Nullable
    private static ResourceProfile appliedProfile(
            @Nullable JustinVertexResourceRequirements current) {
        var profile = current == null ? null : current.getAppliedResourceProfile();
        if (profile == null
                || profile.equals(ResourceProfile.UNKNOWN)
                || profile.equals(ResourceProfile.ANY)
                || !profile.equals(ResourceProfileParser.parse(profile.toString()))) {
            return null;
        }
        return profile;
    }

    private Optional<Map<String, String>> getJustinResourceProfiles(
            Context context, ScalingConfigurations.ScalingConfiguration scaling) throws Exception {
        var conf = context.getConfiguration();
        Map<JobVertexID, JustinVertexResourceRequirements> live = null;
        var overrides = new HashMap<String, String>();
        for (var entry : scaling.getScaling().entrySet()) {
            var id = entry.getKey();
            var information = entry.getValue();
            ResourceProfile profile;
            if (!hasCacheHits(information)) {
                // A chained SQL operator can use managed memory without accessing the block cache.
                if (live == null) {
                    live = readResourceRequirements(context, true);
                }
                var current = live.get(id);
                profile =
                        current != null && current.isDefaultSlot()
                                ? ResourceProfile.UNKNOWN
                                : appliedProfile(current);
                if (profile == null) {
                    LOG.warn(
                            "Justin: Deferring rescale until the applied resource profile for {} is available",
                            id);
                    return Optional.empty();
                }
            } else {
                var memoryMb = information.getManagedMemoryMB();
                profile =
                        memoryMb >= 0
                                ? getResourceProfile(conf, memoryMb)
                                : getResourceProfile(conf, information.getMemoryLevel());
            }
            overrides.put(id.toString(), profile.toString());
        }
        return Optional.of(overrides);
    }

    private static boolean hasCacheHits(ScalingConfigurations.ScalingInformation information) {
        double hitRate = information.getAvgCacheHitRate();
        return Double.isFinite(hitRate) && hitRate > 0;
    }


    private boolean checkIfBlockedAndTriggerScalingEvent(
            Context context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Configuration conf,
            Instant now) {
        var scaleEnabled = conf.get(SCALING_ENABLED);
        var isExcluded = CalendarUtils.inExcludedPeriods(conf, now);
        String message;
        if (!scaleEnabled) {
            message =
                    SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                            + String.format(
                                    SCALING_EXECUTION_DISABLED_REASON,
                                    SCALING_ENABLED.key(),
                                    false);
        } else if (isExcluded) {
            message =
                    SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                            + String.format(
                                    SCALING_EXECUTION_DISABLED_REASON,
                                    EXCLUDED_PERIODS.key(),
                                    conf.get(EXCLUDED_PERIODS));
        } else {
            message = SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
        }
        autoScalerEventHandler.handleScalingEvent(
                context, scalingSummaries, message, conf.get(SCALING_EVENT_INTERVAL));

        return !scaleEnabled || isExcluded;
    }

    private static ResourceProfile getResourceProfile(Configuration conf, int memoryLevel) {
        var memory = conf.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        if (memory.getGibiBytes() > 3) { // 4 GB
            return ResourceProfile.newBuilder()
                    .setCpuCores(1.0)
                    .setTaskHeapMemoryMB(363)
                    .setTaskOffHeapMemoryMB(0)
                    .setManagedMemoryMB(memoryLevel == -1 ? 0 : (int) (343 * Math.pow(2, memoryLevel)))
                    .setNetworkMemoryMB(84)
                    .build();
        } else {
            return  ResourceProfile.newBuilder()
                    .setCpuCores(1.0)
                    .setTaskHeapMemoryMB(134)
                    .setTaskOffHeapMemoryMB(0)
                    .setManagedMemoryMB(memoryLevel == -1 ? 0 : (int) (158 * Math.pow(2, memoryLevel)))
                    .setNetworkMemoryMB(39)
                    .build();
        }
    }

    private static ResourceProfile getResourceProfile(Configuration conf, double managedMemoryMB) {
        var memory = conf.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        int roundedManagedMemoryMB = (int) Math.ceil(Math.max(0.0, managedMemoryMB));
        if (memory.getGibiBytes() > 3) { // 4 GB
            return ResourceProfile.newBuilder()
                    .setCpuCores(1.0)
                    .setTaskHeapMemoryMB(363)
                    .setTaskOffHeapMemoryMB(0)
                    .setManagedMemoryMB(roundedManagedMemoryMB)
                    .setNetworkMemoryMB(84)
                    .build();
        } else {
            return  ResourceProfile.newBuilder()
                    .setCpuCores(1.0)
                    .setTaskHeapMemoryMB(134)
                    .setTaskOffHeapMemoryMB(0)
                    .setManagedMemoryMB(roundedManagedMemoryMB)
                    .setNetworkMemoryMB(39)
                    .build();
        }
    }

    /**
     * Original Justin scaling policy (fallback when A4S is disabled).
     */
    private void policy(Context context, ScalingConfigurations.ScalingConfiguration scaling, Configuration conf) {
        LOG.info("Justin: Applying Justin scaling policy for job {}", context.getJobID());
        scaling.getScaling().forEach((id, information) -> {
            var previousInformation =
                    scalingConfigurations.getPreviousScalingInformation(
                            context.getJobID(),
                            id,
                            periods.getOrDefault(context.getJobID(), 0));
            if (!hasCacheHits(information)) {
                if (previousInformation != null) {
                    information.setMemoryLevel(previousInformation.getMemoryLevel());
                }
            } else {
                if (previousInformation != null) {
                    if ( previousInformation.getParallelism() != information.getParallelism()) {
                        if (previousInformation.isVerticalScaling()) { // Previous decision was elastic scaling
                            LOG.info("Justin: Previous decision was elastic scaling");
                            if (((information.getAvgCacheHitRate() - previousInformation.getAvgCacheHitRate() > conf.get(IMPROVED_CACHE_HIT_RATE_THRESHOLD))
                                    || information.getAvgStateLatency() < previousInformation.getAvgStateLatency()) ) { // We see an improvement from vertical scaling

                                LOG.info("Justin: We see an improvement from vertical scaling -> {} {}, {} {}"
                                        , information.getAvgCacheHitRate()
                                        , previousInformation.getAvgCacheHitRate()
                                        , information.getAvgStateLatency()
                                        , previousInformation.getAvgStateLatency());
                                if (information.getAvgThroughput() > previousInformation.getAvgThroughput() * (1.0 + conf.get(MIN_IMPROVED_THROUGHPUT))
                                    && information.getAvgCacheHitRate() < conf.get(MAX_CACHE_HIT_RATE_THRESHOLD)) { // Improved Tput and cache hit ratio below max thold
                                    LOG.info("Justin: Improved Tput and cache hit ratio below max thold -> {} > {} "
                                            , information.getAvgThroughput()
                                            , previousInformation.getAvgThroughput() * (1.0 + conf.get(MIN_IMPROVED_THROUGHPUT)));
                                    if (previousInformation.getMemoryLevel()+1 < ScalingConfigurations.MAX_MEMORY_LEVEL) { // Can scale up
                                        LOG.info("Justin: Cant scale up");
                                        information.setParallelism(previousInformation.getParallelism());
                                        information.setMemoryLevel(previousInformation.getMemoryLevel()+1);
                                        information.setVerticalScaling(true);
                                    } else { // Max level
                                        information.setMemoryLevel(previousInformation.getMemoryLevel());
                                        information.setHorizontalScaling(true);
                                    }

                                } else { // Improvement not enough to increase tput
                                    LOG.info("Justin: Improvement not enough to increase tput");
                                    information.setMemoryLevel(previousInformation.getMemoryLevel());
                                    information.setHorizontalScaling(true);
                                }
                            } else { // We see no an improvement from vertical scaling, rollback
                                LOG.info("Justin: We see no an improvement from vertical scaling, rollback");
                                information.setMemoryLevel(previousInformation.getMemoryLevel()-1);
                                information.setStopVerticalScaling(true);
                                information.setHorizontalScaling(true);
                            }
                        } else {
                            LOG.info("Justin: No previous vertical decision");
                            if (((information.getAvgCacheHitRate() < conf.get(MIN_CACHE_HIT_RATE_THRESHOLD))
                                    || information.getAvgStateLatency() > conf.get(STATE_ACCESS_LATENCY_THRESHOLD))
                              && previousInformation.getMemoryLevel()+1 < ScalingConfigurations.MAX_MEMORY_LEVEL) {
                                LOG.info("Justin: indicators show that we should scale up");
                                information.setParallelism(previousInformation.getParallelism());
                                information.setMemoryLevel(previousInformation.getMemoryLevel()+1);
                                information.setVerticalScaling(true);

                            } else {
                                information.setHorizontalScaling(true);
                                information.setMemoryLevel(previousInformation.getMemoryLevel());
                            }
                        }
                    }
                } else { // First decision
                    LOG.info("Making first scaling decision for job {}", context.getJobID());
                    if (information.getParallelism() != 1) { // Scale out decision
                        if (information.getAvgCacheHitRate() < conf.get(MIN_CACHE_HIT_RATE_THRESHOLD)
                                || information.getAvgStateLatency() > conf.get(STATE_ACCESS_LATENCY_THRESHOLD)) {
                            information.setParallelism(1);
                            information.setMemoryLevel(1);
                            information.setVerticalScaling(true);
                        } else {
                            information.setHorizontalScaling(true);
                        }
                    }
                }
            }
        });
    }

    public static void scalingTriggered(JobID jobID) {
        var newPeriod = periods.getOrDefault(jobID, 0) + 1;
        periods.put(jobID, newPeriod);
        LOG.info("Incrementing scaling period for job {}. New period: {}",
                jobID,
                newPeriod);
    }
}
