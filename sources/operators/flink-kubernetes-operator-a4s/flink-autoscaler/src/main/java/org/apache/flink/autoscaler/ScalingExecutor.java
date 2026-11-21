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

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.FinalScalingAction.VertexRequest;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheAction;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.ManagedMemoryDecision;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.resources.NoopResourceCheck;
import org.apache.flink.autoscaler.resources.ResourceCheck;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.a4s.A4S;
import org.apache.flink.autoscaler.a4s.A4S.Decision;
import org.apache.flink.autoscaler.a4s.OnlineCacheCost.Reason;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.autoscaler.tuning.MemoryTuning;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.AppliedSlotSnapshot;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ConfigurationInfoEntry;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
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
    private static final long BYTES_PER_MIB = 1L << 20;

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
        CacheAction action = scaleResource(context, evaluatedMetrics, scalingHistory, scalingTracking,
                now, jobTopology, delayedScaleDown, null);
        evaluatedMetrics.getCacheEvaluation().setAction(action);
        return action == CacheAction.POLICY_REQUEST;
    }

    CacheAction scaleUpWithCurrentProfiles(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            @Nullable Map<JobVertexID, CacheObservationBatch> observations,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown) throws Exception {
        return scaleResource(context, evaluatedMetrics, scalingHistory, scalingTracking, now,
                jobTopology, delayedScaleDown, observations == null ? Map.of() : observations);
    }

    private CacheAction scaleResource(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown,
            @Nullable Map<JobVertexID, CacheObservationBatch> overloadObservations) throws Exception {
        boolean throughputOnly = overloadObservations != null;
        var conf = context.getConfiguration();
        var restartTime = scalingTracking.getMaxRestartTimeOrDefault(conf);

        var scalingSummaries =
                computeScalingSummary(
                        context,
                        evaluatedMetrics,
                        scalingHistory,
                        restartTime,
                        jobTopology,
                        throughputOnly ? new DelayedScaleDown() : delayedScaleDown,
                        throughputOnly);

        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return CacheAction.POLICY_NO_CHANGE;
        }

        if (throughputOnly || (conf.get(JUSTIN_ENABLED) && conf.get(A4S_ENABLED))) {
            return finalizeA4sRequest(context, evaluatedMetrics, scalingHistory, scalingTracking,
                    now, jobTopology, delayedScaleDown, overloadObservations, scalingSummaries);
        }

        updateRecommendedParallelism(evaluatedMetrics.getVertexMetrics(), scalingSummaries);
        if (checkIfBlockedAndTriggerScalingEvent(context, scalingSummaries, conf, now)) {
            return CacheAction.POLICY_HOLD;
        }
        var configOverrides = MemoryTuning.tuneTaskManagerMemory(
                context, evaluatedMetrics, jobTopology, scalingSummaries, autoScalerEventHandler);
        var tunedConfig = conf.get(MEMORY_TUNING_ENABLED)
                ? configOverrides.newConfigWithOverrides(conf) : conf;
        if (scalingWouldExceedMaxResources(
                tunedConfig, jobTopology, evaluatedMetrics, scalingSummaries, context)) {
            return CacheAction.POLICY_HOLD;
        }

        addToScalingHistoryAndStore(autoScalerStateStore, context, scalingHistory, now, scalingSummaries);
        scalingTracking.addScalingRecord(now, new ScalingRecord());
        autoScalerStateStore.storeScalingTracking(context, scalingTracking);
        var requestedParallelism =
                getVertexParallelismOverrides(evaluatedMetrics.getVertexMetrics(), scalingSummaries);
        if (conf.get(JUSTIN_ENABLED)) {
            var currentScalingConf = scalingConfigurations.setCurrentConfiguration(
                    context.getJobID(), evaluatedMetrics.getVertexMetrics(), scalingSummaries,
                    periods.getOrDefault(context.getJobID(), 0));
            policy(context, currentScalingConf, conf);
            LOG.info(scalingConfigurations.toString());
            requestedParallelism = getVertexParallelismOverrides(currentScalingConf);
            autoScalerStateStore.storeResourceProfileOverrides(
                    context, getVertexResourceProfileOverrides(context.getJobID(), scalingConfigurations, conf));
        }
        autoScalerStateStore.storeParallelismOverrides(context, requestedParallelism);
        autoScalerStateStore.storeConfigChanges(context, configOverrides);
        delayedScaleDown.clearAll();
        return CacheAction.POLICY_REQUEST;
    }

    private CacheAction finalizeA4sRequest(
            Context context,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> history,
            ScalingTracking tracking,
            Instant now,
            JobTopology topology,
            DelayedScaleDown delayedScaleDown,
            @Nullable Map<JobVertexID, CacheObservationBatch> overloadObservations,
            Map<JobVertexID, ScalingSummary> ordinary) throws Exception {
        var conf = context.getConfiguration();
        boolean throughputOnly = overloadObservations != null;
        if (!throughputOnly && conf.get(A4S_ONLINE_CACHE_COSTS_ENABLED)
                && !conf.get(A4S_ONLINE_CACHE_COSTS_SHADOW)) {
            return CacheAction.ONLINE_CACHE_ONLY_ACTUATION_UNAVAILABLE;
        }
        var evaluation = evaluated.getCacheEvaluation();
        if ((!conf.get(SCALING_ENABLED) || CalendarUtils.inExcludedPeriods(conf, now))
                && checkIfBlockedAndTriggerScalingEvent(context, ordinary, conf, now)) {
            return CacheAction.POLICY_HOLD;
        }
        var frozen = new HashSet<>(topology.getFinishedVertices());
        evaluated.getVertexMetrics().keySet().stream()
                .filter(id -> conf.get(VERTEX_EXCLUDE_IDS).contains(id.toHexString()))
                .forEach(frozen::add);
        ordinary = new HashMap<>(ordinary);
        frozen.forEach(ordinary::remove);
        if (ordinary.isEmpty()) {
            return CacheAction.POLICY_NO_CHANGE;
        }

        Map<JobVertexID, Decision> placement = Map.of();
        if (!throughputOnly && hasManagedRocksDb(conf, writeBufferRatio(conf))) {
            var curves = new HashMap<>(evaluated.getMemoryParallelismCurves());
            frozen.forEach(curves::remove);
            var placementInput = new EvaluatedMetrics(
                    evaluated.getVertexMetrics(), evaluated.getGlobalMetrics(), curves);
            placement = new A4S(topology, placementInput, ordinary).makeDecision(conf);
        }

        var parallelism = ResourceCheckUtils.computeNewParallelisms(
                ordinary, evaluated.getVertexMetrics());
        for (var entry : placement.entrySet()) {
            parallelism.put(entry.getKey(), entry.getValue().getParallelism());
        }
        if (!validFinalParallelism(context, evaluated, topology, ordinary, parallelism, frozen)) {
            evaluation.setHoldReason(Reason.FINAL_PARALLELISM_UNSUPPORTED);
            return CacheAction.POLICY_HOLD;
        }

        Map<JobVertexID, ResourceProfile> profiles;
        var observedProfiles = new HashMap<JobVertexID, ResourceProfile>();
        AppliedSlotSnapshot appliedSlots = null;
        if (throughputOnly) {
            var resolved = preserveLiveProfiles(context, evaluated, overloadObservations, frozen, observedProfiles);
            if (resolved.isEmpty()) {
                if (evaluation.getHoldReason() == null) {
                    evaluation.setHoldReason(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED);
                }
                return CacheAction.POLICY_HOLD;
            }
            profiles = resolved.get().profiles;
            appliedSlots = resolved.get().appliedSlots;
        } else {
            var live = readCurrentRequirements(context);
            if (live.isEmpty()
                    || !live.get().getJobVertexParallelisms().keySet().equals(parallelism.keySet())) {
                evaluation.setHoldReason(Reason.FINAL_PROFILE_UNVERIFIED);
                return CacheAction.POLICY_HOLD;
            }
            for (var id : parallelism.keySet()) {
                ResourceProfile profile = currentResourceProfile(live.get(), id, currentParallelism(evaluated, id));
                if (profile == null || !FinalScalingAction.canEncode(profile)) {
                    evaluation.setHoldReason(Reason.FINAL_PROFILE_UNVERIFIED);
                    return CacheAction.POLICY_HOLD;
                }
                observedProfiles.put(id, profile);
            }
            if (!resolveA4sManagedMemory(context, evaluated, placement, frozen, observedProfiles)) {
                return CacheAction.POLICY_HOLD;
            }
            profiles = new HashMap<>(observedProfiles);
            var memories = evaluation.getManagedMemoryDecisions();
            for (var entry : memories.entrySet()) {
                var id = entry.getKey();
                var memory = entry.getValue();
                if (placement.containsKey(id) && memory.getUnconstrainedManagedBytes() != null
                        && memory.getFinalManagedBytes() < memory.getUnconstrainedManagedBytes()) {
                    evaluation.setHoldReason(Reason.FINAL_CACHE_CAPACITY_UNSUPPORTED);
                    return CacheAction.POLICY_HOLD;
                }
                if (memory.getNominalNativeCacheBytes() != null) {
                    profiles.put(id, ResourceProfile.newBuilder(observedProfiles.get(id))
                            .setManagedMemory(new MemorySize(memory.getFinalManagedBytes())).build());
                }
            }
        }

        Map<JobVertexID, VertexRequest> vertices = new HashMap<>();
        var summaries = new HashMap<JobVertexID, ScalingSummary>();
        CacheAction mode = throughputOnly
                ? CacheAction.THROUGHPUT_SCALE_UP_RETAINED_PROFILES : CacheAction.POLICY_REQUEST;
        var restartTime = tracking.getMaxRestartTimeOrDefault(conf);
        for (var entry : parallelism.entrySet()) {
            var id = entry.getKey();
            int current = currentParallelism(evaluated, id);
            int requested = entry.getValue();
            var prior = observedProfiles.get(id);
            var profile = profiles.get(id);
            if (prior == null || profile == null || !(throughputOnly && appliedSlots != null
                    ? FinalScalingAction.canEncodeExact(profile) : FinalScalingAction.canEncode(profile))) {
                evaluation.setHoldReason(Reason.FINAL_PROFILE_UNVERIFIED);
                return CacheAction.POLICY_HOLD;
            }

            var metrics = evaluated.getVertexMetrics().get(id);
            var memory = evaluation.getManagedMemoryDecisions().get(id);
            var decision = evaluation.getVertices().get(id);
            var reason = decision == null ? evaluated.getCacheCostFailures().get(id) : decision.getReason();
            if (!throughputOnly && !placement.containsKey(id)
                    && memory != null && memory.getNominalNativeCacheBytes() != null && reason == null) {
                reason = Reason.CAPACITY_UNSUPPORTED;
            }
            double required = AutoScalerUtils.getRequiredProcessingCapacity(metrics, conf, restartTime);
            if (placement.containsKey(id)
                    && (!Double.isFinite(required)
                            || Double.compare(evaluated.getMemoryParallelismCurves().get(id)
                                    .getTargetThroughput(), required) != 0)) {
                evaluation.setHoldReason(Reason.FINAL_CACHE_CAPACITY_UNSUPPORTED);
                return CacheAction.POLICY_HOLD;
            }
            vertices.put(id, new VertexRequest(current, requested, profile,
                    Double.isFinite(required) ? required : null,
                    memory == null ? null : memory.getNominalNativeCacheBytes(), reason));

            if (requested != current || (!frozen.contains(id) && prior != null && !profile.equals(prior))) {
                var snapshot = new HashMap<>(metrics);
                snapshot.put(RECOMMENDED_PARALLELISM, EvaluatedScalingMetric.of(requested));
                var original = ordinary.get(id);
                if (placement.containsKey(id) && (original == null
                        || requested != original.getNewParallelism())) {
                    snapshot.put(EXPECTED_PROCESSING_RATE, EvaluatedScalingMetric.of(required));
                }
                summaries.put(id, new ScalingSummary(current, requested, snapshot));
            }
        }

        if (summaries.isEmpty()) {
            return CacheAction.POLICY_NO_CHANGE;
        }
        updateRecommendedParallelism(evaluated.getVertexMetrics(), summaries);
        if (checkIfBlockedAndTriggerScalingEvent(context, summaries, conf, now)) {
            return CacheAction.POLICY_HOLD;
        }

        boolean profileOnly = summaries.values().stream()
                .noneMatch(summary -> summary.getCurrentParallelism() != summary.getNewParallelism());
        var configOverrides = throughputOnly || profileOnly ? new ConfigChanges()
                : MemoryTuning.tuneTaskManagerMemory(
                        context, evaluated, topology, summaries, autoScalerEventHandler);
        var tunedConfig = !throughputOnly && conf.get(MEMORY_TUNING_ENABLED)
                ? configOverrides.newConfigWithOverrides(conf) : conf;
        var latest = tracking.getLatestScalingRecordEntry().orElse(null);
        if (profileOnly && latest != null
                && awaitsAppliedProfile(latest.getValue(), latest.getKey(), context, evaluated, observedProfiles)) {
            evaluation.setHoldReason(Reason.FINAL_PROFILE_UNVERIFIED);
            return CacheAction.POLICY_HOLD;
        }
        Instant requestedAt = latest != null && !now.isAfter(latest.getKey())
                ? latest.getKey().plusNanos(1) : now;
        var action = new FinalScalingAction(
                evaluation.getId().toString(), requestedAt, mode, vertices, configOverrides);
        if (managedMemoryWouldExceedResources(context, tunedConfig, evaluated, action)
                || finalWorkerRequestExceedsResources(
                        context, evaluated, summaries, observedProfiles, profiles, throughputOnly, appliedSlots)) {
            if (evaluation.getHoldReason() == null) {
                evaluation.setHoldReason(Reason.FINAL_RESOURCE_LIMIT);
            }
            return CacheAction.POLICY_HOLD;
        }

        if (latest != null && latest.getValue().getRestartDuration() == null
                && latest.getValue().matchesRequest(action)) {
            // Repeat the same validated request under its original identity and history entry.
            action = new FinalScalingAction(latest.getValue().getDecisionId(),
                    latest.getKey(), mode, vertices, configOverrides);
        } else {
            var requestId = action.getDecisionId();
            var requestedProfiles = action.getResourceProfileOverrides();
            summaries.forEach((id, summary) -> {
                summary.setDecisionId(requestId);
                summary.setRequestedResourceProfile(requestedProfiles.get(id.toHexString()));
            });
            addToScalingHistoryAndStore(autoScalerStateStore, context, history, requestedAt, summaries);
            tracking.addScalingRecord(requestedAt, ScalingRecord.requested(action));
            autoScalerStateStore.storeScalingTracking(context, tracking);
        }
        var requestedParallelism = action.getParallelismOverrides();
        var requestedProfiles = action.getResourceProfileOverrides();
        autoScalerStateStore.storeParallelismOverrides(context, requestedParallelism);
        autoScalerStateStore.storeResourceProfileOverrides(context, requestedProfiles);
        if (!throughputOnly) {
            autoScalerStateStore.storeConfigChanges(context, configOverrides);
        }
        evaluation.recordRequestedOverrides(requestedParallelism, requestedProfiles);
        evaluation.setFinalAction(action);
        delayedScaleDown.clearAll();
        return mode;
    }

    private boolean awaitsAppliedProfile(
            ScalingRecord record, Instant requestedAt, Context context, EvaluatedMetrics evaluated,
            Map<JobVertexID, ResourceProfile> observedProfiles) {
        if (!Boolean.TRUE.equals(record.getRealizerReturned())
                || record.getStartingParallelism() == null
                || !record.getStartingParallelism().equals(record.getRequestedParallelism())
                || record.getRequestedProfiles() == null) {
            return false;
        }
        var conf = context.getConfiguration();
        var exempt = conf.get(A4S_CACHE_METRICS_EXEMPT_VERTICES);
        long nowMillis = System.currentTimeMillis();
        boolean observedCache = false;
        for (var entry : observedProfiles.entrySet()) {
            var vertex = entry.getKey();
            var batch = evaluated.getCacheObservations().get(vertex);
            if (exempt.contains(vertex.toHexString()) && configuredFloor(conf, vertex) == null
                    && (batch == null || batch.status == CacheObservation.Status.ABSENT)) {
                continue;
            }
            var profile = entry.getValue();
            if (!FinalScalingAction.encodeProfile(profile).equals(record.getRequestedProfiles().get(vertex.toHexString()))
                    || batch == null || !batch.hasPairedLocalObservations()
                    || batch.subtasks.stream().anyMatch(row -> row.endpoint.appliedSlot == null
                            || row.window.observedAtMillis < requestedAt.toEpochMilli())
                    || !corroboratesManagedBudget(context.getJobID(), vertex,
                            currentParallelism(evaluated, vertex), profile.getManagedMemory().getBytes(),
                            writeBufferRatio(conf), batch, nowMillis)) {
                return true;
            }
            observedCache = true;
        }
        return !observedCache;
    }

    private static int currentParallelism(EvaluatedMetrics evaluated, JobVertexID vertex) {
        return (int) evaluated.getVertexMetrics().get(vertex).get(PARALLELISM).getCurrent();
    }

    private boolean validFinalParallelism(
            Context context, EvaluatedMetrics evaluated, JobTopology topology,
            Map<JobVertexID, ScalingSummary> ordinary, Map<JobVertexID, Integer> finalParallelism,
            Set<JobVertexID> frozen) {
        var conf = context.getConfiguration();
        for (var entry : finalParallelism.entrySet()) {
            var id = entry.getKey();
            int requested = entry.getValue();
            int current = currentParallelism(evaluated, id);
            if (current <= 0 || requested <= 0) {
                return false;
            }
            if (requested == current) {
                continue;
            }
            var summary = ordinary.get(id);
            if (summary == null || frozen.contains(id)
                    || Integer.compare(requested, current)
                            != Integer.compare(summary.getNewParallelism(), current)) {
                return false;
            }
            var metrics = evaluated.getVertexMetrics().get(id);
            var info = topology.get(id);
            int maximum = Math.min(info.getMaxParallelism(), (int) metrics.get(MAX_PARALLELISM).getCurrent());
            int partitions = (int) metrics.get(NUM_SOURCE_PARTITIONS).getCurrent();
            if (info.getNumSourcePartitions() > 0) {
                partitions = partitions > 0
                        ? Math.min(partitions, info.getNumSourcePartitions()) : info.getNumSourcePartitions();
            }
            int lower = Math.min(current, conf.get(VERTEX_MIN_PARALLELISM));
            int upper = Math.max(current, conf.get(VERTEX_MAX_PARALLELISM));
            if (requested < Math.min(lower, maximum) || requested > Math.min(upper, maximum)
                    || (partitions > 0 && requested > partitions)) {
                return false;
            }
            if (requested != summary.getNewParallelism()) {
                var inputs = info.getInputs().values();
                double factor = requested > current
                        ? 1 + conf.get(MAX_SCALE_UP_FACTOR) : 1 - conf.get(MAX_SCALE_DOWN_FACTOR);
                int policyLimit = JobVertexScaler.scale(id, current, inputs, partitions, maximum,
                        factor, lower, upper, autoScalerEventHandler, context);
                boolean exceedsLimit = requested > current
                        ? requested > policyLimit : requested < policyLimit;
                if (exceedsLimit || requested != JobVertexScaler.scale(
                        id, current, inputs, partitions, maximum,
                        Math.nextDown((double) requested / current), lower, upper,
                        autoScalerEventHandler, context)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean finalWorkerRequestExceedsResources(
            Context context,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, ScalingSummary> summaries,
            Map<JobVertexID, ResourceProfile> currentProfiles,
            Map<JobVertexID, ResourceProfile> profiles,
            boolean throughputOnly, @Nullable AppliedSlotSnapshot appliedSlots) {
        var snapshot = readLiveWorkers(context);
        if (snapshot.isEmpty()) {
            evaluated.getCacheEvaluation().setHoldReason(throughputOnly
                    ? Reason.OVERLOAD_WORKER_SNAPSHOT_UNAVAILABLE : Reason.FINAL_RESOURCE_LIMIT);
            return true;
        }
        var running = new HashMap<ResourceProfile, Long>();
        var requested = new HashMap<ResourceProfile, Long>();
        profiles.forEach((vertex, profile) -> {
            var summary = summaries.get(vertex);
            long current =
                    (int) evaluated.getVertexMetrics().get(vertex).get(PARALLELISM).getCurrent();
            running.merge(currentProfiles.get(vertex), current, Math::addExact);
            requested.merge(
                    profile, summary == null ? current : summary.getNewParallelism(), Math::addExact);
        });
        var estimate = RetainedProfileResourceAccounting.estimate(
                context.getJobID(), snapshot.get(), running, requested, appliedSlots);
        if (estimate.isEmpty()) {
            evaluated.getCacheEvaluation().setHoldReason(throughputOnly
                    ? Reason.OVERLOAD_RESOURCE_GEOMETRY_UNSUPPORTED : Reason.FINAL_RESOURCE_LIMIT);
            return true;
        }
        var allocation = estimate.get();
        var conf = context.getConfiguration();
        var cpuQuota = conf.getOptional(CPU_QUOTA);
        var memoryQuota = conf.getOptional(MEMORY_QUOTA);
        if ((cpuQuota.isPresent()
                        && (!Double.isFinite(cpuQuota.get())
                                || allocation.cpu.multiply(BigDecimal.valueOf(allocation.requestedWorkers))
                                        .compareTo(BigDecimal.valueOf(cpuQuota.get())) > 0))
                || (memoryQuota.isPresent()
                        && allocation.requestedWorkers
                                > memoryQuota.get().getBytes() / allocation.processMemory.getBytes())) {
            evaluated.getCacheEvaluation().setHoldReason(throughputOnly
                    ? Reason.OVERLOAD_RESOURCE_QUOTA : Reason.FINAL_RESOURCE_LIMIT);
            return true;
        }
        if (!resourceCheck.trySchedule(allocation.currentWorkers, allocation.requestedWorkers,
                allocation.cpu.doubleValue(), allocation.processMemory)) {
            evaluated.getCacheEvaluation().setHoldReason(throughputOnly
                    ? Reason.OVERLOAD_CLUSTER_RESOURCES_UNAVAILABLE : Reason.FINAL_RESOURCE_LIMIT);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    Optional<RetainedProfileResourceAccounting.WorkerSnapshot> readLiveWorkers(Context context) {
        long timeoutMillis = context.getConfiguration().get(FLINK_CLIENT_TIMEOUT).toMillis();
        try (var client = context.getRestClusterClient()) {
            var jobs = client.listJobs().get(timeoutMillis, TimeUnit.MILLISECONDS).stream()
                    .filter(job -> !job.getJobState().isGloballyTerminalState())
                    .collect(Collectors.toList());
            if (jobs.size() != 1 || jobs.get(0).getJobState() != JobStatus.RUNNING
                    || !jobs.get(0).getJobId().equals(context.getJobID())) {
                return Optional.empty();
            }
            var configured = client.sendRequest(ClusterConfigurationInfoHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(), EmptyRequestBody.getInstance())
                    .get(timeoutMillis, TimeUnit.MILLISECONDS);
            var deployed = Configuration.fromMap(configured.stream().collect(Collectors.toMap(
                    ConfigurationInfoEntry::getKey, ConfigurationInfoEntry::getValue)));
            var registered = client.sendRequest(TaskManagersHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(), EmptyRequestBody.getInstance())
                    .get(timeoutMillis, TimeUnit.MILLISECONDS).getTaskManagerInfos();
            var details = registered.stream().map(worker -> {
                var headers = TaskManagerDetailsHeaders.getInstance();
                var parameters = headers.getUnresolvedMessageParameters();
                parameters.taskManagerIdParameter.resolve(worker.getResourceId());
                return client.sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                        .thenApply(report -> {
                            if (!worker.getResourceId().equals(report.getResourceId())
                                    || !Objects.equals(worker.getTotalResource(), report.getTotalResource())
                                    || !Objects.equals(worker.getFreeResource(), report.getFreeResource())) {
                                throw new IllegalStateException("TaskManager changed during snapshot");
                            }
                            return report;
                        });
            }).collect(Collectors.toList());
            CompletableFuture.allOf(details.toArray(new CompletableFuture<?>[0]))
                    .get(timeoutMillis, TimeUnit.MILLISECONDS);
            List<TaskManagerDetailsInfo> workers =
                    details.stream().map(CompletableFuture::join).collect(Collectors.toList());
            return Optional.of(new RetainedProfileResourceAccounting.WorkerSnapshot(deployed, workers));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            LOG.debug("Unable to read registered workers for A4S job {}", context.getJobID(), e);
            return Optional.empty();
        }
    }

    private Optional<RetainedProfiles> preserveLiveProfiles(
            Context context,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, CacheObservationBatch> observations, Set<JobVertexID> frozen,
            Map<JobVertexID, ResourceProfile> observedProfiles) throws InterruptedException {
        var conf = context.getConfiguration();
        double ratio = writeBufferRatio(conf);
        if (!hasManagedRocksDb(conf, ratio)) {
            evaluated.getCacheEvaluation().setHoldReason(Reason.MEMORY_POLICY_UNVERIFIED);
            return Optional.empty();
        }
        var requirements = readCurrentRequirements(context);
        if (requirements.isEmpty()) {
            return Optional.empty();
        }
        var vertices = requirements.get().getJobVertexParallelisms();
        if (!vertices.keySet().equals(evaluated.getVertexMetrics().keySet())) {
            return Optional.empty();
        }
        var overrides = new HashMap<JobVertexID, ResourceProfile>();
        var cacheExempt = conf.get(A4S_CACHE_METRICS_EXEMPT_VERTICES);
        long nowMillis = System.currentTimeMillis();
        var appliedSlots = requirements.get().getAppliedSlots();
        var appliedByVertex = appliedSlots == null ? null
                : RetainedProfileResourceAccounting.groupTasks(appliedSlots, vertices.keySet(), nowMillis);
        if (appliedSlots != null && appliedByVertex == null) {
            return Optional.empty();
        }
        var cacheFreeVertices = vertices.keySet().stream().filter(id -> {
            var batch = observations.get(id);
            return cacheExempt.contains(id.toHexString()) && configuredFloor(conf, id) == null
                    && (batch == null || (batch.status == CacheObservation.Status.ABSENT
                            && batch.subtasks.isEmpty()));
        }).collect(Collectors.toSet());
        var sharedAllocations = new HashSet<String>();
        if (appliedByVertex != null) {
            var usedAllocations = new HashSet<String>();
            var cacheUsers = new HashSet<String>();
            for (var task : appliedSlots.tasks) {
                if (!usedAllocations.add(task.allocation)) {
                    sharedAllocations.add(task.allocation);
                }
                if (!cacheFreeVertices.contains(JobVertexID.fromHexString(task.vertex))
                        && !cacheUsers.add(task.allocation)) {
                    return Optional.empty();
                }
            }
        }
        for (var entry : vertices.entrySet()) {
            var id = entry.getKey();
            int current = (int) evaluated.getVertexMetrics().get(id).get(PARALLELISM).getCurrent();
            var tasks = appliedByVertex == null ? null : appliedByVertex.get(id);
            var profile = tasks == null ? currentResourceProfile(requirements.get(), id, current)
                    : RetainedProfileResourceAccounting.verifiedProfile(tasks, current);
            if (profile == null || !profile.getExtendedResources().isEmpty()) {
                return Optional.empty();
            }
            observedProfiles.put(id, profile);
            boolean cacheFree = cacheFreeVertices.contains(id);
            boolean sharedCacheBudget = !cacheFree && tasks != null
                    && tasks.values().stream().anyMatch(task -> sharedAllocations.contains(task.allocation));
            if (frozen.contains(id) && !sharedCacheBudget) {
                overrides.put(id, profile);
                continue;
            }
            var batch = observations.get(id);
            long managedBytes = profile.getManagedMemory().getBytes();
            if (cacheFree && (managedBytes % BYTES_PER_MIB != 0
                    || profile.getManagedMemory().getMebiBytes() > Integer.MAX_VALUE)) {
                return Optional.empty();
            }
            if (tasks != null && !RetainedProfileResourceAccounting.matchesCacheAttempts(tasks, batch)) {
                return Optional.empty();
            }
            boolean corroborated = corroboratesManagedBudget(
                    context.getJobID(), id, current, managedBytes, ratio, batch, nowMillis)
                    && (!sharedCacheBudget || matchesSharedWriteBudget(
                            tasks, batch, sharedAllocations, writeBufferManagerBytes(managedBytes, ratio)));
            Long trustedManagedBytes = cacheFree || corroborated ? managedBytes : null;
            // A resolved floor can permit recovery without cache timing. Use the full
            // physical profile as the proposal so the transition keeps its managed budget.
            Double proposal = sharedCacheBudget && !corroborated
                    ? (double) managedBytes / BYTES_PER_MIB : null;
            var decision = resolveManagedMemory(conf, id, !cacheFree, proposal, trustedManagedBytes, true);
            if (!recordManagedMemoryDecision(evaluated, id, decision)) {
                return Optional.empty();
            }
            // A verified physical slot is a conservative basis for a new dedicated slot.
            // It is not the amount each task used while sharing that physical slot.
            if ((frozen.contains(id) && decision.getFinalManagedBytes() != managedBytes)
                    || (sharedCacheBudget && !corroborated
                            && decision.getFinalManagedBytes() < managedBytes)) {
                evaluated.getCacheEvaluation().setHoldReason(Reason.MEMORY_BUDGET_INFEASIBLE);
                return Optional.empty();
            }
            overrides.put(id, ResourceProfile.newBuilder()
                    .setCpuCores(profile.getCpuCores())
                    .setTaskHeapMemory(profile.getTaskHeapMemory())
                    .setTaskOffHeapMemory(profile.getTaskOffHeapMemory())
                    .setManagedMemory(new MemorySize(decision.getFinalManagedBytes()))
                    .setNetworkMemory(profile.getNetworkMemory())
                    .build());
        }
        return Optional.of(new RetainedProfiles(overrides, appliedSlots));
    }

    private static final class RetainedProfiles {
        private final Map<JobVertexID, ResourceProfile> profiles;
        @Nullable private final AppliedSlotSnapshot appliedSlots;

        private RetainedProfiles(
                Map<JobVertexID, ResourceProfile> profiles, @Nullable AppliedSlotSnapshot appliedSlots) {
            this.profiles = profiles;
            this.appliedSlots = appliedSlots;
        }
    }

    private static boolean matchesSharedWriteBudget(
            Map<Integer, AppliedSlotSnapshot.Task> tasks, CacheObservationBatch batch,
            Set<String> sharedAllocations, long expectedBytes) {
        for (var observation : batch.subtasks) {
            if (sharedAllocations.contains(tasks.get(observation.subtask).allocation)) {
                var endpoint = observation.endpoint;
                if (endpoint == null || endpoint.appliedSlot == null
                        || endpoint.writeBufferBudget == null
                        || !endpoint.writeBufferBudget.supportsReadback()
                        || endpoint.writeBufferBudget.bytes != expectedBytes) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean corroboratesManagedBudget(
            JobID job, JobVertexID vertex, int parallelism, long managedBytes, double writeBufferRatio,
            @Nullable CacheObservationBatch batch, long nowMillis) {
        if (managedBytes <= 0 || writeBufferManagerBytes(managedBytes, writeBufferRatio) <= 0
                || batch == null || !CacheObservation.supportsVersion(batch.version)
                || batch.expectedSubtasks != parallelism || batch.subtasks.size() != parallelism
                || !batch.status.supportsLocalObservations()) {
            return false;
        }
        // A rate-only rejection has already passed per-cache identity and freshness checks.
        long expectedCache = nominalCacheBytes(managedBytes, writeBufferRatio);
        for (int i = 0; i < parallelism; i++) {
            var observation = batch.subtasks.get(i);
            var window = observation.window;
            if (observation.subtask != i || !job.toString().equals(observation.job)
                    || !vertex.toString().equals(observation.vertex)
                    || !CacheObservation.supportsVersion(observation.version)
                    || observation.status != CacheObservation.Status.VALID
                    || window == null || !window.owners.equals(List.of(1L))
                    || !CacheObservation.QUICK_MRC.equals(window.population)
                    || !CacheObservation.CACHE_POLICY.equals(window.cachePolicy)
                    || !CacheObservation.isNominalQuickMrcCharge(window.chargePolicy)
                    || window.capacityBytes != expectedCache
                    || (observation.endpoint != null && observation.endpoint.appliedSlot != null
                            && observation.endpoint.appliedSlot.managedMemoryBytes != managedBytes)
                    || !CacheObservation.isFresh(window.observedAtMillis, nowMillis)) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    Optional<JustinResourceRequirements> readLiveResourceRequirements(Context context)
            throws Exception {
        var headers = JustinResourceRequirementsHeaders.INSTANCE;
        var parameters = headers.getUnresolvedMessageParameters();
        parameters.jobPathParameter.resolve(context.getJobID());
        try (var client = context.getRestClusterClient()) {
            return client.sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                    .get(10, TimeUnit.SECONDS).asObservedRequirements();
        }
    }

    private Optional<JustinResourceRequirements> readCurrentRequirements(Context context)
            throws InterruptedException {
        try {
            return readLiveResourceRequirements(context);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (Exception unavailable) {
            LOG.debug("Could not verify current Justin resource requirements", unavailable);
            return Optional.empty();
        }
    }

    @Nullable
    private static ResourceProfile currentResourceProfile(
            JustinResourceRequirements requirements, JobVertexID id, int parallelism) {
        var live = requirements.getJobVertexParallelisms().get(id);
        if (live == null || parallelism <= 0 || live.getParallelism().getLowerBound() != 1
                || live.getParallelism().getUpperBound() != parallelism) {
            return null;
        }
        var profile = live.getResourceProfile();
        return profile.equals(ResourceProfile.UNKNOWN) || profile.equals(ResourceProfile.ANY)
                ? null : profile;
    }

    private boolean resolveA4sManagedMemory(
            Context context, EvaluatedMetrics evaluated, Map<JobVertexID, Decision> decisions,
            Set<JobVertexID> frozen, Map<JobVertexID, ResourceProfile> observedProfiles) {
        var conf = context.getConfiguration();
        double ratio = writeBufferRatio(conf);
        var cacheVertices = new HashSet<JobVertexID>();
        for (var entry : evaluated.getVertexMetrics().entrySet()) {
            var id = entry.getKey();
            var observations = evaluated.getCacheObservations().get(id);
            if (frozen.contains(id)) {
                continue;
            }
            if (configuredFloor(conf, id) != null
                    || evaluated.getMemoryParallelismCurves().containsKey(id)
                    || evaluated.getCacheCostFailures().containsKey(id)
                    || entry.getValue().containsKey(ROCKS_DB_BLOCK_CACHE_HIT_RATE)
                    || (observations != null && observations.status != CacheObservation.Status.ABSENT)) {
                cacheVertices.add(id);
            }
        }
        long nowMillis = System.currentTimeMillis();
        double baseMiB = conf.get(A4S_MEMORY_BASE_MB);
        for (var entry : evaluated.getVertexMetrics().entrySet()) {
            var id = entry.getKey();
            if (frozen.contains(id)) {
                continue;
            }
            var point = decisions.get(id);
            boolean cacheRequired = cacheVertices.contains(id);
            Double proposalMiB = point == null && cacheRequired
                    ? null : baseMiB + (point == null ? 0 : point.getMemoryMB());
            Long trustedManagedBytes = null;
            if (cacheRequired && configuredFloor(conf, id) == null) {
                int current = (int) entry.getValue().get(PARALLELISM).getCurrent();
                var profile = observedProfiles.get(id);
                if (profile != null && corroboratesManagedBudget(
                        context.getJobID(), id, current, profile.getManagedMemory().getBytes(),
                        ratio, evaluated.getCacheObservations().get(id), nowMillis)) {
                    trustedManagedBytes = profile.getManagedMemory().getBytes();
                }
            }
            var decision = resolveManagedMemory(
                    conf, id, cacheRequired, proposalMiB, trustedManagedBytes, false);
            if (!recordManagedMemoryDecision(evaluated, id, decision)) {
                return false;
            }
        }
        return true;
    }

    private static boolean recordManagedMemoryDecision(
            EvaluatedMetrics evaluated, JobVertexID vertex, ManagedMemoryDecision decision) {
        var evaluation = evaluated.getCacheEvaluation();
        evaluation.getManagedMemoryDecisions().put(vertex, decision);
        if (decision.getHoldReason() != null) {
            evaluation.setHoldReason(decision.getHoldReason());
            return false;
        }
        return true;
    }

    @Nullable
    private static String configuredFloor(Configuration conf, JobVertexID id) {
        return conf.getString(A4S_MEMORY_FLOOR_PREFIX + id.toHexString(), null);
    }

    private static double writeBufferRatio(Configuration conf) {
        return conf.getDouble("state.backend.rocksdb.memory.write-buffer-ratio", 0.5);
    }

    private static boolean hasManagedRocksDb(Configuration conf, double ratio) {
        return ratio > 0 && ratio < 1
                && conf.getBoolean("state.backend.rocksdb.memory.managed", true)
                && conf.getString("state.backend.rocksdb.memory.fixed-per-slot", null) == null
                && conf.getString("state.backend.rocksdb.memory.fixed-per-tm", null) == null;
    }

    @Nullable
    private static Long parseFloorBytes(@Nullable String floor) {
        if (floor == null) {
            return null;
        }
        try {
            var bytes = toManagedBytes(Double.parseDouble(floor));
            return bytes != null && bytes > 0 ? bytes : null;
        } catch (NumberFormatException invalidFloor) {
            return null;
        }
    }

    @Nullable
    private static Long toManagedBytes(double mib) {
        double bytes = mib * BYTES_PER_MIB;
        return Double.isFinite(bytes) && bytes >= 0 && bytes < Long.MAX_VALUE
                ? (long) Math.ceil(bytes) : null;
    }

    @VisibleForTesting
    static ManagedMemoryDecision resolveManagedMemory(
            Configuration conf, JobVertexID id, boolean cacheRequired,
            @Nullable Double proposalMiB, @Nullable Long trustedManagedBytes,
            boolean preserveCurrent) {
        var floorSetting = configuredFloor(conf, id);
        var configuredFloor = parseFloorBytes(floorSetting);
        Long minimum = configuredFloor != null ? configuredFloor : trustedManagedBytes;
        Long proposed = proposalMiB == null ? null : toManagedBytes(proposalMiB);
        boolean retainWithoutPlacement = !preserveCurrent && proposalMiB == null;
        Long retained = preserveCurrent || (retainWithoutPlacement && configuredFloor == null)
                ? trustedManagedBytes : null;
        Long baseBytes = retainWithoutPlacement ? toManagedBytes(conf.get(A4S_MEMORY_BASE_MB)) : null;
        Long floorLimited = null;
        Reason hold = null;
        double ratio = writeBufferRatio(conf);
        if (cacheRequired && !hasManagedRocksDb(conf, ratio)) {
            hold = Reason.MEMORY_POLICY_UNVERIFIED;
        } else if (floorSetting != null && (configuredFloor == null
                || writeBufferManagerBytes(configuredFloor, ratio) <= 0)) {
            hold = Reason.MEMORY_FLOOR_INVALID;
        } else if (cacheRequired && minimum == null) {
            hold = Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED;
        } else if ((proposalMiB != null && proposed == null)
                || (retainWithoutPlacement && baseBytes == null)) {
            hold = Reason.MEMORY_BUDGET_INFEASIBLE;
        } else {
            Long base = proposed != null ? proposed : retained;
            if (base == null) {
                floorLimited = minimum;
            } else {
                floorLimited = Math.max(base, minimum == null ? 0 : minimum);
            }
            if (retainWithoutPlacement && floorLimited != null) {
                floorLimited = Math.max(floorLimited, baseBytes);
            }
        }

        Long requested = floorLimited;
        if ((!preserveCurrent || cacheRequired) && conf.get(A4S_MANAGED_MEMORY_OVERRIDE_ENABLED)) {
            requested = conf.get(A4S_MANAGED_MEMORY_OVERRIDE_MB) * BYTES_PER_MIB;
        }
        Long finalBytes = null;
        if (hold == null) {
            if (requested == null || requested < 0) {
                hold = Reason.MEMORY_BUDGET_INFEASIBLE;
            } else if (minimum != null && requested < minimum) {
                hold = Reason.MEMORY_OVERRIDE_BELOW_FLOOR;
            } else if ((!preserveCurrent || cacheRequired)
                    && requested > A4S.MAX_MANAGED_MEMORY_MB * BYTES_PER_MIB) {
                hold = Reason.MEMORY_BUDGET_INFEASIBLE;
            } else {
                long wholeMiB = requested / BYTES_PER_MIB;
                finalBytes = (wholeMiB + (requested % BYTES_PER_MIB == 0 ? 0 : 1)) * BYTES_PER_MIB;
            }
        }
        Long nativeBytes = null;
        Long writeBufferBytes = null;
        if (cacheRequired && finalBytes != null) {
            nativeBytes = nominalCacheBytes(finalBytes, ratio);
            writeBufferBytes = writeBufferManagerBytes(finalBytes, ratio);
        }
        return new ManagedMemoryDecision(
                configuredFloor, trustedManagedBytes, proposed, retained, floorLimited,
                requested, finalBytes, nativeBytes, writeBufferBytes, null, hold);
    }

    private static long nominalCacheBytes(long managedBytes, double ratio) {
        return (long) ((3 - ratio) * managedBytes / 3);
    }

    private static long writeBufferManagerBytes(long managedBytes, double ratio) {
        return (long) (2 * (double) managedBytes * ratio / 3);
    }

    private static long configuredManagedMemoryLimit(
            Configuration conf, JobAutoScalerContext<?> context) {
        long processBytes = MemoryTuning.getTotalMemory(conf, context).getBytes();
        long processLimit = processBytes > 0 ? processBytes : Long.MAX_VALUE;
        var managed = conf.getOptional(TaskManagerOptions.MANAGED_MEMORY_SIZE);
        if (managed.isPresent()) {
            return Math.min(managed.get().getBytes(), processLimit);
        }
        try {
            var spec = MemoryTuning.FLINK_MEMORY_UTILS.memoryProcessSpecFromConfig(conf);
            return Math.min(spec.getFlinkMemory().getManaged().getBytes(), processLimit);
        } catch (IllegalConfigurationException unresolved) {
            return processLimit;
        }
    }

    private boolean managedMemoryWouldExceedResources(
            Context context, Configuration conf, EvaluatedMetrics evaluated, FinalScalingAction action) {
        long perManagerBytes = configuredManagedMemoryLimit(conf, context);
        long remainingJobBytes = conf.getOptional(MEMORY_QUOTA)
                .map(MemorySize::getBytes).orElse(Long.MAX_VALUE);
        for (var vertex : action.getVertices().values()) {
            long perTaskBytes = vertex.getProfile().getManagedMemory().getBytes();
            int parallelism = vertex.getParallelism();
            if (perTaskBytes > perManagerBytes
                    || (perTaskBytes > 0 && parallelism > remainingJobBytes / perTaskBytes)) {
                evaluated.getCacheEvaluation().setHoldReason(Reason.MEMORY_BUDGET_INFEASIBLE);
                return true;
            }
            remainingJobBytes -= parallelism * perTaskBytes;
        }
        return false;
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
        return computeScalingSummary(context, evaluatedMetrics, scalingHistory, restartTime,
                jobTopology, delayedScaleDown, false);
    }

    private Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Duration restartTime,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown,
            boolean onlyScaleUp) {
        LOG.debug("Restart time used in scaling summary computation: {}", restartTime);

        if (isJobUnderMemoryPressure(context, evaluatedMetrics.getGlobalMetrics())) {
            LOG.info("Skipping vertex scaling due to memory pressure");
            return Map.of();
        }

        var out = new HashMap<JobVertexID, ScalingSummary>();

        var excludeVertexIdList =
                context.getConfiguration().get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);
        evaluatedMetrics
                .getVertexMetrics()
                .forEach(
                        (v, metrics) -> {
                            if (onlyScaleUp && (jobTopology.getFinishedVertices().contains(v)
                                    || !qualifiesForThroughputScaleUp(
                                            metrics, context.getConfiguration(), restartTime))) {
                                return;
                            }
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
                                if (parallelismChange.isNoChange()
                                        || (onlyScaleUp
                                                && parallelismChange.getNewParallelism() <= currentParallelism)) {
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

    private static boolean qualifiesForThroughputScaleUp(
            Map<ScalingMetric, EvaluatedScalingMetric> metrics, Configuration conf,
            Duration restartTime) {
        var processing = metrics.get(TRUE_PROCESSING_RATE);
        var scaleUp = metrics.get(SCALE_UP_RATE_THRESHOLD);
        var input = metrics.get(TARGET_DATA_RATE);
        var catchUp = metrics.get(CATCH_UP_DATA_RATE);
        if (processing == null || scaleUp == null || input == null || catchUp == null
                || !Double.isFinite(processing.getAverage()) || processing.getAverage() <= 0
                || !Double.isFinite(scaleUp.getCurrent()) || processing.getAverage() >= scaleUp.getCurrent()
                || !Double.isFinite(input.getAverage()) || input.getAverage() < 0
                || !Double.isFinite(catchUp.getCurrent()) || catchUp.getCurrent() < 0) {
            return false;
        }
        double demand = AutoScalerUtils.getRequiredProcessingCapacity(metrics, conf, restartTime);
        return Double.isFinite(demand) && demand > 0;
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

    @VisibleForTesting
    public static Map<String, String> getVertexResourceProfileOverrides(JobID jobID, ScalingConfigurations scalingConfigurations, Configuration conf) {
        var overrides = new HashMap<String, String>();
        scalingConfigurations.getCurrentConfiguration(jobID, periods.getOrDefault(jobID, 0)).getScaling().forEach((id, information) -> {
            var managedMemoryMB = information.getManagedMemoryMB();
            overrides.put(
                    id.toString(),
                    managedMemoryMB >= 0
                            ? String.valueOf(getResourceProfile(conf, managedMemoryMB))
                            : String.valueOf(getResourceProfile(conf, information.getMemoryLevel()))
            );
        } );
        return overrides;
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
            if (information.getAvgCacheHitRate() == 0.0) { // Stateless
                information.setMemoryLevel(-1);
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
