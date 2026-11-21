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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
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
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ConfigurationInfoEntry;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
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
        return scaleResource(context, evaluatedMetrics, scalingHistory, scalingTracking, now,
                jobTopology, delayedScaleDown, null);
    }

    boolean scaleUpWithCurrentProfiles(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, CacheObservationBatch> observations,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown) throws Exception {
        return scaleResource(context, evaluatedMetrics, scalingHistory, scalingTracking, now,
                jobTopology, delayedScaleDown, observations);
    }

    private boolean scaleResource(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown,
            @Nullable Map<JobVertexID, CacheObservationBatch> overloadObservations) throws Exception {
        boolean throughputOnly = overloadObservations != null;
        var failures = evaluatedMetrics.getCacheCostFailures();
        if (throughputOnly && (failures.isEmpty() || failures.values().stream()
                .anyMatch(reason -> reason != Reason.INSUFFICIENT_CURRENT_PARALLELISM))) {
            return false;
        }
        var conf = context.getConfiguration();
        var restartTime = scalingTracking.getMaxRestartTimeOrDefault(conf);

        var scalingSummaries =
                computeScalingSummary(
                        context,
                        evaluatedMetrics,
                        scalingHistory,
                        restartTime,
                        jobTopology,
                        delayedScaleDown,
                        throughputOnly ? failures.keySet() : null);

        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return false;
        }

        Map<JobVertexID, ResourceProfile> retainedProfiles = Map.of();
        if (throughputOnly) {
            var profiles = preserveLiveProfiles(context, evaluatedMetrics, scalingSummaries,
                    overloadObservations);
            if (profiles.isEmpty()) {
                evaluatedMetrics.getCacheEvaluation().setHoldReason(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED);
                return false;
            }
            retainedProfiles = profiles.get();
            if (overloadWouldExceedResources(
                    context, evaluatedMetrics, scalingSummaries, retainedProfiles)) {
                return false;
            }
        }

        updateRecommendedParallelism(evaluatedMetrics.getVertexMetrics(), scalingSummaries);

        if (!throughputOnly && checkIfBlockedAndTriggerScalingEvent(context, scalingSummaries, conf, now)) {
            return false;
        }

        var configOverrides = throughputOnly ? new ConfigChanges()
                : MemoryTuning.tuneTaskManagerMemory(
                        context,
                        evaluatedMetrics,
                        jobTopology,
                        scalingSummaries,
                        autoScalerEventHandler);

        var memoryTuningEnabled = conf.get(AutoScalerOptions.MEMORY_TUNING_ENABLED);
        if (!throughputOnly
                && scalingWouldExceedMaxResources(
                        memoryTuningEnabled ? configOverrides.newConfigWithOverrides(conf) : conf,
                        jobTopology, evaluatedMetrics, scalingSummaries, context)) {
            return false;
        }

        if (throughputOnly && checkIfBlockedAndTriggerScalingEvent(context, scalingSummaries, conf, now)) {
            return false;
        }

        boolean useA4s = conf.get(JUSTIN_ENABLED) && conf.get(A4S_ENABLED);
        Map<JobVertexID, Decision> a4sDecisions = Map.of();
        Map<JobVertexID, Double> retainedMemory = Map.of();
        if (useA4s && !throughputOnly) {
            LOG.debug("A4S: Evaluated Metrics: {}", evaluatedMetrics);
            a4sDecisions = new A4S(jobTopology, evaluatedMetrics, scalingSummaries).makeDecision(conf);
            if (!conf.get(A4S_MANAGED_MEMORY_OVERRIDE_ENABLED)) {
                var retained = retainUnsupportedCacheBudgets(
                        context, evaluatedMetrics, scalingSummaries, a4sDecisions);
                if (retained.isEmpty()) {
                    return false;
                }
                retainedMemory = retained.get();
            }
        }

        addToScalingHistoryAndStore(
                autoScalerStateStore, context, scalingHistory, now, scalingSummaries);

        scalingTracking.addScalingRecord(now, new ScalingRecord());
        autoScalerStateStore.storeScalingTracking(context, scalingTracking);

        var requestedParallelism =
                getVertexParallelismOverrides(evaluatedMetrics.getVertexMetrics(), scalingSummaries);
        autoScalerStateStore.storeParallelismOverrides(context, requestedParallelism);
        Map<String, String> requestedProfiles = null;

        if (throughputOnly) {
            requestedProfiles = formatRetainedProfiles(retainedProfiles);
            autoScalerStateStore.storeResourceProfileOverrides(context, requestedProfiles);
            LOG.info("A4S job={} mode=THROUGHPUT_SCALE_UP_RETAINED_PROFILES vertices={} evaluationId={}",
                    context.getJobID(), scalingSummaries.keySet(),
                    evaluatedMetrics.getCacheEvaluation().getId());
        } else if (conf.get(JUSTIN_ENABLED)) {
            var currentScalingConf =
                    scalingConfigurations.setCurrentConfiguration(
                            context.getJobID(),
                            evaluatedMetrics.getVertexMetrics(),
                            scalingSummaries,
                            periods.getOrDefault(context.getJobID(), 0));
            
            if (useA4s) {
                applyA4sDecisions(conf, currentScalingConf, a4sDecisions, retainedMemory);
            } else {
                policy(context, currentScalingConf, conf);
            }
            
            LOG.info(scalingConfigurations.toString());

            requestedParallelism = getVertexParallelismOverrides(currentScalingConf);
            autoScalerStateStore.storeParallelismOverrides(context, requestedParallelism);

            requestedProfiles =
                    getVertexResourceProfileOverrides(context.getJobID(), this.scalingConfigurations, conf);
            autoScalerStateStore.storeResourceProfileOverrides(context, requestedProfiles);
        }

        autoScalerStateStore.storeConfigChanges(context, configOverrides);
        if (throughputOnly || useA4s) {
            evaluatedMetrics.getCacheEvaluation()
                    .recordRequestedOverrides(requestedParallelism, requestedProfiles);
        }

        // Try to clear all delayed scale down requests after scaling.
        delayedScaleDown.clearAll();

        return true;
    }

    private boolean overloadWouldExceedResources(
            Context context,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, ScalingSummary> summaries,
            Map<JobVertexID, ResourceProfile> profiles) {
        var snapshot = readLiveWorkers(context);
        if (snapshot.isEmpty()) {
            evaluated.getCacheEvaluation().setHoldReason(Reason.OVERLOAD_WORKER_SNAPSHOT_UNAVAILABLE);
            return true;
        }
        var running = new HashMap<ResourceProfile, Long>();
        var requested = new HashMap<ResourceProfile, Long>();
        profiles.forEach((vertex, profile) -> {
            var summary = summaries.get(vertex);
            long current =
                    (int) evaluated.getVertexMetrics().get(vertex).get(PARALLELISM).getCurrent();
            running.merge(profile, current, Math::addExact);
            requested.merge(
                    profile, summary == null ? current : summary.getNewParallelism(), Math::addExact);
        });
        var estimate = RetainedProfileResourceAccounting.estimate(
                context.getJobID(), snapshot.get(), running, requested);
        if (estimate.isEmpty()) {
            evaluated.getCacheEvaluation().setHoldReason(Reason.OVERLOAD_RESOURCE_GEOMETRY_UNSUPPORTED);
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
            evaluated.getCacheEvaluation().setHoldReason(Reason.OVERLOAD_RESOURCE_QUOTA);
            return true;
        }
        if (!resourceCheck.trySchedule(allocation.currentWorkers, allocation.requestedWorkers,
                allocation.cpu.doubleValue(), allocation.processMemory)) {
            evaluated.getCacheEvaluation().setHoldReason(Reason.OVERLOAD_CLUSTER_RESOURCES_UNAVAILABLE);
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

    private Optional<Map<JobVertexID, ResourceProfile>> preserveLiveProfiles(
            Context context,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, ScalingSummary> summaries,
            Map<JobVertexID, CacheObservationBatch> observations) throws Exception {
        var conf = context.getConfiguration();
        if (!conf.getBoolean("state.backend.rocksdb.memory.managed", true)
                || conf.getString("state.backend.rocksdb.memory.fixed-per-slot", null) != null
                || conf.getString("state.backend.rocksdb.memory.fixed-per-tm", null) != null) {
            return Optional.empty();
        }
        double ratio = conf.getDouble("state.backend.rocksdb.memory.write-buffer-ratio", 0.5);
        if (!(ratio > 0 && ratio < 1)) {
            return Optional.empty();
        }
        var requirements = readLiveResourceRequirements(context);
        if (requirements.isEmpty()) {
            return Optional.empty();
        }
        var vertices = requirements.get().getJobVertexParallelisms();
        if (!vertices.keySet().equals(evaluated.getVertexMetrics().keySet())) {
            return Optional.empty();
        }
        var retainedProfiles = new HashMap<JobVertexID, ResourceProfile>();
        long nowMillis = System.currentTimeMillis();
        for (var entry : vertices.entrySet()) {
            var id = entry.getKey();
            var live = entry.getValue();
            var profile = live.getResourceProfile();
            int current = (int) evaluated.getVertexMetrics().get(id).get(PARALLELISM).getCurrent();
            if (current <= 0 || live.getParallelism().getLowerBound() != 1
                    || live.getParallelism().getUpperBound() != current
                    || profile.equals(ResourceProfile.UNKNOWN) || profile.equals(ResourceProfile.ANY)
                    || !profile.getExtendedResources().isEmpty()) {
                return Optional.empty();
            }
            List<MemorySize> memory = List.of(profile.getTaskHeapMemory(), profile.getTaskOffHeapMemory(),
                    profile.getManagedMemory(), profile.getNetworkMemory());
            if (memory.stream().anyMatch(size -> size.getBytes() % (1L << 20) != 0
                    || size.getMebiBytes() > Integer.MAX_VALUE)) {
                return Optional.empty();
            }
            if (summaries.containsKey(id)) {
                long managedBytes = profile.getManagedMemory().getBytes();
                long expectedCache = (long) ((3 - ratio) * managedBytes / 3);
                var batch = observations.get(id);
                if (managedBytes <= 0 || batch == null || batch.subtasks.size() != current
                        || batch.subtasks.stream().anyMatch(observation -> observation.window == null
                                || observation.window.capacityBytes != expectedCache
                                || !CacheObservation.isFresh(
                                        observation.window.observedAtMillis, nowMillis))) {
                    return Optional.empty();
                }
            }
            retainedProfiles.put(id, profile);
        }
        return Optional.of(retainedProfiles);
    }

    private static Map<String, String> formatRetainedProfiles(
            Map<JobVertexID, ResourceProfile> profiles) {
        var overrides = new HashMap<String, String>();
        // Both existing realizers parse decimal MiB values, even for multi-GiB reservations.
        profiles.forEach((id, profile) -> overrides.put(id.toString(),
                "ResourceProfile{cpuCores=" + profile.getCpuCores().getValue()
                        + ", taskHeapMemory=" + overrideMemory(profile.getTaskHeapMemory())
                        + ", taskOffHeapMemory=" + overrideMemory(profile.getTaskOffHeapMemory())
                        + ", managedMemory=" + overrideMemory(profile.getManagedMemory())
                        + ", networkMemory=" + overrideMemory(profile.getNetworkMemory()) + "}"));
        return overrides;
    }

    private static String overrideMemory(MemorySize size) {
        return size.getMebiBytes() + ".000mb (" + size.getBytes() + " bytes)";
    }

    @VisibleForTesting
    Optional<JustinResourceRequirements> readLiveResourceRequirements(Context context)
            throws Exception {
        var headers = JustinResourceRequirementsHeaders.INSTANCE;
        var parameters = headers.getUnresolvedMessageParameters();
        parameters.jobPathParameter.resolve(context.getJobID());
        try (var client = context.getRestClusterClient()) {
            return client.sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                    .get(10, TimeUnit.SECONDS).asJobResourceRequirements();
        }
    }

    private Optional<Map<JobVertexID, Double>> retainUnsupportedCacheBudgets(
            Context context,
            EvaluatedMetrics evaluated,
            Map<JobVertexID, ScalingSummary> summaries,
            Map<JobVertexID, Decision> decisions) throws Exception {
        var candidates = new HashMap<JobVertexID, Integer>();
        evaluated.getVertexMetrics().forEach((id, metrics) -> {
            var curve = evaluated.getMemoryParallelismCurves().get(id);
            var decision = decisions.get(id);
            var summary = summaries.get(id);
            int current = (int) metrics.get(PARALLELISM).getCurrent();
            int requested = decision != null ? decision.getParallelism()
                    : summary != null ? summary.getNewParallelism() : current;
            boolean hasCache = curve != null || metrics.containsKey(ROCKS_DB_BLOCK_CACHE_HIT_RATE);
            if (hasCache && (decision == null || curve == null || !curve.hasSupportedCacheSize(requested))) {
                candidates.put(id, requested);
            }
        });
        if (candidates.isEmpty()) {
            return Optional.of(Map.of());
        }

        var requirements = readLiveResourceRequirements(context);
        if (requirements.isEmpty()) {
            LOG.warn("A4S: Deferring resize because live managed-memory profiles are unavailable");
            return Optional.empty();
        }
        var profiles = requirements.get();
        var retained = new HashMap<JobVertexID, Double>();
        for (var candidate : candidates.entrySet()) {
            var id = candidate.getKey();
            var profile = profiles.getJobVertices().contains(id) ? profiles.getResourceProfile(id) : null;
            if (profile == null || profile.equals(ResourceProfile.UNKNOWN)
                    || profile.equals(ResourceProfile.ANY) || profile.getManagedMemory().getBytes() <= 0) {
                LOG.warn("A4S: Deferring resize because vertex {} has no known managed-memory budget", id);
                return Optional.empty();
            }
            int current = (int) evaluated.getVertexMetrics().get(id).get(PARALLELISM).getCurrent();
            int requested = candidate.getValue();
            long bytesPerSlot = profile.getManagedMemory().getBytes();
            double memoryMb = Math.max(context.getConfiguration().get(A4S_MEMORY_BASE_MB),
                    retainedManagedMemoryMiB(bytesPerSlot, current, requested));
            if (Math.ceil(memoryMb) > A4S.MAX_MANAGED_MEMORY_MB) {
                LOG.warn("A4S: Deferring resize for vertex {} because the retained budget needs {} MiB per slot", id, memoryMb);
                return Optional.empty();
            }
            LOG.info("A4S: Retaining managed-memory budget for vertex {} ({} bytes × {} slots, {} MiB per new slot at parallelism {})",
                    id, bytesPerSlot, current, memoryMb, requested);
            retained.put(id, memoryMb);
        }
        return Optional.of(retained);
    }

    @VisibleForTesting
    static double retainedManagedMemoryMiB(long bytesPerSlot, int current, int requested) {
        return bytesPerSlot / (1024.0 * 1024.0) * current / requested;
    }

    private void applyA4sDecisions(
            Configuration conf,
            ScalingConfigurations.ScalingConfiguration scaling,
            Map<JobVertexID, Decision> decisions,
            Map<JobVertexID, Double> retainedMemory) {
        double base = conf.get(A4S_MEMORY_BASE_MB);
        scaling.getScaling().forEach((id, information) -> {
            var decision = decisions.get(id);
            if (decision != null) {
                information.setParallelism(decision.getParallelism());
            }
            double memory = retainedMemory.getOrDefault(id, (decision == null ? 0.0 : decision.getMemoryMB()) + base);
            if (conf.get(A4S_MANAGED_MEMORY_OVERRIDE_ENABLED)) {
                memory = conf.get(A4S_MANAGED_MEMORY_OVERRIDE_MB);
            }
            information.setManagedMemoryMB(memory);
            LOG.debug("A4S: Vertex {} has parallelism {} and memory {}MB", id, information.getParallelism(), memory);
        });
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
                jobTopology, delayedScaleDown, null);
    }

    private Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Duration restartTime,
            JobTopology jobTopology,
            DelayedScaleDown delayedScaleDown,
            @Nullable Set<JobVertexID> onlyScaleUp) {
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
                            if (onlyScaleUp != null && !onlyScaleUp.contains(v)) {
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
                                        || (onlyScaleUp != null
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
