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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.autoscaler.a4s.MemoryParallelismCurve;
import org.apache.flink.autoscaler.a4s.CacheCostSample;
import org.apache.flink.autoscaler.a4s.OnlineCacheCost;
import org.apache.flink.autoscaler.a4s.QuickMrcCostProjection;
import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.autoscaler.FinalScalingAction;
import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.With;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Collected scaling metrics. */
@Data
@NoArgsConstructor
public class EvaluatedMetrics {
    private Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics;
    private Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics;
    private Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves;
    private Map<JobVertexID, MissRateCurve> missRateCurves;
    private Map<JobVertexID, OnlineCacheCost.Reason> cacheCostFailures;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private Map<JobVertexID, CacheObservationBatch> cacheObservations = Map.of();

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final CacheEvaluation cacheEvaluation = new CacheEvaluation();

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics,
            Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves,
            Map<JobVertexID, MissRateCurve> missRateCurves,
            Map<JobVertexID, OnlineCacheCost.Reason> cacheCostFailures) {
        this.vertexMetrics = vertexMetrics;
        this.globalMetrics = globalMetrics;
        this.memoryParallelismCurves = memoryParallelismCurves;
        this.missRateCurves = missRateCurves;
        this.cacheCostFailures = cacheCostFailures;
    }

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics,
            Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves,
            Map<JobVertexID, MissRateCurve> missRateCurves) {
        this(vertexMetrics, globalMetrics, memoryParallelismCurves, missRateCurves, Map.of());
    }

    public EvaluatedMetrics(
        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics, 
        Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics) {
        this(vertexMetrics, globalMetrics, null, null);
    }

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics,
            Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves) {
        this(vertexMetrics, globalMetrics, memoryParallelismCurves, null);
    }

    public enum CacheMode {
        LEGACY_FIXED,
        OFFLINE_CALIBRATED,
        ONLINE_SHADOW,
        ONLINE_QUALIFIED,
        ONLINE_OVERLOAD,
        INVALID
    }

    public enum CacheAction {
        POLICY_HOLD,
        ONLINE_CACHE_ONLY_ACTUATION_UNAVAILABLE,
        THROUGHPUT_SCALE_UP_RETAINED_PROFILES,
        POLICY_REQUEST,
        POLICY_NO_CHANGE
    }

    /** Diagnostics for one reconciliation. Policy continues to use the existing metrics. */
    @Data
    public static class CacheEvaluation {
        private final UUID id = UUID.randomUUID();
        private final Map<JobVertexID, CacheDecision> vertices = new LinkedHashMap<>();
        private final Map<JobVertexID, CacheObservation.Status> observationHolds =
                new LinkedHashMap<>();
        private final Map<JobVertexID, ManagedMemoryDecision> managedMemoryDecisions =
                new LinkedHashMap<>();
        private boolean cachePlacementEnabled;
        @Nullable private CacheAction action;
        @Nullable private FinalScalingAction finalAction;
        @Nullable private OnlineCacheCost.Reason holdReason;
        @Nullable private Map<String, String> requestedParallelismOverrides;
        @Nullable private Map<String, String> requestedResourceProfileOverrides;

        public void recordRequestedOverrides(
                Map<String, String> parallelism, @Nullable Map<String, String> profiles) {
            requestedParallelismOverrides = Map.copyOf(parallelism);
            requestedResourceProfileOverrides = profiles == null ? null : Map.copyOf(profiles);
        }
    }

    /** Per-task managed-memory resolution. Final bytes describe the request. */
    @Value
    public static class ManagedMemoryDecision {
        @Nullable Long configuredFloorBytes;
        @Nullable Long trustedCurrentManagedBytes;
        @Nullable Long unconstrainedManagedBytes;
        @Nullable Long retainedManagedBytes;
        @Nullable Long floorLimitedManagedBytes;
        @Nullable Long requestedManagedBytes;
        @Nullable Long finalManagedBytes;
        @Nullable Long nominalNativeCacheBytes;
        @Nullable Long nominalWriteBufferManagerBytes;
        @Nullable Long reservedOrPinnedBytes;
        @Nullable OnlineCacheCost.Reason holdReason;
    }

    @Value
    public static class CacheDecision {
        CacheMode mode;
        @With @Nullable CacheMode selectedCostPolicy;
        ObservationEvidence observation;
        @Nullable ObservationEvidence fitStartObservation;
        @Nullable OnlineCacheCost.FitResult fit;
        @With @Nullable OnlineCacheCost.Reason reason;
        @Nullable Double demandRecordsPerSecond;
        @Nullable Long observedCurveMinBytes;
        @Nullable Long observedCurveMaxBytes;
        @Nullable QuickMrcCostProjection.Result projection;
        @Nullable FixedCosts fixedCosts;
        @Nullable Double projectionBaselineSecondsPerInput;

        /** The rejected online outcome is the fallback reason only if shadow selected a policy. */
        @Nullable
        public OnlineCacheCost.Reason getFallbackReason() {
            return mode == CacheMode.ONLINE_SHADOW && selectedCostPolicy != null
                            && reason != OnlineCacheCost.Reason.QUALIFIED
                    ? reason : null;
        }

        @Nullable
        public QuickMrcCostProjection.Recommendation getRecommendation() {
            return projection == null ? null : projection.getRecommendation();
        }

        public String toLogString() {
            var measured = fit == null ? null : fit.getFit();
            var timing = fit == null ? null : fit.getTimingCounts();
            return "selectedCostPolicy=" + selectedCostPolicy
                    + " fallbackReason=" + getFallbackReason()
                    + " observation=" + observation
                    + " fitStartObservation=" + fitStartObservation
                    + " fitReason=" + (fit == null ? null : fit.getReason())
                    + " fitObservationStatus=" + (fit == null ? null : fit.getObservationStatus())
                    + " failingSubtask=" + (fit == null ? null : fit.getFailingSubtask())
                    + " attemptedFitWindow=" + (fit == null ? null : fit.getAttemptedWindow())
                    + " timingCounts=" + timing
                    + " rejectedHitCoverage=" + (timing == null ? null : timing.getHitCoverage())
                    + " rejectedMissCoverage=" + (timing == null ? null : timing.getMissCoverage())
                    + " holdout=" + (fit == null ? null : fit.getHoldout())
                    + " measuredFit=" + (measured == null ? null : summarizeFit(measured))
                    + " measuredSubtasks=" + (measured == null ? null : measured.getSubtasks()
                            .stream().limit(LOGGED_SUBTASKS).map(CacheDecision::costUnits)
                            .collect(Collectors.toList()))
                    + " residualBusySecondsPerInput=" + (measured == null ? null
                            : measured.getCost().getZeroMissSecondsPerRecord())
                    + " slopeSecondsPerInputPerUnitMissFraction=" + (measured == null ? null
                            : measured.getCost().getMissPenaltySecondsPerRecord())
                    + " fittedSubtasks=" + (measured == null ? null : measured.getSubtasks().size())
                    + " demandRecordsPerSecond=" + demandRecordsPerSecond
                    + " observedCurveMinBytes=" + observedCurveMinBytes
                    + " observedCurveMaxBytes=" + observedCurveMaxBytes
                    + " candidateSupportMinTotalBytes="
                    + (projection == null ? null : projection.getMinSupportedTotalBytes())
                    + " candidateSupportMaxTotalBytes="
                    + (projection == null ? null : projection.getMaxSupportedTotalBytes())
                    + " recommendation=" + getRecommendation()
                    + " projectionBaselineSecondsPerInput=" + projectionBaselineSecondsPerInput
                    + " fixedCosts=" + fixedCosts
                    + " uncertainty=unavailable";
        }

        private static String summarizeFit(OnlineCacheCost.Fit fit) {
            return "OnlineCacheCost.Fit(cost=" + fit.getCost()
                    + ", identities=" + fit.getIdentities().stream().limit(LOGGED_SUBTASKS)
                            .collect(Collectors.toList())
                    + ", windowStartMillis=" + fit.getWindowStartMillis()
                    + ", windowEndMillis=" + fit.getWindowEndMillis() + ')';
        }

        private static String costUnits(OnlineCacheCost.SubtaskCost cost) {
            return "{sampledHits=" + cost.getSampledHits()
                    + ", sampledMisses=" + cost.getSampledMisses()
                    + ", hitCoverage=" + cost.getHitCoverage()
                    + ", missCoverage=" + cost.getMissCoverage()
                    + ", inputRecords=" + cost.getRecords()
                    + ", exactLookups=" + cost.getLookups()
                    + ", exactMisses=" + cost.getMisses()
                    + ", meanHitServiceSecondsPerAccess=" + cost.getMeanHitSeconds()
                    + ", meanMissServiceSecondsPerAccess=" + cost.getMeanMissSeconds()
                    + ", extraServiceSecondsPerForegroundMiss="
                    + cost.getExtraSecondsPerForegroundMiss()
                    + ", residualBusySecondsPerInput=" + cost.getZeroMissSecondsPerRecord()
                    + ", slopeSecondsPerInputPerUnitMissFraction="
                    + cost.getMissPenaltySecondsPerRecord() + '}';
        }
    }

    @Value
    public static class FixedCosts {
        double hitSecondsPerAccess;
        double totalMissSecondsPerAccess;

        public double getIncrementalMissSecondsPerAccess() {
            return totalMissSecondsPerAccess - hitSecondsPerAccess;
        }

        @Override
        public String toString() {
            return "{hitSecondsPerAccess=" + hitSecondsPerAccess
                    + ", totalMissSecondsPerAccess=" + totalMissSecondsPerAccess
                    + ", incrementalMissSecondsPerAccess=" + getIncrementalMissSecondsPerAccess()
                    + '}';
        }
    }

    private static final int LOGGED_SUBTASKS = 8;

    @Value
    public static class ObservationEvidence {
        CacheObservation.Status status;
        String expectedPopulation;
        int expectedTimingAbi;
        int expectedSubtasks;
        @Nullable Integer batchExpectedSubtasks;
        @Nullable Integer wireVersion;
        @Nullable Integer receivedSubtasks;
        int receivedTimingSubtasks;
        List<ObservationEndpoint> endpoints;
        List<TimingEndpoint> timingEndpoints;

        public static ObservationEvidence capture(
                int parallelism, CacheObservation.Status status,
                @Nullable CacheObservationBatch batch, List<CacheCostSample> timing) {
            return capture(parallelism, status, batch, timing, null);
        }

        public static ObservationEvidence capture(
                int parallelism, CacheObservation.Status status,
                @Nullable CacheObservationBatch batch, List<CacheCostSample> timing,
                @Nullable Integer failingSubtask) {
            return new ObservationEvidence(status, CacheObservation.QUICK_MRC,
                    OnlineCacheCost.COMPACTION_TIMING_ABI, parallelism,
                    batch == null ? null : batch.expectedSubtasks, batch == null ? null : batch.version,
                    batch == null ? null : batch.subtasks.size(),
                    (int) timing.stream().filter(Objects::nonNull).count(),
                    batch == null ? List.of() : loggedItems(batch.subtasks, failingSubtask)
                            .map(ObservationEndpoint::from).collect(Collectors.toUnmodifiableList()),
                    loggedItems(timing, failingSubtask).filter(Objects::nonNull)
                            .map(TimingEndpoint::from).collect(Collectors.toUnmodifiableList()));
        }

        private static <T> Stream<T> loggedItems(List<T> items, @Nullable Integer failingSubtask) {
            if (failingSubtask != null && failingSubtask >= LOGGED_SUBTASKS
                    && failingSubtask < items.size()) {
                return Stream.concat(items.stream().limit(LOGGED_SUBTASKS - 1),
                        Stream.of(items.get(failingSubtask)));
            }
            return items.stream().limit(LOGGED_SUBTASKS);
        }
    }

    @Value
    public static class ObservationEndpoint {
        int subtask;
        String attempt;
        CacheObservation.Status status;
        @Nullable String process;
        @Nullable Long cacheId;
        @Nullable Long epoch;
        @Nullable Long sequence;
        @Nullable Long capacityBytes;
        @Nullable Integer ownerCount;
        @Nullable String population;
        @Nullable String chargePolicy;
        @Nullable String appliedAllocationId;
        @Nullable Long appliedManagedMemoryBytes;
        @Nullable Long intervalStartNanos;
        @Nullable Long intervalEndNanos;
        @Nullable Long observedAtMillis;
        @Nullable Long exactLookups;
        @Nullable Long exactMisses;

        static ObservationEndpoint from(CacheObservation observation) {
            var window = observation.window;
            var appliedSlot = observation.endpoint == null ? null : observation.endpoint.appliedSlot;
            return new ObservationEndpoint(observation.subtask, observation.attempt,
                    observation.status, window == null ? null : window.process,
                    window == null ? null : window.cacheId, window == null ? null : window.epoch,
                    window == null ? null : window.sequence, window == null ? null : window.capacityBytes,
                    window == null ? null : window.owners.size(), window == null ? null : window.population,
                    window == null ? null : window.chargePolicy,
                    appliedSlot == null ? null : appliedSlot.allocationId,
                    appliedSlot == null ? null : appliedSlot.managedMemoryBytes,
                    window == null ? null : window.startNanos, window == null ? null : window.endNanos,
                    window == null ? null : window.observedAtMillis,
                    window == null ? null : window.references, window == null ? null : window.misses);
        }
    }

    @Value
    public static class TimingEndpoint {
        int subtask;
        @Nullable String attempt;
        @Nullable String process;
        long cacheId;
        long epoch;
        long capacityBytes;
        int timingAbi;
        long observedAtMillis;
        @Nullable Long observedAtNanos;

        static TimingEndpoint from(CacheCostSample sample) {
            return new TimingEndpoint(sample.getSubtask(), sample.getAttempt(), sample.getProcess(),
                    sample.getCacheId(), sample.getCacheEpoch(), sample.getCapacityBytes(),
                    sample.getTimingAbi(), sample.getObservedAtMillis(), sample.getObservedAtNanos());
        }
    }
}
