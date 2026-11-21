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
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.a4s.OnlineCacheCost.Reason;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheAction;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheEvaluation;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.resources.ResourceCheck;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMetricsInfo;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheObservationHoldTest {
    @RegisterExtension
    final LoggerAuditingExtension decisionLogs =
            new LoggerAuditingExtension(JobAutoScalerImpl.class, Level.INFO);

    @Test
    void infeasibleFixedCostsHoldWithFreshCacheTelemetryAndPreserveOverrides() throws Exception {
        var context = TestingAutoscalerUtils.createDefaultJobAutoScalerContext();
        var config = context.getConfiguration();
        config.addAll(ScalingMetricEvaluatorTest.fixedCostConfig());
        config.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        config.set(AutoScalerOptions.SCALING_ENABLED, true);
        var vertex = new JobVertexID();
        var history = ScalingMetricEvaluatorTest.fixedDemandHistory(vertex, 1, 1);
        history.setFullyCollected(true);
        var clock = Clock.fixed(Instant.ofEpochSecond(1), ZoneOffset.UTC);
        var observation = new CacheObservation(
                CacheObservation.VERSION, context.getJobID().toString(), vertex.toString(), 0,
                "attempt", CacheObservation.Status.VALID,
                new CacheObservation.Window(
                        "process", 1, 0, 2, List.of(1L), CacheObservation.QUICK_MRC,
                        CacheObservation.FIXED_4K, CacheObservation.CACHE_POLICY, .1, 200L << 20,
                        1, 10_000_000_001L, clock.millis(), 1000, 1),
                null);
        history.getMetricHistory().get(history.getMetricHistory().lastKey()).setCacheObservations(
                Map.of(vertex, new CacheObservationBatch(
                        CacheObservation.VERSION, CacheObservation.Status.VALID,
                        1, 32, 0, List.of(observation))));

        var stateStore = new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>();
        var previousParallelism = Map.of(vertex.toString(), "1");
        var previousProfiles = Map.of(vertex.toString(), ResourceProfile.newBuilder()
                .setManagedMemoryMB(240).build().toString());
        stateStore.storeParallelismOverrides(context, previousParallelism);
        stateStore.storeResourceProfileOverrides(context, previousProfiles);
        var events = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        var collector = new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(
                history.getJobTopology()) {
            @Override
            public CollectedMetricHistory updateMetrics(
                    JobAutoScalerContext<JobID> ctx,
                    AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> store) {
                return history;
            }
        };
        var preservationCalls = new AtomicInteger();
        var scalingRequests = new AtomicInteger();
        var realizer = new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
            @Override
            public void preserveAppliedResourceOverrides(JobAutoScalerContext<JobID> ctx) {
                preservationCalls.incrementAndGet();
            }

            @Override
            public void realizeParallelismOverrides(
                    JobAutoScalerContext<JobID> ctx, Map<String, String> parallelism,
                    Map<String, String> profiles) {
                scalingRequests.incrementAndGet();
            }
        };
        var autoscaler = new JobAutoScalerImpl<>(
                collector, new ScalingMetricEvaluator(), new ScalingExecutor<>(events, stateStore),
                events, realizer, stateStore);
        autoscaler.setClock(clock);

        assertTrue(autoscaler.checkCacheObservations(context, history));
        autoscaler.scale(context);

        assertThat(events.events).extracting(TestingEventCollector.Event::getMessage).isEmpty();
        assertEquals(0, scalingRequests.get());
        assertEquals(1, preservationCalls.get());
        assertEquals(previousParallelism, stateStore.getParallelismOverrides(context));
        assertEquals(previousProfiles, stateStore.getResourceProfileOverrides(context));
        assertTrue(stateStore.getScalingHistory(context).isEmpty());
        assertTrue(stateStore.getScalingTracking(context).getLatestScalingRecordEntry().isEmpty());
    }

    @Test
    void invalidRequiredCacheMetricsHoldEvenWithPersistedOverrides() throws Exception {
        var context = TestingAutoscalerUtils.createDefaultJobAutoScalerContext();
        context.getConfiguration().set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        var stateStore = new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>();
        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        var vertex = new JobVertexID();
        var topology = new JobTopology(new VertexInfo(vertex, Map.of(), 1, 10));
        var collector = new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(topology) {
            @Override
            public CollectedMetricHistory updateMetrics(
                    JobAutoScalerContext<JobID> ctx,
                    AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> store) {
                var metrics = new CollectedMetrics();
                metrics.setCacheObservations(
                        Map.of(vertex, CacheObservationBatch.invalid(CacheObservation.Status.STALE)));
                return new CollectedMetricHistory(
                        topology, new TreeMap<>(Map.of(Instant.now(), metrics)), Instant.EPOCH);
            }
        };
        context.getConfiguration().set(AutoScalerOptions.A4S_ENABLED, true);
        context.getConfiguration().set(AutoScalerOptions.JUSTIN_ENABLED, true);
        stateStore.storeParallelismOverrides(context, Map.of(vertex.toString(), "2"));
        context.getConfiguration().set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
        AtomicInteger preservationCalls = new AtomicInteger();
        AtomicInteger realizationCalls = new AtomicInteger();
        var recordingRealizer = new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
            @Override
            public void preserveAppliedResourceOverrides(JobAutoScalerContext<JobID> ctx) {
                preservationCalls.incrementAndGet();
            }

            @Override
            public void realizeParallelismOverrides(
                    JobAutoScalerContext<JobID> ctx, Map<String, String> parallelism,
                    Map<String, String> profiles) {
                realizationCalls.incrementAndGet();
            }
        };
        var decision = new AtomicReference<CacheEvaluation>();
        var autoscaler = new JobAutoScalerImpl<>(
                collector,
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventCollector, stateStore),
                eventCollector,
                recordingRealizer,
                stateStore) {
            @Override
            void reportCacheDecision(
                    JobAutoScalerContext<JobID> ctx, CacheEvaluation record, CacheAction action) {
                super.reportCacheDecision(ctx, record, action);
                decision.set(record);
            }
        };

        autoscaler.scale(context);

        assertEquals(1, preservationCalls.get());
        assertEquals(0, realizationCalls.get());
        assertEquals(Map.of(vertex.toString(), "2"), stateStore.getParallelismOverrides(context));
        assertEquals(CacheAction.POLICY_HOLD, decision.get().getAction());
        assertEquals(CacheObservation.Status.STALE, decision.get().getObservationHolds().get(vertex));
        assertEquals(CacheObservation.Status.STALE,
                decision.get().getVertices().get(vertex).getObservation().getStatus());
        assertNull(decision.get().getRequestedParallelismOverrides());
        assertTrue(decisionLogs.getMessages().stream().anyMatch(message ->
                message.contains("reason=STALE") && message.contains("evaluationId=" + decision.get().getId())));
        var history = collector.updateMetrics(context, stateStore);
        assertFalse(autoscaler.checkCacheObservations(context, history));
        var metrics = history.getMetricHistory().get(history.getMetricHistory().lastKey());
        var observation = new CacheObservation(
                CacheObservation.VERSION, context.getJobID().toString(), vertex.toString(), 0,
                "attempt", CacheObservation.Status.VALID,
                new CacheObservation.Window(
                        "process", 1, 0, 2, List.of(1L), CacheObservation.QUICK_MRC,
                        CacheObservation.FIXED_4K, CacheObservation.CACHE_POLICY, .1, 8192,
                        1, 10_000_000_001L, System.currentTimeMillis(), 100, 25),
                null);
        metrics.setCacheObservations(Map.of(vertex, new CacheObservationBatch(
                CacheObservation.VERSION, CacheObservation.Status.VALID, 1, 32, 100,
                List.of(observation))));
        metrics.setMissRateCurves(Map.of(vertex, new MissRateCurve(List.of(new MissRateCurve.Point(4096, .25)))));
        assertTrue(autoscaler.checkCacheObservations(context, history));
    }

    @Test
    void distinguishesAQualifiedProjectionFromAnActuationPath() throws Exception {
        var context = TestingAutoscalerUtils.createDefaultJobAutoScalerContext();
        var config = context.getConfiguration();
        config.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        config.set(AutoScalerOptions.A4S_ENABLED, true);
        config.set(AutoScalerOptions.JUSTIN_ENABLED, true);
        config.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
        var stateStore = new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>();
        var events = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        var vertex = new JobVertexID();
        var topology = new JobTopology(new VertexInfo(vertex, Map.of(), 1, 10));
        var collector = new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(topology) {
            @Override
            public CollectedMetricHistory updateMetrics(
                    JobAutoScalerContext<JobID> ctx,
                    AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> store) {
                var latest = new CollectedMetrics();
                var observation = new CacheObservation(
                        CacheObservation.VERSION, context.getJobID().toString(),
                        vertex.toString(), 0, "attempt", CacheObservation.Status.VALID,
                        new CacheObservation.Window(
                                "process", 1, 0, 2, List.of(1L), CacheObservation.QUICK_MRC,
                                CacheObservation.FIXED_4K, CacheObservation.CACHE_POLICY, .1, 8192,
                                1, 10_000_000_001L, System.currentTimeMillis(), 100, 25),
                        null);
                latest.setCacheObservations(Map.of(vertex, new CacheObservationBatch(
                        CacheObservation.VERSION, CacheObservation.Status.VALID, 1, 32, 0,
                        List.of(observation))));
                latest.setMissRateCurves(Map.of(vertex, new MissRateCurve(List.of(
                        new MissRateCurve.Point(4096, .25)))));
                var history = new CollectedMetricHistory(topology, new TreeMap<>(Map.of(
                        Instant.EPOCH, new CollectedMetrics(), Instant.now(), latest)), Instant.EPOCH);
                history.setFullyCollected(true);
                return history;
            }
        };
        var evaluator = new ScalingMetricEvaluator() {
            @Override
            public EvaluatedMetrics evaluate(
                    org.apache.flink.configuration.Configuration conf,
                    CollectedMetricHistory metrics, Duration restartTime) {
                return new EvaluatedMetrics(
                        Map.of(vertex, new HashMap<>(Map.of(
                                ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(1)))),
                        Map.of());
            }
        };
        var requests = new AtomicInteger();
        var executorDecision = new AtomicReference<CacheEvaluation>();
        var executor = new ScalingExecutor<JobID, JobAutoScalerContext<JobID>>(events, stateStore) {
            @Override
            public boolean scaleResource(
                    JobAutoScalerContext<JobID> ctx, EvaluatedMetrics metrics,
                    Map<JobVertexID, SortedMap<Instant, ScalingSummary>> history,
                    ScalingTracking tracking, Instant now, JobTopology jobTopology,
                    DelayedScaleDown delayedScaleDown) {
                requests.incrementAndGet();
                executorDecision.set(metrics.getCacheEvaluation());
                return false;
            }
        };
        var autoscaler = new JobAutoScalerImpl<>(
                collector, evaluator, executor, events, new TestingScalingRealizer<>(), stateStore);

        autoscaler.scale(context);
        assertEquals(0, requests.get());
        assertEquals(CacheAction.ONLINE_CACHE_ONLY_ACTUATION_UNAVAILABLE,
                autoscaler.lastEvaluatedMetrics.get(context.getJobKey()).getCacheEvaluation().getAction());
        config.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_SHADOW, true);
        autoscaler.scale(context);
        assertEquals(1, requests.get());
        var result = autoscaler.lastEvaluatedMetrics.get(context.getJobKey()).getCacheEvaluation();
        assertSame(executorDecision.get(), result);
        assertEquals(CacheAction.POLICY_NO_CHANGE, result.getAction());
    }

    @Test
    void overloadScalesOnlyTheProvenBottleneckAndPreservesLivePerSubtaskMemory() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.autoscaler.scale(replay.context);

        assertEquals(Map.of(replay.join.toString(), "4", replay.source.toString(), "1",
                        replay.sink.toString(), "1"),
                replay.stateStore.getParallelismOverrides(replay.context));
        var profiles = replay.stateStore.getResourceProfileOverrides(replay.context);
        assertTrue(profiles.get(replay.join.toString()).contains("managedMemory=192.000mb"));
        assertTrue(profiles.get(replay.source.toString()).contains("managedMemory=158.000mb"));
        assertEquals(1, replay.profileReads.get());
        assertEquals(1, replay.requests.get());
        var decision = replay.autoscaler.lastEvaluatedMetrics.get(replay.context.getJobKey()).getCacheEvaluation();
        assertEquals(CacheAction.THROUGHPUT_SCALE_UP_RETAINED_PROFILES, decision.getAction());
        assertTrue(decision.isCachePlacementEnabled());
        assertEquals(profiles, decision.getRequestedResourceProfileOverrides());
        assertEquals("4", decision.getRequestedParallelismOverrides().get(replay.join.toString()));
        assertTrue(decisionLogs.getMessages().stream().anyMatch(message ->
                message.contains("action=THROUGHPUT_SCALE_UP_RETAINED_PROFILES")
                        && message.contains("evaluationId=" + decision.getId())
                        && message.contains("appliedOverrides=unavailable")));
        assertEquals(List.of(3, 6), replay.scheduleCounts);
    }

    @Test
    void overloadHoldsWhenTheLiveProfileDisagreesWithTheObservedCache() throws Exception {
        var replay = new OverloadFixture(159L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.profileReads.get());
        assertEquals(0, replay.requests.get());
        assertTrue(replay.stateStore.getParallelismOverrides(replay.context).isEmpty());
        var decision = replay.autoscaler.lastEvaluatedMetrics.get(replay.context.getJobKey()).getCacheEvaluation();
        assertEquals(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED, decision.getHoldReason());
    }

    @Test
    void overloadQuotaCountsUnchangedVerticesAndRoundsTaskManagersUp() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        var conf = replay.context.getConfiguration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        conf.set(AutoScalerOptions.MEMORY_QUOTA, MemorySize.ofMebiBytes(1744 * 5));
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertTrue(replay.stateStore.getParallelismOverrides(replay.context).isEmpty());
        var decision = replay.autoscaler.lastEvaluatedMetrics.get(replay.context.getJobKey()).getCacheEvaluation();
        assertEquals(Reason.OVERLOAD_RESOURCE_QUOTA, decision.getHoldReason());
    }

    @Test
    void boundsRoutineDecisionLogsAndCorrelatesEveryRequest() {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        var first = new CacheEvaluation();
        var throttled = new CacheEvaluation();
        var next = new CacheEvaluation();
        var request = new CacheEvaluation();
        replay.autoscaler.setClock(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        replay.autoscaler.reportCacheDecision(replay.context, first, CacheAction.POLICY_HOLD);
        replay.autoscaler.reportCacheDecision(replay.context, throttled, CacheAction.POLICY_HOLD);
        replay.autoscaler.setClock(Clock.fixed(Instant.EPOCH.plusSeconds(10), ZoneOffset.UTC));
        replay.autoscaler.reportCacheDecision(replay.context, next, CacheAction.POLICY_HOLD);
        replay.autoscaler.reportCacheDecision(replay.context, request, CacheAction.POLICY_REQUEST);

        var logged = decisionLogs.getMessages();
        assertEquals(3, logged.size());
        assertTrue(logged.get(0).contains("evaluationId=" + first.getId()));
        assertTrue(logged.get(1).contains("evaluationId=" + next.getId()));
        assertTrue(logged.get(2).contains("evaluationId=" + request.getId()));
        assertEquals(CacheAction.POLICY_HOLD, throttled.getAction());
    }

    @Test
    void overloadHoldsWhenSixOneCpuProfilesExceedFourCpuQuota() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        var conf = replay.context.getConfiguration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        conf.set(AutoScalerOptions.CPU_QUOTA, 4.0);
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertTrue(replay.stateStore.getParallelismOverrides(replay.context).isEmpty());
        assertTrue(replay.stateStore.getResourceProfileOverrides(replay.context).isEmpty());
        assertTrue(replay.stateStore.getScalingHistory(replay.context).isEmpty());
    }

    @Test
    void overloadUsesDeployedWorkersAndAcceptsTheExactQuotaBoundary() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        var desired = replay.context.getConfiguration();
        desired.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        desired.set(TaskManagerOptions.CPU_CORES, 0.25);
        desired.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(4096));
        desired.set(AutoScalerOptions.CPU_QUOTA, 6.0);
        desired.set(AutoScalerOptions.MEMORY_QUOTA, MemorySize.ofMebiBytes(1744 * 6));
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.requests.get());
        assertEquals(List.of(3, 6), replay.scheduleCounts);
        assertEquals(1.0, replay.scheduleCpu);
        assertEquals(MemorySize.ofMebiBytes(1744), replay.scheduleMemory);
    }

    @ParameterizedTest
    @ValueSource(strings = {"embedded", "kubernetes-application"})
    void overloadSchedulesWithByteExactNativeWorkerCapacity(String target) throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM, 2);
        replay.workers = Optional.of(replay.observedWorkerSnapshot(
                target, "flink-taskmanager-1-1", 570215624L));
        replay.context.getConfiguration().set(AutoScalerOptions.CPU_QUOTA, 8.0);
        replay.context.getConfiguration().set(
                AutoScalerOptions.MEMORY_QUOTA, MemorySize.ofMebiBytes(4440));
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.requests.get());
        assertEquals(Map.of(replay.source.toString(), "2", replay.join.toString(), "4",
                        replay.sink.toString(), "2"),
                replay.stateStore.getParallelismOverrides(replay.context));
        assertEquals(List.of(2, 2), replay.scheduleCounts);
        assertEquals(4.0, replay.scheduleCpu);
        assertEquals(MemorySize.ofMebiBytes(2220), replay.scheduleMemory);
    }

    @Test
    void overloadHoldsWhenAnEmbeddedWorkerDoesNotBelongToTheDeployedKubernetesCluster()
            throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM, 2);
        replay.workers = Optional.of(replay.observedWorkerSnapshot(
                "embedded", "standalone-taskmanager-1-1", 570215624L));
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertTrue(replay.scheduleCounts.isEmpty());
    }

    @Test
    void overloadHoldsWhenWorkerBytesDisagreeDespiteTheSameWholeMiBReport() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM, 2);
        replay.workers = Optional.of(replay.observedWorkerSnapshot(
                "embedded", "flink-taskmanager-1-1", 570215625L));
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertTrue(replay.scheduleCounts.isEmpty());
        assertTrue(replay.stateStore.getScalingHistory(replay.context).isEmpty());
    }

    @Test
    void overloadUsesManagedMemoryFitSeparatelyFromProcessQuota() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.workers = Optional.of(replay.workerSnapshot(4, 192));
        replay.context.getConfiguration().set(
                AutoScalerOptions.MEMORY_QUOTA, MemorySize.ofMebiBytes(1552 * 4));
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertTrue(replay.scheduleCounts.isEmpty());
        assertTrue(replay.stateStore.getScalingHistory(replay.context).isEmpty());
    }

    @Test
    void overloadRoundsNondivisibleDemandAcrossWholeWorkers() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.workers = Optional.of(replay.workerSnapshot(4, 768));
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.requests.get());
        // One running worker packs the three current slots. Six requested slots require two.
        assertEquals(List.of(1, 2), replay.scheduleCounts);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void overloadHoldsAndPreservesOverridesWhenWorkerEvidenceIsUnavailableOrIncompatible(
            boolean missing) throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.workers = missing ? Optional.empty() : Optional.of(replay.workerSnapshot(1, 160));
        var beforeParallelism = Map.of(replay.join.toString(), "1");
        var beforeProfiles = Map.of(replay.join.toString(), "applied-profile");
        replay.stateStore.storeParallelismOverrides(replay.context, beforeParallelism);
        replay.stateStore.storeResourceProfileOverrides(replay.context, beforeProfiles);
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertEquals(1, replay.preservations.get());
        assertEquals(beforeParallelism, replay.stateStore.getParallelismOverrides(replay.context));
        assertEquals(beforeProfiles, replay.stateStore.getResourceProfileOverrides(replay.context));
        assertTrue(replay.stateStore.getScalingHistory(replay.context).isEmpty());
        assertTrue(replay.stateStore.getScalingTracking(replay.context).getLatestScalingRecordEntry().isEmpty());
    }

    @Test
    void overloadRejectsUnrepresentableRequestedSlotCount() {
        var profile = OverloadFixture.resourceProfile(192);
        var differentProfile = OverloadFixture.resourceProfile(158);
        assertThrows(ArithmeticException.class, () ->
                RetainedProfileResourceAccounting.estimateRequestedWorkers(
                        OverloadFixture.resourceProfile(384),
                        Map.of(profile, 1L, differentProfile, 1L),
                        Map.of(profile, (long) Integer.MAX_VALUE, differentProfile, 1L), 2));
    }

    @Test
    void anUnqualifiedFitOrUnsupportedCurveCannotUseTheOverloadPath() throws Exception {
        for (var reason : List.of(Reason.MISSING_HOLDOUT, Reason.CAPACITY_UNSUPPORTED)) {
            var replay = new OverloadFixture(160L << 20, reason);
            replay.autoscaler.scale(replay.context);

            assertEquals(0, replay.profileReads.get());
            assertEquals(0, replay.requests.get());
        }
    }

    private static final class OverloadFixture {
        private final JobAutoScalerContext<JobID> context =
                TestingAutoscalerUtils.createResourceAwareContext(1.0, "1024 mb");
        private final InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore =
                new InMemoryAutoScalerStateStore<>();
        private final JobVertexID join = new JobVertexID();
        private final JobVertexID source = new JobVertexID();
        private final JobVertexID sink = new JobVertexID();
        private final AtomicInteger profileReads = new AtomicInteger();
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicInteger preservations = new AtomicInteger();
        private List<Integer> scheduleCounts = List.of();
        private double scheduleCpu;
        private MemorySize scheduleMemory;
        private Optional<RetainedProfileResourceAccounting.WorkerSnapshot> workers =
                Optional.of(workerSnapshot(1, 384));
        private final JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;

        private OverloadFixture(long nativeCacheBytes, Reason reason) {
            this(nativeCacheBytes, reason, 1);
        }

        private OverloadFixture(long nativeCacheBytes, Reason reason, int edgeParallelism) {
            var conf = context.getConfiguration();
            conf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
            conf.set(AutoScalerOptions.SCALING_ENABLED, true);
            conf.set(AutoScalerOptions.JUSTIN_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_CACHE_METRICS_EXEMPT_VERTICES,
                    List.of(source.toString(), sink.toString()));
            conf.set(AutoScalerOptions.TARGET_UTILIZATION, .35);
            var topology = new JobTopology(new VertexInfo(source, Map.of(), edgeParallelism, 24),
                    new VertexInfo(join, Map.of(source, ShipStrategy.HASH), 1, 24),
                    new VertexInfo(sink, Map.of(join, ShipStrategy.HASH), edgeParallelism, 24));
            var collector = new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(topology) {
                @Override
                public CollectedMetricHistory updateMetrics(JobAutoScalerContext<JobID> ctx,
                        AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> store) {
                    var latest = new CollectedMetrics();
                    var observation = new CacheObservation(
                            CacheObservation.VERSION, ctx.getJobID().toString(), join.toString(), 0,
                            "attempt", CacheObservation.Status.VALID,
                            new CacheObservation.Window("process", 1, 0, 2, List.of(1L),
                                    CacheObservation.QUICK_MRC, CacheObservation.FIXED_4K,
                                    CacheObservation.CACHE_POLICY, .02, nativeCacheBytes,
                                    1, 10_000_000_001L, System.currentTimeMillis(), 1000, 250), null);
                    latest.setCacheObservations(Map.of(join, new CacheObservationBatch(
                            CacheObservation.VERSION, CacheObservation.Status.VALID,
                            1, 32, 0, List.of(observation))));
                    latest.setMissRateCurves(Map.of(join, new MissRateCurve(List.of(
                            new MissRateCurve.Point(nativeCacheBytes, .25)))));
                    var history = new CollectedMetricHistory(topology, new TreeMap<>(Map.of(
                            Instant.EPOCH, new CollectedMetrics(), Instant.now(), latest)), Instant.EPOCH);
                    history.setFullyCollected(true);
                    return history;
                }
            };
            var evaluator = new ScalingMetricEvaluator() {
                @Override
                public EvaluatedMetrics evaluate(
                        org.apache.flink.configuration.Configuration cfg,
                        CollectedMetricHistory metrics, Duration restartTime) {
                    return new EvaluatedMetrics(
                            Map.of(join, overloadedMetrics(1), source, overloadedMetrics(edgeParallelism),
                                    sink, overloadedMetrics(edgeParallelism)),
                            Map.of(ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(0),
                                    ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.avg(.4),
                                    ScalingMetric.NUM_TASK_SLOTS_USED,
                                    EvaluatedScalingMetric.of(2 * edgeParallelism + 1)),
                            Map.of(), Map.of(), Map.of(join, reason));
                }
            };
            var events = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
            ResourceCheck resourceCheck = (current, requested, cpu, memory) -> {
                scheduleCounts = List.of(current, requested);
                scheduleCpu = cpu;
                scheduleMemory = memory;
                return true;
            };
            var executor = new ScalingExecutor<JobID, JobAutoScalerContext<JobID>>(
                    events, stateStore, resourceCheck) {
                @Override
                Optional<JustinResourceRequirements> readLiveResourceRequirements(
                        JobAutoScalerContext<JobID> ctx) {
                    profileReads.incrementAndGet();
                    return Optional.of(JustinResourceRequirements.newBuilder()
                            .setParallelismForJobVertex(join, 1, 1, resourceProfile(192))
                            .setParallelismForJobVertex(source, 1, edgeParallelism, resourceProfile(158))
                            .setParallelismForJobVertex(sink, 1, edgeParallelism, resourceProfile(158))
                            .build());
                }

                @Override
                Optional<RetainedProfileResourceAccounting.WorkerSnapshot> readLiveWorkers(
                        JobAutoScalerContext<JobID> ctx) {
                    return workers;
                }
            };
            var realizer = new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
                @Override
                public void preserveAppliedResourceOverrides(JobAutoScalerContext<JobID> ctx) {
                    preservations.incrementAndGet();
                }

                @Override
                public void realizeParallelismOverrides(
                        JobAutoScalerContext<JobID> ctx, Map<String, String> p,
                        Map<String, String> profiles) {
                    requests.incrementAndGet();
                }
            };
            autoscaler = new JobAutoScalerImpl<>(
                    collector, evaluator, executor, events, realizer, stateStore);
        }

        private RetainedProfileResourceAccounting.WorkerSnapshot workerSnapshot(double cpu, int managed) {
            var deployed = new Configuration();
            deployed.set(DeploymentOptions.TARGET, "kubernetes-application");
            deployed.set(ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT, true);
            deployed.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
            deployed.set(TaskManagerOptions.CPU_CORES, cpu);
            deployed.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(560));
            deployed.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ZERO);
            deployed.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(managed));
            deployed.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(160));
            deployed.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(160));
            deployed.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ofMebiBytes(128));
            deployed.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(128));
            deployed.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(256));
            deployed.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.ofMebiBytes(128));
            deployed.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.ofMebiBytes(128));
            var capacity = ResourceProfile.newBuilder().setCpuCores(cpu).setTaskHeapMemoryMB(560)
                    .setManagedMemoryMB(managed).setNetworkMemoryMB(160).build();
            var memory = TaskExecutorMemoryConfiguration.create(deployed);
            var profiles = List.of(resourceProfile(158), resourceProfile(192), resourceProfile(158));
            var total = profiles.stream().reduce(ResourceProfile.ZERO, ResourceProfile::merge);
            var taskManagers = capacity.allFieldsNoLessThan(total)
                    ? List.of(worker(capacity, memory, profiles))
                    : profiles.stream().map(profile -> worker(capacity, memory, List.of(profile)))
                            .collect(Collectors.toList());
            return new RetainedProfileResourceAccounting.WorkerSnapshot(deployed, taskManagers);
        }

        private RetainedProfileResourceAccounting.WorkerSnapshot observedWorkerSnapshot(
                String target, String firstWorkerId, long firstTaskHeapBytes) {
            // Worker geometry and REST encodings from the c167 scout. Retained profiles use
            // exact MiB, but TaskManager heap and network capacities have byte remainders.
            var deployed = Configuration.fromMap(Map.of(
                    "execution.target", target,
                    "kubernetes.cluster-id", "flink",
                    "kubernetes.namespace", "scout",
                    "kubernetes.taskmanager.cpu.amount", "4.0",
                    "taskmanager.cpu.cores", "4",
                    "taskmanager.memory.process.size", "2220 mb",
                    "taskmanager.memory.managed.fraction", "0.4",
                    "taskmanager.memory.managed.size", "768m",
                    "taskmanager.numberOfTaskSlots", "4"));
            var total = new ResourceProfileInfo(4.0, 543, 0, 768, 174, Map.of());
            var first = worker(new ResourceID(firstWorkerId), total,
                    new ResourceProfileInfo(0.0, 7, 0, 102, 18, Map.of()),
                    observedMemoryConfiguration(firstTaskHeapBytes),
                    List.of(resourceProfile(158), resourceProfile(158), resourceProfile(192),
                            resourceProfile(158)));
            var second = worker(new ResourceID("flink-taskmanager-1-2"), total,
                    new ResourceProfileInfo(3.0, 409, 0, 610, 135, Map.of()),
                    observedMemoryConfiguration(570215624L), List.of(resourceProfile(158)));
            return new RetainedProfileResourceAccounting.WorkerSnapshot(deployed, List.of(first, second));
        }

        private static TaskExecutorMemoryConfiguration observedMemoryConfiguration(long taskHeapBytes) {
            return new TaskExecutorMemoryConfiguration(
                    134217728L, taskHeapBytes, 134217728L, 0L, 182661941L,
                    805306368L, 268435456L, 232783875L, 1826619389L, 2327838720L);
        }

        private TaskManagerDetailsInfo worker(
                ResourceProfile capacity, TaskExecutorMemoryConfiguration memory,
                List<ResourceProfile> profiles) {
            // A fabricated mismatch is useful evidence for rejection as long as the REST DTO
            // still contains a well-formed worker and slot.
            var used = profiles.stream().reduce(ResourceProfile.ZERO, ResourceProfile::merge);
            var free = capacity.allFieldsNoLessThan(used) ? capacity.subtract(used) : ResourceProfile.ZERO;
            return worker(ResourceID.generate(), ResourceProfileInfo.fromResrouceProfile(capacity),
                    ResourceProfileInfo.fromResrouceProfile(free), memory, profiles);
        }

        private TaskManagerDetailsInfo worker(
                ResourceID id, ResourceProfileInfo total, ResourceProfileInfo free,
                TaskExecutorMemoryConfiguration memory, List<ResourceProfile> profiles) {
            var slots = profiles.stream().map(profile -> new SlotInfo(context.getJobID(), profile))
                    .collect(Collectors.toList());
            return new TaskManagerDetailsInfo(id, "worker", 0, 0, 0, 4, 4 - profiles.size(), total, free,
                    new HardwareDescription(4, 0, 0, 0), memory, false,
                    slots,
                    new TaskManagerMetricsInfo(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            0, 0, 0, 0, 0, 0, List.of()));
        }

        private static Map<ScalingMetric, EvaluatedScalingMetric> overloadedMetrics(int p) {
            return new HashMap<>(Map.of(
                    ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(p),
                    ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(24),
                    ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(0),
                    ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0),
                    ScalingMetric.TARGET_DATA_RATE, EvaluatedScalingMetric.avg(23_233),
                    ScalingMetric.TRUE_PROCESSING_RATE, EvaluatedScalingMetric.avg(23_233),
                    ScalingMetric.SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(50_000),
                    ScalingMetric.SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(80_000)));
        }

        private static ResourceProfile resourceProfile(int managedMiB) {
            return ResourceProfile.newBuilder().setCpuCores(1).setTaskHeapMemoryMB(134)
                    .setTaskOffHeapMemoryMB(0).setManagedMemoryMB(managedMiB)
                    .setNetworkMemoryMB(39).build();
        }
    }
}
