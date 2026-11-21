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
import org.apache.flink.autoscaler.a4s.MemoryParallelismCurve;
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
import org.apache.flink.autoscaler.tuning.ConfigChanges;
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
import java.util.concurrent.TimeoutException;
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
                        Map.of(vertex, OverloadFixture.balancedMetrics(1)), healthyGlobalMetrics(),
                        Map.of(vertex, MemoryParallelismCurve.builder().targetThroughput(1000)
                                .addPoint(1, 1).build()));
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
    void retainedWriteBudgetIsPerTaskWhenScalingFromOneToFour() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.QUALIFIED);
        replay.useFixedPolicy();
        replay.autoscaler.scale(replay.context);

        assertEquals("4", replay.stateStore.getParallelismOverrides(replay.context)
                .get(replay.join.toString()));
        var profile = replay.stateStore.getResourceProfileOverrides(replay.context)
                .get(replay.join.toString());
        assertTrue(profile.contains("managedMemory=192.000mb"), profile);
        var budget = replay.evaluation().getCacheEvaluation().getManagedMemoryDecisions().get(replay.join);
        assertEquals(192L << 20, budget.getTrustedCurrentManagedBytes().longValue());
        assertEquals(192L << 20, budget.getRetainedManagedBytes().longValue());
        assertEquals(192L << 20, budget.getFinalManagedBytes().longValue());
        assertEquals(160L << 20, budget.getNominalNativeCacheBytes().longValue());
        assertEquals(64L << 20, budget.getNominalWriteBufferManagerBytes().longValue());
        assertNull(budget.getReservedOrPinnedBytes());
    }

    @Test
    void fixedPolicyCanUseAnExplicitLowerFloorAndRoundsUp() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.QUALIFIED);
        replay.useFixedPolicy();
        replay.setManagedFloor("96.1");
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.requests.get());
        assertEquals(0, replay.profileReads.get());
        var budget = replay.evaluation().getCacheEvaluation().getManagedMemoryDecisions().get(replay.join);
        assertEquals((long) Math.ceil(96.1 * (1L << 20)), budget.getConfiguredFloorBytes().longValue());
        assertNull(budget.getRetainedManagedBytes());
        assertEquals(97L << 20, budget.getFinalManagedBytes().longValue());
        assertTrue(replay.stateStore.getResourceProfileOverrides(replay.context)
                .get(replay.join.toString()).contains("managedMemory=97.000mb"));
    }

    @Test
    void configuredFloorAllowsThroughputRecoveryWithoutNativeCorroboration() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.MISSING_OBSERVATIONS);
        replay.observationAvailable = false;
        replay.curveAvailable = false;
        replay.setManagedFloor("96.1");
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.requests.get());
        var evaluation = replay.evaluation().getCacheEvaluation();
        assertEquals(CacheAction.THROUGHPUT_SCALE_UP_RETAINED_PROFILES, evaluation.getAction());
        var budget = evaluation.getManagedMemoryDecisions().get(replay.join);
        assertNull(budget.getTrustedCurrentManagedBytes());
        assertEquals(97L << 20, budget.getFinalManagedBytes().longValue());
        assertTrue(replay.stateStore.getResourceProfileOverrides(replay.context)
                .get(replay.join.toString()).contains("managedMemory=97.000mb"));
    }

    @Test
    void fixedAndOnlinePoliciesHoldOnAnOverrideBelowTheExactFloor() throws Exception {
        for (boolean online : List.of(false, true)) {
            var replay = new OverloadFixture(160L << 20, Reason.QUALIFIED);
            if (!online) {
                replay.useFixedPolicy();
            }
            replay.setManagedFloor("96.1");
            var conf = replay.context.getConfiguration();
            conf.set(AutoScalerOptions.A4S_MANAGED_MEMORY_OVERRIDE_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_MANAGED_MEMORY_OVERRIDE_MB, 96);
            replay.assertNoRequest();

            var evaluation = replay.evaluation().getCacheEvaluation();
            assertEquals(CacheAction.POLICY_HOLD, evaluation.getAction());
            assertEquals(Reason.MEMORY_OVERRIDE_BELOW_FLOOR, evaluation.getHoldReason());
            var budget = evaluation.getManagedMemoryDecisions().get(replay.join);
            assertEquals(96L << 20, budget.getRequestedManagedBytes().longValue());
            assertNull(budget.getFinalManagedBytes());
        }
    }

    @Test
    void fixedPolicyHoldsWhenTheReportedProfileIsUncorroborated() throws Exception {
        var replay = new OverloadFixture(159L << 20, Reason.QUALIFIED);
        replay.useFixedPolicy();
        replay.assertNoRequest();

        var evaluation = replay.evaluation().getCacheEvaluation();
        var budget = evaluation.getManagedMemoryDecisions().get(replay.join);
        assertEquals(CacheAction.POLICY_HOLD, evaluation.getAction());
        assertEquals(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED, evaluation.getHoldReason());
        assertNull(budget.getTrustedCurrentManagedBytes());
        assertNull(budget.getFinalManagedBytes());
    }

    @Test
    void safeWriteBudgetHoldsWhenItExceedsTheManagerManagedMemoryPool() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        replay.context.getConfiguration().set(
                TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(128));
        replay.assertNoRequest();

        var evaluation = replay.evaluation().getCacheEvaluation();
        assertEquals(Reason.MEMORY_BUDGET_INFEASIBLE, evaluation.getHoldReason());
        assertEquals(192L << 20, evaluation.getManagedMemoryDecisions()
                .get(replay.join).getFinalManagedBytes().longValue());
    }

    @Test
    void safeWriteBudgetHoldsWhenItExceedsTheDerivedManagedMemoryPool() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.QUALIFIED);
        replay.useFixedPolicy();
        replay.context.getConfiguration().set(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.2f);
        replay.assertNoRequest();

        var evaluation = replay.evaluation().getCacheEvaluation();
        assertEquals(Reason.MEMORY_BUDGET_INFEASIBLE, evaluation.getHoldReason());
        assertEquals(192L << 20, evaluation.getManagedMemoryDecisions()
                .get(replay.join).getFinalManagedBytes().longValue());
    }

    @Test
    void nominalNativeCapacitiesFollowTheRuntimeWriteBufferRatio() {
        var vertex = new JobVertexID();
        var config = new Configuration();
        for (long managedMiB : List.of(32L, 192L)) {
            long managed = managedMiB << 20;
            config.setString(AutoScalerOptions.A4S_MEMORY_FLOOR_PREFIX + vertex.toHexString(),
                    Long.toString(managedMiB));
            var decision = ScalingExecutor.resolveManagedMemory(config, vertex, true, 10.0, null, false);
            assertEquals(managed, decision.getFinalManagedBytes().longValue());
            assertEquals(5 * managed / 6, decision.getNominalNativeCacheBytes().longValue());
            assertEquals(managed / 3, decision.getNominalWriteBufferManagerBytes().longValue());
            assertNull(decision.getReservedOrPinnedBytes());
        }
    }

    @Test
    void distinctProposalsCanResolveToTheSameFloor() {
        var vertex = new JobVertexID();
        var config = new Configuration();
        config.setString(AutoScalerOptions.A4S_MEMORY_FLOOR_PREFIX + vertex.toHexString(), "128");
        var smaller = ScalingExecutor.resolveManagedMemory(config, vertex, true, 64.0, null, false);
        var larger = ScalingExecutor.resolveManagedMemory(config, vertex, true, 96.0, null, false);
        assertEquals(64L << 20, smaller.getUnconstrainedManagedBytes().longValue());
        assertEquals(96L << 20, larger.getUnconstrainedManagedBytes().longValue());
        assertEquals(128L << 20, smaller.getFloorLimitedManagedBytes().longValue());
        assertEquals(smaller.getFinalManagedBytes(), larger.getFinalManagedBytes());
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
    void unsupportedProjectionAndMissingTimingDoNotVetoOrdinaryThroughputScaleUp() throws Exception {
        for (var reason : List.of(Reason.CAPACITY_UNSUPPORTED, Reason.MISSING_TIMING)) {
            var replay = new OverloadFixture(160L << 20, reason);
            replay.autoscaler.scale(replay.context);

            assertEquals(1, replay.requests.get(), reason.name());
            assertEquals(1, replay.profileReads.get());
            assertTrue(Integer.parseInt(replay.stateStore.getParallelismOverrides(replay.context)
                    .get(replay.join.toString())) > 1);
            assertEquals(reason, replay.evaluation().getCacheCostFailures().get(replay.join));
            assertEquals(CacheAction.THROUGHPUT_SCALE_UP_RETAINED_PROFILES,
                    replay.evaluation().getCacheEvaluation().getAction());
        }
    }

    @Test
    void aQualifiedSameParallelismCacheProjectionDoesNotVetoThroughputRecovery() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.QUALIFIED);
        replay.autoscaler.scale(replay.context);

        assertTrue(replay.evaluation().getMemoryParallelismCurves().containsKey(replay.join));
        assertEquals(1, replay.requests.get());
        assertEquals(CacheAction.THROUGHPUT_SCALE_UP_RETAINED_PROFILES,
                replay.evaluation().getCacheEvaluation().getAction());
    }

    @Test
    void missingCurveCanRecoverUsingASeparateCurrentCapacityObservation() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.MISSING_OBSERVATIONS);
        replay.curveAvailable = false;
        replay.autoscaler.scale(replay.context);

        assertEquals(CacheObservation.Status.INCOMPLETE,
                replay.evaluation().getCacheEvaluation().getObservationHolds().get(replay.join));
        assertEquals(1, replay.requests.get());
    }

    @Test
    void rateSkewWithValidIndividualCapacityDoesNotBlockRecovery() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.MISSING_OBSERVATIONS);
        replay.batchStatus = CacheObservation.Status.AGGREGATE_RATE_SKEW;
        replay.curveAvailable = false;
        replay.autoscaler.scale(replay.context);

        assertEquals(CacheObservation.Status.INCOMPLETE,
                replay.evaluation().getCacheEvaluation().getObservationHolds().get(replay.join));
        assertEquals(1, replay.requests.get());
    }

    @Test
    void throughputRecoveryDoesNotRunOrClearMemoryTuningInShadowMode() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.MISSING_OBSERVATIONS);
        replay.curveAvailable = false;
        var conf = replay.context.getConfiguration();
        conf.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_SHADOW, true);
        conf.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
        var stored = new ConfigChanges().addOverride(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        replay.stateStore.storeConfigChanges(replay.context, stored);

        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.requests.get());
        assertEquals(0, replay.configRealizations.get());
        assertEquals(stored.getOverrides(), replay.stateStore.getConfigChanges(replay.context).getOverrides());
    }

    @Test
    void invalidThroughputDataAndUncollectedWindowsHold() throws Exception {
        for (double invalid : List.of(Double.NaN, Double.POSITIVE_INFINITY, 0., -1.)) {
            var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
            replay.joinMetrics.put(ScalingMetric.TRUE_PROCESSING_RATE, EvaluatedScalingMetric.avg(invalid));
            replay.assertNoRequest();
            assertEquals(0, replay.profileReads.get());
        }

        var invalidDemand = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        invalidDemand.joinMetrics.put(ScalingMetric.TARGET_DATA_RATE,
                EvaluatedScalingMetric.avg(Double.POSITIVE_INFINITY));
        invalidDemand.assertNoRequest();
        assertEquals(0, invalidDemand.profileReads.get());

        var warming = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        warming.fullyCollected = false;
        warming.assertNoRequest();
        assertEquals(0, warming.profileReads.get());
        assertEquals(Reason.INSUFFICIENT_HISTORY, warming.evaluation().getCacheEvaluation().getHoldReason());
    }

    @Test
    void recoveryRespectsExecutionExclusionsAndParallelismLimits() throws Exception {
        var disabled = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        disabled.context.getConfiguration().set(AutoScalerOptions.SCALING_ENABLED, false);
        disabled.assertNoRequest();

        var excludedTime = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        excludedTime.context.getConfiguration().set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * * * * ?"));
        excludedTime.assertNoRequest();

        var excluded = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        excluded.context.getConfiguration()
                .set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(excluded.join.toHexString()));
        excluded.assertNoRequest();

        var configuredLimit = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        configuredLimit.context.getConfiguration().set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 1);
        configuredLimit.assertNoRequest();

        var vertexLimit = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        vertexLimit.joinMetrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(1));
        vertexLimit.assertNoRequest();

        var sourceLimit = new OverloadFixture(160L << 20, Reason.MISSING_TIMING, 2);
        sourceLimit.joinMetrics.putAll(OverloadFixture.balancedMetrics(1));
        sourceLimit.sourceMetrics.putAll(OverloadFixture.overloadedMetrics(2));
        sourceLimit.sourceMetrics.put(ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(2));
        sourceLimit.assertNoRequest();
    }

    @Test
    void recoveryReusesOrdinaryScaleFactorAndSourcePartitionRules() throws Exception {
        var factorLimit = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        factorLimit.context.getConfiguration().set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 1.);
        factorLimit.autoscaler.scale(factorLimit.context);
        assertEquals("2", factorLimit.stateStore.getParallelismOverrides(factorLimit.context)
                .get(factorLimit.join.toString()));

        var sourceLimit = new OverloadFixture(160L << 20, Reason.MISSING_TIMING, 2);
        sourceLimit.joinMetrics.putAll(OverloadFixture.balancedMetrics(1));
        sourceLimit.sourceMetrics.putAll(OverloadFixture.overloadedMetrics(2));
        sourceLimit.sourceMetrics.put(ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(4));
        sourceLimit.autoscaler.scale(sourceLimit.context);
        assertEquals(Map.of(sourceLimit.join.toString(), "1", sourceLimit.source.toString(), "4",
                        sourceLimit.sink.toString(), "2"),
                sourceLimit.stateStore.getParallelismOverrides(sourceLimit.context));
    }

    @Test
    void missingProfileOrNativeCorroborationProducesSpecificMemoryHold() throws Exception {
        var withoutProfile = new OverloadFixture(160L << 20, Reason.MISSING_TIMING);
        withoutProfile.profileUnavailable = true;
        withoutProfile.assertNoRequest();
        assertEquals(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED,
                withoutProfile.evaluation().getCacheEvaluation().getHoldReason());

        var withoutNativeProof = new OverloadFixture(160L << 20, Reason.MISSING_OBSERVATIONS);
        withoutNativeProof.observationAvailable = false;
        withoutNativeProof.curveAvailable = false;
        withoutNativeProof.assertNoRequest();
        assertEquals(1, withoutNativeProof.profileReads.get());
        assertEquals(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED,
                withoutNativeProof.evaluation().getCacheEvaluation().getHoldReason());
    }

    @Test
    void recoveryVerifiesReappliedProfilesEvenForUnchangedParallelism() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.MISSING_TIMING, 2);
        replay.context.getConfiguration().set(AutoScalerOptions.A4S_CACHE_METRICS_EXEMPT_VERTICES, List.of(replay.sink.toString()));
        replay.sourceCacheBytes = 130L << 20;
        replay.assertNoRequest();
        assertEquals(Reason.OVERLOAD_LIVE_PROFILE_UNVERIFIED,
                replay.evaluation().getCacheEvaluation().getHoldReason());

        // The unchanged source's 158 MiB profile must also match its measured caches.
        replay.sourceCacheBytes = (long) ((3 - .5) * (158L << 20) / 3);
        replay.autoscaler.scale(replay.context);
        assertEquals(1, replay.requests.get());
        assertEquals("2", replay.stateStore.getParallelismOverrides(replay.context)
                .get(replay.source.toString()));
    }

    @Test
    void observedTopologyTransitionDropsThePreviousEvaluationBeforeWarmup() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.QUALIFIED);
        replay.autoscaler.scale(replay.context);
        assertEquals(1, replay.evaluation().getVertexMetrics().get(replay.join)
                .get(ScalingMetric.PARALLELISM).getCurrent());
        assertTrue(replay.evaluation().getMemoryParallelismCurves().containsKey(replay.join));

        replay.observedJoinParallelism = 4;
        replay.fullyCollected = false;
        replay.observationAvailable = false;
        replay.curveAvailable = false;
        replay.autoscaler.scale(replay.context);

        assertNull(replay.evaluation());
        assertEquals(1, replay.requests.get());
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
        private final AtomicInteger configRealizations = new AtomicInteger();
        private final Map<ScalingMetric, EvaluatedScalingMetric> joinMetrics = overloadedMetrics(1);
        private final Map<ScalingMetric, EvaluatedScalingMetric> sourceMetrics;
        private final JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;
        private boolean fullyCollected = true;
        private boolean observationAvailable = true;
        private boolean curveAvailable = true;
        private CacheObservation.Status batchStatus = CacheObservation.Status.VALID;
        private boolean profileUnavailable;
        private long sourceCacheBytes;
        private int observedJoinParallelism = 1;

        private OverloadFixture(long nativeCacheBytes, Reason reason) {
            this(nativeCacheBytes, reason, 1);
        }

        private OverloadFixture(long nativeCacheBytes, Reason reason, int edgeParallelism) {
            sourceMetrics = balancedMetrics(edgeParallelism);
            if (edgeParallelism == 2) {
                workers = Optional.of(observedWorkerSnapshot(
                        "embedded", "flink-taskmanager-1-1", 570215624L));
            }
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
                    var batches = new HashMap<JobVertexID, CacheObservationBatch>();
                    if (observationAvailable) {
                        batches.put(join, new CacheObservationBatch(
                                CacheObservation.VERSION, batchStatus,
                                1, 32, 0, List.of(observation)));
                    }
                    if (sourceCacheBytes > 0) {
                        batches.put(source, new CacheObservationBatch(
                                CacheObservation.VERSION, CacheObservation.Status.VALID, 2, 64, 0,
                                List.of(sourceObservation(ctx, 0), sourceObservation(ctx, 1))));
                    }
                    latest.setCacheObservations(batches);
                    if (curveAvailable) {
                        latest.setMissRateCurves(Map.of(join, new MissRateCurve(List.of(
                                new MissRateCurve.Point(nativeCacheBytes, .25)))));
                    }
                    var observedTopology = new JobTopology(new VertexInfo(source, Map.of(), edgeParallelism, 24),
                            new VertexInfo(join, Map.of(source, ShipStrategy.HASH),
                                    observedJoinParallelism, 24),
                            new VertexInfo(sink, Map.of(join, ShipStrategy.HASH), edgeParallelism, 24));
                    var history = new CollectedMetricHistory(observedTopology, new TreeMap<>(Map.of(
                            Instant.EPOCH, new CollectedMetrics(), Instant.now(), latest)), Instant.EPOCH);
                    history.setFullyCollected(fullyCollected);
                    return history;
                }

                private CacheObservation sourceObservation(JobAutoScalerContext<JobID> ctx, int subtask) {
                    return new CacheObservation(CacheObservation.VERSION, ctx.getJobID().toString(),
                            source.toString(), subtask, "source-attempt-" + subtask, CacheObservation.Status.VALID,
                            new CacheObservation.Window("process", 2 + subtask, 0, 2, List.of(1L),
                                    CacheObservation.QUICK_MRC, CacheObservation.FIXED_4K,
                                    CacheObservation.CACHE_POLICY, .02, sourceCacheBytes,
                                    1, 10_000_000_001L, System.currentTimeMillis(), 1000, 250), null);
                }
            };
            var evaluator = new ScalingMetricEvaluator() {
                @Override
                public EvaluatedMetrics evaluate(
                        org.apache.flink.configuration.Configuration cfg,
                        CollectedMetricHistory metrics, Duration restartTime) {
                    var curves = reason == Reason.QUALIFIED
                            ? Map.of(join, MemoryParallelismCurve.builder().targetThroughput(30_000)
                                    .addPoint(1, 160).build())
                            : Map.<JobVertexID, MemoryParallelismCurve>of();
                    var globalMetrics = new HashMap<>(healthyGlobalMetrics());
                    globalMetrics.put(ScalingMetric.NUM_TASK_SLOTS_USED, EvaluatedScalingMetric.of(2 * edgeParallelism + 1));
                    var evaluated = new EvaluatedMetrics(
                            Map.of(join, joinMetrics, source, sourceMetrics,
                                    sink, balancedMetrics(edgeParallelism)),
                            globalMetrics, curves, Map.of(),
                            reason == Reason.QUALIFIED ? Map.of() : Map.of(join, reason));
                    var latest = metrics.getMetricHistory().get(metrics.getMetricHistory().lastKey());
                    evaluated.setCacheObservations(latest.getCacheObservations());
                    return evaluated;
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
                        JobAutoScalerContext<JobID> ctx) throws TimeoutException {
                    profileReads.incrementAndGet();
                    if (profileUnavailable) {
                        throw new TimeoutException();
                    }
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

                @Override
                public void realizeConfigOverrides(JobAutoScalerContext<JobID> ctx, ConfigChanges changes) {
                    configRealizations.incrementAndGet();
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

        private void useFixedPolicy() {
            var conf = context.getConfiguration();
            conf.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, false);
            conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        }

        private void setManagedFloor(String mib) {
            context.getConfiguration().setString(
                    AutoScalerOptions.A4S_MEMORY_FLOOR_PREFIX + join.toHexString(), mib);
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

        private static Map<ScalingMetric, EvaluatedScalingMetric> balancedMetrics(int parallelism) {
            var metrics = overloadedMetrics(parallelism);
            metrics.put(ScalingMetric.SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(20_000));
            return metrics;
        }

        private EvaluatedMetrics evaluation() {
            return autoscaler.lastEvaluatedMetrics.get(context.getJobKey());
        }

        private void assertNoRequest() throws Exception {
            autoscaler.scale(context);
            assertEquals(0, requests.get());
            assertTrue(stateStore.getParallelismOverrides(context).isEmpty());
            assertTrue(stateStore.getResourceProfileOverrides(context).isEmpty());
        }

        private static ResourceProfile resourceProfile(int managedMiB) {
            return ResourceProfile.newBuilder().setCpuCores(1).setTaskHeapMemoryMB(134)
                    .setTaskOffHeapMemoryMB(0).setManagedMemoryMB(managedMiB)
                    .setNetworkMemoryMB(39).build();
        }
    }

    private static Map<ScalingMetric, EvaluatedScalingMetric> healthyGlobalMetrics() {
        return Map.of(ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(0),
                ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.avg(.4),
                ScalingMetric.HEAP_MEMORY_USED, EvaluatedScalingMetric.avg(128L << 20),
                ScalingMetric.MANAGED_MEMORY_USED, EvaluatedScalingMetric.avg(192L << 20),
                ScalingMetric.METASPACE_MEMORY_USED, EvaluatedScalingMetric.avg(128L << 20),
                ScalingMetric.NUM_TASK_SLOTS_USED, EvaluatedScalingMetric.of(3));
    }
}
