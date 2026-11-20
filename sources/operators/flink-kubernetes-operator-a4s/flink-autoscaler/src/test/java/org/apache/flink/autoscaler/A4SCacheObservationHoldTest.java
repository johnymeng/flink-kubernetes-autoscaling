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
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheObservationHoldTest {
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
        var autoscaler = new JobAutoScalerImpl<>(
                collector,
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventCollector, stateStore),
                eventCollector,
                recordingRealizer,
                stateStore);

        autoscaler.scale(context);

        assertEquals(1, preservationCalls.get());
        assertEquals(0, realizationCalls.get());
        assertEquals(Map.of(vertex.toString(), "2"), stateStore.getParallelismOverrides(context));
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
        var executor = new ScalingExecutor<JobID, JobAutoScalerContext<JobID>>(events, stateStore) {
            @Override
            public boolean scaleResource(
                    JobAutoScalerContext<JobID> ctx, EvaluatedMetrics metrics,
                    Map<JobVertexID, SortedMap<Instant, ScalingSummary>> history,
                    ScalingTracking tracking, Instant now, JobTopology jobTopology,
                    DelayedScaleDown delayedScaleDown) {
                requests.incrementAndGet();
                return false;
            }
        };
        var autoscaler = new JobAutoScalerImpl<>(
                collector, evaluator, executor, events, new TestingScalingRealizer<>(), stateStore);

        autoscaler.scale(context);
        assertEquals(0, requests.get());
        config.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_SHADOW, true);
        autoscaler.scale(context);
        assertEquals(1, requests.get());
    }

    @Test
    void overloadScalesOnlyTheProvenBottleneckAndPreservesLivePerSubtaskMemory() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.autoscaler.scale(replay.context);

        assertEquals(Map.of(replay.join.toString(), "4", replay.source.toString(), "2"),
                replay.stateStore.getParallelismOverrides(replay.context));
        var profiles = replay.stateStore.getResourceProfileOverrides(replay.context);
        assertTrue(profiles.get(replay.join.toString()).contains("managedMemory=192.000mb"));
        assertTrue(profiles.get(replay.source.toString()).contains("managedMemory=158.000mb"));
        assertEquals(1, replay.profileReads.get());
        assertEquals(1, replay.requests.get());
    }

    @Test
    void overloadHoldsWhenTheLiveProfileDisagreesWithTheObservedCache() throws Exception {
        var replay = new OverloadFixture(159L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        replay.autoscaler.scale(replay.context);

        assertEquals(1, replay.profileReads.get());
        assertEquals(0, replay.requests.get());
        assertTrue(replay.stateStore.getParallelismOverrides(replay.context).isEmpty());
    }

    @Test
    void overloadQuotaCountsUnchangedVerticesAndRoundsTaskManagersUp() throws Exception {
        var replay = new OverloadFixture(160L << 20, Reason.INSUFFICIENT_CURRENT_PARALLELISM);
        var conf = replay.context.getConfiguration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        conf.set(AutoScalerOptions.MEMORY_QUOTA, MemorySize.ofMebiBytes(1536));
        replay.autoscaler.scale(replay.context);

        assertEquals(0, replay.requests.get());
        assertTrue(replay.stateStore.getParallelismOverrides(replay.context).isEmpty());
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
                TestingAutoscalerUtils.createDefaultJobAutoScalerContext();
        private final InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore =
                new InMemoryAutoScalerStateStore<>();
        private final JobVertexID join = new JobVertexID();
        private final JobVertexID source = new JobVertexID();
        private final AtomicInteger profileReads = new AtomicInteger();
        private final AtomicInteger requests = new AtomicInteger();
        private final JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;

        private OverloadFixture(long nativeCacheBytes, Reason reason) {
            var conf = context.getConfiguration();
            conf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
            conf.set(AutoScalerOptions.SCALING_ENABLED, true);
            conf.set(AutoScalerOptions.JUSTIN_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_ONLINE_CACHE_COSTS_ENABLED, true);
            conf.set(AutoScalerOptions.A4S_CACHE_METRICS_EXEMPT_VERTICES, List.of(source.toString()));
            conf.set(AutoScalerOptions.TARGET_UTILIZATION, .35);
            var topology = new JobTopology(new VertexInfo(source, Map.of(), 2, 24),
                    new VertexInfo(join, Map.of(source, ShipStrategy.HASH), 1, 24));
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
                            Map.of(join, overloadedMetrics(1), source, overloadedMetrics(2)),
                            Map.of(ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(0),
                                    ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.avg(.4)),
                            Map.of(), Map.of(), Map.of(join, reason));
                }
            };
            var events = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
            var executor = new ScalingExecutor<JobID, JobAutoScalerContext<JobID>>(events, stateStore) {
                @Override
                Optional<JustinResourceRequirements> readLiveResourceRequirements(
                        JobAutoScalerContext<JobID> ctx) {
                    profileReads.incrementAndGet();
                    return Optional.of(JustinResourceRequirements.newBuilder()
                            .setParallelismForJobVertex(join, 1, 1, resourceProfile(192))
                            .setParallelismForJobVertex(source, 1, 2, resourceProfile(158))
                            .build());
                }
            };
            var realizer = new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
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
