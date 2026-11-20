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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
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
        AtomicInteger realizationCalls = new AtomicInteger();
        var recordingRealizer = new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
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
}
