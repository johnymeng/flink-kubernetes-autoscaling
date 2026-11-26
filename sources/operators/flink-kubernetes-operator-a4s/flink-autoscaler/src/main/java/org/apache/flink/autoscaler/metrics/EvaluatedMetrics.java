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

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.autoscaler.utils.a4s.A4SAggregatedMetricsResponseBody.CacheCounters;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.time.Instant;
import java.util.Map;

/** Collected scaling metrics. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EvaluatedMetrics {
    private Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics;
    private Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics;
    private Map<JobVertexID, MissRateCurve> missRateCurves = Map.of();
    private Instant windowStart;
    private Instant windowEnd;
    private Instant jobRunningSince;
    private Map<JobVertexID, Long> observedSourceBacklogs = Map.of();
    private Map<JobVertexID, Long> observedBlockCacheCapacities = Map.of();
    private Map<JobVertexID, Long> observedBlockCacheUsage = Map.of();
    private Map<JobVertexID, CacheCounters> observedBlockCacheCounters = Map.of();
    private Map<JobVertexID, Long> observedStateSizes = Map.of();
    private Map<JobVertexID, TaskTimeFractions> taskTimeFractions = Map.of();

    /** Fractions of the classified task time over the metrics window. */
    @Value
    public static class TaskTimeFractions {
        double busy;
        double idle;
        double backpressured;
    }

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics) {
        this(vertexMetrics, globalMetrics, Map.of());
    }

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics,
            Map<JobVertexID, MissRateCurve> missRateCurves) {
        this(vertexMetrics, globalMetrics, missRateCurves, null, null, null);
    }

    public EvaluatedMetrics(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics,
            Map<JobVertexID, MissRateCurve> missRateCurves,
            Instant windowStart,
            Instant windowEnd,
            Instant jobRunningSince) {
        this(
                vertexMetrics,
                globalMetrics,
                missRateCurves,
                windowStart,
                windowEnd,
                jobRunningSince,
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of());
    }
}
