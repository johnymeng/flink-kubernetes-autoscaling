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

import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.autoscaler.a4s.CacheCostSample;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/** Collected scaling metrics. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CollectedMetrics {
    private Map<JobVertexID, Map<ScalingMetric, Double>> vertexMetrics;
    private Map<ScalingMetric, Double> globalMetrics;
    private Map<JobVertexID, MissRateCurve> missRateCurves;
    private Map<JobVertexID, List<CacheCostSample>> cacheCostSamples;

    private Map<JobVertexID, CacheObservationBatch> cacheObservations;

    public CollectedMetrics(
            Map<JobVertexID, Map<ScalingMetric, Double>> vertexMetrics,
            Map<ScalingMetric, Double> globalMetrics,
            Map<JobVertexID, MissRateCurve> missRateCurves,
            Map<JobVertexID, List<CacheCostSample>> cacheCostSamples) {
        this(vertexMetrics, globalMetrics, missRateCurves, cacheCostSamples, null);
    }

    public CollectedMetrics(
            Map<JobVertexID, Map<ScalingMetric, Double>> vertexMetrics,
            Map<ScalingMetric, Double> globalMetrics,
            Map<JobVertexID, MissRateCurve> missRateCurves) {
        this(vertexMetrics, globalMetrics, missRateCurves, null);
    }

    public CollectedMetrics(
            Map<JobVertexID, Map<ScalingMetric, Double>> vertexMetrics,
            Map<ScalingMetric, Double> globalMetrics) {
        this(vertexMetrics, globalMetrics, null);
    }
}
