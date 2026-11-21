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

package org.apache.flink.autoscaler.utils;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.CATCH_UP_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.REQUIRED_PROCESSING_CAPACITY;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;

/** AutoScaler utilities. */
public class AutoScalerUtils {

    /** Terms of the ordinary throughput sizing budget, in records per second. */
    @Value
    public static class CapacityBudget {
        double arrivalsRecordsPerSecond;
        double atTargetUtilizationRecordsPerSecond;
        double lagDrainRecordsPerSecond;
        double restartBacklogRecordsPerSecond;
        double requiredRecordsPerSecond;
    }

    public static double getRequiredProcessingCapacity(
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Configuration conf,
            Duration restartTime) {
        var budget = evaluatedMetrics.get(REQUIRED_PROCESSING_CAPACITY);
        if (budget != null) {
            return budget.getCurrent();
        }
        // Callers with metrics created outside the evaluator still use the ordinary formula.
        return getTargetProcessingCapacity(
                evaluatedMetrics, conf, conf.get(TARGET_UTILIZATION), true, restartTime);
    }

    public static double getTargetProcessingCapacity(
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Configuration conf,
            double targetUtilization,
            boolean withRestart,
            Duration restartTime) {
        return calculateCapacityBudget(
                        evaluatedMetrics, conf, targetUtilization, withRestart, restartTime)
                .getRequiredRecordsPerSecond();
    }

    public static CapacityBudget calculateCapacityBudget(
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Configuration conf,
            double targetUtilization,
            boolean withRestart,
            Duration restartTime) {
        double lagDrain = evaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent();
        if (Double.isNaN(lagDrain)) {
            return new CapacityBudget(Double.NaN, Double.NaN, lagDrain, Double.NaN, Double.NaN);
        }

        double arrivals = evaluatedMetrics.get(TARGET_DATA_RATE).getAverage();
        if (Double.isNaN(arrivals)) {
            return new CapacityBudget(arrivals, Double.NaN, lagDrain, Double.NaN, Double.NaN);
        }

        double catchUpSeconds = conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds();
        targetUtilization = Math.min(1., Math.max(0., targetUtilization));
        double restartBacklog =
                !withRestart || catchUpSeconds == 0
                        ? 0
                        : (arrivals * restartTime.toSeconds()) / catchUpSeconds;
        if (targetUtilization == 0) {
            return new CapacityBudget(
                    arrivals, Double.POSITIVE_INFINITY, lagDrain, restartBacklog,
                    Double.POSITIVE_INFINITY);
        }

        double atTargetUtilization = arrivals / targetUtilization;
        double required = Math.round(lagDrain + restartBacklog + atTargetUtilization);
        return new CapacityBudget(
                arrivals, atTargetUtilization, lagDrain, restartBacklog, required);
    }

    /**
     * Temporarily exclude vertex from scaling for this run. This does not update the
     * scalingRealizer.
     */
    public static boolean excludeVertexFromScaling(Configuration conf, JobVertexID jobVertexId) {
        return excludeVerticesFromScaling(conf, List.of(jobVertexId));
    }

    public static boolean excludeVerticesFromScaling(
            Configuration conf, Collection<JobVertexID> ids) {
        Set<String> excludedIds = new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));
        boolean anyAdded = false;
        for (JobVertexID id : ids) {
            String hexString = id.toHexString();
            anyAdded |= excludedIds.add(hexString);
        }
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, new ArrayList<>(excludedIds));
        return anyAdded;
    }
}
