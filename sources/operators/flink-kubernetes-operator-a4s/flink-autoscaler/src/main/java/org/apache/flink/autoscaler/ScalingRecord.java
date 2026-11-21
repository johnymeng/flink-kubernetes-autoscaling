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

import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheAction;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Class for tracking scaling details, including time it took for the job to transition to the
 * target parallelism.
 */
@Data
@NoArgsConstructor
public class ScalingRecord {
    private Duration restartDuration;

    // Nullable fields keep the existing history format readable.
    private String decisionId;
    private CacheAction requestMode;
    private Map<String, String> startingParallelism;
    private Map<String, String> requestedParallelism;
    private Map<String, String> requestedProfiles;
    private Map<String, String> configOverrides;
    private Set<String> configRemovals;
    private Boolean realizerReturned;

    public ScalingRecord(Duration restartDuration) {
        this.restartDuration = restartDuration;
    }

    static ScalingRecord requested(FinalScalingAction action) {
        var record = new ScalingRecord();
        record.decisionId = action.getDecisionId();
        record.requestMode = action.getMode();
        record.startingParallelism = action.getStartingParallelism();
        record.requestedParallelism = action.getParallelismOverrides();
        record.requestedProfiles = action.getResourceProfileOverrides();
        record.configOverrides = action.getConfigOverrides();
        record.configRemovals = action.getConfigRemovals();
        record.realizerReturned = false;
        return record;
    }

    boolean matchesRequest(FinalScalingAction action) {
        return decisionId != null && requestMode == action.getMode()
                && action.getStartingParallelism().equals(startingParallelism)
                && action.getParallelismOverrides().equals(requestedParallelism)
                && action.getResourceProfileOverrides().equals(requestedProfiles)
                && action.getConfigOverrides().equals(configOverrides)
                && action.getConfigRemovals().equals(configRemovals);
    }
}
