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

package io.a4s.api;

import java.beans.ConstructorProperties;
import java.util.Objects;

/** Proposed allocation and modeled capacity for an operator. */
public final class OperatorDecision {
    public final OperatorConfiguration target;
    public final Long targetTotalCacheBytes;
    public final Long modeledMinimumTotalCacheBytes;
    public final Double predictedRecordsPerSecond;
    public final Double predictionUncertainty;

    @ConstructorProperties({
        "target",
        "targetTotalCacheBytes",
        "modeledMinimumTotalCacheBytes",
        "predictedRecordsPerSecond",
        "predictionUncertainty"
    })
    public OperatorDecision(
            OperatorConfiguration target,
            Long targetTotalCacheBytes,
            Long modeledMinimumTotalCacheBytes,
            Double predictedRecordsPerSecond,
            Double predictionUncertainty) {
        Objects.requireNonNull(target, "target");
        Checks.require(
                Objects.equals(targetTotalCacheBytes, target.totalCacheBytes()),
                "Inconsistent per-task and total cache");
        Checks.nonnegative(modeledMinimumTotalCacheBytes, "modeledMinimumTotalCacheBytes");
        Checks.nonnegative(predictedRecordsPerSecond, "predictedRecordsPerSecond");
        Checks.nonnegative(predictionUncertainty, "predictionUncertainty");
        this.target = target;
        this.targetTotalCacheBytes = targetTotalCacheBytes;
        this.modeledMinimumTotalCacheBytes = modeledMinimumTotalCacheBytes;
        this.predictedRecordsPerSecond = predictedRecordsPerSecond;
        this.predictionUncertainty = predictionUncertainty;
    }
}
