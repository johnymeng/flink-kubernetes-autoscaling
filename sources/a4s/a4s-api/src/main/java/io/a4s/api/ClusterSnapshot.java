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
import java.util.List;
import java.util.Objects;

/** Self-contained measurement window with operators in topological order. */
public final class ClusterSnapshot {
    public final String jobId;
    public final String snapshotId;
    public final String executionGeneration;
    public final long measuredAtMillis;
    public final long windowStartMillis;
    public final long windowEndMillis;
    public final boolean metricsComplete;
    public final List<OperatorMetrics> operators;
    public final List<Worker> workers;
    public final String stateBackend;
    public final Long lastReconfigurationMillis;
    public final boolean reconfigurationInProgress;
    public final PlatformCapabilities capabilities;

    @ConstructorProperties({
        "jobId",
        "snapshotId",
        "executionGeneration",
        "measuredAtMillis",
        "windowStartMillis",
        "windowEndMillis",
        "metricsComplete",
        "operators",
        "workers",
        "stateBackend",
        "lastReconfigurationMillis",
        "reconfigurationInProgress",
        "capabilities"
    })
    public ClusterSnapshot(
            String jobId,
            String snapshotId,
            String executionGeneration,
            long measuredAtMillis,
            long windowStartMillis,
            long windowEndMillis,
            boolean metricsComplete,
            List<OperatorMetrics> operators,
            List<Worker> workers,
            String stateBackend,
            Long lastReconfigurationMillis,
            boolean reconfigurationInProgress,
            PlatformCapabilities capabilities) {
        Checks.identity(jobId, "jobId");
        Checks.identity(snapshotId, "snapshotId");
        Checks.identity(executionGeneration, "executionGeneration");
        Checks.require(
                windowStartMillis >= 0
                        && windowStartMillis <= windowEndMillis
                        && windowEndMillis <= measuredAtMillis,
                "Invalid snapshot measurement window");
        operators = List.copyOf(operators);
        workers = List.copyOf(workers);
        Checks.require(
                !operators.isEmpty() && operators.size() <= 256, "Expected 1 to 256 operators");
        var seen = new java.util.HashSet<String>();
        for (OperatorMetrics operator : operators) {
            Checks.require(
                    seen.containsAll(operator.upstreamOperators) && seen.add(operator.operatorId),
                    "Invalid operator topology");
        }
        seen.clear();
        for (Worker worker : workers) {
            Checks.require(seen.add(worker.workerId), "Duplicate worker");
        }
        Checks.nonnegative(lastReconfigurationMillis, "lastReconfigurationMillis");
        Objects.requireNonNull(capabilities, "capabilities");
        this.jobId = jobId;
        this.snapshotId = snapshotId;
        this.executionGeneration = executionGeneration;
        this.measuredAtMillis = measuredAtMillis;
        this.windowStartMillis = windowStartMillis;
        this.windowEndMillis = windowEndMillis;
        this.metricsComplete = metricsComplete;
        this.operators = operators;
        this.workers = workers;
        this.stateBackend = stateBackend;
        this.lastReconfigurationMillis = lastReconfigurationMillis;
        this.reconfigurationInProgress = reconfigurationInProgress;
        this.capabilities = capabilities;
    }
}
