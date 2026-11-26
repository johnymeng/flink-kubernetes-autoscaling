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

/** Policy bounds. Allocations are in bytes and throughput is in records per second. */
public final class ScalingConstraints {
    public final long minCachePerTaskBytes;
    public final long maxCachePerTaskBytes;
    public final Long totalCacheBudgetBytes;
    public final Long maxManagedMemoryPerTaskBytes;
    public final Double cpuBudgetCores;
    public final long cooldownMillis;
    public final long maxSnapshotAgeMillis;
    public final Long maxDowntimeMillis;
    public final Double latencyObjectiveMillis;
    public final Long backlogRecoveryMillis;
    public final boolean requiresPlacement;

    @ConstructorProperties({
        "minCachePerTaskBytes",
        "maxCachePerTaskBytes",
        "totalCacheBudgetBytes",
        "maxManagedMemoryPerTaskBytes",
        "cpuBudgetCores",
        "cooldownMillis",
        "maxSnapshotAgeMillis",
        "maxDowntimeMillis",
        "latencyObjectiveMillis",
        "backlogRecoveryMillis",
        "requiresPlacement"
    })
    public ScalingConstraints(
            long minCachePerTaskBytes,
            long maxCachePerTaskBytes,
            Long totalCacheBudgetBytes,
            Long maxManagedMemoryPerTaskBytes,
            Double cpuBudgetCores,
            long cooldownMillis,
            long maxSnapshotAgeMillis,
            Long maxDowntimeMillis,
            Double latencyObjectiveMillis,
            Long backlogRecoveryMillis,
            boolean requiresPlacement) {
        Checks.require(
                minCachePerTaskBytes >= 0 && maxCachePerTaskBytes >= minCachePerTaskBytes,
                "Invalid cache bounds");
        Checks.nonnegative(totalCacheBudgetBytes, "totalCacheBudgetBytes");
        Checks.nonnegative(maxManagedMemoryPerTaskBytes, "maxManagedMemoryPerTaskBytes");
        Checks.nonnegative(cpuBudgetCores, "cpuBudgetCores");
        Checks.nonnegative(cooldownMillis, "cooldownMillis");
        Checks.require(maxSnapshotAgeMillis > 0, "maxSnapshotAgeMillis must be positive");
        Checks.nonnegative(maxDowntimeMillis, "maxDowntimeMillis");
        Checks.nonnegative(latencyObjectiveMillis, "latencyObjectiveMillis");
        Checks.nonnegative(backlogRecoveryMillis, "backlogRecoveryMillis");
        this.minCachePerTaskBytes = minCachePerTaskBytes;
        this.maxCachePerTaskBytes = maxCachePerTaskBytes;
        this.totalCacheBudgetBytes = totalCacheBudgetBytes;
        this.maxManagedMemoryPerTaskBytes = maxManagedMemoryPerTaskBytes;
        this.cpuBudgetCores = cpuBudgetCores;
        this.cooldownMillis = cooldownMillis;
        this.maxSnapshotAgeMillis = maxSnapshotAgeMillis;
        this.maxDowntimeMillis = maxDowntimeMillis;
        this.latencyObjectiveMillis = latencyObjectiveMillis;
        this.backlogRecoveryMillis = backlogRecoveryMillis;
        this.requiresPlacement = requiresPlacement;
    }
}
