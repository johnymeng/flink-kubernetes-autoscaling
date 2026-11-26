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
import java.util.Map;

/** Optional measurements for diagnostics and future performance models. */
public final class OperatorTelemetry {
    public final Double inputRecordsPerSecond;
    public final Double outputRecordsPerSecond;
    public final Double processingCapacityRecordsPerSecond;
    public final Double busyFraction;
    public final Double idleFraction;
    public final Double backpressuredFraction;
    public final Double cpuUtilization;
    public final Long allocatedTotalMemoryBytes;
    public final Long allocatedMemoryPerTaskBytes;
    public final Long cacheUsedBytes;
    public final Long stateSizeBytes;
    public final Long cacheHits;
    public final Long cacheMisses;
    public final Long stateAccessNanos;
    public final Long stateAccessCount;
    public final Long sourceBacklogRecords;
    public final Map<String, String> taskPlacement;

    @ConstructorProperties({
        "inputRecordsPerSecond",
        "outputRecordsPerSecond",
        "processingCapacityRecordsPerSecond",
        "busyFraction",
        "idleFraction",
        "backpressuredFraction",
        "cpuUtilization",
        "allocatedTotalMemoryBytes",
        "allocatedMemoryPerTaskBytes",
        "cacheUsedBytes",
        "stateSizeBytes",
        "cacheHits",
        "cacheMisses",
        "stateAccessNanos",
        "stateAccessCount",
        "sourceBacklogRecords",
        "taskPlacement"
    })
    public OperatorTelemetry(
            Double inputRecordsPerSecond,
            Double outputRecordsPerSecond,
            Double processingCapacityRecordsPerSecond,
            Double busyFraction,
            Double idleFraction,
            Double backpressuredFraction,
            Double cpuUtilization,
            Long allocatedTotalMemoryBytes,
            Long allocatedMemoryPerTaskBytes,
            Long cacheUsedBytes,
            Long stateSizeBytes,
            Long cacheHits,
            Long cacheMisses,
            Long stateAccessNanos,
            Long stateAccessCount,
            Long sourceBacklogRecords,
            Map<String, String> taskPlacement) {
        Checks.nonnegative(inputRecordsPerSecond, "inputRecordsPerSecond");
        Checks.nonnegative(outputRecordsPerSecond, "outputRecordsPerSecond");
        Checks.nonnegative(
                processingCapacityRecordsPerSecond, "processingCapacityRecordsPerSecond");
        Checks.fraction(busyFraction, "busyFraction");
        Checks.fraction(idleFraction, "idleFraction");
        Checks.fraction(backpressuredFraction, "backpressuredFraction");
        Checks.fraction(cpuUtilization, "cpuUtilization");
        Checks.nonnegative(allocatedTotalMemoryBytes, "allocatedTotalMemoryBytes");
        Checks.nonnegative(allocatedMemoryPerTaskBytes, "allocatedMemoryPerTaskBytes");
        Checks.nonnegative(cacheUsedBytes, "cacheUsedBytes");
        Checks.nonnegative(stateSizeBytes, "stateSizeBytes");
        Checks.nonnegative(cacheHits, "cacheHits");
        Checks.nonnegative(cacheMisses, "cacheMisses");
        Checks.nonnegative(stateAccessNanos, "stateAccessNanos");
        Checks.nonnegative(stateAccessCount, "stateAccessCount");
        taskPlacement = taskPlacement == null ? Map.of() : Map.copyOf(taskPlacement);
        this.inputRecordsPerSecond = inputRecordsPerSecond;
        this.outputRecordsPerSecond = outputRecordsPerSecond;
        this.processingCapacityRecordsPerSecond = processingCapacityRecordsPerSecond;
        this.busyFraction = busyFraction;
        this.idleFraction = idleFraction;
        this.backpressuredFraction = backpressuredFraction;
        this.cpuUtilization = cpuUtilization;
        this.allocatedTotalMemoryBytes = allocatedTotalMemoryBytes;
        this.allocatedMemoryPerTaskBytes = allocatedMemoryPerTaskBytes;
        this.cacheUsedBytes = cacheUsedBytes;
        this.stateSizeBytes = stateSizeBytes;
        this.cacheHits = cacheHits;
        this.cacheMisses = cacheMisses;
        this.stateAccessNanos = stateAccessNanos;
        this.stateAccessCount = stateAccessCount;
        this.sourceBacklogRecords = sourceBacklogRecords;
        this.taskPlacement = taskPlacement;
    }
}
