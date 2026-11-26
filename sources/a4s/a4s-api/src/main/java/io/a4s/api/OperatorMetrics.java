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

/** Normalized measurements, demand and legal parallelisms for one operator. */
public final class OperatorMetrics {
    public final String operatorId;
    public final String operatorType;
    /** Unknown statefulness requires state-preserving transitions. */
    public final Boolean stateful;

    public final OperatorConfiguration current;
    public final int minParallelism;
    public final int maxParallelism;
    /** Empty means all values in the inclusive range. */
    public final List<Integer> permittedParallelisms;
    /** Initial parallelism proposed by the platform demand policy. */
    public final int startingParallelism;
    /** Whether A4S requires a measured MRC to size this operator. */
    public final boolean cacheSensitive;

    public final Double targetRecordsPerSecond;
    public final List<String> upstreamOperators;
    /** Operator-wide actual native cache capacity in bytes. */
    public final List<CachePoint> missRateCurve;
    /** Operator-wide aggregate or a single task projected across partitions. */
    public final StackDistanceHistogram histogram;

    public final OperatorTelemetry telemetry;

    @ConstructorProperties({
        "operatorId",
        "operatorType",
        "stateful",
        "current",
        "minParallelism",
        "maxParallelism",
        "permittedParallelisms",
        "startingParallelism",
        "cacheSensitive",
        "targetRecordsPerSecond",
        "upstreamOperators",
        "missRateCurve",
        "histogram",
        "telemetry"
    })
    public OperatorMetrics(
            String operatorId,
            String operatorType,
            Boolean stateful,
            OperatorConfiguration current,
            int minParallelism,
            int maxParallelism,
            List<Integer> permittedParallelisms,
            int startingParallelism,
            boolean cacheSensitive,
            Double targetRecordsPerSecond,
            List<String> upstreamOperators,
            List<CachePoint> missRateCurve,
            StackDistanceHistogram histogram,
            OperatorTelemetry telemetry) {
        Checks.identity(operatorId, "operatorId");
        Checks.identity(operatorType, "operatorType");
        Objects.requireNonNull(current, "current");
        Checks.require(
                minParallelism > 0
                        && maxParallelism >= minParallelism
                        && maxParallelism <= 1_048_576,
                "Invalid parallelism range");
        Checks.require(startingParallelism > 0, "Invalid starting parallelism");
        permittedParallelisms = List.copyOf(permittedParallelisms);
        int previous = 0;
        for (int allowed : permittedParallelisms) {
            Checks.require(
                    allowed > previous && allowed >= minParallelism && allowed <= maxParallelism,
                    "Permitted parallelisms must be sorted, unique and inside the range");
            previous = allowed;
        }
        Checks.nonnegative(targetRecordsPerSecond, "targetRecordsPerSecond");
        upstreamOperators = List.copyOf(upstreamOperators);
        missRateCurve = List.copyOf(missRateCurve);
        long previousCapacity = -1;
        double previousMissRate = 1;
        for (CachePoint point : missRateCurve) {
            Checks.require(
                    point.capacityBytes > previousCapacity && point.missRate <= previousMissRate,
                    "Unordered or nonmonotonic miss-rate curve");
            previousCapacity = point.capacityBytes;
            previousMissRate = point.missRate;
        }
        Checks.require(
                !(histogram != null && !missRateCurve.isEmpty()),
                "Supply either the MRC or a histogram");
        this.operatorId = operatorId;
        this.operatorType = operatorType;
        this.stateful = stateful;
        this.current = current;
        this.minParallelism = minParallelism;
        this.maxParallelism = maxParallelism;
        this.permittedParallelisms = permittedParallelisms;
        this.startingParallelism = startingParallelism;
        this.cacheSensitive = cacheSensitive;
        this.targetRecordsPerSecond = targetRecordsPerSecond;
        this.upstreamOperators = upstreamOperators;
        this.missRateCurve = missRateCurve;
        this.histogram = histogram;
        this.telemetry = telemetry;
    }
}
