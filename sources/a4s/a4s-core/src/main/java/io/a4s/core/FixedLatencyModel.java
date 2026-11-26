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

package io.a4s.core;

import io.a4s.api.CachePoint;
import io.a4s.api.OperatorConfiguration;
import io.a4s.api.OperatorMetrics;
import io.a4s.api.PolicyConfiguration;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/** A4S's existing per-record cost equation with externally supplied latency coefficients. */
public final class FixedLatencyModel implements PerformanceModel {
    private final double hitSeconds;
    private final double missPenaltySeconds;

    public FixedLatencyModel(PolicyConfiguration policy) {
        hitSeconds = policy.hitSeconds;
        missPenaltySeconds = policy.missSeconds - policy.hitSeconds;
    }

    public double maximumMissRate(int parallelism, double recordsPerSecond) {
        return Math.min(1, ((parallelism / recordsPerSecond) - hitSeconds) / missPenaltySeconds);
    }

    @Override
    public OptionalLong minimumCacheBytes(
            OperatorMetrics current, int parallelism, List<CachePoint> measuredCurve) {
        double maximumMissRate = maximumMissRate(parallelism, current.targetRecordsPerSecond);
        return maximumMissRate < 0
                ? OptionalLong.empty()
                : MissRateCurves.supportedCapacity(measuredCurve, maximumMissRate);
    }

    private double predictAtRate(int parallelism, double missRate) {
        return parallelism / (hitSeconds + missRate * missPenaltySeconds);
    }

    @Override
    public OptionalDouble predict(OperatorMetrics current, OperatorConfiguration candidate) {
        if (candidate.cachePerTaskBytes == null) {
            return OptionalDouble.empty();
        }
        var curve =
                current.histogram == null
                        ? current.missRateCurve
                        : MissRateCurves.fromHistogram(current.histogram);
        return predict(current, candidate, curve);
    }

    @Override
    public OptionalDouble predict(
            OperatorMetrics current, OperatorConfiguration candidate, List<CachePoint> measuredCurve) {
        var cache = candidate.totalCacheBytes();
        if (cache == null
                || !current.missRateCurve.isEmpty()
                        && current.capacityAxis != OperatorMetrics.CapacityAxis.MEASURED_CACHE_BYTES) {
            return OptionalDouble.empty();
        }
        var missRate = MissRateCurves.interpolate(measuredCurve, cache);
        if (missRate.isEmpty()) {
            return OptionalDouble.empty();
        }
        double throughput = predictAtRate(candidate.parallelism, missRate.getAsDouble());
        return Double.isFinite(throughput) ? OptionalDouble.of(throughput) : OptionalDouble.empty();
    }
}
