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
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/** Lazily evaluates the minimum measured operator-wide cache required at each parallelism. */
public final class MemoryParallelismCurve {
    private final List<CachePoint> points;
    private final PerformanceModel model;
    private final OperatorMetrics metrics;

    public MemoryParallelismCurve(OperatorMetrics metrics, PerformanceModel model) {
        this.points =
                metrics.histogram == null
                        ? metrics.missRateCurve
                        : MissRateCurves.fromHistogram(metrics.histogram);
        this.metrics = metrics;
        this.model = model;
    }

    public boolean hasMeasurements() {
        return !points.isEmpty();
    }

    public OptionalLong minimumCacheBytes(int parallelism) {
        return model.minimumCacheBytes(metrics, parallelism, points);
    }

    /** Forecast the final cache allocation within the observed operator-wide capacity range. */
    public OptionalDouble predict(OperatorConfiguration candidate) {
        Long capacity = candidate.totalCacheBytes();
        if (capacity == null
                || !hasMeasurements()
                || capacity < points.get(0).capacityBytes
                || capacity > points.get(points.size() - 1).capacityBytes) {
            return OptionalDouble.empty();
        }
        return model.predict(metrics, candidate);
    }
}
