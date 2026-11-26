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

/** Aggregates an explicitly defined access population and interval in byte-distance buckets. */
public final class StackDistanceHistogram {
    public enum Estimator {
        QUICK_MRC,
        SHARDS
    }

    public enum Counters {
        INTERVAL,
        CUMULATIVE
    }

    public final Estimator estimator;
    public final Counters counterKind;
    public final String population;
    public final String cacheScope;
    public final String counterGeneration;
    public final String sampling;
    public final long startMillis;
    public final long endMillis;
    public final int partitions;
    public final List<Long> capacityBytesPerPartition;
    public final List<Long> frequencies;
    public final long missesOutsideBuckets;

    @ConstructorProperties({
        "estimator",
        "counterKind",
        "population",
        "cacheScope",
        "counterGeneration",
        "sampling",
        "startMillis",
        "endMillis",
        "partitions",
        "capacityBytesPerPartition",
        "frequencies",
        "missesOutsideBuckets"
    })
    public StackDistanceHistogram(
            Estimator estimator,
            Counters counterKind,
            String population,
            String cacheScope,
            String counterGeneration,
            String sampling,
            long startMillis,
            long endMillis,
            int partitions,
            List<Long> capacityBytesPerPartition,
            List<Long> frequencies,
            long missesOutsideBuckets) {
        this.estimator = Objects.requireNonNull(estimator, "estimator");
        this.counterKind = Objects.requireNonNull(counterKind, "counterKind");
        Checks.identity(population, "population");
        Checks.identity(cacheScope, "cacheScope");
        Checks.identity(counterGeneration, "counterGeneration");
        Checks.identity(sampling, "sampling");
        Checks.require(startMillis >= 0 && endMillis > startMillis, "Invalid histogram window");
        Checks.require(partitions > 0 && missesOutsideBuckets >= 0, "Invalid histogram counters");
        this.population = population;
        this.cacheScope = cacheScope;
        this.counterGeneration = counterGeneration;
        this.sampling = sampling;
        this.startMillis = startMillis;
        this.endMillis = endMillis;
        this.partitions = partitions;
        this.capacityBytesPerPartition = List.copyOf(capacityBytesPerPartition);
        this.frequencies = List.copyOf(frequencies);
        this.missesOutsideBuckets = missesOutsideBuckets;
        Checks.require(
                frequencies.size() == capacityBytesPerPartition.size(),
                "Mismatched histogram buckets");
        long previous = 0;
        long total = missesOutsideBuckets;
        for (int i = 0; i < frequencies.size(); i++) {
            long capacity = this.capacityBytesPerPartition.get(i);
            long frequency = this.frequencies.get(i);
            Checks.require(capacity > previous && frequency >= 0, "Invalid histogram bucket");
            previous = capacity;
            total = Math.addExact(total, frequency);
            Math.multiplyExact(capacity, partitions);
        }
    }
}
