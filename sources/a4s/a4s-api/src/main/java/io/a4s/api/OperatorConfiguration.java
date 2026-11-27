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

/** Operator allocation; absent values denote unknown or unavailable measurements. */
public final class OperatorConfiguration {
    public final int parallelism;
    public final Long cachePerTaskBytes;
    public final Long managedMemoryPerTaskBytes;
    public final Double cpuCoresPerTask;

    @ConstructorProperties({
        "parallelism",
        "cachePerTaskBytes",
        "managedMemoryPerTaskBytes",
        "cpuCoresPerTask"
    })
    public OperatorConfiguration(
            int parallelism,
            Long cachePerTaskBytes,
            Long managedMemoryPerTaskBytes,
            Double cpuCoresPerTask) {
        Checks.require(parallelism > 0, "Parallelism must be positive");
        Checks.nonnegative(cachePerTaskBytes, "cachePerTaskBytes");
        Checks.nonnegative(managedMemoryPerTaskBytes, "managedMemoryPerTaskBytes");
        Checks.nonnegative(cpuCoresPerTask, "cpuCoresPerTask");
        if (cachePerTaskBytes != null) {
            Math.multiplyExact(cachePerTaskBytes, parallelism);
        }
        this.parallelism = parallelism;
        this.cachePerTaskBytes = cachePerTaskBytes;
        this.managedMemoryPerTaskBytes = managedMemoryPerTaskBytes;
        this.cpuCoresPerTask = cpuCoresPerTask;
    }

    public Long totalCacheBytes() {
        return cachePerTaskBytes == null
                ? null
                : Math.multiplyExact(cachePerTaskBytes, parallelism);
    }
}
