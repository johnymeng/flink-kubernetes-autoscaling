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

/** Optional migration costs; unknown costs are represented by absent fields. */
public final class MigrationEstimate {
    public final Long stateBytes;
    public final Long restoreMillis;
    public final Long checkpointMillis;
    public final Long initializationMillis;
    public final Long cacheWarmupMillis;
    public final Long networkBytes;
    public final Long backlogRecords;
    public final Long downtimeMillis;

    @ConstructorProperties({
        "stateBytes",
        "restoreMillis",
        "checkpointMillis",
        "initializationMillis",
        "cacheWarmupMillis",
        "networkBytes",
        "backlogRecords",
        "downtimeMillis"
    })
    public MigrationEstimate(
            Long stateBytes,
            Long restoreMillis,
            Long checkpointMillis,
            Long initializationMillis,
            Long cacheWarmupMillis,
            Long networkBytes,
            Long backlogRecords,
            Long downtimeMillis) {
        Checks.nonnegative(stateBytes, "stateBytes");
        Checks.nonnegative(restoreMillis, "restoreMillis");
        Checks.nonnegative(checkpointMillis, "checkpointMillis");
        Checks.nonnegative(initializationMillis, "initializationMillis");
        Checks.nonnegative(cacheWarmupMillis, "cacheWarmupMillis");
        Checks.nonnegative(networkBytes, "networkBytes");
        Checks.nonnegative(backlogRecords, "backlogRecords");
        Checks.nonnegative(downtimeMillis, "downtimeMillis");
        this.stateBytes = stateBytes;
        this.restoreMillis = restoreMillis;
        this.checkpointMillis = checkpointMillis;
        this.initializationMillis = initializationMillis;
        this.cacheWarmupMillis = cacheWarmupMillis;
        this.networkBytes = networkBytes;
        this.backlogRecords = backlogRecords;
        this.downtimeMillis = downtimeMillis;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MigrationEstimate)) {
            return false;
        }
        var estimate = (MigrationEstimate) other;
        return Objects.equals(stateBytes, estimate.stateBytes)
                && Objects.equals(restoreMillis, estimate.restoreMillis)
                && Objects.equals(checkpointMillis, estimate.checkpointMillis)
                && Objects.equals(initializationMillis, estimate.initializationMillis)
                && Objects.equals(cacheWarmupMillis, estimate.cacheWarmupMillis)
                && Objects.equals(networkBytes, estimate.networkBytes)
                && Objects.equals(backlogRecords, estimate.backlogRecords)
                && Objects.equals(downtimeMillis, estimate.downtimeMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                stateBytes,
                restoreMillis,
                checkpointMillis,
                initializationMillis,
                cacheWarmupMillis,
                networkBytes,
                backlogRecords,
                downtimeMillis);
    }
}
