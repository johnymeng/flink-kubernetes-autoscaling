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

/** Fixed-latency cost policy and optional legacy slot reservation conversion. */
public final class PolicyConfiguration {
    public final Mode mode;
    public final int maxAttempts;
    public final double hitSeconds;
    public final double missSeconds;
    public final long managedMemoryBaseBytes;
    public final Long managedMemoryOverrideBytes;

    @ConstructorProperties({
        "mode",
        "maxAttempts",
        "hitSeconds",
        "missSeconds",
        "managedMemoryBaseBytes",
        "managedMemoryOverrideBytes"
    })
    public PolicyConfiguration(
            Mode mode,
            int maxAttempts,
            double hitSeconds,
            double missSeconds,
            long managedMemoryBaseBytes,
            Long managedMemoryOverrideBytes) {
        Objects.requireNonNull(mode, "mode");
        Checks.require(maxAttempts > 0 && maxAttempts <= 10_000, "Invalid maxAttempts");
        Checks.require(
                Double.isFinite(hitSeconds)
                        && Double.isFinite(missSeconds)
                        && hitSeconds > 0
                        && missSeconds > hitSeconds,
                "Invalid hit/miss latency");
        Checks.require(managedMemoryBaseBytes >= 0, "Negative managed memory base");
        Checks.require(
                managedMemoryOverrideBytes == null || managedMemoryOverrideBytes > 0,
                "Invalid managed memory override");
        Checks.require(
                mode == Mode.LEGACY_SLOT_RESERVATION
                        || (managedMemoryBaseBytes == 0 && managedMemoryOverrideBytes == null),
                "Managed reservation options require the legacy policy");
        this.mode = mode;
        this.maxAttempts = maxAttempts;
        this.hitSeconds = hitSeconds;
        this.missSeconds = missSeconds;
        this.managedMemoryBaseBytes = managedMemoryBaseBytes;
        this.managedMemoryOverrideBytes = managedMemoryOverrideBytes;
    }

    public enum Mode {
        CACHE,
        LEGACY_SLOT_RESERVATION
    }
}
