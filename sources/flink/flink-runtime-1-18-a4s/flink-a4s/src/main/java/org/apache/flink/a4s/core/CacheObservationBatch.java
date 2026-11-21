/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"). You may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.a4s.core;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Quality and constituent cache observations returned for one vertex. */
public final class CacheObservationBatch {
    public static final String FIELD_NAME = "cacheObservations";

    public final int version;
    public final CacheObservation.Status status;
    public final int expectedSubtasks;
    public final long wireBytes;
    public final long oldestAgeMillis;
    public final List<CacheObservation> subtasks;

    @JsonCreator
    public CacheObservationBatch(
            @JsonProperty("version") int version,
            @JsonProperty("status") CacheObservation.Status status,
            @JsonProperty("expectedSubtasks") int expectedSubtasks,
            @JsonProperty("wireBytes") long wireBytes,
            @JsonProperty("oldestAgeMillis") long oldestAgeMillis,
            @JsonProperty("subtasks") List<CacheObservation> subtasks) {
        this.version = version;
        this.status = Objects.requireNonNull(status);
        this.expectedSubtasks = expectedSubtasks;
        this.wireBytes = wireBytes;
        this.oldestAgeMillis = oldestAgeMillis;
        List<CacheObservation> copy = new ArrayList<>(subtasks);
        copy.forEach(Objects::requireNonNull);
        this.subtasks = Collections.unmodifiableList(copy);
    }

    public CacheObservationBatch withoutHistograms() {
        return new CacheObservationBatch(
                version, status, expectedSubtasks, wireBytes, oldestAgeMillis,
                subtasks.stream().map(CacheObservation::withoutHistogram).collect(Collectors.toList()));
    }

    public static CacheObservationBatch invalid(CacheObservation.Status status) {
        return new CacheObservationBatch(CacheObservation.VERSION, status, 0, 0, -1, Collections.emptyList());
    }
}
