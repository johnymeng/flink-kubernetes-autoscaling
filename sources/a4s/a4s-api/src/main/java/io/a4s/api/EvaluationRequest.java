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

/** Versioned request shared by the embedded engine, HTTP runner and replay. */
public final class EvaluationRequest {
    public final int schemaVersion;
    public final ClusterSnapshot snapshot;
    public final ScalingConstraints constraints;
    public final PolicyConfiguration policy;
    public final PolicyState previousState;
    /** Model time persisted with the request for deterministic replay. */
    public final long evaluatedAtMillis;

    @ConstructorProperties({
        "schemaVersion",
        "snapshot",
        "constraints",
        "policy",
        "previousState",
        "evaluatedAtMillis"
    })
    public EvaluationRequest(
            int schemaVersion,
            ClusterSnapshot snapshot,
            ScalingConstraints constraints,
            PolicyConfiguration policy,
            PolicyState previousState,
            long evaluatedAtMillis) {
        Checks.require(schemaVersion == SCHEMA_VERSION, "Unsupported API schema version");
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(constraints, "constraints");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(previousState, "previousState");
        Checks.require(
                evaluatedAtMillis >= snapshot.measuredAtMillis, "Model time precedes snapshot");
        this.schemaVersion = schemaVersion;
        this.snapshot = snapshot;
        this.constraints = constraints;
        this.policy = policy;
        this.previousState = previousState;
        this.evaluatedAtMillis = evaluatedAtMillis;
    }

    public static final int SCHEMA_VERSION = 1;
}
