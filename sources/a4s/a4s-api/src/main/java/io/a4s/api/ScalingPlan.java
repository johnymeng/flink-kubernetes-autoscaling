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
import java.util.Objects;

/** Versioned recommendation tied to a single snapshot and execution generation. */
public final class ScalingPlan {
    public final int schemaVersion;
    public final String decisionId;
    public final String snapshotId;
    public final String jobId;
    public final String executionGeneration;
    public final Action action;
    public final Map<String, OperatorConfiguration> currentConfiguration;
    public final Map<String, OperatorDecision> operators;
    public final MigrationEstimate migration;
    public final String rationale;
    public final String modelVersion;
    public final long expiresAtMillis;
    public final boolean requiresStatePreservation;
    public final int attempts;
    public final PolicyState policyState;

    @ConstructorProperties({
        "schemaVersion",
        "decisionId",
        "snapshotId",
        "jobId",
        "executionGeneration",
        "action",
        "currentConfiguration",
        "operators",
        "migration",
        "rationale",
        "modelVersion",
        "expiresAtMillis",
        "requiresStatePreservation",
        "attempts",
        "policyState"
    })
    public ScalingPlan(
            int schemaVersion,
            String decisionId,
            String snapshotId,
            String jobId,
            String executionGeneration,
            Action action,
            Map<String, OperatorConfiguration> currentConfiguration,
            Map<String, OperatorDecision> operators,
            MigrationEstimate migration,
            String rationale,
            String modelVersion,
            long expiresAtMillis,
            boolean requiresStatePreservation,
            int attempts,
            PolicyState policyState) {
        Checks.require(
                schemaVersion == EvaluationRequest.SCHEMA_VERSION,
                "Unsupported API schema version");
        Checks.identity(decisionId, "decisionId");
        Checks.identity(snapshotId, "snapshotId");
        Checks.identity(jobId, "jobId");
        Checks.identity(executionGeneration, "executionGeneration");
        Objects.requireNonNull(action, "action");
        currentConfiguration = Map.copyOf(currentConfiguration);
        operators = Map.copyOf(operators);
        Checks.require(
                action == Action.RESCALE
                        ? operators.keySet().equals(currentConfiguration.keySet())
                        : operators.isEmpty(),
                "Plan contains incompatible operators");
        Objects.requireNonNull(rationale, "rationale");
        Checks.identity(modelVersion, "modelVersion");
        Checks.require(expiresAtMillis >= 0 && attempts >= 0, "Invalid plan metadata");
        Objects.requireNonNull(policyState, "policyState");
        this.schemaVersion = schemaVersion;
        this.decisionId = decisionId;
        this.snapshotId = snapshotId;
        this.jobId = jobId;
        this.executionGeneration = executionGeneration;
        this.action = action;
        this.currentConfiguration = currentConfiguration;
        this.operators = operators;
        this.migration = migration;
        this.rationale = rationale;
        this.modelVersion = modelVersion;
        this.expiresAtMillis = expiresAtMillis;
        this.requiresStatePreservation = requiresStatePreservation;
        this.attempts = attempts;
        this.policyState = policyState;
    }

    public enum Action {
        NO_CHANGE,
        RESCALE,
        INSUFFICIENT_METRICS,
        INFEASIBLE_CONFIGURATION,
        UNSUPPORTED_ACTION
    }
}
