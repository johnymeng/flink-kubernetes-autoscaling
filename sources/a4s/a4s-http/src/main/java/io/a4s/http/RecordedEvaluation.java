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

package io.a4s.http;

import io.a4s.api.EvaluationRequest;
import io.a4s.api.ScalingPlan;
import java.beans.ConstructorProperties;
import java.util.Objects;

/** One controller evaluation for offline replay. A missing recommendation indicates no response. */
public final class RecordedEvaluation {
    public static final int VERSION = 1;
    public final int recordingVersion;
    public final EvaluationRequest request;
    public final ScalingPlan recommendation;

    @ConstructorProperties({"recordingVersion", "request", "recommendation"})
    public RecordedEvaluation(
            int recordingVersion, EvaluationRequest request, ScalingPlan recommendation) {
        if (recordingVersion != VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported recording version: " + recordingVersion);
        }
        this.recordingVersion = recordingVersion;
        this.request = Objects.requireNonNull(request, "Recorded request is required");
        this.recommendation = recommendation;
    }
}
