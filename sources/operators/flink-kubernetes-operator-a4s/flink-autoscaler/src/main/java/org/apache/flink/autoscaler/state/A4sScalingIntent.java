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

package org.apache.flink.autoscaler.state;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;

import java.util.Map;
import java.util.Objects;

/** A Flink target validated against one running execution and ready for conditional submission. */
@Value
public class A4sScalingIntent {
    String jobId;
    String executionToken;
    long expiresAtMillis;
    Map<String, String> parallelism;
    Map<String, String> resourceProfiles;
    Map<String, Integer> lowerBounds;

    @JsonCreator
    public A4sScalingIntent(
            @JsonProperty("jobId") String jobId,
            @JsonProperty("executionToken") String executionToken,
            @JsonProperty("expiresAtMillis") long expiresAtMillis,
            @JsonProperty("parallelism") Map<String, String> parallelism,
            @JsonProperty("resourceProfiles") Map<String, String> resourceProfiles,
            @JsonProperty("lowerBounds") Map<String, Integer> lowerBounds) {
        this.jobId = Objects.requireNonNull(jobId);
        this.executionToken = Objects.requireNonNull(executionToken);
        this.expiresAtMillis = expiresAtMillis;
        this.parallelism = Map.copyOf(parallelism);
        this.resourceProfiles = Map.copyOf(resourceProfiles);
        this.lowerBounds = Map.copyOf(lowerBounds);
        if (executionToken.isBlank()
                || this.parallelism.isEmpty()
                || !this.parallelism.keySet().equals(this.resourceProfiles.keySet())
                || !this.parallelism.keySet().equals(this.lowerBounds.keySet())) {
            throw new IllegalArgumentException("Incomplete conditional A4S target");
        }
    }
}
