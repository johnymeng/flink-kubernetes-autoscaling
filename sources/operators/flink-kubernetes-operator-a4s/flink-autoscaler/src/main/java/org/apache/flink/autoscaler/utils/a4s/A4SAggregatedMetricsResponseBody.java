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

package org.apache.flink.autoscaler.utils.a4s;

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.MissRateCurveObservation;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** REST response carrying a measured A4S curve or an availability status. */
public class A4SAggregatedMetricsResponseBody extends MissRateCurveObservation
        implements ResponseBody {

    public static final String FIELD_NAME_OBSERVED_BACKLOG = "observedSourceBacklogRecords";

    @Nullable private final Long observedSourceBacklogRecords;

    public A4SAggregatedMetricsResponseBody(@Nullable MissRateCurve scaledMrc) {
        this(null, scaledMrc);
    }

    public A4SAggregatedMetricsResponseBody(
            @Nullable Status status, @Nullable MissRateCurve scaledMrc) {
        this(status, scaledMrc, null);
    }

    @JsonCreator
    public A4SAggregatedMetricsResponseBody(
            @JsonProperty(FIELD_NAME_STATUS) @Nullable Status status,
            @JsonProperty(FIELD_NAME_SCALED_MRC) @Nullable MissRateCurve scaledMrc,
            @JsonProperty(FIELD_NAME_OBSERVED_BACKLOG) @Nullable Long observedSourceBacklogRecords) {
        super(status, scaledMrc);
        this.observedSourceBacklogRecords = observedSourceBacklogRecords;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_OBSERVED_BACKLOG)
    @Nullable
    public Long getObservedSourceBacklogRecords() {
        return observedSourceBacklogRecords;
    }
}
