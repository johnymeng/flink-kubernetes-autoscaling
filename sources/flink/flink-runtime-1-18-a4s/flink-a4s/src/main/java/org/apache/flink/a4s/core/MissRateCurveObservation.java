/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Optional;

/** A measured miss-rate curve or a reason why a curve is unavailable. */
public class MissRateCurveObservation {
    public static final String FIELD_NAME_STATUS = "mrcStatus";
    public static final String FIELD_NAME_SCALED_MRC = "scaledMrc";

    public enum Status {
        AVAILABLE,
        TASK_METRICS_UNAVAILABLE,
        HISTOGRAM_UNAVAILABLE,
        INCOMPLETE_HISTOGRAMS,
        NO_SAMPLES,
        NO_FINITE_BUCKETS,
        INVALID_HISTOGRAM,
        INVALID_RESPONSE,
        REQUEST_FAILED,
        UNAVAILABLE
    }

    private final Status status;

    @JsonProperty(FIELD_NAME_SCALED_MRC)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final MissRateCurve curve;

    protected MissRateCurveObservation(@Nullable Status status, @Nullable MissRateCurve curve) {
        if (curve != null && !curve.getPoints().isEmpty()) {
            boolean valid = (status == null || status == Status.AVAILABLE) && curve.isUsable();
            this.status = valid ? Status.AVAILABLE : Status.INVALID_RESPONSE;
            this.curve = valid ? curve : null;
        } else {
            // Older endpoints sent a missing status and an empty or null curve.
            this.status =
                    status == null
                            ? Status.UNAVAILABLE
                            : (status == Status.AVAILABLE ? Status.INVALID_RESPONSE : status);
            this.curve = null;
        }
    }

    @JsonProperty(FIELD_NAME_STATUS)
    public Status getMrcStatus() {
        return status;
    }

    @JsonIgnore
    public Optional<MissRateCurve> getCurve() {
        return Optional.ofNullable(curve);
    }
}
