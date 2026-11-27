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

package org.apache.flink.autoscaler.topology;

import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;

import lombok.AllArgsConstructor;
import lombok.Value;

/** Vertex io metrics. */
@Value
@AllArgsConstructor
public class IOMetrics {

    public static final IOMetrics FINISHED_METRICS = new IOMetrics(0, 0, 0);

    long numRecordsIn;
    long numRecordsOut;
    double accumulatedBusyTime;
    boolean numRecordsOutComplete;

    public IOMetrics(long numRecordsIn, long numRecordsOut, double accumulatedBusyTime) {
        this(numRecordsIn, numRecordsOut, accumulatedBusyTime, false);
    }

    public static IOMetrics from(IOMetricsInfo metricsInfo) {
        return new IOMetrics(
                metricsInfo.getRecordsRead(),
                metricsInfo.getRecordsWritten(),
                metricsInfo.getAccumulatedBusy(),
                metricsInfo.isRecordsWrittenComplete());
    }
}
