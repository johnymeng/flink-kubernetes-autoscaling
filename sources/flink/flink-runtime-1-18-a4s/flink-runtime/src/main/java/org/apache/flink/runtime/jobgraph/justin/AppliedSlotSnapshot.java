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

package org.apache.flink.runtime.jobgraph.justin;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Current executions and their physical slot offers from the JobMaster main thread. */
public final class AppliedSlotSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    public final long capturedAtMillis;
    public final List<Task> tasks;

    @JsonCreator
    public AppliedSlotSnapshot(
            @JsonProperty("capturedAtMillis") long capturedAtMillis,
            @JsonProperty("tasks") List<Task> tasks) {
        this.capturedAtMillis = capturedAtMillis;
        this.tasks = Collections.unmodifiableList(new ArrayList<>(tasks));
    }

    /** One current RUNNING attempt. Sharing is represented by a repeated allocation ID. */
    public static final class Task implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String vertex;
        public final int subtask;
        public final String attempt;
        public final String worker;
        public final String allocation;
        public final ResourceProfile profile;

        @JsonCreator
        public Task(
                @JsonProperty("vertex") String vertex,
                @JsonProperty("subtask") int subtask,
                @JsonProperty("attempt") String attempt,
                @JsonProperty("worker") String worker,
                @JsonProperty("allocation") String allocation,
                @JsonProperty("profile") ResourceProfile profile) {
            this.vertex = vertex;
            this.subtask = subtask;
            this.attempt = attempt;
            this.worker = worker;
            this.allocation = allocation;
            this.profile = profile;
        }
    }
}
