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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Class containing information for a slot of {@link
 * org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlotInformation}.
 */
public class SlotInfo implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_RESOURCE = "resource";

    public static final String FIELD_NAME_JOB_ID = "jobId";

    @JsonProperty(FIELD_NAME_RESOURCE)
    private final ResourceProfileInfo resource;

    @JsonProperty(FIELD_NAME_JOB_ID)
    @JsonSerialize(using = JobIDSerializer.class)
    private final JobID jobId;

    @JsonProperty("allocationId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable private final String allocationId;

    @JsonProperty("exactProfile")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable private final ResourceProfile exactProfile;

    public SlotInfo(JobID jobId, ResourceProfileInfo resource) {
        this(jobId, resource, null, null);
    }

    @JsonCreator
    public SlotInfo(
            @JsonDeserialize(using = JobIDDeserializer.class) @JsonProperty(FIELD_NAME_JOB_ID)
                    JobID jobId,
            @JsonProperty(FIELD_NAME_RESOURCE) ResourceProfileInfo resource,
            @JsonProperty("allocationId") @Nullable String allocationId,
            @JsonProperty("exactProfile") @Nullable ResourceProfile exactProfile) {
        this.allocationId = allocationId;
        this.exactProfile = exactProfile;
        this.jobId = Preconditions.checkNotNull(jobId);
        this.resource = Preconditions.checkNotNull(resource);
    }

    public SlotInfo(JobID jobId, ResourceProfile resource) {
        this(jobId, ResourceProfileInfo.fromResrouceProfile(resource));
    }

    public SlotInfo(JobID jobId, ResourceProfile resource, String allocationId) {
        this(jobId, ResourceProfileInfo.fromResrouceProfile(resource), allocationId, resource);
    }

    @JsonIgnore
    @Nullable
    public String getAllocationId() {
        return allocationId;
    }

    @JsonIgnore
    @Nullable
    public ResourceProfile getExactProfile() {
        return exactProfile;
    }

    @JsonIgnore
    public JobID getJobId() {
        return jobId;
    }

    @JsonIgnore
    public ResourceProfileInfo getResource() {
        return resource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotInfo that = (SlotInfo) o;
        return Objects.equals(jobId, that.jobId) && Objects.equals(resource, that.resource)
                && Objects.equals(allocationId, that.allocationId)
                && Objects.equals(exactProfile, that.exactProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, resource, allocationId, exactProfile);
    }
}
