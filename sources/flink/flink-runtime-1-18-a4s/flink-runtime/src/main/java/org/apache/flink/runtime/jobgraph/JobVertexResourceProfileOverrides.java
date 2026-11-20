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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Applies Justin resource requests when the dispatcher submits a replacement job. */
public final class JobVertexResourceProfileOverrides {
    private static final Logger LOG =
            LoggerFactory.getLogger(JobVertexResourceProfileOverrides.class);

    private static final ConfigOption<Boolean> ENABLED =
            ConfigOptions.key("pipeline.jobvertex-resourceprofile-overrides-on-submit.enabled")
                    .booleanType()
                    .defaultValue(false);

    private static final ConfigOption<Map<String, String>> PROFILES =
            ConfigOptions.key("pipeline.jobvertex-resourceprofile-overrides")
                    .mapType()
                    .defaultValue(Collections.emptyMap());

    private JobVertexResourceProfileOverrides() {}

    public static void apply(JobGraph jobGraph, Configuration clusterConfiguration) {
        Configuration jobConfiguration = jobGraph.getJobConfiguration();
        if (!jobConfiguration.getOptional(ENABLED)
                .orElseGet(() -> clusterConfiguration.get(ENABLED))) {
            return;
        }

        Map<String, String> overrides = new HashMap<>(clusterConfiguration.get(PROFILES));
        overrides.putAll(jobConfiguration.get(PROFILES));

        // Validate the complete request before mutating any vertex. Vertex IDs must match the
        // submitted graph because restore and resource assignment use those same IDs.
        Map<JobVertex, ResourceProfile> resolved = new HashMap<>();
        overrides.forEach((id, value) -> {
            JobVertex vertex = jobGraph.findVertexByID(JobVertexID.fromHexString(id));
            checkArgument(vertex != null, "Unknown job vertex in resource profile overrides: %s", id);
            resolved.put(vertex, parseProfile(value));
        });

        resolved.forEach((vertex, profile) -> {
            // Give a vertex its own group, as in the Justin in-place scheduler path. In particular
            // this must not replace the requested resources for other vertices in its old group.
            SlotSharingGroup group = new SlotSharingGroup();
            group.setResourceProfile(profile);
            vertex.setSlotSharingGroup(group);
            LOG.info(
                    "AUTOSCALER_PROFILE_APPLIED job={} vertex={} parallelism={} managed_bytes={}",
                    jobGraph.getJobID(),
                    vertex.getID(),
                    vertex.getParallelism(),
                    profile.getManagedMemory().getBytes());
        });
    }

    private static ResourceProfile parseProfile(String value) {
        String prefix = "ResourceProfile{";
        checkArgument(
                value.startsWith(prefix) && value.endsWith("}"),
                "Expected a concrete Justin resource profile: %s",
                value);
        String[] fields = value.substring(prefix.length(), value.length() - 1).split(", ", -1);
        checkArgument(fields.length == 5, "Expected five Justin resource profile fields: %s", value);
        return ResourceProfile.newBuilder()
                .setCpuCores(Double.parseDouble(readField(fields[0], "cpuCores")))
                .setTaskHeapMemory(parseMemory(fields[1], "taskHeapMemory"))
                .setTaskOffHeapMemory(parseMemory(fields[2], "taskOffHeapMemory"))
                .setManagedMemory(parseMemory(fields[3], "managedMemory"))
                .setNetworkMemory(parseMemory(fields[4], "networkMemory"))
                .build();
    }

    private static MemorySize parseMemory(String field, String name) {
        String value = readField(field, name);
        // ResourceProfile renders large values with a rounded prefix and an exact byte count.
        if (value.endsWith(" bytes)")) {
            int exactBytes = value.lastIndexOf('(');
            checkArgument(exactBytes >= 0, "Malformed byte count in resource profile: %s", field);
            value = value.substring(exactBytes + 1, value.length() - 1);
        }
        return MemorySize.parse(value);
    }

    private static String readField(String field, String name) {
        String prefix = name + "=";
        checkArgument(field.startsWith(prefix), "Expected resource field %s: %s", name, field);
        return field.substring(prefix.length());
    }
}
