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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.autoscaler.utils.justin.JustinVertexResourceRequirements;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesScalingRealizer;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeMemoryScalingTest {
    @Test
    void memoryOnlyUpdateKeepsUnchangedVerticesAndThenBecomesANoop() {
        var stateful = new JobVertexID();
        var source = new JobVertexID();
        var p = new JustinVertexResourceRequirements.Parallelism(2, 2);
        var old =
                ResourceProfile.newBuilder()
                        .setCpuCores(.2)
                        .setTaskHeapMemory(new MemorySize(67108865))
                        .setNetworkMemory(new MemorySize(15728641))
                        .setManagedMemoryMB(42)
                        .build();
        var next = ResourceProfile.newBuilder(old).setManagedMemoryMB(58).build();
        var sourceProfile = ResourceProfile.newBuilder(old).setManagedMemoryMB(16).build();
        var live =
                Map.of(
                        stateful,
                        new JustinVertexResourceRequirements(p, old),
                        source,
                        new JustinVertexResourceRequirements(p, sourceProfile));
        var desired = new Configuration();
        desired.set(PipelineOptions.PARALLELISM_OVERRIDES, Map.of(stateful.toString(), "2"));
        desired.set(
                KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES,
                Map.of(stateful.toString(), next.toString()));
        var result =
                NativeFlinkService.proposeJustinResources(live, desired, new Configuration())
                        .orElseThrow();
        assertEquals(live.keySet(), result.keySet());
        assertEquals(live.get(source), result.get(source));
        assertNotEquals(live, result);
        assertEquals(p, result.get(stateful).getParallelism());
        assertEquals(next, result.get(stateful).getResourceProfile());
        assertEquals(
                result,
                NativeFlinkService.proposeJustinResources(result, desired, desired).orElseThrow());
        assertTrue(
                NativeFlinkService.proposeJustinResources(result, new Configuration(), desired)
                        .isEmpty());
    }

    @Test
    void operatorRequestPreservesMemoryWhenReadByTheRuntime() throws Exception {
        var id = new JobVertexID();
        var profile =
                ResourceProfile.newBuilder()
                        .setCpuCores(.2)
                        .setTaskHeapMemory(new MemorySize(67108865))
                        .setNetworkMemory(new MemorySize(15728641))
                        .setManagedMemoryMB(58)
                        .build();
        var request =
                new org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsBody(
                        new org.apache.flink.autoscaler.utils.justin.JustinResourceRequirements(
                                Map.of(
                                        id,
                                        new JustinVertexResourceRequirements(
                                                new JustinVertexResourceRequirements.Parallelism(
                                                        1, 2),
                                                profile))));
        var mapper = org.apache.flink.util.jackson.JacksonMapperFactory.createObjectMapper();
        var decoded =
                mapper.readValue(
                                mapper.writeValueAsString(request),
                                org.apache.flink.runtime.rest.messages.job.justin
                                        .JustinResourceRequirementsBody.class)
                        .asJobResourceRequirements()
                        .orElseThrow();
        assertEquals(profile, decoded.getResourceProfile(id));
        var response =
                new org.apache.flink.runtime.rest.messages.job.justin
                        .JustinResourceRequirementsBody(decoded);
        var observed =
                mapper.readValue(
                                mapper.writeValueAsString(response),
                                org.apache.flink.autoscaler.utils.justin
                                        .JustinResourceRequirementsBody.class)
                        .asJustinResourceRequirements()
                        .orElseThrow();
        assertEquals(profile, observed.getJobVertexParallelisms().get(id).getResourceProfile());
        var sameParallelismDifferentMemory =
                org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(
                                id,
                                1,
                                2,
                                ResourceProfile.newBuilder(profile).setManagedMemoryMB(42).build())
                        .build();
        assertNotEquals(decoded, sameParallelismDifferentMemory);
    }
}
