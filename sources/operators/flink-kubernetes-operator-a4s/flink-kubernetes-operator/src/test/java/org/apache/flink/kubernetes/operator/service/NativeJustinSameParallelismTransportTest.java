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

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsBody;
import org.apache.flink.autoscaler.utils.justin.JustinVertexResourceRequirements;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesScalingRealizer;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NativeJustinSameParallelismTransportTest {
    @Test
    void changedSameParallelismRequestKeepsEveryVertexAndExactMemory() throws Exception {
        long requested = (224L << 20) + 37;
        var updates = updatesForSameParallelism(requested);
        assertEquals(1, updates.size());
        var mapper = RestMapperUtils.getStrictObjectMapper();
        var body = mapper.readValue(mapper.writeValueAsBytes(updates.get(0)), JustinResourceRequirementsBody.class);
        var vertices = body.asJustinResourceRequirements().orElseThrow().getJobVertexParallelisms();
        assertEquals(2, vertices.size());
        assertEquals(2, vertices.values().stream()
                .filter(vertex -> vertex.getParallelism().getUpperBound() == 2).count());
        assertEquals(1, vertices.values().stream()
                .filter(vertex -> vertex.getResourceProfile().getManagedMemory().getBytes() == requested)
                .count());
    }

    @Test
    void identicalNumericRequirementsDoNotIssueASecondRequest() {
        assertTrue(updatesForSameParallelism(192L << 20).isEmpty());
    }

    @Test
    void exactByteSuffixTakesPrecedenceOverHumanReadableMemory() {
        var profile = NativeFlinkService.parseResourceProfile(
                "ResourceProfile{cpuCores=1, taskHeapMemory=134.000mb (140509184 bytes), "
                        + "taskOffHeapMemory=0 bytes, managedMemory=224.000mb (234881061 bytes), "
                        + "networkMemory=39.000mb (40894464 bytes)}");
        assertEquals(234881061L, profile.getManagedMemory().getBytes());
    }

    @SuppressWarnings("unchecked")
    private static List<JustinResourceRequirementsBody> updatesForSameParallelism(long requestedManaged) {
        var vertex = new JobVertexID();
        var unaffected = new JobVertexID();
        var configured = new Configuration();
        var requested = new Configuration();
        var parallelism = Map.of(vertex.toHexString(), "2");
        var before = ResourceProfile.newBuilder().setCpuCores(1).setManagedMemoryMB(192).build();
        var allRequirements = Map.of(
                vertex, new JustinVertexResourceRequirements(
                        new JustinVertexResourceRequirements.Parallelism(1, 2), before),
                unaffected, new JustinVertexResourceRequirements(
                        new JustinVertexResourceRequirements.Parallelism(2, 2), before));
        configured.set(PipelineOptions.PARALLELISM_OVERRIDES, parallelism);
        configured.set(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES,
                Map.of(vertex.toHexString(), encoded(192L << 20)));
        requested.set(PipelineOptions.PARALLELISM_OVERRIDES, parallelism);
        requested.set(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES,
                Map.of(vertex.toHexString(), encoded(requestedManaged)));

        var deployment = new FlinkDeployment();
        deployment.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        var context = (FlinkResourceContext<FlinkDeployment>) mock(FlinkResourceContext.class);
        when(context.getResource()).thenReturn(deployment);
        when(context.getObserveConfig()).thenReturn(configured);
        RestClusterClient<String> client = mock(RestClusterClient.class);
        List<JustinResourceRequirementsBody> updates = new ArrayList<>();
        doAnswer(call -> {
            updates.add(call.getArgument(2));
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }).when(client).sendRequest(any(), any(), any());
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(new Configuration());
        var service = new NativeFlinkService(null, null, null, operatorConfig, mock(EventRecorder.class)) {
            @Override
            public RestClusterClient<String> getClusterClient(Configuration configuration) {
                return client;
            }

            @Override
            protected Map<JobVertexID, JustinVertexResourceRequirements> getJustinResources(
                    RestClusterClient<String> cluster, AbstractFlinkResource<?, ?> resource) {
                return allRequirements;
            }
        };

        assertTrue(service.justin(context, requested));
        return updates;
    }

    private static String encoded(long managedBytes) {
        return "ResourceProfile{cpuCores=1, taskHeapMemory=0 bytes, taskOffHeapMemory=0 bytes, "
                + "managedMemory=" + managedBytes + " bytes, networkMemory=0 bytes}";
    }
}
