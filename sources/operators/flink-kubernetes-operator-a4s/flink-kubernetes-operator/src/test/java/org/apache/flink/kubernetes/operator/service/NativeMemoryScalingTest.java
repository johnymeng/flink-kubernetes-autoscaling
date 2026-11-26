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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.realizer.ScalingRealizer.A4sIntentResult;
import org.apache.flink.autoscaler.state.A4sScalingIntent;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirements;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsBody;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsUpdateHeaders;
import org.apache.flink.autoscaler.utils.justin.JustinVertexResourceRequirements;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesScalingRealizer;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.util.RestClientException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void conditionalWriteDiscardsConflictAndObservesLostReply(boolean conflict) throws Exception {
        var jobId = new JobID();
        var vertex = new JobVertexID();
        var before = ResourceProfile.newBuilder().setManagedMemoryMB(42).build();
        var after = ResourceProfile.newBuilder().setManagedMemoryMB(58).build();
        var intent =
                new A4sScalingIntent(
                        jobId.toString(),
                        "before",
                        Long.MAX_VALUE,
                        Map.of(vertex.toString(), "2"),
                        Map.of(vertex.toString(), after.toString()),
                        Map.of(vertex.toString(), 1));
        var deployment = TestUtils.buildApplicationCluster();
        deployment.getStatus().getJobStatus().setJobId(jobId.toString());
        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        var conf = new Configuration();
        conf.set(FlinkConfigBuilder.FLINK_VERSION, FlinkVersion.v1_18);
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        var context = mock(FlinkResourceContext.class);
        when(context.getResource()).thenReturn(deployment);
        when(context.getObserveConfig()).thenReturn(conf);
        var client = (RestClusterClient<String>) mock(RestClusterClient.class);
        var service =
                new NativeFlinkService(
                        null,
                        null,
                        null,
                        FlinkOperatorConfiguration.fromConfiguration(conf),
                        null) {
                    @Override
                    public RestClusterClient<String> getClusterClient(Configuration config) {
                        return client;
                    }
                };

        var requirements = new AtomicReference<>(observed(vertex, before, before, "before"));
        var headers = JustinResourceRequirementsHeaders.INSTANCE;
        when(client.sendRequest(
                        eq(headers),
                        any(JobMessageParameters.class),
                        eq(EmptyRequestBody.getInstance())))
                .thenAnswer(
                        invocation -> {
                            var url =
                                    MessageParameters.resolveUrl(
                                            headers.getTargetRestEndpointURL(),
                                            invocation.getArgument(1));
                            assertTrue(url.contains("include-applied-profile=true"));
                            assertTrue(url.contains("include-execution-token=true"));
                            return CompletableFuture.completedFuture(requirements.get());
                        });
        when(client.sendRequest(
                        any(JustinResourceRequirementsUpdateHeaders.class),
                        any(JobMessageParameters.class),
                        any(JustinResourceRequirementsBody.class)))
                .thenAnswer(
                        invocation -> {
                            JustinResourceRequirementsBody body = invocation.getArgument(2);
                            assertEquals("before", body.executionToken().orElseThrow());
                            assertEquals(
                                    after,
                                    body.asJustinResourceRequirements()
                                            .orElseThrow()
                                            .getResourceProfile(vertex));
                            if (conflict) {
                                return CompletableFuture.failedFuture(
                                        new RestClientException(
                                                "stale", HttpResponseStatus.CONFLICT));
                            }
                            requirements.set(observed(vertex, after, before, "after-request"));
                            return CompletableFuture.failedFuture(new IOException("reply lost"));
                        });
        var clock = Clock.systemUTC();
        assertEquals(
                conflict ? A4sIntentResult.DISCARDED : A4sIntentResult.PENDING,
                service.reconcileA4sIntent(context, intent, clock, true));

        if (!conflict) {
            assertEquals(
                    A4sIntentResult.PENDING,
                    service.reconcileA4sIntent(context, intent, clock, true));
            requirements.set(observed(vertex, after, after, "after-rescale"));
            var running =
                    new JobDetailsInfo.JobVertexDetailsInfo(
                            vertex,
                            null,
                            "operator",
                            16,
                            2,
                            ExecutionState.RUNNING,
                            1,
                            -1,
                            1,
                            Map.of(ExecutionState.RUNNING, 2),
                            new IOMetricsInfo(0, true, 0, true, 0, true, 0, true, 0L, 0L, 0.0));
            when(client.getJobDetails(jobId))
                    .thenReturn(
                            CompletableFuture.completedFuture(
                                    new JobDetailsInfo(
                                            jobId,
                                            "test",
                                            false,
                                            JobStatus.RUNNING,
                                            1,
                                            -1,
                                            1,
                                            16,
                                            2,
                                            Map.of(),
                                            List.of(running),
                                            Map.of(),
                                            new JobPlanInfo.RawJson("{}"))));
            assertEquals(
                    A4sIntentResult.APPLIED,
                    service.reconcileA4sIntent(context, intent, clock, true));
        }
        verify(client, times(1))
                .sendRequest(
                        any(JustinResourceRequirementsUpdateHeaders.class),
                        any(JobMessageParameters.class),
                        any(JustinResourceRequirementsBody.class));
    }

    private static JustinResourceRequirementsBody observed(
            JobVertexID vertex, ResourceProfile requested, ResourceProfile applied, String token) {
        return new JustinResourceRequirementsBody(
                new JustinResourceRequirements(
                        Map.of(
                                vertex,
                                new JustinVertexResourceRequirements(
                                        new JustinVertexResourceRequirements.Parallelism(1, 2),
                                        requested,
                                        applied))),
                token);
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
