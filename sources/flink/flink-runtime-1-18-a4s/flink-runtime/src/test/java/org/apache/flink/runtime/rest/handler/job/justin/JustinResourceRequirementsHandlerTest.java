/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.justin;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.jobgraph.justin.JustinVertexResourceRequirements;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests compatibility of the Justin requirements response across client versions. */
class JustinResourceRequirementsHandlerTest {

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void resourceMetadataIsIndependentlyOptIn(boolean includeApplied, boolean includeDefault)
            throws Exception {
        final JobID jobId = new JobID();
        final JobVertexID vertexId = new JobVertexID();
        final ResourceProfile requested =
                ResourceProfile.newBuilder().setManagedMemoryMB(343).build();
        final ResourceProfile applied =
                ResourceProfile.newBuilder().setManagedMemoryMB(640).build();
        final JustinResourceRequirements requirements =
                new JustinResourceRequirements(
                        Collections.singletonMap(
                                vertexId,
                                new JustinVertexResourceRequirements(
                                        new JustinVertexResourceRequirements.Parallelism(1, 1),
                                        requested,
                                        applied,
                                        true)));
        final RestfulGateway gateway = mock(RestfulGateway.class);
        when(gateway.requestJustinResourceRequirements(jobId))
                .thenReturn(CompletableFuture.completedFuture(requirements));
        final JustinResourceRequirementsHandler handler =
                new JustinResourceRequirementsHandler(
                        () -> CompletableFuture.completedFuture(gateway),
                        Time.seconds(5),
                        Collections.emptyMap());
        final Map<String, List<String>> query = new HashMap<>();
        if (includeApplied) {
            query.put("include-applied-profile", Collections.singletonList("true"));
        }
        if (includeDefault) {
            query.put("include-default-slot", Collections.singletonList("true"));
        }
        final JustinResourceRequirementsBody response =
                handler.handleRequest(
                                HandlerRequest.resolveParametersAndCreate(
                                        EmptyRequestBody.getInstance(),
                                        JustinResourceRequirementsHeaders.INSTANCE
                                                .getUnresolvedMessageParameters(),
                                        Collections.singletonMap(
                                                JobIDPathParameter.KEY, jobId.toString()),
                                        query,
                                        Collections.emptyList()),
                                gateway)
                        .get();
        final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        final byte[] wire = mapper.writeValueAsBytes(response);
        final JsonNode vertex = mapper.readTree(wire).path(vertexId.toString());
        assertThat(vertex.has("defaultSlot")).isEqualTo(includeDefault);
        assertThat(vertex.path("defaultSlot").asBoolean()).isEqualTo(includeDefault);
        final JustinResourceRequirements decoded =
                mapper.readValue(wire, JustinResourceRequirementsBody.class)
                        .asJobResourceRequirements()
                        .orElseThrow();
        assertThat(decoded.getResourceProfile(vertexId))
                .isEqualTo(includeDefault ? ResourceProfile.UNKNOWN : requested);
        assertThat(vertex.at("/resourceProfile/managedMemory/bytes").asLong())
                .isEqualTo(requested.getManagedMemory().getBytes());
        if (includeApplied) {
            assertThat(vertex.at("/appliedResourceProfile/managedMemory/bytes").asLong())
                    .isEqualTo(applied.getManagedMemory().getBytes());
        } else {
            assertThat(vertex.has("appliedResourceProfile")).isFalse();
        }
    }
}
