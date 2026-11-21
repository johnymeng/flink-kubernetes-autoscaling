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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.justin.JustinResourceRequirementsBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class JustinResourceRequirementsSerializationTest {

    @Test
    void resourceOnlyDifferenceSurvivesSerialization() throws Exception {
        JustinVertexResourceRequirements.Parallelism parallelism =
                new JustinVertexResourceRequirements.Parallelism(1, 2);
        ResourceProfile first = ResourceProfile.newBuilder().setManagedMemoryMB(192).build();
        ResourceProfile second = ResourceProfile.newBuilder().setManagedMemoryMB(193).build();
        JustinVertexResourceRequirements before =
                new JustinVertexResourceRequirements(parallelism, first);
        JustinVertexResourceRequirements after =
                new JustinVertexResourceRequirements(parallelism, second);

        assertThat(before).isNotEqualTo(after);
        JustinVertexResourceRequirements restored = InstantiationUtil.clone(after);
        assertThat(restored).isEqualTo(after).hasSameHashCodeAs(after);
    }

    @Test
    void exposesExactPhysicalProfileSeparatelyAndNeverAcceptsItAsAnUpdate() throws Exception {
        JobVertexID vertex = new JobVertexID();
        ResourceProfile physical = ResourceProfile.newBuilder().setCpuCores(1)
                .setTaskHeapMemory(new MemorySize(142553906))
                .setManagedMemoryMB(192)
                .setNetworkMemory(new MemorySize(45665485)).build();
        JustinResourceRequirements request = JustinResourceRequirements.newBuilder()
                .setParallelismForJobVertex(vertex, 1, 1, ResourceProfile.UNKNOWN).build();
        AppliedSlotSnapshot applied = new AppliedSlotSnapshot(17L, Collections.singletonList(
                new AppliedSlotSnapshot.Task(vertex.toHexString(), 0, "attempt", "worker",
                        "00000000000000000000000000000001", physical)));
        JustinResourceRequirements report = new JustinResourceRequirements(
                request.getJobVertexParallelisms(), applied);
        ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        JustinResourceRequirementsBody restored = mapper.readValue(mapper.writeValueAsBytes(new JustinResourceRequirementsBody(
                InstantiationUtil.clone(report))), JustinResourceRequirementsBody.class);

        assertThat(restored.asJobResourceRequirements().get().getAppliedSlots()).isNull();
        JustinResourceRequirements observed = restored.asObservedRequirements().get();
        assertThat(observed.getResourceProfile(vertex)).isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(observed.getAppliedSlots().capturedAtMillis).isEqualTo(17L);
        assertThat(observed.getAppliedSlots().tasks.get(0).profile).isEqualTo(physical);
        assertThat(observed.getAppliedSlots().tasks.get(0).attempt).isEqualTo("attempt");
    }

    @Test
    void restoresParallelismAndMemoryProfilesFromThePersistedJobGraph() throws Exception {
        JobGraph graph = new JobGraph("in-place scaling");
        SingleJobJobGraphStore store = new SingleJobJobGraphStore(graph);
        JobVertexID source = new JobVertexID();
        JobVertexID join = new JobVertexID();
        ResourceProfile profile =
                ResourceProfile.newBuilder().setCpuCores(1.0).setManagedMemoryMB(96).build();
        JustinResourceRequirements requirements =
                JustinResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(source, 1, 2, ResourceProfile.UNKNOWN)
                        .setParallelismForJobVertex(join, 3, 8, profile)
                        .build();

        store.putJustinResourceRequirements(graph.getJobID(), requirements);
        JobGraph recoveredGraph =
                InstantiationUtil.clone(store.recoverJobGraph(graph.getJobID()));
        JustinResourceRequirements recovered =
                JustinResourceRequirements.readFromJobGraph(recoveredGraph).get();

        assertThat(recovered.getJobVertices()).containsExactlyInAnyOrder(source, join);
        assertThat(recovered.getParallelism(source).getLowerBound()).isEqualTo(1);
        assertThat(recovered.getParallelism(source).getUpperBound()).isEqualTo(2);
        assertThat(recovered.getResourceProfile(source)).isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(recovered.getParallelism(join).getLowerBound()).isEqualTo(3);
        assertThat(recovered.getParallelism(join).getUpperBound()).isEqualTo(8);
        assertThat(recovered.getResourceProfile(join)).isEqualTo(profile);
    }
}
