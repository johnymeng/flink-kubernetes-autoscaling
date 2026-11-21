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
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

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
