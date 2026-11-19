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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/** Verifies that savepoint replacements receive the controller's exact resource profiles. */
public class JobVertexResourceProfileOverridesTest {
    private static final String ENABLED =
            "pipeline.jobvertex-resourceprofile-overrides-on-submit.enabled";
    private static final String PROFILES = "pipeline.jobvertex-resourceprofile-overrides";

    private static ResourceProfile profile(long managedBytes) {
        return ResourceProfile.newBuilder()
                .setCpuCores(1.5)
                .setTaskHeapMemoryMB(134)
                .setTaskOffHeapMemoryMB(0)
                .setManagedMemory(new MemorySize(managedBytes))
                .setNetworkMemoryMB(39)
                .build();
    }

    @Test
    public void leavesExistingSlotSharingUntouchedUnlessEnabled() {
        JobVertex vertex = new JobVertex("source");
        SlotSharingGroup original = new SlotSharingGroup();
        vertex.setSlotSharingGroup(original);
        JobGraph graph = new JobGraph(new JobID(), "disabled", vertex);

        JobVertexResourceProfileOverrides.apply(
                graph,
                Configuration.fromMap(
                        Map.of(PROFILES, vertex.getID() + ":" + profile(17))));

        assertSame(original, vertex.getSlotSharingGroup());
    }

    @Test
    public void appliesExactMemoryAndPreservesOtherVertices() {
        JobVertex vertex = new JobVertex("join");
        JobVertex other = new JobVertex("source");
        SlotSharingGroup original = new SlotSharingGroup();
        vertex.setSlotSharingGroup(original);
        other.setSlotSharingGroup(original);
        vertex.setParallelism(3);
        JobGraph graph = new JobGraph(new JobID(), "enabled", vertex, other);
        ResourceProfile expected = profile(17_000_001);
        Configuration conf =
                Configuration.fromMap(
                        Map.of(
                                ENABLED, "true",
                                PROFILES, encode(
                                        Map.of(vertex.getID().toHexString(), expected.toString()))));

        JobVertexResourceProfileOverrides.apply(graph, conf);

        assertEquals(expected, vertex.getSlotSharingGroup().getResourceProfile());
        assertEquals(3, vertex.getParallelism());
        assertSame(original, other.getSlotSharingGroup());
    }

    @Test
    public void validatesTheWholeRequestBeforeChangingAnyVertex() {
        JobVertex vertex = new JobVertex("join");
        SlotSharingGroup original = new SlotSharingGroup();
        vertex.setSlotSharingGroup(original);
        JobGraph graph = new JobGraph(new JobID(), "invalid", vertex);
        Configuration conf =
                Configuration.fromMap(
                        Map.of(
                                ENABLED, "true",
                                PROFILES, encode(
                                        Map.of(
                                                vertex.getID().toString(), profile(100).toString(),
                                                new JobVertexID().toString(), profile(200).toString()))));

        assertThrows(
                IllegalArgumentException.class,
                () -> JobVertexResourceProfileOverrides.apply(graph, conf));
        assertSame(original, vertex.getSlotSharingGroup());
    }

    private static String encode(Map<String, String> profiles) {
        return ConfigurationUtils.convertValue(profiles, String.class);
    }
}
