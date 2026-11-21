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

package org.apache.flink.autoscaler;

import org.apache.flink.autoscaler.a4s.OnlineCacheCost.Reason;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics.CacheAction;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Value;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/** The concrete Justin request used by validation, history and realization. */
@Value
public class FinalScalingAction {
    private static final long BYTES_PER_MIB = 1L << 20;

    String decisionId;
    Instant requestedAt;
    CacheAction mode;
    Map<JobVertexID, VertexRequest> vertices;
    Map<String, String> configOverrides;
    Set<String> configRemovals;

    public FinalScalingAction(
            String decisionId, Instant requestedAt, CacheAction mode,
            Map<JobVertexID, VertexRequest> vertices, ConfigChanges changes) {
        this.decisionId = decisionId;
        this.requestedAt = requestedAt;
        this.mode = mode;
        this.vertices = Map.copyOf(vertices);
        this.configOverrides = Map.copyOf(changes.getOverrides());
        this.configRemovals = Set.copyOf(changes.getRemovals());
    }

    ConfigChanges configChanges() {
        var changes = new ConfigChanges();
        changes.getOverrides().putAll(configOverrides);
        changes.getRemovals().addAll(configRemovals);
        return changes;
    }

    @Value
    public static class VertexRequest {
        int currentParallelism;
        int parallelism;
        ResourceProfile profile;
        @Nullable Double requiredRecordsPerSecond;
        @Nullable Long nominalNativeCacheBytes;
        @Nullable Reason cacheReason;
    }

    public Map<String, String> getParallelismOverrides() {
        return overrideMap(vertex -> Integer.toString(vertex.getParallelism()));
    }

    public Map<String, String> getStartingParallelism() {
        return overrideMap(vertex -> Integer.toString(vertex.getCurrentParallelism()));
    }

    public Map<String, String> getResourceProfileOverrides() {
        return overrideMap(vertex -> encodeProfile(vertex.getProfile()));
    }

    public boolean changesParallelism() {
        return vertices.values().stream()
                .anyMatch(vertex -> vertex.getCurrentParallelism() != vertex.getParallelism());
    }

    private Map<String, String> overrideMap(Function<VertexRequest, String> value) {
        var overrides = new TreeMap<String, String>();
        vertices.forEach((id, vertex) -> overrides.put(id.toHexString(), value.apply(vertex)));
        return Collections.unmodifiableMap(overrides);
    }

    static boolean canEncode(ResourceProfile profile) {
        return !ResourceProfile.UNKNOWN.equals(profile) && !ResourceProfile.ANY.equals(profile)
                && profile.getExtendedResources().isEmpty()
                && profile.getCpuCores().getValue().signum() > 0
                && List.of(profile.getTaskHeapMemory(), profile.getTaskOffHeapMemory(),
                                profile.getManagedMemory(), profile.getNetworkMemory())
                        .stream().allMatch(memory -> memory.getBytes() % BYTES_PER_MIB == 0
                                && memory.getMebiBytes() <= Integer.MAX_VALUE);
    }

    static String encodeProfile(ResourceProfile profile) {
        return "ResourceProfile{cpuCores=" + profile.getCpuCores().getValue()
                + ", taskHeapMemory=" + formatMemory(profile.getTaskHeapMemory())
                + ", taskOffHeapMemory=" + formatMemory(profile.getTaskOffHeapMemory())
                + ", managedMemory=" + formatMemory(profile.getManagedMemory())
                + ", networkMemory=" + formatMemory(profile.getNetworkMemory()) + "}";
    }

    private static String formatMemory(MemorySize memory) {
        return memory.getMebiBytes() + ".000mb (" + memory.getBytes() + " bytes)";
    }
}
