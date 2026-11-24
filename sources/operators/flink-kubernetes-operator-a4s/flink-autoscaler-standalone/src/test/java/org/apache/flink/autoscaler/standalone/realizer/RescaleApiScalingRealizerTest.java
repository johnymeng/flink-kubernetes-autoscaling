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

package org.apache.flink.autoscaler.standalone.realizer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.DelayedScaleDown;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingExecutor;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirements;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsBody;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsHeaders;
import org.apache.flink.autoscaler.utils.justin.JustinResourceRequirementsUpdateHeaders;
import org.apache.flink.autoscaler.utils.justin.JustinVertexResourceRequirements;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.autoscaler.standalone.realizer.RescaleApiScalingRealizer.SCALING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link RescaleApiScalingRealizer}. */
class RescaleApiScalingRealizerTest {

    @Test
    void testResourceProfilePreservesExactMemorySizes() {
        var profile =
                ResourceProfile.newBuilder()
                        .setCpuCores(.25)
                        .setTaskHeapMemory(new MemorySize(67108865))
                        .setTaskOffHeapMemory(new MemorySize(4096))
                        .setManagedMemoryMB(2048)
                        .setNetworkMemory(new MemorySize(15728641))
                        .build();
        assertThat(RescaleApiScalingRealizer.parseResourceProfile(profile.toString()))
                .isEqualTo(profile);
    }

    /**
     * Test whether scalingRealizer behaves as expected when the resource is changed or isn't
     * changed.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUpdateResourceRequirements(boolean resourceIsChanged) throws Exception {
        var jobID = new JobID();
        var jobVertex1 = new JobVertexID().toHexString();
        var jobVertex2 = new JobVertexID().toHexString();
        var currentResourceRequirements = Map.of(jobVertex1, "5", jobVertex2, "10");

        var newResourceRequirements = currentResourceRequirements;
        if (resourceIsChanged) {
            newResourceRequirements = Map.of(jobVertex1, "7", jobVertex2, "12");
        }

        var closeFuture = new CompletableFuture<Void>();
        var updatedRequirements = new CompletableFuture<Optional<JobResourceRequirements>>();

        var jobContext =
                createJobAutoScalerContext(
                        jobID,
                        getRestClusterClient(
                                jobID,
                                createResourceRequirements(currentResourceRequirements),
                                updatedRequirements,
                                closeFuture));
        jobContext
                .getConfiguration()
                .set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        RescaleApiScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer =
                new RescaleApiScalingRealizer<>(eventCollector);

        assertThat(updatedRequirements).isNotDone();
        assertThat(closeFuture).isNotDone();
        scalingRealizer.realizeParallelismOverrides(jobContext, newResourceRequirements);

        // The ResourceRequirements should be updated when the newResourceRequirements is changed.
        if (resourceIsChanged) {
            assertThat(updatedRequirements)
                    .isCompletedWithValue(
                            Optional.of(createResourceRequirements(newResourceRequirements)));
            assertThat(eventCollector.events).hasSize(1);
            var event = eventCollector.events.peek();
            assertThat(event.getContext()).isEqualTo(jobContext);
            assertThat(event.getReason()).isEqualTo(SCALING);
        } else {
            assertThat(updatedRequirements).isNotDone();
            assertThat(eventCollector.events).isEmpty();
        }
        assertThat(closeFuture).isDone();
    }

    @ParameterizedTest
    @ValueSource(ints = {1536, 2560})
    void testThroughputRescalePreservesAppliedMemoryBytes(int managedMemoryMiB) throws Exception {
        var jobId = new JobID();
        var vertex = new JobVertexID();
        var managedMemory = new MemorySize(MemorySize.ofMebiBytes(managedMemoryMiB).getBytes() + 5);
        var current =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.25)
                        .setTaskHeapMemory(new MemorySize(1610612737L))
                        .setTaskOffHeapMemory(new MemorySize(257))
                        .setManagedMemory(managedMemory)
                        .setNetworkMemory(new MemorySize(1048577))
                        .build();
        var legacyProfile =
                ResourceProfile.newBuilder().setCpuCores(1).setManagedMemoryMB(343).build();
        var requirements =
                new JustinResourceRequirements(
                        Map.of(
                                vertex,
                                new JustinVertexResourceRequirements(
                                        new JustinVertexResourceRequirements.Parallelism(1, 1),
                                        legacyProfile,
                                        current)));
        var mapper = RestMapperUtils.getStrictObjectMapper();
        var response =
                mapper.readValue(
                        mapper.writeValueAsBytes(new JustinResourceRequirementsBody(requirements)),
                        JustinResourceRequirementsBody.class);
        var legacyResponse =
                new JustinResourceRequirementsBody(
                        JustinResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(vertex, 1, 1, legacyProfile)
                                .build());
        var updated = new CompletableFuture<JustinResourceRequirementsBody>();
        var context =
                createJobAutoScalerContext(
                        jobId,
                        () ->
                                new RestClusterClient<>(
                                        new Configuration(),
                                        "test-cluster",
                                        (c, e) -> new StandaloneClientHAServices("localhost")) {
                                    @Override
                                    public <
                                                    M extends MessageHeaders<R, P, U>,
                                                    U extends MessageParameters,
                                                    R extends RequestBody,
                                                    P extends ResponseBody>
                                            CompletableFuture<P> sendRequest(M h, U p, R r) {
                                        assertThat(
                                                        ((JobMessageParameters) p)
                                                                .jobPathParameter.getValue())
                                                .isEqualTo(jobId);
                                        if (h instanceof JustinResourceRequirementsHeaders) {
                                            var url =
                                                    MessageParameters.resolveUrl(
                                                            h.getTargetRestEndpointURL(), p);
                                            var body =
                                                    url.endsWith("?include-applied-profile=true")
                                                            ? response
                                                            : legacyResponse;
                                            return CompletableFuture.completedFuture(
                                                    h.getResponseClass().cast(body));
                                        }
                                        assertThat(h)
                                                .isInstanceOf(
                                                        JustinResourceRequirementsUpdateHeaders
                                                                .class);
                                        updated.complete((JustinResourceRequirementsBody) r);
                                        return CompletableFuture.completedFuture(null);
                                    }
                                });
        var conf = context.getConfiguration();
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(8192));
        conf.set(AutoScalerOptions.JUSTIN_ENABLED, true);
        conf.set(AutoScalerOptions.A4S_ENABLED, true);
        conf.set(AutoScalerOptions.SCALING_ENABLED, true);
        conf.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, false);
        conf.set(AutoScalerOptions.MEMORY_SCALING_ENABLED, false);
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);

        var vertexMetrics =
                new HashMap<>(
                        Map.of(
                                ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(1),
                                ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(128),
                                ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(0),
                                ScalingMetric.TARGET_DATA_RATE,
                                        new EvaluatedScalingMetric(110, 110),
                                ScalingMetric.TRUE_PROCESSING_RATE,
                                        new EvaluatedScalingMetric(100, 100),
                                ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0),
                                ScalingMetric.ROCKS_DB_BLOCK_CACHE_HIT_RATE,
                                        EvaluatedScalingMetric.of(0)));
        vertexMetrics.put(ScalingMetric.SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(125));
        vertexMetrics.put(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(200));
        var globalMetrics =
                Map.of(
                        ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.HEAP_MEMORY_USED, EvaluatedScalingMetric.avg(0),
                        ScalingMetric.MANAGED_MEMORY_USED, EvaluatedScalingMetric.avg(0),
                        ScalingMetric.METASPACE_MEMORY_USED, EvaluatedScalingMetric.avg(0));
        var events = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        var stateStore = new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>();
        assertThat(
                        new ScalingExecutor<>(events, stateStore)
                                .scaleResource(
                                        context,
                                        new EvaluatedMetrics(
                                                Map.of(vertex, vertexMetrics),
                                                globalMetrics,
                                                Map.of()),
                                        new HashMap<>(),
                                        new ScalingTracking(),
                                        Instant.now(),
                                        new JobTopology(new VertexInfo(vertex, Map.of(), 1, 128)),
                                        new DelayedScaleDown()))
                .isTrue();

        new RescaleApiScalingRealizer<>(events)
                .realizeParallelismOverrides(
                        context,
                        stateStore.getParallelismOverrides(context),
                        stateStore.getResourceProfileOverrides(context));
        assertThat(updated).isDone();
        var sent =
                mapper.readValue(
                                mapper.writeValueAsBytes(updated.join()),
                                JustinResourceRequirementsBody.class)
                        .asJustinResourceRequirements()
                        .orElseThrow();
        assertThat(sent.getParallelism(vertex).getUpperBound()).isGreaterThan(1);
        assertThat(sent.getResourceProfile(vertex).getManagedMemory().getBytes())
                .isEqualTo(managedMemory.getBytes());
        assertThat(sent.getResourceProfile(vertex)).isEqualTo(current);
    }

    @Test
    void testDisableAdaptiveScheduler() throws Exception {
        var jobID = new JobID();
        var jobVertex1 = new JobVertexID().toHexString();
        var jobVertex2 = new JobVertexID().toHexString();
        var resourceRequirements = Map.of(jobVertex1, "5", jobVertex2, "10");

        var jobContext =
                createJobAutoScalerContext(
                        jobID,
                        () ->
                                fail(
                                        "The rest client shouldn't be created if the adaptive scheduler is disable."));

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        RescaleApiScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer =
                new RescaleApiScalingRealizer<>(eventCollector);

        scalingRealizer.realizeParallelismOverrides(jobContext, resourceRequirements);
        assertThat(eventCollector.events).isEmpty();
    }

    @Test
    void testJobNotRunning() throws Exception {
        var jobID = new JobID();
        var jobVertex1 = new JobVertexID().toHexString();
        var jobVertex2 = new JobVertexID().toHexString();
        var resourceRequirements = Map.of(jobVertex1, "5", jobVertex2, "10");

        var conf = new Configuration();
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        var jobContext =
                new JobAutoScalerContext<>(
                        jobID,
                        jobID,
                        JobStatus.CANCELLING,
                        conf,
                        null,
                        () ->
                                fail(
                                        "The rest client shouldn't be created if the job isn't running."));

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        RescaleApiScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer =
                new RescaleApiScalingRealizer<>(eventCollector);

        scalingRealizer.realizeParallelismOverrides(jobContext, resourceRequirements);
        assertThat(eventCollector.events).isEmpty();
    }

    private static JobAutoScalerContext<JobID> createJobAutoScalerContext(
            JobID jobID,
            SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier) {
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, new Configuration(), null, restClientSupplier);
    }

    private JobResourceRequirements createResourceRequirements(
            Map<String, String> parallelismOverrides) {
        Map<JobVertexID, JobVertexResourceRequirements> vertexResources = new HashMap<>();
        parallelismOverrides.forEach(
                (key, value) ->
                        vertexResources.put(
                                JobVertexID.fromHexString(key),
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(
                                                1, Integer.parseInt(value)))));
        return new JobResourceRequirements(vertexResources);
    }

    private static SupplierWithException<RestClusterClient<String>, Exception> getRestClusterClient(
            JobID expectedJobID,
            JobResourceRequirements currentRequirements,
            CompletableFuture<Optional<JobResourceRequirements>> updatedRequirements,
            CompletableFuture<Void> closeFuture) {

        return () ->
                new RestClusterClient<>(
                        new Configuration(),
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {

                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(M h, U p, R r) {
                        if (h instanceof JobResourceRequirementsHeaders) {
                            if (expectedJobID.equals(
                                    ((JobMessageParameters) p).jobPathParameter.getValue())) {
                                return (CompletableFuture<P>)
                                        CompletableFuture.completedFuture(
                                                new JobResourceRequirementsBody(
                                                        currentRequirements));
                            }
                        } else if (r instanceof JobResourceRequirementsBody) {
                            if (expectedJobID.equals(
                                    ((JobMessageParameters) p).jobPathParameter.getValue())) {
                                updatedRequirements.complete(
                                        ((JobResourceRequirementsBody) r)
                                                .asJobResourceRequirements());
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        fail("Unknown request");
                        return null;
                    }

                    @Override
                    public void close() {
                        super.close();
                        closeFuture.complete(null);
                    }
                };
    }
}
