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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservation.Status;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionHistory;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails.CurrentAttempts;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.A4SHistogramMetricCodec;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpDeserializer;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpSerializer;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricSerializationResult;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SCacheCostSample;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheObservationTransportTest {
    private final JobID job = new JobID();
    private final JobVertexID vertex = new JobVertexID();
    private final ExecutionGraphID graph = new ExecutionGraphID();
    private final MetricStore store = new MetricStore();
    private final A4SAggregatingVertexMetricsHandler handler =
            new A4SAggregatingVertexMetricsHandler(
                    () -> CompletableFuture.completedFuture(null),
                    Time.seconds(1),
                    Collections.emptyMap(),
                    Runnable::run,
                    new MetricFetcher() {
                        @Override
                        public void update() {}

                        @Override
                        public MetricStore getMetricStore() {
                            return store;
                        }

                        @Override
                        public long getLastUpdateTime() {
                            return 0;
                        }
                    });

    @Test
    void roundTripsCompressedObservationThroughMetricDumpAndStrictRestJson() throws Exception {
        long[] bins = new long[40_000];
        for (int index = 0; index < bins.length; index += 19) {
            bins[index] = 37;
        }
        String raw = observation(0, 0, bins, System.currentTimeMillis()).toMetricString();
        assertTrue(raw.length() > 65_535);
        String wire = A4SHistogramMetricCodec.encode(raw);
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, wire);
        A4SAggregatedMetricsResponseBody response = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.VALID, response.getCacheObservations().status);
        assertEquals(wire.length(), response.getCacheObservations().wireBytes);
        assertEquals(bins.length, response.getScaledMrc().getPoints().size());
        assertEquals(raw, response.getCacheObservations().subtasks.get(0).toMetricString());
    }

    @Test
    void transportsOnePairedEndpointForTheCurrentAttemptAndRejectsInvalidAcquisition() throws Exception {
        long now = System.currentTimeMillis();
        long[] bins = new long[40_000];
        bins[0] = 7;
        CacheObservation original = observation(0, 0, bins, now);
        CacheObservation paired = paired(original, now, 1, 3);
        String wire = A4SHistogramMetricCodec.encode(paired.toMetricString());
        assertTrue(wire.startsWith("a4s-gzip-v1:"));
        assertTrue(!A4SHistogramMetricCodec.isOmitted(wire));
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, wire);
        A4SAggregatedMetricsResponseBody matched = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.VALID, matched.getCacheObservations().status);
        assertEquals(1, matched.getCacheCostSamples().size());
        CacheObservation.Endpoint endpoint = matched.getCacheObservations().subtasks.get(0).endpoint;
        assertEquals(40_000, endpoint.cumulativeHistogram.getNumBuckets());
        assertEquals(3L, endpoint.ownershipGeneration);
        assertEquals(1_000L, endpoint.totalLookups);
        assertEquals(Status.INCOMPLETE, response(expectedAttempts(0, 0, 1, 2, 3)).getCacheObservations().status);
        assertEquals(Status.ABSENT, response(Collections.singletonMap(0, attempt(0, 1))).getCacheObservations().status);

        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME,
                A4SHistogramMetricCodec.encode(paired(original, now, 100_000_000L, 3).toMetricString()));
        A4SAggregatedMetricsResponseBody skewed = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.SKEWED, skewed.getCacheObservations().status);
        assertTrue(skewed.getCacheCostSamples().isEmpty());

        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME,
                A4SHistogramMetricCodec.encode(paired(original, now, 1, 2).toMetricString()));
        A4SAggregatedMetricsResponseBody incompatible = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.INCOMPATIBLE, incompatible.getCacheObservations().status);
        assertTrue(incompatible.getCacheCostSamples().isEmpty());

        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, wire);
        addGauge(0, 0, "another-db", A4SCacheCostSample.METRIC_NAME, paired.endpoint.timing);
        A4SAggregatedMetricsResponseBody unmatched = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.DUPLICATE, unmatched.getCacheObservations().status);
        assertTrue(unmatched.getCacheCostSamples().isEmpty());
        addGauge(0, 0, "another-db", A4SCacheCostSample.METRIC_NAME, "");
        addGauge(0, 0, "another-db", CacheObservation.METRIC_NAME, wire);
        A4SAggregatedMetricsResponseBody duplicate = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.DUPLICATE, duplicate.getCacheObservations().status);
        assertTrue(duplicate.getCacheCostSamples().isEmpty());
    }

    @Test
    void carriesObservedSlotBytesWithoutInferringThemFromTheRequestedProfile() throws Exception {
        long now = System.currentTimeMillis();
        CacheObservation base = paired(observation(0, 0, new long[] {7}, now), now, 1, 3);
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, base.toMetricString());
        CacheObservation old = response(expectedAttempts(0, 0)).getCacheObservations().subtasks.get(0);
        assertNull(old.endpoint.appliedSlot);

        CacheObservation.Endpoint prior = base.endpoint;
        CacheObservation.AppliedSlot slot = new CacheObservation.AppliedSlot(
                "0123456789abcdef0123456789abcdef", (192L << 20) + 37);
        CacheObservation withReceipt = new CacheObservation(
                base.version, base.job, base.vertex, base.subtask, base.attempt,
                base.status, base.window, base.histogram, new CacheObservation.Endpoint(
                        prior.ownershipGeneration, prior.acquisitionStartNanos, prior.nativeNanos,
                        prior.acquisitionEndNanos, prior.totalLookups, prior.totalMisses,
                        prior.cumulativeHistogram, prior.timing, slot));
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, withReceipt.toMetricString());
        CacheObservation live = response(expectedAttempts(0, 0)).getCacheObservations().subtasks.get(0);
        assertEquals(base.attempt, live.attempt);
        assertEquals(base.window.cacheId, live.window.cacheId);
        assertEquals(base.window.epoch, live.window.epoch);
        assertEquals(base.window.capacityBytes, live.window.capacityBytes);
        assertEquals(slot.allocationId, live.endpoint.appliedSlot.allocationId);
        assertEquals(slot.managedMemoryBytes, live.endpoint.appliedSlot.managedMemoryBytes);
    }

    @Test
    void preservesPairedTaskDataWhenOnlyPooledLookupRateIsUnsupported() throws Exception {
        long now = System.currentTimeMillis();
        for (int subtask = 0; subtask < 2; subtask++) {
            CacheObservation original = observation(subtask, 0, new long[] {7}, now);
            CacheObservation.Window old = original.window;
            CacheObservation observation = new CacheObservation(
                    original.version, original.job, original.vertex, subtask, original.attempt,
                    Status.VALID,
                    new CacheObservation.Window(
                            old.process, old.cacheId, old.epoch, old.sequence, old.owners,
                            old.population, old.chargePolicy, old.cachePolicy, old.samplingRate,
                            old.capacityBytes, old.startNanos, old.endNanos, now,
                            1_000L / (subtask + 1), 100L),
                    original.histogram);
            addGauge(subtask, 0, "rocksdb", CacheObservation.METRIC_NAME,
                    paired(observation, now, 1, 3).toMetricString());
        }
        A4SAggregatedMetricsResponseBody withRateSkew = response(expectedAttempts(0, 0, 1));
        assertEquals(Status.AGGREGATE_RATE_SKEW, withRateSkew.getCacheObservations().status);
        assertTrue(withRateSkew.getScaledMrc().getPoints().isEmpty());
        assertTrue(withRateSkew.getCacheObservations().hasPairedLocalObservations());
        assertEquals(2, withRateSkew.getCacheCostSamples().size());

        A4SAggregatedMetricsResponseBody missing = response(expectedAttempts(0, 0, 1, 2));
        assertEquals(Status.INCOMPLETE, missing.getCacheObservations().status);
        assertTrue(missing.getCacheCostSamples().isEmpty());
    }

    private CacheObservation paired(CacheObservation row, long now, long afterNanos, int abi) {
        long nativeNanos = row.window.endNanos;
        String timing = abi == 3
                ? "3:" + now + ":" + row.attempt
                        + ":" + row.window.cacheId
                        + ":1048576:0:1000:300:700:300:8:4:1800:24000:400:82.0:23:179:process:"
                        + nativeNanos
                : "1:" + now + ":" + row.attempt
                        + ":1:1048576:0:1000:300:700:300:8:4:1800:24000:400:82.0";
        return new CacheObservation(CacheObservation.ALIGNED_VERSION, row.job, row.vertex,
                row.subtask, row.attempt, row.status, row.window, row.histogram,
                new CacheObservation.Endpoint(
                        3, nativeNanos - 1, nativeNanos, nativeNanos + afterNanos,
                        1_000, 300, row.histogram, timing));
    }

    @Test
    void optionalNativeBudgetSurvivesTransportWithoutChangingCacheOrTimingIdentity() {
        long now = System.currentTimeMillis();
        CacheObservation row = paired(observation(0, 0, new long[] {7}, now), now, 1, 3);
        assertNull(CacheObservation.fromMetricString(row.toMetricString()).endpoint.writeBufferBudget);
        for (int version : new int[] {1, 2}) {
            CacheObservation.Endpoint nativeEndpoint = new CacheObservation.Endpoint(
                    row.endpoint.ownershipGeneration, row.endpoint.acquisitionStartNanos,
                    row.endpoint.nativeNanos, row.endpoint.acquisitionEndNanos,
                    row.endpoint.totalLookups, row.endpoint.totalMisses,
                    row.endpoint.cumulativeHistogram, row.endpoint.timing, null,
                    new CacheObservation.WriteBufferBudget(version, 67_108_864L));
            CacheObservation observed = new CacheObservation(row.version, row.job, row.vertex,
                    row.subtask, row.attempt, row.status, row.window, row.histogram, nativeEndpoint);
            CacheObservation decoded = CacheObservation.fromMetricString(observed.toMetricString());
            assertEquals(row.version, decoded.version);
            assertEquals(row.window.capacityBytes, decoded.window.capacityBytes);
            assertEquals(row.endpoint.timing, decoded.endpoint.timing);
            assertEquals(67_108_864L, decoded.endpoint.writeBufferBudget.bytes);
            assertEquals(version == 1, decoded.endpoint.writeBufferBudget.supportsReadback());
        }
    }

    @Test
    void delayedPreResizeRosterCannotRemoveReplacementTimingEndpoint() throws Exception {
        assertReplacementSurvivesRoster(2, false, false);
    }

    @Test
    void replacementDumpThatArrivesBeforeItsRosterStaysAvailable() throws Exception {
        assertReplacementSurvivesRoster(1, false, false);
    }

    @Test
    void rosterFailureDoesNotDiscardSuccessfulReplacementDump() throws Exception {
        assertReplacementSurvivesRoster(1, true, false);
    }

    @Test
    void synchronousQueryFailureStillWaitsForTheOutstandingRoster() throws Exception {
        assertReplacementSurvivesRoster(1, false, true);
    }

    private void assertReplacementSurvivesRoster(
            int replacementQuery, boolean rosterFails, boolean firstQueryThrows) throws Exception {
        long now = System.currentTimeMillis();
        CacheObservation replacement = paired(observation(1, 1, new long[] {7}, now), now, 1, 3);
        CacheObservation existingSubtask = paired(observation(0, 1, new long[] {7}, now), now, 1, 3);
        CompletableFuture<MultipleJobsDetails> oldRoster = new CompletableFuture<MultipleJobsDetails>();
        AtomicInteger rosterRequests = new AtomicInteger();
        AtomicInteger queryRequests = new AtomicInteger();
        AtomicInteger discoveryRequests = new AtomicInteger();
        ResourceID taskManager = ResourceID.generate();
        RestfulGateway gateway = (RestfulGateway) Proxy.newProxyInstance(
                RestfulGateway.class.getClassLoader(), new Class<?>[] {RestfulGateway.class},
                (ignored, method, args) -> {
                    switch (method.getName()) {
                        case "requestMultipleJobDetails":
                            return rosterRequests.incrementAndGet() == 1
                                    ? oldRoster : CompletableFuture.completedFuture(roster(1));
                        case "requestMetricQueryServiceAddresses":
                            return CompletableFuture.completedFuture(Collections.emptyList());
                        case "requestTaskManagerMetricQueryServiceAddresses":
                            if (discoveryRequests.incrementAndGet() == 1 && firstQueryThrows) {
                                throw new IllegalStateException("fixture discovery failure");
                            }
                            return CompletableFuture.completedFuture(Collections.singletonList(Tuple2.of(taskManager, "tm")));
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        MetricQueryServiceGateway service = new MetricQueryServiceGateway() {
            @Override
            public String getAddress() {
                return "tm";
            }

            @Override
            public String getHostname() {
                return "tm";
            }

            @Override
            public CompletableFuture<MetricDumpSerialization.MetricSerializationResult> queryMetrics(
                    Time timeout) {
                MetricDumpSerializer serializer = new MetricDumpSerializer();
                try {
                    if (queryRequests.incrementAndGet() == replacementQuery) {
                        Gauge<String> gauge = () -> A4SHistogramMetricCodec.encode(replacement.toMetricString());
                        QueryScopeInfo.OperatorQueryScopeInfo scope = new QueryScopeInfo.OperatorQueryScopeInfo(
                                job.toString(), vertex.toString(), 1, 1, "operator", "rocksdb");
                        Gauge<String> otherGauge = () ->
                                A4SHistogramMetricCodec.encode(existingSubtask.toMetricString());
                        QueryScopeInfo.OperatorQueryScopeInfo otherScope = new QueryScopeInfo.OperatorQueryScopeInfo(
                                job.toString(), vertex.toString(), 0, 1, "operator", "rocksdb");
                        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
                        gauges.put(gauge, Tuple2.of(scope, CacheObservation.METRIC_NAME));
                        gauges.put(otherGauge, Tuple2.of(otherScope, CacheObservation.METRIC_NAME));
                        return CompletableFuture.completedFuture(serializer.serialize(
                                Collections.emptyMap(), gauges, Collections.emptyMap(), Collections.emptyMap()));
                    }
                    return CompletableFuture.completedFuture(
                            serializer.serialize(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
                } finally {
                    serializer.close();
                }
            }
        };
        MetricFetcherImpl<RestfulGateway> fetcher = new MetricFetcherImpl<>(
                () -> CompletableFuture.completedFuture(gateway),
                ignored -> CompletableFuture.completedFuture(service), Runnable::run, Time.seconds(1), 1);
        fetcher.update();
        nextFetch(fetcher);
        assertNull(fetcher.getMetricStore().getSubtaskAttemptMetricStore(
                job.toString(), vertex.toString(), 1, 1));
        assertEquals(1, rosterRequests.get());
        assertEquals(firstQueryThrows ? 0 : 1, queryRequests.get());
        if (rosterFails) {
            oldRoster.completeExceptionally(new java.util.concurrent.TimeoutException("fixture"));
        } else {
            oldRoster.complete(roster(0));
        }
        nextFetch(fetcher);
        A4SAggregatingVertexMetricsHandler rest = new A4SAggregatingVertexMetricsHandler(
                () -> CompletableFuture.completedFuture(gateway), Time.seconds(1), Collections.emptyMap(),
                Runnable::run, fetcher);
        A4SAggregatedMetricsResponseBody response =
                rest.processVersionedRequest(job.toString(), vertex.toString(),
                        expectedAttempts(1, 0, 1), true);
        assertEquals(Status.VALID, response.getCacheObservations().status);
        assertEquals(2, response.getCacheCostSamples().size());
        assertEquals(replacement.attempt, response.getCacheCostSamples().get(1).attempt);
        assertEquals(replacement.window.sequence, response.getCacheObservations().subtasks.get(1).window.sequence);
    }

    private MultipleJobsDetails roster(int attemptNumber) {
        CurrentAttempts active = new CurrentAttempts(attemptNumber, Collections.singleton(attemptNumber), false);
        Map<Integer, CurrentAttempts> subtasks = new HashMap<>();
        subtasks.put(0, active);
        if (attemptNumber != 0) {
            subtasks.put(1, active);
        }
        return new MultipleJobsDetails(Collections.singletonList(new JobDetails(
                job, "job", 0, 0, 0, JobStatus.RUNNING, 0,
                new int[ExecutionState.values().length], subtasks.size(),
                Collections.singletonMap(vertex.toString(), subtasks))));
    }

    private static void nextFetch(MetricFetcherImpl<?> fetcher) throws Exception {
        // Advance the scheduling gate without sleeping or changing production timeouts.
        Field lastUpdate = MetricFetcherImpl.class.getDeclaredField("lastUpdateTime");
        lastUpdate.setAccessible(true);
        lastUpdate.setLong(fetcher, 0L);
        fetcher.update();
    }

    @Test
    void restRequestUsesEveryCurrentAttemptFromExecutionGraph() throws Exception {
        long now = System.currentTimeMillis();
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME,
                observation(0, 0, new long[] {1}, now).toMetricString());
        addGauge(1, 0, "rocksdb", CacheObservation.METRIC_NAME,
                observation(1, 0, new long[] {1}, now).toMetricString());

        ArchivedExecutionJobVertex currentVertex = new ArchivedExecutionJobVertex(
                new ArchivedExecutionVertex[] {task(0, 0), task(1, 1)},
                vertex, "vertex", 2, 2, ResourceProfile.UNKNOWN, new StringifiedAccumulatorResult[0]);
        ArchivedExecutionGraph currentGraph = new ArchivedExecutionGraph(
                job, "job", Collections.singletonMap(vertex, currentVertex), Collections.singletonList(currentVertex),
                new long[JobStatus.values().length], JobStatus.RUNNING, null, "{}",
                new StringifiedAccumulatorResult[0], Collections.emptyMap(), new ExecutionConfig().archive(),
                false, null, null, null, null, null, null);
        RestfulGateway gateway = (RestfulGateway) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {RestfulGateway.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("requestJob")) {
                        return CompletableFuture.completedFuture(currentGraph);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        Map<String, String> path = new HashMap<>();
        path.put(JobIDPathParameter.KEY, job.toString());
        path.put(JobVertexIdPathParameter.KEY, vertex.toString());
        HandlerRequest<EmptyRequestBody> request = HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                handler.getMessageHeaders().getUnresolvedMessageParameters(),
                path,
                Collections.singletonMap("get", Collections.singletonList("cacheObservations")),
                Collections.emptyList());
        assertEquals(Status.INCOMPLETE, handler.handleRequest(request, gateway).get().getCacheObservations().status);
        addGauge(1, 1, "rocksdb", CacheObservation.METRIC_NAME,
                observation(1, 1, new long[] {1}, now).toMetricString());
        assertEquals(Status.VALID, handler.handleRequest(request, gateway).get().getCacheObservations().status);
    }

    private ArchivedExecutionVertex task(int index, int number) {
        ArchivedExecution result = new ArchivedExecution(
                new StringifiedAccumulatorResult[0], null, attempt(index, number),
                ExecutionState.RUNNING, null, null, null,
                new long[ExecutionState.values().length], new long[ExecutionState.values().length]);
        return new ArchivedExecutionVertex(index, "task", result, new ExecutionHistory(0));
    }

    @Test
    void badInputsInvalidateOnlyTheirVertexAndPreserveIndependentTiming() throws Exception {
        long now = System.currentTimeMillis();
        String raw = observation(0, 0, new long[] {10, 20, 30}, now).toMetricString();
        assertEquals(raw, A4SHistogramMetricCodec.encode(raw));
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, raw);
        addGauge(
                0, 0, "rocksdb", A4SCacheCostSample.METRIC_NAME,
                "3:" + now + ":" + attempt(0, 0)
                        + ":1:1048576:0:1000:300:700:300:8:4:1800:24000:400:82.0:23:179:process:9000");
        Map<Integer, ExecutionAttemptID> expected = new HashMap<>();
        expected.put(0, attempt(0, 0));
        expected.put(1, attempt(1, 0));
        assertEquals(Status.INCOMPLETE, response(expected).getCacheObservations().status);
        assertEquals(Status.ABSENT, response(Collections.singletonMap(0, attempt(0, 1))).getCacheObservations().status);
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, "a4s-gzip-v1:AQID");
        A4SAggregatedMetricsResponseBody malformed = response(Collections.singletonMap(0, attempt(0, 0)));
        assertEquals(Status.MALFORMED, malformed.getCacheObservations().status);
        assertTrue(malformed.getScaledMrc().getPoints().isEmpty());
        JsonNode json = RestMapperUtils.getStrictObjectMapper().valueToTree(malformed);
        assertEquals(1, json.get("cacheCostSamples").size());
        JsonNode timing = json.get("cacheCostSamples").get(0);
        assertEquals("process", timing.get("process").asText());
        assertEquals(9000, timing.get("observedAtNanos").asLong());
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, "a4s-gzip-v1:omitted");
        assertEquals(Status.OVERSIZE, response(Collections.singletonMap(0, attempt(0, 0))).getCacheObservations().status);
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME,
                observation(0, 0, new long[] {1}, now - 31_000).toMetricString());
        assertEquals(Status.STALE, response(Collections.singletonMap(0, attempt(0, 0))).getCacheObservations().status);
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, raw);
        assertEquals(Status.VALID, response(Collections.singletonMap(0, attempt(0, 0))).getCacheObservations().status);
    }

    @Test
    void legacyAndUnknownSendersCannotProvideVersionedObservations() throws Exception {
        String legacy = new StackDistanceHistogram(new long[] {1}, 1, 1, 1, 4096).toMetricString();
        addGauge(0, 0, "rocksdb", "stack-distance-histogram", legacy);
        assertEquals(Status.UNSUPPORTED_VERSION,
                response(Collections.singletonMap(0, attempt(0, 0))).getCacheObservations().status);
        String unknown = observation(0, 0, new long[] {1}, System.currentTimeMillis())
                .toMetricString().replace("\"version\":1", "\"version\":99");
        addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, unknown);
        assertEquals(Status.UNSUPPORTED_VERSION,
                response(Collections.singletonMap(0, attempt(0, 0))).getCacheObservations().status);
    }

    @Test
    void missingAndDuplicateFieldsAreMalformed() throws Exception {
        String valid =
                observation(0, 0, new long[] {1}, System.currentTimeMillis()).toMetricString();
        for (String raw : Arrays.asList(
                valid.replace("\"subtask\":0,", ""),
                valid.replace("\"misses\":100000", "\"misses\":null"),
                valid.replace("\"epoch\":0", "\"epoch\":0,\"epoch\":0"))) {
            addGauge(0, 0, "rocksdb", CacheObservation.METRIC_NAME, raw);
            assertEquals(Status.MALFORMED,
                    response(Collections.singletonMap(0, attempt(0, 0))).getCacheObservations().status);
        }
    }

    @Test
    void countsModifiedUtfBytesAndRejectsTruncatedGzip() {
        char[] characters = new char[33_000];
        Arrays.fill(characters, 'é');
        String raw = new String(characters);
        String encoded = A4SHistogramMetricCodec.encode(raw);
        assertTrue(encoded.startsWith("a4s-gzip-v1:"));
        assertEquals(raw, A4SHistogramMetricCodec.decode(encoded));
        assertThrows(IllegalArgumentException.class,
                () -> A4SHistogramMetricCodec.decode(encoded.substring(0, encoded.length() - 8)));
    }

    private Map<Integer, ExecutionAttemptID> expectedAttempts(int number, int... subtasks) {
        Map<Integer, ExecutionAttemptID> attempts = new HashMap<>();
        for (int subtask : subtasks) {
            attempts.put(subtask, attempt(subtask, number));
        }
        return attempts;
    }

    private ExecutionAttemptID attempt(int subtask, int number) {
        return new ExecutionAttemptID(graph, new ExecutionVertexID(vertex, subtask), number);
    }

    private CacheObservation observation(int subtask, int number, long[] bins, long now) {
        return new CacheObservation(
                CacheObservation.VERSION,
                job.toString(),
                vertex.toString(),
                subtask,
                attempt(subtask, number).toString(),
                Status.VALID,
                new CacheObservation.Window(
                        "process", subtask + 1, 0, 2, Collections.singletonList(1L), CacheObservation.QUICK_MRC,
                        CacheObservation.FIXED_4K, CacheObservation.CACHE_POLICY, .02, 1_048_576,
                        1_000, 10_000_001_000L, now, 1_000_000, 100_000),
                new StackDistanceHistogram(bins, 100, 1, 1, 4096));
    }

    private void addGauge(int subtask, int number, String scope, String name, String value)
            throws Exception {
        QueryScopeInfo scopeInfo = new QueryScopeInfo.OperatorQueryScopeInfo(
                job.toString(), vertex.toString(), subtask, number, "operator", scope);
        Gauge<String> gauge = () -> value;
        MetricDumpSerializer serializer = new MetricDumpSerializer();
        try {
            MetricSerializationResult dump = serializer.serialize(
                    Collections.emptyMap(), Collections.singletonMap(gauge, Tuple2.of(scopeInfo, name)), Collections.emptyMap(), Collections.emptyMap());
            assertEquals(1, dump.numGauges);
            new MetricDumpDeserializer().deserialize(dump).forEach(store::add);
        } finally {
            serializer.close();
        }
    }

    private A4SAggregatedMetricsResponseBody response(Map<Integer, ExecutionAttemptID> expected)
            throws Exception {
        ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        A4SAggregatedMetricsResponseBody response = handler.processVersionedRequest(job.toString(), vertex.toString(), expected, true);
        return mapper.readValue(mapper.writeValueAsBytes(response), A4SAggregatedMetricsResponseBody.class);
    }
}
