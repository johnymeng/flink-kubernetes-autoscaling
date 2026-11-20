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
import org.apache.flink.runtime.metrics.A4SHistogramMetricCodec;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpDeserializer;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpSerializer;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricSerializationResult;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SCacheCostSample;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                .toMetricString().replace("\"version\":1", "\"version\":2");
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
