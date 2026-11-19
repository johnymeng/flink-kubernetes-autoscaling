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

import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.A4SHistogramMetricCodec;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link A4SAggregatingVertexMetricsHandler}. */
public class A4SAggregatingVertexMetricsHandlerTest extends TestLogger {

    private static final Time TIMEOUT = Time.milliseconds(50L);

    @Mock private RestfulGateway restfulGateway;

    private JobID jobId;
    private JobVertexID jobVertexId;
    private MetricFetcher metricFetcher;
    private MetricStore metricStore;
    private A4SAggregatingVertexMetricsHandler handler;
    private AutoCloseable mocks;

    @Before
    public void setUp() {
        mocks = MockitoAnnotations.openMocks(this);

        jobId = JobID.generate();
        jobVertexId = new JobVertexID();
        metricFetcher =
                new MetricFetcherImpl<>(
                        () -> null,
                        ignoredRpcAddress -> null,
                        Executors.directExecutor(),
                        TestingUtils.TIMEOUT,
                        MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL.defaultValue());
        metricStore = metricFetcher.getMetricStore();

        GatewayRetriever<RestfulGateway> leaderRetriever =
                new GatewayRetriever<RestfulGateway>() {
                    @Override
                    public CompletableFuture<RestfulGateway> getFuture() {
                        return CompletableFuture.completedFuture(restfulGateway);
                    }
                };

        handler =
                new A4SAggregatingVertexMetricsHandler(
                        leaderRetriever,
                        TIMEOUT,
                        Collections.emptyMap(),
                        Executors.directExecutor(),
                        metricFetcher);
    }

    @After
    public void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    private HandlerRequest<EmptyRequestBody> createRequest(String jobId, String jobVertexId)
            throws Exception {
        return createRequest(jobId, jobVertexId, Collections.emptyMap());
    }

    private HandlerRequest<EmptyRequestBody> createRequest(
            String jobId, String jobVertexId, Map<String, List<String>> queryParameters)
            throws Exception {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId);
        pathParameters.put(JobVertexIdPathParameter.KEY, jobVertexId);

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                handler.getMessageHeaders().getUnresolvedMessageParameters(),
                pathParameters,
                queryParameters,
                Collections.emptyList());
    }

    @Test
    public void testOptInCacheCostSamplesPreserveLegacyResponse() throws Exception {
        long now = System.currentTimeMillis();
        addCacheCostGauge("window-join", now);
        JsonNode legacy = jsonResponse(Collections.emptyMap());
        assertFalse(legacy.has("cacheCostSamples"));

        JsonNode response =
                jsonResponse(
                        Collections.singletonMap(
                                "get", Collections.singletonList("cacheCostSamples")));
        assertThat(response.get("scaledMrc"), equalTo(legacy.get("scaledMrc")));
        assertTrue(response.has("cacheCostSamples"));
        assertThat(response.get("cacheCostSamples").size(), equalTo(1));
        JsonNode sample = response.get("cacheCostSamples").get(0);
        assertThat(sample.get("subtask").asInt(), equalTo(0));
        assertThat(sample.get("attempt").asText(), equalTo("execution_vertex_0"));
        assertThat(sample.get("cacheId").asLong(), equalTo(3L));
        assertThat(sample.get("capacityBytes").asLong(), equalTo(524288L));
        assertThat(sample.get("cacheEpoch").asLong(), equalTo(1L));
        assertThat(sample.get("lookups").asLong(), equalTo(1000L));
        assertThat(sample.get("misses").asLong(), equalTo(300L));
        assertThat(sample.get("coveredHits").asLong(), equalTo(700L));
        assertThat(sample.get("coveredMisses").asLong(), equalTo(300L));
        assertThat(sample.get("sampledHits").asLong(), equalTo(8L));
        assertThat(sample.get("sampledMisses").asLong(), equalTo(4L));
        assertThat(sample.get("hitNanos").asLong(), equalTo(1800L));
        assertThat(sample.get("missNanos").asLong(), equalTo(24000L));
        assertThat(sample.get("recordsIn").asLong(), equalTo(400L));
        assertThat(sample.get("busyMillis").asDouble(), equalTo(82.0));
        assertThat(sample.get("observedAtMillis").asLong(), equalTo(now));
    }

    @Test
    public void testIncompleteOrStaleCacheCostSamplesAreOmitted() throws Exception {
        Map<String, List<String>> optIn =
                Collections.singletonMap("get", Collections.singletonList("cacheCostSamples"));
        addCacheCostGauge("window-join", System.currentTimeMillis() - 600_000);
        assertFalse(jsonResponse(optIn).has("cacheCostSamples"));

        long now = System.currentTimeMillis();
        addCacheCostGauge("window-join", now);
        addCacheCostGauge("window-aggregate", now);
        assertFalse(jsonResponse(optIn).has("cacheCostSamples"));
    }

    @Test
    public void testBlankHistogramDoesNotHideCacheCostSamples() throws Exception {
        addHistogramGauge(" ");
        addCacheCostGauge("window-join", System.currentTimeMillis());
        JsonNode response =
                jsonResponse(
                        Collections.singletonMap(
                                "get", Collections.singletonList("cacheCostSamples")));
        assertThat(response.get("scaledMrc").size(), equalTo(0));
        assertThat(response.get("cacheCostSamples").size(), equalTo(1));
    }

    @Test
    public void testLargeHistogramProducesTheSameCurveAcrossTheMetricTransport() throws Exception {
        for (int bucketCount : new int[] {38_483, 41_684}) {
            long[] buckets = new long[bucketCount];
            for (int position = 0; position < buckets.length; position += 31) {
                buckets[position] = position % 70;
            }
            String raw = new StackDistanceHistogram(buckets, 17, 1, 1, 4096).toMetricString();
            assertTrue(raw.length() > 65_535);
            addHistogramGauge(raw);
            JsonNode expected = jsonResponse(Collections.emptyMap()).get("scaledMrc");
            assertThat(expected.size(), equalTo(bucketCount));

            String encoded = A4SHistogramMetricCodec.encode(raw);
            assertTrue(encoded.length() <= 65_535);
            assertThat(A4SHistogramMetricCodec.decode(encoded), equalTo(raw));
            addHistogramGauge(encoded);
            assertThat(jsonResponse(Collections.emptyMap()).get("scaledMrc"), equalTo(expected));
        }
        assertThat(A4SHistogramMetricCodec.encode("{}"), equalTo("{}"));
        assertThat(A4SHistogramMetricCodec.encode(""), equalTo(""));
        assertThat(A4SHistogramMetricCodec.decode(""), equalTo(""));
    }

    @Test
    public void testInvalidHistogramNeverProducesAPartialCurveOrHidesCacheCostSamples()
            throws Exception {
        String valid = new StackDistanceHistogram(new long[] {1, 2}, 1, 1, 1, 4096).toMetricString();
        addHistogramGauge(valid);
        addCacheCostGauge(0, "window-join", System.currentTimeMillis(), "2", ":23:179");
        Map<String, List<String>> optIn =
                Collections.singletonMap(
                        "get", Collections.singletonList("cacheCostSamples,cacheCostCompaction"));
        JsonNode complete = jsonResponse(optIn);
        assertFalse(complete.get("scaledMrc").isEmpty());
        assertThat(complete.get("cacheCostSamples").get(0).get("timingAbi").asInt(), equalTo(3));

        byte[] chunk = new byte[4096];
        Arrays.fill(chunk, (byte) '0');
        assertThat(
                A4SHistogramMetricCodec.decode(gzipMetric(chunk, 1024)).length(),
                equalTo(4 * 1024 * 1024));
        String tooLarge = gzipMetric(chunk, 1025);
        assertTrue(tooLarge.length() < 65_535);
        assertThat(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> A4SHistogramMetricCodec.decode(tooLarge))
                        .getMessage(),
                equalTo("QuickMRC histogram exceeds expanded byte limit"));
        char[] tooLargeRaw = new char[4 * 1024 * 1024 + 1];
        Arrays.fill(tooLargeRaw, '0');
        String producerOmission = A4SHistogramMetricCodec.encode(new String(tooLargeRaw));
        assertFalse(producerOmission.isEmpty());
        assertThrows(
                IllegalArgumentException.class,
                () -> A4SHistogramMetricCodec.decode(producerOmission));
        char[] wire = new char[65_535];
        Arrays.fill(wire, 'A');
        String oversizedWire = "a4s-gzip-v1:" + new String(wire);
        assertThat(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> A4SHistogramMetricCodec.decode(oversizedWire))
                        .getMessage(),
                equalTo("QuickMRC histogram exceeds encoded byte limit"));

        for (String invalid :
                Arrays.asList(
                        "a4s-gzip-v1:*",
                        "a4s-gzip-v1:AQID",
                        gzipMetric(new byte[] {(byte) 0xff}, 1),
                        gzipMetric("invalid json".getBytes(StandardCharsets.UTF_8), 1),
                        tooLarge,
                        producerOmission,
                        oversizedWire,
                        new StackDistanceHistogram(new long[] {1}, 1, 1, 2, 4096)
                                .toMetricString())) {
            addHistogramGauge(1, invalid);
            JsonNode response = jsonResponse(optIn);
            assertTrue(response.get("scaledMrc").isEmpty());
            assertThat(response.get("cacheCostSamples"), equalTo(complete.get("cacheCostSamples")));
        }
    }

    private static String gzipMetric(byte[] chunk, int repeats) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            for (int i = 0; i < repeats; i++) {
                gzip.write(chunk);
            }
        }
        return "a4s-gzip-v1:" + Base64.getEncoder().encodeToString(compressed.toByteArray());
    }

    private void addHistogramGauge(String value) {
        addHistogramGauge(0, value);
    }

    private void addHistogramGauge(int subtask, String value) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, 0, ""),
                        "stack-distance-histogram",
                        value));
    }

    @Test
    public void testCompactionClassificationRequiresExplicitOptIn() throws Exception {
        long now = System.currentTimeMillis();
        Map<String, List<String>> original =
                Collections.singletonMap("get", Collections.singletonList("cacheCostSamples"));
        Map<String, List<String>> extended =
                Collections.singletonMap(
                        "get", Collections.singletonList("cacheCostSamples,cacheCostCompaction"));
        addCacheCostGauge("window-join", now);
        JsonNode abi2 = jsonResponse(extended).get("cacheCostSamples").get(0);
        assertThat(abi2.get("timingAbi").asInt(), equalTo(2));
        assertFalse(abi2.has("compactionMisses"));
        assertFalse(abi2.has("compactionHits"));

        addCacheCostGauge(0, "window-join", now, "2", ":23:179");
        JsonNode compatibility = jsonResponse(original).get("cacheCostSamples").get(0);
        assertFalse(compatibility.has("timingAbi"));
        assertFalse(compatibility.has("compactionMisses"));
        assertFalse(compatibility.has("compactionHits"));

        JsonNode abi3 = jsonResponse(extended).get("cacheCostSamples").get(0);
        assertThat(abi3.get("timingAbi").asInt(), equalTo(3));
        assertThat(abi3.get("compactionHits").asLong(), equalTo(23L));
        assertThat(abi3.get("compactionMisses").asLong(), equalTo(179L));
        assertThat(abi3.get("observedAtMillis").asLong(), equalTo(now));

        addCacheCostGauge(0, "window-join", now, "2", ":0:0");
        JsonNode zero = jsonResponse(extended).get("cacheCostSamples").get(0);
        assertThat(zero.get("compactionHits").asLong(), equalTo(0L));
        assertThat(zero.get("compactionMisses").asLong(), equalTo(0L));

        addCacheCostGauge(0, "window-join", now, "2", ":-1:179");
        assertFalse(jsonResponse(extended).has("cacheCostSamples"));
    }

    @Test
    public void testCacheCostSamplesAreSortedBySubtask() throws Exception {
        long now = System.currentTimeMillis();
        addCacheCostGauge(16, "window-join", now);
        addCacheCostGauge(1, "window-join", now);
        JsonNode samples =
                jsonResponse(
                                Collections.singletonMap(
                                        "get", Collections.singletonList("cacheCostSamples")))
                        .get("cacheCostSamples");
        assertThat(samples.get(0).get("subtask").asInt(), equalTo(1));
        assertThat(samples.get(1).get("subtask").asInt(), equalTo(16));
    }

    private void addCacheCostGauge(String operator, long observedAtMillis) {
        addCacheCostGauge(0, operator, observedAtMillis);
    }

    private void addCacheCostGauge(int subtask, String operator, long observedAtMillis) {
        addCacheCostGauge(subtask, operator, observedAtMillis, "1", "");
    }

    private void addCacheCostGauge(
            int subtask, String operator, long observedAtMillis, String schema, String suffix) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.OperatorQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, 0, operator,
                                "rocksdb"),
                        "a4s-cache-cost-sample",
                        schema + ":"
                                + observedAtMillis
                                + ":execution_vertex_0:3:524288:1:1000:300:700:300:8:4:1800:24000:400:82.0"
                                + suffix));
    }

    private JsonNode jsonResponse(Map<String, List<String>> queryParameters) throws Exception {
        Object response =
                handler.handleRequest(
                                createRequest(
                                        jobId.toString(), jobVertexId.toString(), queryParameters),
                                restfulGateway)
                        .get();
        return RestMapperUtils.getStrictObjectMapper().valueToTree(response);
    }

    @Test
    public void testHandleRequestComputesScaledMrcFromSubtaskHistograms() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {4L, 2L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 1, 0, ""),
                        "foo.stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {1L, 1L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));

        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        MissRateCurve curve = response.getScaledMrc();
        assertThat(curve.getPoints(), hasSize(3));
        assertThat(curve.getPoints().get(0).getCacheSizeBytes(), equalTo(8192L));
        assertThat(curve.getPoints().get(0).getMissRate(), closeTo(0.5, 1.0e-9));
        assertThat(curve.getPoints().get(1).getCacheSizeBytes(), equalTo(16384L));
        assertThat(curve.getPoints().get(1).getMissRate(), closeTo(0.2, 1.0e-9));
        assertThat(curve.getPoints().get(2).getCacheSizeBytes(), equalTo(24576L));
        assertThat(curve.getPoints().get(2).getMissRate(), closeTo(0.0, 1.0e-9));
    }

    @Test
    public void testHandleRequest_taskIdIsolation() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {4L, 2L, 2L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));

        JobVertexID jobVertexId2 = new JobVertexID();
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId2.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        new StackDistanceHistogram(new long[] {1L, 1L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));

        A4SAggregatedMetricsResponseBody response1 =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        MissRateCurve curve1 = response1.getScaledMrc();
        assertThat(curve1.getPoints(), hasSize(3));
        assertThat(curve1.getPoints().get(0).getCacheSizeBytes(), equalTo(4096L));
        assertThat(curve1.getPoints().get(0).getMissRate(), closeTo(0.5, 1.0e-9));
        assertThat(curve1.getPoints().get(1).getCacheSizeBytes(), equalTo(4096L * 2));
        assertThat(curve1.getPoints().get(1).getMissRate(), closeTo(0.25, 1.0e-9));
        assertThat(curve1.getPoints().get(2).getCacheSizeBytes(), equalTo(4096L * 3));
        assertThat(curve1.getPoints().get(2).getMissRate(), closeTo(0.0, 1.0e-9));

        A4SAggregatedMetricsResponseBody response2 =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId2.toString()),
                                restfulGateway)
                        .get();
        MissRateCurve curve2 = response2.getScaledMrc();
        assertThat(curve2.getPoints(), hasSize(3));
        assertThat(curve2.getPoints().get(0).getCacheSizeBytes(), equalTo(4096L));
        assertThat(curve2.getPoints().get(0).getMissRate(), closeTo(2.0 / 3.0, 1.0e-9));
        assertThat(curve2.getPoints().get(1).getCacheSizeBytes(), equalTo(4096L * 2));
        assertThat(curve2.getPoints().get(1).getMissRate(), closeTo(1.0 / 3.0, 1.0e-9));
        assertThat(curve2.getPoints().get(2).getCacheSizeBytes(), equalTo(4096L * 3));
        assertThat(curve2.getPoints().get(2).getMissRate(), closeTo(0, 1.0e-9));
    }

    @Test
    public void testHandleRequest_incorrectNameIgnored() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "foo",
                        new StackDistanceHistogram(new long[] {4L, 2L, 1L}, 0L, 1, 1L, 4096L)
                                .toMetricString()));
        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        assertTrue(response.getScaledMrc().getPoints().isEmpty());
    }

    @Test
    public void testHandleRequestReturnsEmptyCurveWhenTaskMetricStoreMissing() throws Exception {
        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        assertTrue(response.getScaledMrc().getPoints().isEmpty());
    }

    @Test
    public void testHandleRequestOmitsCurveForMalformedLegacyHistogram() throws Exception {
        addHistogramGauge("{\"bucketCounts\": [1, \"bad\", 3]}");
        JsonNode response = jsonResponse(Collections.emptyMap());
        assertTrue(response.get("scaledMrc").isEmpty());
        assertFalse(response.has("cacheCostSamples"));
    }
}
