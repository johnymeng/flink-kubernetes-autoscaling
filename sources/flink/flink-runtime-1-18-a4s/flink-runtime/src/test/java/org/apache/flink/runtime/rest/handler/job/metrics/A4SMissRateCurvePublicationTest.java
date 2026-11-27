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
import org.apache.flink.a4s.core.MissRateCurveObservation.Status;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Verifies that the producer publishes a curve only when there is measured support. */
class A4SMissRateCurvePublicationTest {
    private final JobID job = new JobID();
    private final JobVertexID source = new JobVertexID();
    private final JobVertexID join = new JobVertexID();
    private final MetricStore store = new MetricStore();

    private final A4SAggregatingVertexMetricsHandler handler =
            new A4SAggregatingVertexMetricsHandler(
                    () -> CompletableFuture.completedFuture(null),
                    Time.seconds(1),
                    Collections.emptyMap(),
                    Runnable::run,
                    new MetricFetcher() {
                        @Override
                        public MetricStore getMetricStore() {
                            return store;
                        }

                        @Override
                        public void update() {}

                        @Override
                        public long getLastUpdateTime() {
                            return 0;
                        }
                    });

    @Test
    void missingSourceAndColdJoinAreAbsentUntilMeasurementsArrive() throws Exception {
        ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        assertUnavailable(source, "TASK_METRICS_UNAVAILABLE");
        addGauge(source, 0, "other-metric", "1");
        assertUnavailable(source, "HISTOGRAM_UNAVAILABLE");

        addHistogram(join, new long[] {0, 0}, 0);
        assertUnavailable(join, "NO_SAMPLES");
        addHistogram(join, new long[0], 9);
        assertUnavailable(join, "NO_FINITE_BUCKETS");
        addHistogram(join, new long[] {0, 0}, 9);
        assertUnavailable(join, "NO_FINITE_BUCKETS");

        addHistogram(join, new long[] {3, 1}, 4);
        MissRateCurve warm =
                mapper.readValue(
                                mapper.writeValueAsString(response(join)),
                                A4SAggregatedMetricsResponseBody.class)
                        .getCurve()
                        .orElseThrow();
        assertEquals(2, warm.getPoints().size());
        assertEquals(4096L, warm.getPoints().get(0).getCacheSizeBytes());
        assertEquals(0.625, warm.getPoints().get(0).getMissRate(), 1.0e-9);
        assertFalse(response(source).getCurve().isPresent());
        assertEquals("AVAILABLE",
                mapper.readTree(mapper.writeValueAsString(response(join))).path("mrcStatus").asText());
    }

    @Test
    void rejectsMalformedAndOverflowingHistogramsAndRecoversOnNextSample() throws Exception {
        addGauge(join, 0, "stack-distance-histogram", "{\"bucketCounts\":[\"bad\"]}");
        assertUnavailable(join, "INVALID_HISTOGRAM");

        String incomplete =
                "{\"bucketCounts\":[3,1],\"numPartitions\":1,\"bucketSizeScaling\":1,"
                        + "\"cacheItemSizeBytes\":4096}";
        addGauge(join, 0, "stack-distance-histogram", incomplete);
        assertUnavailable(join, "INVALID_HISTOGRAM");

        String overflowing =
                "{\"bucketCounts\":[9223372036854775807,1],\"completeMisses\":0,"
                        + "\"numPartitions\":1,\"bucketSizeScaling\":1,\"cacheItemSizeBytes\":4096}";
        addGauge(join, 0, "stack-distance-histogram", overflowing);
        assertUnavailable(join, "INVALID_HISTOGRAM");

        addGauge(join, 0, "stack-distance-histogram",
                new StackDistanceHistogram(new long[] {3, 1}, 4, 1, Long.MAX_VALUE, 4096L)
                        .toMetricString());
        assertUnavailable(join, "INVALID_HISTOGRAM");

        addHistogram(join, new long[] {3, 1}, 4);
        assertEquals(2, response(join).getCurve().orElseThrow().getPoints().size());
    }

    @Test
    void publishesHistogramFromRocksDbSanitizedMetricName() throws Exception {
        addGauge(
                join,
                0,
                "TumblingEventTimeWindows.rocksdb_stack-distance-histogram",
                new StackDistanceHistogram(new long[] {3, 1}, 4, 1, 1L, 4096L).toMetricString());

        A4SAggregatedMetricsResponseBody observation = response(join);
        assertEquals(Status.AVAILABLE, observation.getMrcStatus());
        assertEquals(2, observation.getCurve().orElseThrow().getPoints().size());
    }

    @Test
    void rejectsAnObservedSubtaskMissingItsHistogram() throws Exception {
        addHistogram(join, new long[] {3, 1}, 4);
        addGauge(join, 1, "other-metric", "1");
        assertUnavailable(join, "INCOMPLETE_HISTOGRAMS");
    }

    @Test
    void rejectsAnUnsampledSubtaskWithoutDiscardingCompleteMissSamples() throws Exception {
        addHistogram(join, new long[] {3, 1}, 4);
        addGauge(
                join,
                1,
                "stack-distance-histogram",
                new StackDistanceHistogram(new long[0], 0, 1, 1L, 4096L).toMetricString());
        assertUnavailable(join, "INCOMPLETE_HISTOGRAMS");

        addGauge(
                join,
                1,
                "stack-distance-histogram",
                new StackDistanceHistogram(new long[0], 2, 1, 1L, 4096L).toMetricString());
        MissRateCurve curve = response(join).getCurve().orElseThrow();
        assertEquals(8192L, curve.getPoints().get(0).getCacheSizeBytes());
        assertEquals(0.7, curve.getPoints().get(0).getMissRate(), 1.0e-9);
    }

    @Test
    void rejectsAmbiguousSubtaskHistograms() throws Exception {
        addHistogram(join, new long[] {3, 1}, 4);
        addGauge(join, 0, "another-backend.rocksdb_stack-distance-histogram",
                new StackDistanceHistogram(new long[] {3, 1}, 4, 1, 1L, 4096L)
                        .toMetricString());
        assertUnavailable(join, "INVALID_HISTOGRAM");
    }

    private void assertUnavailable(JobVertexID vertex, String status) throws Exception {
        ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();
        JsonNode json = mapper.readTree(mapper.writeValueAsString(response(vertex)));
        assertEquals(status, json.path("mrcStatus").asText());
        assertFalse(json.has("scaledMrc"), "Do not publish a null or empty curve");
    }

    private void addHistogram(JobVertexID vertex, long[] counts, long completeMisses) {
        addGauge(vertex, 0, "stack-distance-histogram",
                new StackDistanceHistogram(counts, completeMisses, 1, 1L, 4096L)
                        .toMetricString());
    }

    private void addGauge(JobVertexID vertex, int subtask, String metricName, String value) {
        store.add(new MetricDump.GaugeDump(
                new QueryScopeInfo.TaskQueryScopeInfo(job.toString(), vertex.toString(), subtask, 0, ""),
                metricName, value));
    }

    private A4SAggregatedMetricsResponseBody response(JobVertexID vertex) throws Exception {
        Map<String, String> path = new HashMap<>();
        path.put(JobIDPathParameter.KEY, job.toString());
        path.put(JobVertexIdPathParameter.KEY, vertex.toString());
        return handler.handleRequest(
                        HandlerRequest.resolveParametersAndCreate(
                                EmptyRequestBody.getInstance(),
                                handler.getMessageHeaders().getUnresolvedMessageParameters(),
                                path,
                                Collections.emptyMap(),
                                Collections.emptyList()),
                        null)
                .get();
    }
}
