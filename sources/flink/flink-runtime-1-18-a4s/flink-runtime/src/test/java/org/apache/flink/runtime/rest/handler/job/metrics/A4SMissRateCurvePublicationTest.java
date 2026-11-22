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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        var unused = response(source);
        assertNull(unused.getScaledMrc(), "No metrics must not become an empty curve");
        var mapper = RestMapperUtils.getStrictObjectMapper();
        var serialized = mapper.readTree(mapper.writeValueAsString(unused));
        assertTrue(serialized.path("scaledMrc").isNull());

        addHistogram(join, new long[] {0, 0}, 0);
        assertNull(response(join).getScaledMrc(), "An idle cache has no observations");
        addHistogram(join, new long[0], 9);
        assertNull(response(join).getScaledMrc(), "No finite buckets means no supported size");

        addHistogram(join, new long[] {3, 1}, 4);
        MissRateCurve warm =
                mapper.readValue(
                                mapper.writeValueAsString(response(join)),
                                A4SAggregatedMetricsResponseBody.class)
                        .getScaledMrc();
        assertEquals(2, warm.getPoints().size());
        assertEquals(4096L, warm.getPoints().get(0).getCacheSizeBytes());
        assertEquals(0.625, warm.getPoints().get(0).getMissRate(), 1.0e-9);
        assertNull(response(source).getScaledMrc());
    }

    private void addHistogram(JobVertexID vertex, long[] counts, long completeMisses) {
        store.add(new MetricDump.GaugeDump(
                new QueryScopeInfo.TaskQueryScopeInfo(job.toString(), vertex.toString(), 0, 0, ""),
                "stack-distance-histogram",
                new StackDistanceHistogram(counts, completeMisses, 1, 1L, 4096L)
                        .toMetricString()));
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
