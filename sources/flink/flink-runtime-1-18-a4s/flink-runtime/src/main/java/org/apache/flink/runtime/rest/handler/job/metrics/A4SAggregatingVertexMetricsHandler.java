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
import org.apache.flink.a4s.core.CacheObservationBatch;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.a4s.logging.A4SMetricsFlowStep;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.A4SHistogramMetricCodec;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SCacheCostSample;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Request handler that returns A4S-specific aggregated metrics for a job vertex. */
public class A4SAggregatingVertexMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                A4SAggregatedMetricsResponseBody,
                AggregatedSubtaskMetricsParameters> {

    private static final String STACK_DISTANCE_HISTOGRAM_METRIC_NAME = "stack-distance-histogram";

    private final Executor executor;
    private static final int MAX_VERTEX_WIRE_BYTES = 16 * 1024 * 1024;

    private final MetricFetcher fetcher;
    private final Time requestTimeout;

    public A4SAggregatingVertexMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                A4SAggregatedVertexMetricsHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
        this.requestTimeout = timeout;
    }

    @Override
    protected CompletableFuture<A4SAggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID vertexID = request.getPathParameter(JobVertexIdPathParameter.class);
        List<String> selectors = request.getQueryParameter(MetricsFilterParameter.class);
        boolean includeCacheCosts =
                selectors.contains(A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_COST_SAMPLES);
        boolean includeClassification =
                selectors.contains(A4SCacheCostSample.COMPACTION_QUERY_SELECTOR);

        if (selectors.contains(CacheObservationBatch.FIELD_NAME)) {
            return gateway.requestJob(jobId, requestTimeout)
                    .handleAsync(
                            (graph, error) -> {
                                if (error != null || graph == null) {
                                    return invalidResponse(Status.REST_UNAVAILABLE);
                                }
                                Map<Integer, ExecutionAttemptID> expected =
                                        expectedExecutions(graph.getJobVertex(vertexID));
                                if (expected.isEmpty()) {
                                    return invalidResponse(Status.INCOMPLETE);
                                }
                                return processVersionedRequest(
                                        jobId.toString(), vertexID.toString(), expected, includeCacheCosts);
                            },
                            executor);
        }

        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return processRequest(
                                jobId, vertexID, includeCacheCosts, includeClassification);
                    } catch (Exception e) {
                        log.warn(
                                "A4S [{}]: jobId={} vertexId={} message={}",
                                A4SMetricsFlowStep.HANDLER_FAILED.name(),
                                jobId,
                                vertexID,
                                e.getMessage(),
                                e);
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Could not retrieve A4S metrics.",
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR));
                    }
                },
                this.executor);
    }

    private static A4SAggregatedMetricsResponseBody invalidResponse(Status status) {
        return new A4SAggregatedMetricsResponseBody(
                null, null, CacheObservationBatch.invalid(status));
    }

    private static Map<Integer, ExecutionAttemptID> expectedExecutions(
            @Nullable AccessExecutionJobVertex vertex) {
        if (vertex == null) {
            return Map.of();
        }
        Map<Integer, ExecutionAttemptID> expected = new TreeMap<>();
        for (AccessExecutionVertex task : vertex.getTaskVertices()) {
            AccessExecution attempt = task.getCurrentExecutionAttempt();
            if (attempt == null
                    || task.getCurrentExecutions().size() != 1
                    || attempt.getState() != ExecutionState.RUNNING) {
                return Map.of();
            }
            expected.put(task.getParallelSubtaskIndex(), attempt.getAttemptId());
        }
        return expected.size() == vertex.getParallelism() ? expected : Map.of();
    }

    A4SAggregatedMetricsResponseBody processVersionedRequest(
            String job, String vertex, Map<Integer, ExecutionAttemptID> expected, boolean includeCacheCosts) {
        fetcher.update();
        MetricStore store = fetcher.getMetricStore();
        long now = System.currentTimeMillis();
        Map<Integer, String> expectedIds = new TreeMap<>();
        List<CacheObservation> observations = new ArrayList<>();
        List<A4SCacheCostSample> samples = new ArrayList<>();
        long wireBytes = 0;
        long maxAge = -1;
        Status transportStatus = Status.VALID;
        // MetricStore views are backed by concurrent maps. Copy just the relevant gauge strings,
        // then decode and build curves without holding the global metric lock.
        Map<Integer, List<String>> observationGauges = new TreeMap<>();
        synchronized (store) {
            expected.forEach(
                    (subtask, attempt) -> {
                        expectedIds.put(subtask, attempt.toString());
                        var metrics = store.getSubtaskAttemptMetricStore(
                                job, vertex, subtask, attempt.getAttemptNumber());
                        if (metrics == null) {
                            return;
                        }
                        List<String> matching = new ArrayList<>();
                        metrics.metrics.forEach((name, value) -> {
                            if (name.endsWith("." + CacheObservation.METRIC_NAME)) {
                                matching.add(value);
                            }
                        });
                        if (matching.isEmpty()
                                && getStackDistanceHistogramRaw(metrics.metrics) != null) {
                            matching.add(null); // unversioned legacy histogram
                        }
                        observationGauges.put(subtask, matching);
                        if (includeCacheCosts) {
                            A4SCacheCostSample sample = readCacheCostSample(
                                    subtask, metrics.metrics, now, true, true);
                            if (sample != null && sample.attempt.equals(expectedIds.get(subtask))) {
                                samples.add(sample);
                            }
                        }
                    });
        }
        int decodedChars = 0;
        for (var entry : observationGauges.entrySet()) {
            List<String> matching = entry.getValue();
            if (matching.isEmpty()) {
                continue;
            }
            if (matching.size() != 1) {
                transportStatus = Status.DUPLICATE;
                continue;
            }
            String raw = matching.get(0);
            if (raw == null) {
                transportStatus = Status.UNSUPPORTED_VERSION;
                continue;
            }
            if (raw.length() > 65_535 || A4SHistogramMetricCodec.isOmitted(raw)) {
                transportStatus = Status.OVERSIZE;
                continue;
            }
            int bytes = A4SHistogramMetricCodec.modifiedUtfBytes(raw);
            wireBytes += bytes;
            if (bytes > 65_535) {
                transportStatus = Status.OVERSIZE;
                continue;
            }
            if (wireBytes > MAX_VERTEX_WIRE_BYTES) {
                transportStatus = Status.OVERSIZE;
                break;
            }
            try {
                String json = A4SHistogramMetricCodec.decode(raw);
                decodedChars += json.length();
                if (decodedChars > MAX_VERTEX_WIRE_BYTES) {
                    transportStatus = Status.OVERSIZE;
                    break;
                }
                CacheObservation observation = CacheObservation.fromMetricString(json);
                if (observation.window != null && observation.window.observedAtMillis > 0) {
                    maxAge = Math.max(maxAge, now - observation.window.observedAtMillis);
                }
                if (observation.subtask != entry.getKey()) {
                    transportStatus = Status.ATTEMPT_MISMATCH;
                } else {
                    observations.add(observation);
                }
            } catch (A4SHistogramMetricCodec.SizeLimitException oversized) {
                transportStatus = Status.OVERSIZE;
            } catch (IllegalArgumentException invalid) {
                transportStatus = Status.MALFORMED;
            }
        }
        A4SCacheObservationAggregator.Result aggregated =
                transportStatus == Status.VALID
                        ? A4SCacheObservationAggregator.aggregate(
                                job, vertex, expectedIds, observations, now)
                        : new A4SCacheObservationAggregator.Result(transportStatus);
        if (aggregated.status != Status.VALID) {
            log.debug(
                    "A4S cache observations job={} vertex={} status={} coverage={}/{} ageMs={} wireBytes={}",
                    job, vertex, aggregated.status, observations.size(), expected.size(), maxAge, wireBytes);
        }
        return new A4SAggregatedMetricsResponseBody(
                aggregated.curve, samples,
                new CacheObservationBatch(
                        CacheObservation.VERSION, aggregated.status, expected.size(), wireBytes,
                        maxAge, observations));
    }

    private A4SAggregatedMetricsResponseBody processRequest(
            JobID jobId,
            JobVertexID vertexID,
            boolean includeCacheCosts,
            boolean includeClassification)
            throws Exception {
        this.fetcher.update();
        MetricStore store = this.fetcher.getMetricStore();

        MetricStore.TaskMetricStore taskMetricStore =
                store.getTaskMetricStore(jobId.toString(), vertexID.toString());
        if (taskMetricStore == null) {
            MissRateCurve emptyMrc = buildScaledMrcPoints(Collections.emptyList());
            return new A4SAggregatedMetricsResponseBody(emptyMrc);
        }

        MissRateCurve scaledMrc;
        try {
            List<StackDistanceHistogram> subtaskHistograms = new ArrayList<>();
            for (MetricStore.SubtaskMetricStore subtask :
                    taskMetricStore.getAllSubtaskMetricStores().values()) {
                String histogramRaw = getStackDistanceHistogramRaw(subtask.metrics);
                if (histogramRaw == null) {
                    continue;
                }
                StackDistanceHistogram histogram =
                        StackDistanceHistogram.fromMetricString(
                                A4SHistogramMetricCodec.decode(histogramRaw));
                subtaskHistograms.add(histogram);
            }
            scaledMrc = buildScaledMrcPoints(subtaskHistograms);
        } catch (IllegalArgumentException invalidHistogram) {
            log.debug(
                    "Omitting QuickMRC curve for job {}, vertex {} with an invalid histogram.",
                    jobId,
                    vertexID,
                    invalidHistogram);
            scaledMrc = buildScaledMrcPoints(Collections.emptyList());
        }

        List<A4SCacheCostSample> samples = Collections.emptyList();
        if (includeCacheCosts) {
            synchronized (store) {
                samples =
                        readCacheCostSamples(
                                store,
                                jobId.toString(),
                                vertexID.toString(),
                                taskMetricStore,
                                includeClassification);
            }
        }
        return new A4SAggregatedMetricsResponseBody(scaledMrc, samples);
    }

    private List<A4SCacheCostSample> readCacheCostSamples(
            MetricStore store,
            String jobId,
            String vertexId,
            MetricStore.TaskMetricStore task,
            boolean includeClassification) {
        Map<Integer, Integer> representativeAttempts =
                store.getRepresentativeAttempts()
                        .getOrDefault(jobId, Collections.emptyMap())
                        .getOrDefault(vertexId, Collections.emptyMap());
        List<A4SCacheCostSample> samples = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> entry :
                task.getAllSubtaskMetricStores().entrySet()) {
            int subtask = entry.getKey();
            Integer attempt = representativeAttempts.get(subtask);
            Map<Integer, MetricStore.ComponentMetricStore> attempts =
                    entry.getValue().getAllAttemptsMetricStores();
            if (attempt == null && attempts.size() != 1) {
                continue;
            }
            MetricStore.ComponentMetricStore active =
                    attempt == null ? attempts.values().iterator().next() : attempts.get(attempt);
            if (active == null) {
                continue;
            }
            A4SCacheCostSample sample =
                    readCacheCostSample(subtask, active.metrics, now, includeClassification, false);
            if (sample != null) {
                samples.add(sample);
            }
        }
        samples.sort(Comparator.comparingInt(sample -> sample.subtask));
        return samples;
    }

    @Nullable
    private A4SCacheCostSample readCacheCostSample(
            int subtask, Map<String, String> metrics, long now,
            boolean includeClassification, boolean includeProcess) {
        String raw = null;
        for (Map.Entry<String, String> entry : metrics.entrySet()) {
            if (entry.getKey().endsWith("." + A4SCacheCostSample.METRIC_NAME)) {
                if (raw != null) {
                    // A chained task can have several RocksDB operators. The task counters
                    // cannot be attributed to one DB if several are present.
                    return null;
                }
                raw = entry.getValue();
            }
        }
        return raw == null
                ? null
                : A4SCacheCostSample.fromMetricString(subtask, raw, now, includeClassification, includeProcess);
    }

    @Nullable
    private String getStackDistanceHistogramRaw(Map<String, String> metrics) {
        for (Map.Entry<String, String> metricEntry : metrics.entrySet()) {
            String metricKey = metricEntry.getKey();
            String metricValue = metricEntry.getValue();
            if (metricKey == null || metricValue == null) {
                continue;
            }
            if (metricKey.contains(STACK_DISTANCE_HISTOGRAM_METRIC_NAME)
                    && !metricValue.trim().isEmpty()) {
                return metricValue;
            }
        }
        return null;
    }

    private MissRateCurve buildScaledMrcPoints(List<StackDistanceHistogram> subtaskHistograms) {
        if (subtaskHistograms.isEmpty()) {
            return new MissRateCurve(Collections.emptyList());
        }
        StackDistanceHistogram mergedHistogram = StackDistanceHistogram.merge(subtaskHistograms);
        return MissRateCurve.fromStackDistanceHistogram(mergedHistogram);
    }
}
