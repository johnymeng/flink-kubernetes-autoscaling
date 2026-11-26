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
import org.apache.flink.a4s.logging.A4SMetricsFlowStep;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
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
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.concurrent.CompletableFuture.completedFuture;

/** Request handler that returns A4S-specific aggregated metrics for a job vertex. */
public class A4SAggregatingVertexMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                A4SAggregatedMetricsResponseBody,
                AggregatedSubtaskMetricsParameters> {

    private static final String STACK_DISTANCE_HISTOGRAM_METRIC_NAME = "stack-distance-histogram";
    private static final String ROCKSDB_SANITIZED_HISTOGRAM_METRIC_NAME =
            "rocksdb_" + STACK_DISTANCE_HISTOGRAM_METRIC_NAME;

    private final Executor executor;
    private final MetricFetcher fetcher;

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
    }

    @Override
    protected CompletableFuture<A4SAggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID vertexID = request.getPathParameter(JobVertexIdPathParameter.class);
        boolean includeBacklog =
                request.getQueryParameter(MetricsFilterParameter.class)
                        .contains(A4SAggregatedMetricsResponseBody.FIELD_NAME_OBSERVED_BACKLOG);
        boolean includeCacheCapacity =
                request.getQueryParameter(MetricsFilterParameter.class)
                        .contains(A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_CAPACITY);

        return CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                CompletableFuture<ArchivedExecutionGraph> graph =
                                        includeBacklog || includeCacheCapacity
                                                ? gateway.requestJob(jobId, timeout)
                                                : null;
                                CompletableFuture<A4SAggregatedMetricsResponseBody> curve =
                                        processRequest(jobId, vertexID, gateway, graph);
                                if (graph == null) {
                                    return curve;
                                }
                                return curve.thenCombine(
                                        graph.exceptionally(failure -> null),
                                        (response, liveGraph) -> {
                                            List<Map<String, String>> attempts =
                                                    liveGraph == null
                                                            ? null
                                                            : currentAttemptMetrics(
                                                                    fetcher.getMetricStore(),
                                                                    jobId,
                                                                    vertexID,
                                                                    liveGraph.getJobVertex(
                                                                            vertexID));
                                            return new A4SAggregatedMetricsResponseBody(
                                                    response.getMrcStatus(),
                                                    response.getCurve().orElse(null),
                                                    includeBacklog
                                                            ? aggregateBacklog(attempts)
                                                            : null,
                                                    includeCacheCapacity
                                                            ? aggregateSlotCacheCapacity(attempts)
                                                            : null);
                                        });
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
                        this.executor)
                .thenCompose(response -> response);
    }

    private CompletableFuture<A4SAggregatedMetricsResponseBody> processRequest(
            JobID jobId,
            JobVertexID vertexID,
            RestfulGateway gateway,
            @Nullable CompletableFuture<ArchivedExecutionGraph> graph) {
        this.fetcher.update();
        MetricStore store = this.fetcher.getMetricStore();

        MetricStore.TaskMetricStore taskMetricStore =
                store.getTaskMetricStore(jobId.toString(), vertexID.toString());
        if (taskMetricStore == null) {
            return completedFuture(new A4SAggregatedMetricsResponseBody(Status.TASK_METRICS_UNAVAILABLE, null));
        }

        Map<Integer, MetricStore.SubtaskMetricStore> subtasks =
                taskMetricStore.getAllSubtaskMetricStores();
        if (subtasks.isEmpty()) {
            return completedFuture(new A4SAggregatedMetricsResponseBody(Status.TASK_METRICS_UNAVAILABLE, null));
        }

        try {
            List<StackDistanceHistogram> subtaskHistograms = new ArrayList<>();
            for (MetricStore.SubtaskMetricStore subtask : subtasks.values()) {
                String histogramRaw = getStackDistanceHistogramRaw(subtask.metrics);
                if (histogramRaw != null && !histogramRaw.isEmpty()) {
                    StackDistanceHistogram histogram =
                            StackDistanceHistogram.fromMetricString(histogramRaw);
                    if (histogram.getNumPartitions() <= 0) {
                        throw new IllegalArgumentException("Histogram has no positive partition count");
                    }
                    subtaskHistograms.add(histogram);
                }
            }

            if (subtaskHistograms.isEmpty()) {
                return completedFuture(new A4SAggregatedMetricsResponseBody(Status.HISTOGRAM_UNAVAILABLE, null));
            }
            if (subtaskHistograms.size() != subtasks.size()) {
                return completedFuture(new A4SAggregatedMetricsResponseBody(Status.INCOMPLETE_HISTOGRAMS, null));
            }

            if (subtaskHistograms.stream().anyMatch(histogram -> histogram.getShards() != null)) {
                return (graph == null ? gateway.requestJob(jobId, timeout) : graph)
                        .thenApply(
                                liveGraph ->
                                        buildShardsResponse(
                                                store,
                                                jobId,
                                                vertexID,
                                                liveGraph.getJobVertex(vertexID)));
            }
            return completedFuture(buildResponse(jobId, vertexID, subtaskHistograms, 0));
        } catch (IllegalArgumentException | ArithmeticException e) {
            return completedFuture(invalidHistogram(jobId, vertexID, e));
        }
    }

    @Nullable
    private static List<Map<String, String>> currentAttemptMetrics(
            MetricStore store,
            JobID jobId,
            JobVertexID vertexId,
            @Nullable AccessExecutionJobVertex vertex) {
        if (vertex == null
                || vertex.getParallelism() <= 0
                || vertex.getTaskVertices().length != vertex.getParallelism()) {
            return null;
        }
        List<Map<String, String>> attempts = new ArrayList<>();
        HashSet<Integer> indices = new HashSet<>();
        for (AccessExecutionVertex task : vertex.getTaskVertices()) {
            int index = task.getParallelSubtaskIndex();
            if (index < 0
                    || index >= vertex.getParallelism()
                    || !indices.add(index)
                    || task.getExecutionState() != ExecutionState.RUNNING
                    || task.getCurrentExecutions().size() != 1) {
                return null;
            }
            MetricStore.ComponentMetricStore attempt =
                    store.getSubtaskAttemptMetricStore(
                            jobId.toString(),
                            vertexId.toString(),
                            index,
                            task.getCurrentExecutionAttempt().getAttemptNumber());
            if (attempt == null) {
                return null;
            }
            attempts.add(attempt.metrics);
        }
        return attempts;
    }

    @Nullable
    private static Long aggregateBacklog(@Nullable List<Map<String, String>> attempts) {
        if (attempts == null) {
            return null;
        }
        long total = 0;
        for (Map<String, String> attempt : attempts) {
            Long pending = readPendingRecords(attempt);
            if (pending == null || pending > Long.MAX_VALUE - total) {
                return null;
            }
            total += pending;
        }
        return total;
    }

    @Nullable
    private static Long aggregateSlotCacheCapacity(@Nullable List<Map<String, String>> attempts) {
        if (attempts == null) {
            return null;
        }
        long total = 0;
        HashSet<UUID> caches = new HashSet<>();
        for (Map<String, String> attempt : attempts) {
            String allocation = null;
            for (Map.Entry<String, String> entry : attempt.entrySet()) {
                if (!MetricNames.A4S_SLOT_BLOCK_CACHE.equals(entry.getKey())
                        && !entry.getKey().endsWith("." + MetricNames.A4S_SLOT_BLOCK_CACHE)) {
                    continue;
                }
                if (allocation != null && !allocation.equals(entry.getValue())) {
                    return null;
                }
                allocation = entry.getValue();
            }
            if (allocation == null || allocation.indexOf(':') != 36) {
                return null;
            }
            try {
                UUID cache = UUID.fromString(allocation.substring(0, 36));
                long capacity = Long.parseLong(allocation.substring(37));
                // Aliases within an attempt can describe one cache. Across subtasks they
                // indicate shared ownership and cannot be used as per-vertex capacity.
                if (capacity <= 0 || capacity > Long.MAX_VALUE - total || !caches.add(cache)) {
                    return null;
                }
                total += capacity;
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return total;
    }

    @Nullable
    private static Long readPendingRecords(Map<String, String> metrics) {
        Long pending = null;
        for (Map.Entry<String, String> metric : metrics.entrySet()) {
            String name = metric.getKey();
            if (!"pendingRecords".equals(name) && !name.endsWith(".pendingRecords")) {
                continue;
            }
            // A vertex may expose connector-specific aliases or multiple readers.
            // Only an unambiguous counter is safe to attribute to this subtask.
            if (pending != null) {
                return null;
            }
            try {
                pending = Long.parseLong(metric.getValue());
                if (pending < 0) {
                    return null;
                }
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return pending;
    }

    private A4SAggregatedMetricsResponseBody buildShardsResponse(
            MetricStore store,
            JobID jobId,
            JobVertexID vertexID,
            @Nullable AccessExecutionJobVertex vertex) {
        if (vertex == null || vertex.getTaskVertices().length != vertex.getParallelism()) {
            return new A4SAggregatedMetricsResponseBody(Status.INCOMPLETE_HISTOGRAMS, null);
        }
        try {
            List<StackDistanceHistogram> histograms = new ArrayList<>();
            for (AccessExecutionVertex task : vertex.getTaskVertices()) {
                if (task.getExecutionState() != ExecutionState.RUNNING) {
                    return new A4SAggregatedMetricsResponseBody(Status.INCOMPLETE_HISTOGRAMS, null);
                }
                MetricStore.ComponentMetricStore attempt =
                        store.getSubtaskAttemptMetricStore(
                                jobId.toString(),
                                vertexID.toString(),
                                task.getParallelSubtaskIndex(),
                                task.getCurrentExecutionAttempt().getAttemptNumber());
                String raw = attempt == null ? null : getStackDistanceHistogramRaw(attempt.metrics);
                if (raw == null || raw.isEmpty()) {
                    return new A4SAggregatedMetricsResponseBody(Status.INCOMPLETE_HISTOGRAMS, null);
                }
                histograms.add(StackDistanceHistogram.fromMetricString(raw));
            }
            return buildResponse(jobId, vertexID, histograms, vertex.getParallelism());
        } catch (IllegalArgumentException | ArithmeticException e) {
            return invalidHistogram(jobId, vertexID, e);
        }
    }

    private A4SAggregatedMetricsResponseBody buildResponse(
            JobID jobId,
            JobVertexID vertexID,
            List<StackDistanceHistogram> histograms,
            int shardsParallelism) {
        try {
            StackDistanceHistogram merged =
                    shardsParallelism == 0
                            ? StackDistanceHistogram.merge(histograms)
                            : StackDistanceHistogram.mergeShards(histograms, shardsParallelism);
            long totalFrequency = merged.getTotalFrequency();
            if (totalFrequency == 0) {
                return new A4SAggregatedMetricsResponseBody(Status.NO_SAMPLES, null);
            }
            if (histograms.stream().anyMatch(histogram -> histogram.getTotalFrequency() == 0)) {
                return new A4SAggregatedMetricsResponseBody(Status.INCOMPLETE_HISTOGRAMS, null);
            }
            if (totalFrequency == merged.getCompleteMisses()) {
                return new A4SAggregatedMetricsResponseBody(Status.NO_FINITE_BUCKETS, null);
            }
            return new A4SAggregatedMetricsResponseBody(
                    Status.AVAILABLE, MissRateCurve.fromStackDistanceHistogram(merged));
        } catch (IllegalArgumentException | ArithmeticException e) {
            return invalidHistogram(jobId, vertexID, e);
        }
    }

    private A4SAggregatedMetricsResponseBody invalidHistogram(
            JobID jobId, JobVertexID vertexID, RuntimeException e) {
        log.debug("Invalid RocksDB histogram for job {} vertex {}", jobId, vertexID, e);
        return new A4SAggregatedMetricsResponseBody(Status.INVALID_HISTOGRAM, null);
    }

    @Nullable
    private String getStackDistanceHistogramRaw(Map<String, String> metrics) {
        String histogram = null;
        for (Map.Entry<String, String> metricEntry : metrics.entrySet()) {
            String metricKey = metricEntry.getKey();
            String metricValue = metricEntry.getValue();
            if (metricKey == null || metricValue == null) {
                continue;
            }
            if (metricKey.equals(STACK_DISTANCE_HISTOGRAM_METRIC_NAME)
                    || metricKey.endsWith("." + STACK_DISTANCE_HISTOGRAM_METRIC_NAME)
                    || metricKey.equals(ROCKSDB_SANITIZED_HISTOGRAM_METRIC_NAME)
                    || metricKey.endsWith("." + ROCKSDB_SANITIZED_HISTOGRAM_METRIC_NAME)) {
                if (histogram != null) {
                    throw new IllegalArgumentException("Expected one RocksDB histogram per subtask");
                }
                histogram = metricValue;
            }
        }
        return histogram;
    }
}
