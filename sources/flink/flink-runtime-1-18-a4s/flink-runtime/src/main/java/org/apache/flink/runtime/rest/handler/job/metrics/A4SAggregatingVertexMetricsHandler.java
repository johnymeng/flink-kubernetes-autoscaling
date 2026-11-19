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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.a4s.core.MissRateCurve;
import org.apache.flink.a4s.core.StackDistanceHistogram;
import org.apache.flink.a4s.logging.A4SMetricsFlowStep;
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
        List<String> selectors = request.getQueryParameter(MetricsFilterParameter.class);
        boolean includeCacheCosts =
                selectors.contains(A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_COST_SAMPLES);
        boolean includeClassification =
                selectors.contains(A4SCacheCostSample.COMPACTION_QUERY_SELECTOR);

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
                    readCacheCostSample(subtask, active.metrics, now, includeClassification);
            if (sample != null) {
                samples.add(sample);
            }
        }
        samples.sort(Comparator.comparingInt(sample -> sample.subtask));
        return samples;
    }

    @Nullable
    private A4SCacheCostSample readCacheCostSample(
            int subtask, Map<String, String> metrics, long now, boolean includeClassification) {
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
                : A4SCacheCostSample.fromMetricString(subtask, raw, now, includeClassification);
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
