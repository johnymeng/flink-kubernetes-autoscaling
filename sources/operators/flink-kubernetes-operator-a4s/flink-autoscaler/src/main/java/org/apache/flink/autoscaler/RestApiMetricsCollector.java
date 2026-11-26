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

import org.apache.flink.api.common.JobID;
import org.apache.flink.a4s.core.MissRateCurveObservation.Status;
import org.apache.flink.autoscaler.utils.a4s.A4SAggregatedMetricsResponseBody;
import org.apache.flink.autoscaler.utils.a4s.A4SAggregatedVertexMetricsHeaders;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregateTaskManagerMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedTaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.autoscaler.metrics.FlinkMetric.HEAP_MEMORY_MAX;
import static org.apache.flink.autoscaler.metrics.FlinkMetric.HEAP_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.FlinkMetric.MANAGED_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.FlinkMetric.METASPACE_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.FlinkMetric.TOTAL_GC_TIME_PER_SEC;

/** Metric collector using flink rest api. */
public class RestApiMetricsCollector<KEY, Context extends JobAutoScalerContext<KEY>>
        extends ScalingMetricCollector<KEY, Context> {
    private static final Logger LOG = LoggerFactory.getLogger(RestApiMetricsCollector.class);

    private static final Map<String, FlinkMetric> COMMON_TM_METRIC_NAMES =
            Map.of(
                    "Status.JVM.Memory.Heap.Max", HEAP_MEMORY_MAX,
                    "Status.JVM.Memory.Heap.Used", HEAP_MEMORY_USED,
                    "Status.Flink.Memory.Managed.Used", MANAGED_MEMORY_USED,
                    "Status.JVM.Memory.Metaspace.Used", METASPACE_MEMORY_USED);
    private static final Map<String, FlinkMetric> TM_METRIC_NAMES_WITH_GC =
            ImmutableMap.<String, FlinkMetric>builder()
                    .putAll(COMMON_TM_METRIC_NAMES)
                    .put("Status.JVM.GarbageCollector.All.TimeMsPerSecond", TOTAL_GC_TIME_PER_SEC)
                    .build();

    @Override
    protected Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> queryAllAggregatedMetrics(
            Context ctx, Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames) {

        return filteredVertexMetricNames.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e -> queryAggregatedVertexMetrics(ctx, e.getKey(), e.getValue())));
    }

    @Override
    protected Map<JobVertexID, A4SAggregatedMetricsResponseBody> queryAllA4sMetrics(
            Context ctx, JobTopology topology, Collection<JobVertexID> jobVertexIds) {
        var conf = ctx.getConfiguration();
        boolean captureBacklog =
                conf.get(AutoScalerOptions.JUSTIN_ENABLED)
                        && conf.get(AutoScalerOptions.A4S_ENABLED);
        var observations = new HashMap<JobVertexID, A4SAggregatedMetricsResponseBody>();
        for (JobVertexID vertex : jobVertexIds) {
            var observation =
                    captureBacklog && topology.isSource(vertex)
                            ? queryVertexA4sMetrics(ctx, ctx.getJobID(), vertex, true)
                            : queryVertexMissRateCurve(ctx, ctx.getJobID(), vertex);
            observations.put(vertex, observation);
            if (observation.getMrcStatus() != Status.AVAILABLE) {
                LOG.debug(
                        "QuickMRC status for job {}, vertex {}: {}",
                        ctx.getJobID(),
                        vertex,
                        observation.getMrcStatus());
            }
        }
        return observations;
    }

    @SneakyThrows
    protected Map<FlinkMetric, AggregatedMetric> queryAggregatedVertexMetrics(
            Context ctx, JobVertexID jobVertexID, Map<String, FlinkMetric> metrics) {

        LOG.debug("Querying metrics {} for {}", metrics, jobVertexID);

        var parameters = new AggregatedSubtaskMetricsParameters();
        var pathIt = parameters.getPathParameters().iterator();

        ((JobIDPathParameter) pathIt.next()).resolve(ctx.getJobID());
        ((JobVertexIdPathParameter) pathIt.next()).resolve(jobVertexID);

        parameters
                .getQueryParameters()
                .iterator()
                .next()
                .resolveFromString(StringUtils.join(metrics.keySet(), ","));

        try (var restClient = ctx.getRestClusterClient()) {

            var responseBody =
                    restClient
                            .sendRequest(
                                    AggregatedSubtaskMetricsHeaders.getInstance(),
                                    parameters,
                                    EmptyRequestBody.getInstance())
                            .get();

            return aggregateByFlinkMetric(metrics, responseBody);
        }
    }

    protected A4SAggregatedMetricsResponseBody queryVertexMissRateCurve(
            Context ctx, JobID jobId, JobVertexID jobVertexID) {
        return queryVertexA4sMetrics(ctx, jobId, jobVertexID, false);
    }

    protected A4SAggregatedMetricsResponseBody queryVertexA4sMetrics(
            Context ctx, JobID jobId, JobVertexID jobVertexID, boolean includeBacklog) {
        var parameters = new AggregatedSubtaskMetricsParameters();
        var pathIt = parameters.getPathParameters().iterator();
        ((JobIDPathParameter) pathIt.next()).resolve(jobId);
        ((JobVertexIdPathParameter) pathIt.next()).resolve(jobVertexID);
        var conf = ctx.getConfiguration();
        boolean a4s =
                conf.get(AutoScalerOptions.JUSTIN_ENABLED)
                        && conf.get(AutoScalerOptions.A4S_ENABLED);
        boolean includeCacheCapacity =
                a4s && conf.getBoolean("state.backend.rocksdb.metrics.block-cache-capacity", false);
        boolean includeCacheUsage =
                a4s && conf.getBoolean("state.backend.rocksdb.metrics.block-cache-usage", false);
        boolean includeCacheCounters =
                a4s
                        && conf.getBoolean("state.backend.rocksdb.metrics.block-cache-hit", false)
                        && conf.getBoolean("state.backend.rocksdb.metrics.block-cache-miss", false);
        boolean includeStateSize =
                a4s
                        && conf.getBoolean(
                                "state.backend.rocksdb.metrics.estimate-live-data-size", false);
        if (includeBacklog || a4s) {
            var filter = (MetricsFilterParameter) parameters.getQueryParameters().iterator().next();
            var fields = new ArrayList<String>();
            if (includeBacklog) {
                fields.add(A4SAggregatedMetricsResponseBody.FIELD_NAME_OBSERVED_BACKLOG);
            }
            if (includeCacheCapacity) {
                fields.add(A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_CAPACITY);
            }
            if (includeCacheUsage) {
                fields.add(A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_USAGE);
            }
            if (includeCacheCounters) {
                fields.add(A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_COUNTERS);
            }
            if (includeStateSize) {
                fields.add(A4SAggregatedMetricsResponseBody.FIELD_NAME_STATE_SIZE);
            }
            if (a4s) {
                fields.add(A4SAggregatedMetricsResponseBody.FIELD_NAME_TASK_TIMES);
            }
            filter.resolve(fields);
        }

        try (var restClient = ctx.getRestClusterClient()) {
            A4SAggregatedMetricsResponseBody responseBody =
                    restClient
                            .sendRequest(
                                    A4SAggregatedVertexMetricsHeaders.getInstance(),
                                    parameters,
                                    EmptyRequestBody.getInstance())
                            .get();
            return responseBody == null
                    ? new A4SAggregatedMetricsResponseBody(Status.INVALID_RESPONSE, null)
                    : responseBody;
        } catch (Exception e) {
            LOG.debug(
                    "Unable to fetch A4S miss-rate curve for job {}, vertex {}, continuing without MRC",
                    jobId,
                    jobVertexID,
                    e);
            return new A4SAggregatedMetricsResponseBody(Status.REQUEST_FAILED, null);
        }
    }

    @Override
    @SneakyThrows
    protected Map<FlinkMetric, Metric> queryJmMetrics(Context ctx) {
        Map<String, FlinkMetric> metrics =
                Map.of(
                        "taskSlotsTotal", FlinkMetric.NUM_TASK_SLOTS_TOTAL,
                        "taskSlotsAvailable", FlinkMetric.NUM_TASK_SLOTS_AVAILABLE);
        try (var restClient = ctx.getRestClusterClient()) {
            return queryJmMetrics(restClient, metrics);
        }
    }

    @SneakyThrows
    protected Map<FlinkMetric, Metric> queryJmMetrics(
            RestClusterClient<?> restClient, Map<String, FlinkMetric> metrics) {

        var parameters = new JobManagerMetricsMessageParameters();
        var queryParamIt = parameters.getQueryParameters().iterator();

        MetricsFilterParameter filterParameter = (MetricsFilterParameter) queryParamIt.next();
        filterParameter.resolve(List.copyOf(metrics.keySet()));

        var responseBody =
                restClient
                        .sendRequest(
                                JobManagerMetricsHeaders.getInstance(),
                                parameters,
                                EmptyRequestBody.getInstance())
                        .get();

        return responseBody.getMetrics().stream()
                .collect(Collectors.toMap(m -> metrics.get(m.getId()), m -> m));
    }

    @Override
    protected Map<FlinkMetric, AggregatedMetric> queryTmMetrics(Context ctx) throws Exception {
        try (var restClient = ctx.getRestClusterClient()) {
            // Unfortunately we cannot simply query for all metric names as Flink doesn't return
            // anything if any of the metric names is missing
            boolean hasGcMetrics =
                    jobsWithGcMetrics.computeIfAbsent(
                            ctx.getJobKey(),
                            k -> {
                                boolean gcMetricsFound =
                                        !queryAggregatedTmMetrics(
                                                        restClient, TM_METRIC_NAMES_WITH_GC)
                                                .isEmpty();
                                if (!gcMetricsFound) {
                                    LOG.debug("No GC metrics found, using only heap information");
                                } else {
                                    LOG.debug("TaskManager GC metrics found");
                                }
                                return gcMetricsFound;
                            });
            var tmMetrics =
                    queryAggregatedTmMetrics(
                            restClient,
                            hasGcMetrics ? TM_METRIC_NAMES_WITH_GC : COMMON_TM_METRIC_NAMES);
            if (!tmMetrics.isEmpty()) {
                return tmMetrics;
            } else {
                // If metrics are missing that means we cant find even the required ones
                // Let's error out and retry in the next loop.
                jobsWithGcMetrics.remove(ctx.getJobKey());
                throw new RuntimeException("Missing required TM metrics");
            }
        }
    }

    @SneakyThrows
    protected Map<FlinkMetric, AggregatedMetric> queryAggregatedTmMetrics(
            RestClusterClient<?> restClient, Map<String, FlinkMetric> metrics) {

        var parameters = new AggregateTaskManagerMetricsParameters();
        var queryParamIt = parameters.getQueryParameters().iterator();

        MetricsFilterParameter filterParameter = (MetricsFilterParameter) queryParamIt.next();
        filterParameter.resolve(List.copyOf(metrics.keySet()));

        MetricsAggregationParameter aggregationParameter =
                (MetricsAggregationParameter) queryParamIt.next();
        aggregationParameter.resolve(
                List.of(
                        MetricsAggregationParameter.AggregationMode.MIN,
                        MetricsAggregationParameter.AggregationMode.MAX,
                        MetricsAggregationParameter.AggregationMode.AVG));

        var responseBody =
                restClient
                        .sendRequest(
                                AggregatedTaskManagerMetricsHeaders.getInstance(),
                                parameters,
                                EmptyRequestBody.getInstance())
                        .get();

        return aggregateByFlinkMetric(metrics, responseBody);
    }

    private Map<FlinkMetric, AggregatedMetric> aggregateByFlinkMetric(
            Map<String, FlinkMetric> metrics, AggregatedMetricsResponseBody responseBody) {
        return responseBody.getMetrics().stream()
                .collect(
                        Collectors.toMap(
                                m -> metrics.get(m.getId()),
                                m -> m,
                                (m1, m2) ->
                                        new AggregatedMetric(
                                                m1.getId() + "-" + m2.getId(),
                                                m1.getMin() != null
                                                        ? Math.min(m1.getMin(), m2.getMin())
                                                        : null,
                                                m1.getMax() != null
                                                        ? Math.max(m1.getMax(), m2.getMax())
                                                        : null,
                                                // Average can't be computed
                                                null,
                                                m1.getSum() != null
                                                        ? m1.getSum() + m2.getSum()
                                                        : null,
                                                m1.getSkew() != null
                                                        ? Math.max(m1.getSkew(), m2.getSkew())
                                                        : null)));
    }
}
