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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId);
        pathParameters.put(JobVertexIdPathParameter.KEY, jobVertexId);

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                handler.getMessageHeaders().getUnresolvedMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @Test
    public void testShardsRequiresTheCurrentAttemptAndClipsFinalCacheSize() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        ArchivedExecutionVertex task = mock(ArchivedExecutionVertex.class);
        ArchivedExecution attempt = mock(ArchivedExecution.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(1);
        when(vertex.getTaskVertices()).thenReturn(new ArchivedExecutionVertex[] {task});
        when(task.getExecutionState()).thenReturn(ExecutionState.RUNNING);
        when(task.getCurrentExecutionAttempt()).thenReturn(attempt);
        when(attempt.getAttemptNumber()).thenReturn(1);

        String histogram =
                StackDistanceHistogram.fromShardsMrcArray(
                                new long[] {Double.doubleToRawLongBits(1.0), 7, 3, 8,
                                    4, 4, 0, 0, 0, 0, 1, 0, 1, 2})
                        .toMetricString();
        putShardsHistogram(0, histogram);
        HandlerRequest<EmptyRequestBody> request =
                createRequest(jobId.toString(), jobVertexId.toString());
        assertThat(
                handler.handleRequest(request, restfulGateway).get().getMrcStatus(),
                equalTo(Status.INCOMPLETE_HISTOGRAMS));

        putShardsHistogram(1, histogram);
        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(request, restfulGateway).get();
        assertThat(response.getMrcStatus(), equalTo(Status.AVAILABLE));
        List<MissRateCurve.Point> points = response.getCurve().orElseThrow().getPoints();
        assertThat(points, hasSize(3));
        assertThat(points.get(2).getCacheSizeBytes(), equalTo(8L));
        assertThat(points.get(2).getMissRate(), closeTo(0.5, 1e-9));
    }

    private void putShardsHistogram(int attempt, String histogram) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, attempt, ""),
                        "rocksdb_stack-distance-histogram",
                        histogram));
    }

    @Test
    public void testSlotCacheCapacityRequiresDistinctCurrentAllocations() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex[] tasks = {runningTask(0, 1), runningTask(1, 2)};
        when(vertex.getTaskVertices()).thenReturn(tasks);
        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap(
                                "get",
                                Collections.singletonList(
                                        A4SAggregatedMetricsResponseBody
                                                .FIELD_NAME_CACHE_CAPACITY)),
                        Collections.emptyList());

        final String cache0 = "34d73888-3125-4cdf-9295-5ac3794d77e1";
        final String cache1 = "e6a1ef14-d1e9-4014-bc02-2bd6fa55ab04";
        putSlotCache(0, 1, "op", cache0 + ":67108864");
        putSlotCache(1, 1, "op", cache1 + ":9007199254740993");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCapacityBytes());

        putSlotCache(1, 2, "op", cache0 + ":67108864");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCapacityBytes());

        putSlotCache(0, 1, "alias", cache0 + ":67108864");
        putSlotCache(1, 2, "op", cache1 + ":9007199254740993");
        assertEquals(
                Long.valueOf(9007199321849857L),
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCapacityBytes());
        assertNull(
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get()
                        .getObservedBlockCacheCapacityBytes());

        putSlotCache(0, 1, "alias", cache1 + ":67108864");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCapacityBytes());

        putSlotCache(0, 1, "alias", cache0 + ":67108864");
        putSlotCache(1, 2, "op", cache1 + ":" + Long.MAX_VALUE);
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCapacityBytes());
    }

    private void putSlotCache(int subtask, int attempt, String scope, String reading) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, attempt, scope),
                        MetricNames.A4S_SLOT_BLOCK_CACHE,
                        reading));
    }

    @Test
    public void testSlotCacheUsageUsesNewestSampleOncePerCurrentCache() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex[] tasks = {runningTask(0, 1), runningTask(1, 2)};
        when(vertex.getTaskVertices()).thenReturn(tasks);
        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap(
                                "get",
                                Collections.singletonList(
                                        A4SAggregatedMetricsResponseBody.FIELD_NAME_CACHE_USAGE)),
                        Collections.emptyList());

        String cache0 = "34d73888-3125-4cdf-9295-5ac3794d77e1";
        String cache1 = "e6a1ef14-d1e9-4014-bc02-2bd6fa55ab04";
        putSlotUsage(0, 1, "first", cache0 + ":1:20");
        putSlotUsage(0, 1, "alias", cache0 + ":3:9007199254740993");
        putSlotUsage(1, 1, "first", cache1 + ":100:100");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());

        putSlotUsage(1, 2, "first", cache1 + ":2:0");
        assertEquals(
                Long.valueOf(9007199254740993L),
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());
        assertNull(
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());

        putSlotUsage(0, 1, "first", cache0 + ":3:20");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());

        putSlotUsage(0, 1, "first", cache0 + ":3:9007199254740993");
        putSlotUsage(1, 2, "first", cache0 + ":4:0");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());

        putSlotUsage(1, 2, "first", cache1 + ":4:-1");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());
        putSlotUsage(1, 2, "first", cache1 + ":5:" + Long.MAX_VALUE);
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheUsageBytes());
    }

    private void putSlotUsage(int subtask, int attempt, String scope, String reading) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, attempt, scope),
                        MetricNames.A4S_SLOT_BLOCK_CACHE_USAGE,
                        reading));
    }

    @Test
    public void testCacheCountersUseLatestSampleOfEachCurrentDatabase() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex[] tasks = {runningTask(0, 1), runningTask(1, 2)};
        when(vertex.getTaskVertices()).thenReturn(tasks);
        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap(
                                "get",
                                Collections.singletonList(
                                        A4SAggregatedMetricsResponseBody
                                                .FIELD_NAME_CACHE_COUNTERS)),
                        Collections.emptyList());

        String first = "34d73888-3125-4cdf-9295-5ac3794d77e1";
        String second = "e6a1ef14-d1e9-4014-bc02-2bd6fa55ab04";
        String third = "b191c482-0832-4f2f-b4f9-aac3542b07ae";
        putCacheCounters(0, 1, "first", first + ":1:14:0");
        putCacheCounters(0, 1, "alias", first + ":2:9007199254740993:0");
        putCacheCounters(0, 1, "chained", second + ":1:3:4");
        putCacheCounters(1, 1, "retired", third + ":100:20:20");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters());

        putCacheCounters(1, 2, "current", first + ":3:9007199254740994:0");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters());

        putCacheCounters(1, 2, "current", third + ":2:0:5");
        A4SAggregatedMetricsResponseBody.CacheCounters counters =
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters();
        assertEquals(9007199254740996L, counters.hits);
        assertEquals(9L, counters.misses);
        assertNull(
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters());

        putCacheCounters(0, 1, "alias", first + ":2:2:0");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters());
        putCacheCounters(0, 1, "alias", first + ":1:9007199254740993:0");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters());

        putCacheCounters(0, 1, "alias", first + ":2:" + Long.MAX_VALUE + ":0");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedBlockCacheCounters());
    }

    private void putCacheCounters(int subtask, int attempt, String scope, String reading) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, attempt, scope),
                        MetricNames.A4S_BLOCK_CACHE_COUNTERS,
                        reading));
    }

    @Test
    public void testStateAccessTimingUsesOnlyCurrentCompleteDatabaseSamples() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex[] tasks = {runningTask(0, 1), runningTask(1, 2)};
        when(vertex.getTaskVertices()).thenReturn(tasks);
        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap("get", Collections.singletonList(
                                A4SAggregatedMetricsResponseBody.FIELD_NAME_STATE_ACCESS_TIMING)),
                        Collections.emptyList());

        String first = "34d73888-3125-4cdf-9295-5ac3794d77e1";
        String second = "e6a1ef14-d1e9-4014-bc02-2bd6fa55ab04";
        putStateAccessTiming(0, 1, "first", first + ":1:1000:1");
        putStateAccessTiming(0, 1, "alias", first + ":2:9007199254741000:2");
        putStateAccessTiming(1, 1, "retired", second + ":100:9000:9");
        assertNull(handler.handleRequest(request, restfulGateway).get().getObservedStateAccessTiming());
        putStateAccessTiming(1, 2, "current", second + ":1:0:0");
        A4SAggregatedMetricsResponseBody.StateAccessTiming timing =
                handler.handleRequest(request, restfulGateway).get().getObservedStateAccessTiming();
        assertEquals(9007199254741000L, timing.nanos);
        assertEquals(2, timing.count);
        assertNull(handler.handleRequest(createRequest(jobId.toString(), jobVertexId.toString()), restfulGateway)
                .get().getObservedStateAccessTiming());
        putStateAccessTiming(1, 2, "current", second + ":2:1:0");
        assertNull(handler.handleRequest(request, restfulGateway).get().getObservedStateAccessTiming());
    }

    private void putStateAccessTiming(int subtask, int attempt, String scope, String reading) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, attempt, scope),
                        MetricNames.A4S_ROCKSDB_GET_TIMING,
                        reading));
    }

    @Test
    public void testLiveStateSizeCountsDistinctDatabasesFromCurrentAttempts() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex[] tasks = {runningTask(0, 1), runningTask(1, 2)};
        when(vertex.getTaskVertices()).thenReturn(tasks);
        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap(
                                "get",
                                Collections.singletonList(
                                        A4SAggregatedMetricsResponseBody.FIELD_NAME_STATE_SIZE)),
                        Collections.emptyList());

        String first = "34d73888-3125-4cdf-9295-5ac3794d77e1";
        String second = "e6a1ef14-d1e9-4014-bc02-2bd6fa55ab04";
        String third = "b191c482-0832-4f2f-b4f9-aac3542b07ae";
        putStateSize(0, 1, "db", first + ":1:" + Long.MAX_VALUE);
        putStateSize(0, 1, "alias", first + ":2:9007199254740993");
        putStateSize(0, 1, "chained", second + ":1:4");
        putStateSize(1, 1, "retired", third + ":100:8");
        assertNull(
                handler.handleRequest(request, restfulGateway).get().getObservedStateSizeBytes());

        putStateSize(1, 2, "current", third + ":2:0");
        assertEquals(
                Long.valueOf(9007199254740997L),
                handler.handleRequest(request, restfulGateway).get().getObservedStateSizeBytes());
        assertNull(
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get()
                        .getObservedStateSizeBytes());

        putStateSize(1, 2, "current", first + ":3:0");
        assertNull(
                handler.handleRequest(request, restfulGateway).get().getObservedStateSizeBytes());
        putStateSize(1, 2, "current", third + ":3:" + Long.MAX_VALUE);
        assertNull(
                handler.handleRequest(request, restfulGateway).get().getObservedStateSizeBytes());
        putStateSize(1, 2, "current", third + ":3:1");
        putStateSize(0, 1, "db", first + ":2:1");
        assertNull(
                handler.handleRequest(request, restfulGateway).get().getObservedStateSizeBytes());
    }

    private void putStateSize(int subtask, int attempt, String scope, String reading) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, attempt, scope),
                        MetricNames.A4S_ROCKSDB_STATE_SIZE,
                        reading));
    }

    @Test
    public void testTaskTimesRequireAllCountersAndTrackAttemptIdentity() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex first = runningTask(0, 1);
        ArchivedExecutionVertex second = runningTask(1, 2);
        ArchivedExecution firstAttempt = first.getCurrentExecutionAttempt();
        ArchivedExecution secondAttempt = second.getCurrentExecutionAttempt();
        when(firstAttempt.getAttemptId()).thenReturn(createExecutionAttemptId());
        when(secondAttempt.getAttemptId()).thenReturn(createExecutionAttemptId());
        when(vertex.getTaskVertices()).thenReturn(new ArchivedExecutionVertex[] {first, second});
        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap(
                                "get",
                                Collections.singletonList(
                                        A4SAggregatedMetricsResponseBody.FIELD_NAME_TASK_TIMES)),
                        Collections.emptyList());

        putTaskTime(0, 1, "80:800:0");
        putTaskTime(1, 1, "50:50:50");
        assertNull(handler.handleRequest(request, restfulGateway).get().getObservedTaskTimes());

        putTaskTime(1, 2, "20:700:20");
        A4SAggregatedMetricsResponseBody.TaskTimeTotals times =
                handler.handleRequest(request, restfulGateway).get().getObservedTaskTimes();
        assertEquals(100.0, times.busyMillis, 0.0);
        assertEquals(1500L, times.idleMillis);
        assertEquals(20L, times.backpressuredMillis);

        when(vertex.getTaskVertices()).thenReturn(new ArchivedExecutionVertex[] {second, first});
        assertEquals(
                times.rosterId,
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedTaskTimes()
                        .rosterId);
        when(secondAttempt.getAttemptId()).thenReturn(createExecutionAttemptId());
        assertFalse(
                times.rosterId.equals(
                        handler.handleRequest(request, restfulGateway)
                                .get()
                                .getObservedTaskTimes()
                                .rosterId));

        putTaskTime(1, 2, "NaN:700:20");
        assertNull(handler.handleRequest(request, restfulGateway).get().getObservedTaskTimes());
        assertNull(
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get()
                        .getObservedTaskTimes());
    }

    private void putTaskTime(int subtask, int attempt, String value) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), subtask, attempt, ""),
                        MetricNames.A4S_TASK_TIME_TOTALS,
                        value));
    }

    @Test
    public void testSourceBacklogRequiresEveryCurrentAttempt() throws Exception {
        ArchivedExecutionGraph graph = mock(ArchivedExecutionGraph.class);
        ArchivedExecutionJobVertex vertex = mock(ArchivedExecutionJobVertex.class);
        when(restfulGateway.requestJob(jobId, TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(graph));
        when(graph.getJobVertex(jobVertexId)).thenReturn(vertex);
        when(vertex.getParallelism()).thenReturn(2);
        ArchivedExecutionVertex[] tasks = {runningTask(0, 1), runningTask(1, 2)};
        when(vertex.getTaskVertices()).thenReturn(tasks);

        Map<String, String> paths = new HashMap<>();
        paths.put(JobIDPathParameter.KEY, jobId.toString());
        paths.put(JobVertexIdPathParameter.KEY, jobVertexId.toString());
        HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        handler.getMessageHeaders().getUnresolvedMessageParameters(),
                        paths,
                        Collections.singletonMap(
                                "get",
                                Collections.singletonList(
                                        A4SAggregatedMetricsResponseBody
                                                .FIELD_NAME_OBSERVED_BACKLOG)),
                        Collections.emptyList());

        putPendingRecords(0, 0, "Source", "95");
        putPendingRecords(1, 0, "Source", "7");
        putPendingRecords(0, 1, "Source", "0");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedSourceBacklogRecords());

        putPendingRecords(1, 2, "Source", "0");
        A4SAggregatedMetricsResponseBody complete =
                handler.handleRequest(request, restfulGateway).get();
        assertEquals(Long.valueOf(0), complete.getObservedSourceBacklogRecords());
        assertEquals(Status.HISTOGRAM_UNAVAILABLE, complete.getMrcStatus());
        A4SAggregatedMetricsResponseBody legacy =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();
        assertNull(legacy.getObservedSourceBacklogRecords());
        assertFalse(
                RestMapperUtils.getStrictObjectMapper()
                        .writeValueAsString(legacy)
                        .contains(A4SAggregatedMetricsResponseBody.FIELD_NAME_OBSERVED_BACKLOG));

        putPendingRecords(1, 2, "Source", "13");
        assertEquals(
                Long.valueOf(13),
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedSourceBacklogRecords());

        putPendingRecords(1, 2, "Source", "NaN");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedSourceBacklogRecords());
        putPendingRecords(1, 2, "Source", "13");

        putPendingRecords(1, 2, "Alias", "13");
        assertNull(
                handler.handleRequest(request, restfulGateway)
                        .get()
                        .getObservedSourceBacklogRecords());
    }

    private static ArchivedExecutionVertex runningTask(int index, int attemptNumber) {
        ArchivedExecutionVertex task = mock(ArchivedExecutionVertex.class);
        ArchivedExecution attempt = mock(ArchivedExecution.class);
        when(task.getExecutionState()).thenReturn(ExecutionState.RUNNING);
        when(task.getParallelSubtaskIndex()).thenReturn(index);
        when(task.getCurrentExecutionAttempt()).thenReturn(attempt);
        when(task.getCurrentExecutions()).thenReturn(Collections.singletonList(attempt));
        when(attempt.getAttemptNumber()).thenReturn(attemptNumber);
        return task;
    }

    private void putPendingRecords(int index, int attempt, String scope, String value) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), index, attempt, scope),
                        "pendingRecords",
                        value));
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

        MissRateCurve curve = response.getCurve().orElseThrow();
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

        MissRateCurve curve1 = response1.getCurve().orElseThrow();
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
        MissRateCurve curve2 = response2.getCurve().orElseThrow();
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

        assertThat(response.getMrcStatus(), equalTo(Status.HISTOGRAM_UNAVAILABLE));
        assertFalse(response.getCurve().isPresent());
    }

    @Test
    public void testHandleRequestReturnsAbsentCurveWhenTaskMetricStoreMissing() throws Exception {
        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();

        assertThat(response.getMrcStatus(), equalTo(Status.TASK_METRICS_UNAVAILABLE));
        assertFalse(response.getCurve().isPresent());
    }

    @Test
    public void testHandleRequestReportsMalformedHistogram() throws Exception {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId.toString(), jobVertexId.toString(), 0, 0, ""),
                        "stack-distance-histogram",
                        "{\"bucketCounts\": [1, \"bad\", 3]}"));

        A4SAggregatedMetricsResponseBody response =
                handler.handleRequest(
                                createRequest(jobId.toString(), jobVertexId.toString()),
                                restfulGateway)
                        .get();
        assertThat(response.getMrcStatus(), equalTo(Status.INVALID_HISTOGRAM));
        assertFalse(response.getCurve().isPresent());
    }
}
