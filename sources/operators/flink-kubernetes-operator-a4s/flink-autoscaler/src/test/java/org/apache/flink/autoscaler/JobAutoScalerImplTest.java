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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.exceptions.NotReadyException;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.metrics.TestMetrics;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.A4sScalingIntent;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Map.entry;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_SCALING_HISTORY_AGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for JobAutoScalerImpl. */
public class JobAutoScalerImplTest {

    private JobAutoScalerContext<JobID> context;
    private TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();
        context.getConfiguration().set(AUTOSCALER_ENABLED, true);

        scalingRealizer = new TestingScalingRealizer<>();
        eventCollector = new TestingEventCollector<>();
        stateStore = new InMemoryAutoScalerStateStore<>();
    }

    @Test
    void testMetricReporting() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        JobTopology jobTopology = new JobTopology(new VertexInfo(jobVertexID, Map.of(), 1, 10));

        var metricsCollector =
                new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(jobTopology);
        metricsCollector.updateMetrics(
                jobVertexID,
                TestMetrics.builder()
                        .numRecordsIn(0)
                        .numRecordsOut(0)
                        .numRecordsInPerSec(500.)
                        .maxBusyTimePerSec(420)
                        .pendingRecords(0L)
                        .build());

        ScalingMetricEvaluator evaluator = new ScalingMetricEvaluator();
        ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor =
                new ScalingExecutor<>(eventCollector, stateStore);

        var autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector,
                        evaluator,
                        scalingExecutor,
                        eventCollector,
                        scalingRealizer,
                        stateStore);

        autoscaler.scale(context);

        metricsCollector.updateMetrics(jobVertexID, m -> m.setNumRecordsIn(100));
        autoscaler.scale(context);

        MetricGroup metricGroup = autoscaler.flinkMetrics.get(context.getJobKey()).getMetricGroup();
        assertEquals(
                0.42,
                getGaugeValue(
                        metricGroup,
                        AutoscalerFlinkMetrics.AVERAGE,
                        AutoscalerFlinkMetrics.JOB_VERTEX_ID,
                        jobVertexID.toHexString(),
                        ScalingMetric.LOAD.name()),
                "Expected scaling metric LOAD was not reported. Reporting is broken");
    }

    @SuppressWarnings("unchecked")
    private static double getGaugeValue(
            MetricGroup metricGroup, String gaugeName, String... nestedMetricGroupNames) {
        for (String nestedMetricGroupName : nestedMetricGroupNames) {
            metricGroup =
                    ((Map<String, GenericMetricGroup>)
                                    Whitebox.getInternalState(metricGroup, "groups"))
                            .get(nestedMetricGroupName);
        }
        var metrics = (Map<String, Metric>) Whitebox.getInternalState(metricGroup, "metrics");
        return ((Gauge<Double>) metrics.get(gaugeName)).getValue();
    }

    @Test
    void testErrorReporting() throws Exception {
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);

        autoscaler.scale(context);
        Assertions.assertEquals(
                1, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());

        autoscaler.scale(context);
        Assertions.assertEquals(
                2, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());

        assertEquals(0, autoscaler.flinkMetrics.get(context.getJobKey()).getNumScalingsCount());
    }

    @Test
    public void testTolerateRecoverableExceptions() throws Exception {
        TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>
                collectorWhichThrowsRecoverableException =
                        new TestingMetricsCollector<>(new JobTopology(Collections.emptySet())) {
                            @Override
                            protected Collection<String> queryAggregatedMetricNames(
                                    RestClusterClient<?> restClient,
                                    JobID jobID,
                                    JobVertexID jobVertexID) {
                                throw new NotReadyException(new Exception());
                            }
                        };
        collectorWhichThrowsRecoverableException.setJobUpdateTs(Instant.now());

        var autoscaler =
                new JobAutoScalerImpl<>(
                        collectorWhichThrowsRecoverableException,
                        null,
                        null,
                        eventCollector,
                        scalingRealizer,
                        stateStore);

        // Should not produce an error
        autoscaler.scale(context);
        Assertions.assertEquals(
                0, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());
    }

    @Test
    public void testRealizeParallelismOverridesExceptions() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        JobTopology jobTopology = new JobTopology(new VertexInfo(jobVertexID, Map.of(), 1, 20));
        var metricsCollector =
                new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(jobTopology);
        ScalingRealizer<JobID, JobAutoScalerContext<JobID>>
                realizeParallelismOverridesWithExceptionsScalingRealizer =
                        new ScalingRealizer<>() {
                            @Override
                            public void realizeParallelismOverrides(JobAutoScalerContext<JobID> context, Map<String, String> parallelismOverrides, Map<String, String> justinOverrides) throws Exception {

                            }

                            @Override
                            public void realizeConfigOverrides(
                                    JobAutoScalerContext context, ConfigChanges configChanges) {}

                            @Override
                            public void realizeParallelismOverrides(
                                    JobAutoScalerContext context, Map parallelismOverrides) {
                                throw new RuntimeException(
                                        "Test Realize Parallelism Overrides Exceptions.");
                            }
                        };
        stateStore.storeParallelismOverrides(context, Map.of(jobVertexID.toHexString(), "2"));

        var autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector,
                        null,
                        null,
                        eventCollector,
                        realizeParallelismOverridesWithExceptionsScalingRealizer,
                        stateStore);

        // Should produce an error
        autoscaler.scale(context);
        Assertions.assertEquals(
                1, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());
    }

    @Test
    void a4sRealizesOnlyCompleteOverridesAfterSuccessfulPersistence() throws Exception {
        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.A4S_ENABLED, true);
        conf.set(AutoScalerOptions.JUSTIN_ENABLED, true);
        conf.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, false);

        var failFlush = new AtomicBoolean(true);
        var store =
                new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public void flush(JobAutoScalerContext<JobID> ctx) {
                        if (failFlush.get()) {
                            throw new IllegalStateException("Injected persistence failure");
                        }
                    }
                };
        var vertex = new JobVertexID();
        var parallelism = Map.of(vertex.toHexString(), "2");
        var profiles =
                Map.of(
                        vertex.toHexString(),
                        ResourceProfile.newBuilder().setManagedMemoryMB(116).build().toString());
        var realizedProfiles = new HashMap<String, String>();
        var realizer =
                new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public void realizeParallelismOverrides(
                            JobAutoScalerContext<JobID> ctx,
                            Map<String, String> requested,
                            Map<String, String> requestedProfiles) {
                        super.realizeParallelismOverrides(ctx, requested);
                        realizedProfiles.putAll(requestedProfiles);
                    }
                };
        var topology = new JobTopology(new VertexInfo(vertex, Map.of(), 1, 20));
        var metricsCollector =
                new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(topology) {
                    @Override
                    public CollectedMetricHistory updateMetrics(
                            JobAutoScalerContext<JobID> ctx,
                            AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore) {
                        return new CollectedMetricHistory(topology, new TreeMap<>(), Instant.EPOCH);
                    }
                };
        var autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector, null, null, eventCollector, realizer, store);

        store.storeParallelismOverrides(context, parallelism);
        store.storeResourceProfileOverrides(context, profiles);
        autoscaler.scale(context);
        assertThat(realizer.events).isEmpty();
        assertThat(store.getParallelismOverrides(context)).isEmpty();
        assertThat(store.getResourceProfileOverrides(context)).isEmpty();

        failFlush.set(false);
        store.storeParallelismOverrides(context, parallelism);
        autoscaler.scale(context);
        assertThat(realizer.events).isEmpty();

        store.storeResourceProfileOverrides(context, profiles);
        autoscaler.scale(context);
        assertThat(realizer.events).hasSize(1);
        assertEquals(parallelism, realizer.events.getFirst().getParallelismOverrides());
        assertEquals(profiles, realizedProfiles);
    }

    @Test
    void reconcilesConditionalTargetOnlyAfterFlushAndCommitsObservedOverrides() throws Exception {
        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.JUSTIN_ENABLED, true);
        conf.set(AutoScalerOptions.A4S_ENABLED, true);
        conf.set(AutoScalerOptions.A4S_CONDITIONAL_JUSTIN_ENABLED, true);
        conf.set(SCALING_ENABLED, true);
        var vertex = new JobVertexID();
        var intent =
                new A4sScalingIntent(
                        context.getJobID().toString(),
                        "observed-token",
                        Long.MAX_VALUE,
                        A4sScalingIntent.fingerprint(conf),
                        Map.of(vertex.toString(), "2"),
                        Map.of(vertex.toString(), ResourceProfile.ZERO.toString()),
                        Map.of(vertex.toString(), 1));
        var failFlush = new AtomicBoolean(true);
        var persistedIntent = new AtomicReference<A4sScalingIntent>();
        var store =
                new InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public void flush(JobAutoScalerContext<JobID> ctx) {
                        if (failFlush.get()) {
                            throw new IllegalStateException(
                                    "Injected ConfigMap persistence failure");
                        }
                        persistedIntent.set(getA4sScalingIntent(ctx).orElse(null));
                    }
                };
        var topology = new JobTopology(new VertexInfo(vertex, Map.of(), 1, 20));
        var metricsCollector =
                new TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>(topology) {
                    @Override
                    public CollectedMetricHistory updateMetrics(
                            JobAutoScalerContext<JobID> ctx,
                            AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore) {
                        store.storeA4sScalingIntent(ctx, intent);
                        return new CollectedMetricHistory(topology, new TreeMap<>(), Instant.EPOCH);
                    }
                };
        var reconciliations = new AtomicInteger();
        var realizer =
                new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public A4sIntentResult reconcileA4sIntent(
                            JobAutoScalerContext<JobID> ctx,
                            A4sScalingIntent pending,
                            Clock clock,
                            boolean allowSubmission) {
                        assertThat(pending).isEqualTo(intent).isEqualTo(persistedIntent.get());
                        assertThat(allowSubmission).isTrue();
                        return reconciliations.incrementAndGet() == 1
                                ? A4sIntentResult.PENDING
                                : A4sIntentResult.APPLIED;
                    }
                };
        var autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector, null, null, eventCollector, realizer, store);

        assertThrows(IllegalStateException.class, () -> autoscaler.scale(context));
        assertThat(reconciliations.get()).isZero();
        assertThat(store.getA4sScalingIntent(context)).isEmpty();

        failFlush.set(false);
        autoscaler.scale(context);
        assertThat(store.getA4sScalingIntent(context)).contains(intent);
        assertThat(store.getParallelismOverrides(context)).isEmpty();
        assertThat(store.getResourceProfileOverrides(context)).isEmpty();

        autoscaler.scale(context);
        assertThat(reconciliations.get()).isEqualTo(2);
        assertThat(store.getA4sScalingIntent(context)).isEmpty();
        assertThat(store.getParallelismOverrides(context)).isEqualTo(intent.getParallelism());
        assertThat(store.getResourceProfileOverrides(context))
                .isEqualTo(intent.getResourceProfiles());
        assertThat(realizer.events).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"quota", "policy", "guard", "disabled", "legacy-intent"})
    void changedSettingsPreventResubmissionButAllowConfirmation(String change) throws Exception {
        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.JUSTIN_ENABLED, true);
        conf.set(AutoScalerOptions.A4S_ENABLED, true);
        conf.set(AutoScalerOptions.A4S_CONDITIONAL_JUSTIN_ENABLED, true);
        conf.set(AutoScalerOptions.CPU_QUOTA, 4.0);
        String fingerprint =
                change.equals("legacy-intent") ? null : A4sScalingIntent.fingerprint(conf);
        var vertex = new JobVertexID().toString();
        var intent =
                new A4sScalingIntent(
                        context.getJobID().toString(),
                        "observed-token",
                        Long.MAX_VALUE,
                        fingerprint,
                        Map.of(vertex, "2"),
                        Map.of(vertex, ResourceProfile.ZERO.toString()),
                        Map.of(vertex, 1));
        stateStore.storeA4sScalingIntent(context, intent);
        stateStore.flush(context);

        switch (change) {
            case "quota":
                conf.set(AutoScalerOptions.CPU_QUOTA, 1.0);
                break;
            case "policy":
                conf.set(AutoScalerOptions.JUSTIN_ENABLED, false);
                break;
            case "guard":
                conf.set(AutoScalerOptions.A4S_CONDITIONAL_JUSTIN_ENABLED, false);
                break;
            case "disabled":
                conf.set(AUTOSCALER_ENABLED, false);
                break;
            default:
                assertThat(change).isEqualTo("legacy-intent");
        }
        var reconciliations = new AtomicInteger();
        var realizer =
                new TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public A4sIntentResult reconcileA4sIntent(
                            JobAutoScalerContext<JobID> ctx,
                            A4sScalingIntent pending,
                            Clock clock,
                            boolean allowSubmission) {
                        assertThat(allowSubmission).isFalse();
                        assertThat(pending).isEqualTo(intent);
                        return reconciliations.incrementAndGet() == 1
                                ? A4sIntentResult.PENDING
                                : A4sIntentResult.APPLIED;
                    }
                };
        var autoscaler =
                new JobAutoScalerImpl<>(null, null, null, eventCollector, realizer, stateStore);

        autoscaler.scale(context);
        assertThat(stateStore.getA4sScalingIntent(context)).contains(intent);
        assertThat(stateStore.getParallelismOverrides(context)).isEmpty();
        autoscaler.scale(context);
        assertThat(reconciliations.get()).isEqualTo(2);
        assertThat(stateStore.getA4sScalingIntent(context)).isEmpty();
        assertThat(stateStore.getParallelismOverrides(context)).isEqualTo(intent.getParallelism());
        assertThat(stateStore.getResourceProfileOverrides(context))
                .isEqualTo(intent.getResourceProfiles());
        assertThat(realizer.events).isEmpty();
    }

    @Test
    void testParallelismOverrides() throws Exception {
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);

        // Initially we should return empty overrides, do not crate any state
        assertThat(autoscaler.getParallelismOverrides(context)).isEmpty();
        assertThat(stateStore.getParallelismOverrides(context)).isEmpty();

        var v1 = new JobVertexID().toString();
        var v2 = new JobVertexID().toString();
        stateStore.storeParallelismOverrides(context, Map.of(v1, "1", v2, "2"));
        stateStore.flush(context);

        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));

        assertThat(stateStore.getParallelismOverrides(context)).isEqualTo(Map.of(v1, "1", v2, "2"));

        // Disabling autoscaler should clear overrides
        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "false");

        autoscaler.scale(context);
        assertThat(autoscaler.getParallelismOverrides(context)).isEmpty();
        assertParallelismOverrides(null);

        // Make sure we don't update in kubernetes once removed
        autoscaler.scale(context);

        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "true");
        autoscaler.applyParallelismOverrides(context);

        assertThat(stateStore.getParallelismOverrides(context)).isEmpty();
        assertParallelismOverrides(null);

        stateStore.storeParallelismOverrides(context, Map.of(v1, "1", v2, "2"));
        stateStore.flush(context);
        autoscaler.applyParallelismOverrides(context);

        assertThat(autoscaler.getParallelismOverrides(context)).isEqualTo(Map.of(v1, "1", v2, "2"));
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));

        context.getConfiguration().setString(SCALING_ENABLED.key(), "false");

        autoscaler.applyParallelismOverrides(context);
        assertThat(autoscaler.getParallelismOverrides(context)).isEqualTo(Map.of(v1, "1", v2, "2"));
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));

        // Test error handling
        // Invalid config
        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "asd");
        autoscaler.scale(context);
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));
    }

    @Test
    public void testApplyAutoscalerParallelism() throws Exception {
        var overrides = new HashMap<String, String>();
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore) {
                    public Map<String, String> getParallelismOverrides(
                            JobAutoScalerContext<JobID> ctx) {
                        return new HashMap<>(overrides);
                    }
                };

        // Verify no scalingRealizer if overrides are empty
        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(null);

        // Make sure overrides are applied to the scalingRealizer
        var v1 = new JobVertexID();
        overrides.put(v1.toHexString(), "2");

        // Verify no upgrades if overrides are empty
        autoscaler.applyParallelismOverrides(context);

        assertParallelismOverrides(Map.of(v1.toHexString(), "2"));

        // We set a user override for v1, it should be ignored and the autoscaler override should
        // take precedence
        context.getConfiguration()
                .setString(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1");
        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1.toHexString(), "2"));

        // Define partly overlapping overrides, user overrides for new vertices should be applied
        var v2 = new JobVertexID();

        context.getConfiguration()
                .setString(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1," + v2 + ":4");
        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1.toString(), "2", v2.toString(), "4"));

        // Make sure user overrides apply to excluded vertices
        context.getConfiguration()
                .setString(AutoScalerOptions.VERTEX_EXCLUDE_IDS.key(), v1.toString());
        context.getConfiguration()
                .setString(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1," + v2 + ":4");

        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1.toString(), "1", v2.toString(), "4"));
    }

    @Test
    void testApplyConfigOverrides() throws Exception {
        context.getConfiguration().set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);

        // Initially we should return empty overrides, do not crate any state
        assertThat(stateStore.getConfigChanges(context).getOverrides()).isEmpty();

        ConfigChanges config = new ConfigChanges();
        config.addOverride(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.42f);
        config.addRemoval(TaskManagerOptions.TASK_HEAP_MEMORY);
        stateStore.storeConfigChanges(context, config);
        stateStore.flush(context);

        context.getConfiguration().set(AutoScalerOptions.A4S_ENABLED, true);
        context.getConfiguration().set(AutoScalerOptions.JUSTIN_ENABLED, true);
        autoscaler.applyConfigOverrides(context);
        assertThat(scalingRealizer.events).isEmpty();
        context.getConfiguration().set(AutoScalerOptions.A4S_ENABLED, false);
        context.getConfiguration().set(AutoScalerOptions.JUSTIN_ENABLED, false);

        autoscaler.applyConfigOverrides(context);
        var event = getEvent();
        assertThat(event.getConfigChanges().getOverrides())
                .containsExactly(entry(TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(), "0.42"));
        assertThat(event.getConfigChanges().getRemovals())
                .containsExactly(TaskManagerOptions.TASK_HEAP_MEMORY.key());
        assertThat(stateStore.getConfigChanges(context)).isEqualTo(config);

        // Disabling autoscaler should clear overrides
        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "false");
        autoscaler.scale(context);
        autoscaler.applyConfigOverrides(context);
        assertThat(getEvent().getConfigChanges().getOverrides()).isEmpty();
    }

    @Test
    void testAutoscalerDisabled() throws Exception {
        context.getConfiguration().setBoolean(AUTOSCALER_ENABLED, false);
        context.getConfiguration().set(VERTEX_SCALING_HISTORY_AGE, Duration.ofMillis(200));

        var scalingHistory = new TreeMap<Instant, ScalingSummary>();
        scalingHistory.put(Instant.ofEpochMilli(100), new ScalingSummary());
        scalingHistory.put(Instant.ofEpochMilli(200), new ScalingSummary());

        stateStore.storeScalingHistory(context, Map.of(new JobVertexID(), scalingHistory));
        assertFalse(stateStore.getScalingHistory(context).isEmpty());

        stateStore.storeParallelismOverrides(context, Map.of("vertex", "4"));
        assertFalse(stateStore.getParallelismOverrides(context).isEmpty());

        TreeMap<Instant, CollectedMetrics> metrics = new TreeMap<>();
        metrics.put(Instant.now(), new CollectedMetrics());
        stateStore.storeCollectedMetrics(context, metrics);
        assertFalse(stateStore.getCollectedMetrics(context).isEmpty());

        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);
        autoscaler.scale(context);

        assertTrue(stateStore.getScalingHistory(context).isEmpty());
        assertTrue(stateStore.getScalingHistory(context).isEmpty());
        assertTrue(stateStore.getParallelismOverrides(context).isEmpty());
    }

    private void assertParallelismOverrides(Map<String, String> expectedOverrides) {
        TestingScalingRealizer.Event<JobID, JobAutoScalerContext<JobID>> scalingEvent;
        do {
            scalingEvent = getEvent();
        } while (scalingEvent != null && scalingEvent.getParallelismOverrides() == null);

        if (scalingEvent == null) {
            return;
        }
        assertThat(scalingEvent).isNotNull();
        assertEquals(expectedOverrides, scalingEvent.getParallelismOverrides());
    }

    @Nullable
    private TestingScalingRealizer.Event<JobID, JobAutoScalerContext<JobID>> getEvent() {
        return scalingRealizer.events.poll();
    }
}
