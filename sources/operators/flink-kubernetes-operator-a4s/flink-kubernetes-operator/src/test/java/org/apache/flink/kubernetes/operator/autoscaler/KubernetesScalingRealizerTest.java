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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.realizer.ScalingRealizer.A4sIntentResult;
import org.apache.flink.autoscaler.state.A4sScalingIntent;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.diff.ReflectiveDiffBuilder;
import org.apache.flink.kubernetes.operator.service.NativeFlinkService;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests for KubernetesScalingRealizer. */
public class KubernetesScalingRealizerTest {

    @Test
    public void testApplyOverrides() {
        // Required to keep the test config context on legacy Flink YAML converters.
        //GlobalConfiguration.setStandardYaml(false);

        KubernetesJobAutoScalerContext ctx =
                TestingKubernetesAutoscalerUtils.createContext("test", null);

        new KubernetesScalingRealizer()
                .realizeParallelismOverrides(ctx, Map.of("a", "1", "b", "2"));

        assertThat(
                        ctx.getResource()
                                .getSpec()
                                .getFlinkConfiguration()
                                .get(PipelineOptions.PARALLELISM_OVERRIDES.key()))
                .satisfiesAnyOf(
                        // Currently no enforced order inside the overrides string
                        overrides -> assertThat(overrides).isEqualTo("a:1,b:2"),
                        overrides -> assertThat(overrides).isEqualTo("b:2,a:1"));
    }

    @Test
    public void testAutoscalerOverridesStringDoesNotChangeUnlessOverridesChange() {
        // Create an overrides map which returns the keys in a deterministic order
        LinkedHashMap<String, String> newOverrides = new LinkedHashMap<>();
        newOverrides.put("b", "2");
        newOverrides.put("a", "1");

        assertOverridesDoNotChange("a:1,b:2", newOverrides);
        assertOverridesDoNotChange("b:2,a:1", newOverrides);
    }

    @Test
    public void testApplyMemoryOverrides() {
        KubernetesJobAutoScalerContext ctx =
                TestingKubernetesAutoscalerUtils.createContext("test", null);

        ConfigChanges overrides = new ConfigChanges();
        MemorySize memoryOverride = MemorySize.ofMebiBytes(4096);
        overrides.addOverride(TaskManagerOptions.TOTAL_PROCESS_MEMORY, memoryOverride);
        new KubernetesScalingRealizer().realizeConfigOverrides(ctx, overrides);

        assertThat(ctx.getResource()).isInstanceOf(FlinkDeployment.class);
        assertThat(
                        ((FlinkDeployment) ctx.getResource())
                                .getSpec()
                                .getTaskManager()
                                .getResource()
                                .getMemory())
                .isEqualTo(String.valueOf(memoryOverride.getBytes()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void changesDeploymentStatusOnlyAfterTheRuntimeConfirmsTheTarget() throws Exception {
        var deployment = TestUtils.buildApplicationCluster();
        deployment.getMetadata().setUid("evaluated-uid");
        deployment.getMetadata().setGeneration(1L);
        var jobId = new JobID();
        var vertex = new JobVertexID().toString();
        var clock = Clock.systemUTC();
        var realizer = new KubernetesScalingRealizer();
        var initialConfig = Configuration.fromMap(deployment.getSpec().getFlinkConfiguration());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, initialConfig, clock);
        var reconciliation = deployment.getStatus().getReconciliationStatus();
        reconciliation.markReconciledSpecAsStable();

        var service = mock(NativeFlinkService.class);
        var context = mock(FlinkResourceContext.class);
        when(context.getResource()).thenReturn(deployment);
        when(context.getDeploymentMode()).thenReturn(KubernetesDeploymentMode.NATIVE);
        when(context.getFlinkService()).thenReturn(service);
        var client = mock(KubernetesClient.class);
        var latest = (NamespaceableResource<FlinkDeployment>) mock(NamespaceableResource.class);
        when(context.getKubernetesClient()).thenReturn(client);
        when(client.resource(deployment)).thenReturn(latest);
        when(context.getObserveConfig()).thenReturn(initialConfig);
        when(context.getDeployConfig(deployment.getSpec()))
                .thenAnswer(
                        ignored ->
                                Configuration.fromMap(
                                        deployment.getSpec().getFlinkConfiguration()));
        var autoscalerContext =
                new KubernetesJobAutoScalerContext(
                        jobId,
                        JobStatus.RUNNING,
                        initialConfig,
                        new UnregisteredMetricsGroup(),
                        null,
                        context);
        var intent =
                new A4sScalingIntent(
                        jobId.toString(),
                        "token",
                        Long.MAX_VALUE,
                        null,
                        Map.of(vertex, "2"),
                        Map.of(vertex, "profile"),
                        Map.of(vertex, 1));

        var edited = TestUtils.buildApplicationCluster();
        edited.getMetadata().setUid("evaluated-uid");
        edited.getMetadata().setGeneration(2L);
        when(latest.get()).thenReturn(edited);
        assertThat(realizer.reconcileA4sIntent(autoscalerContext, intent, clock, true))
                .isEqualTo(A4sIntentResult.PENDING);
        verifyNoInteractions(service);

        when(latest.get()).thenReturn(deployment);
        when(service.reconcileA4sIntent(context, intent, clock, true))
                .thenReturn(
                        A4sIntentResult.DISCARDED,
                        A4sIntentResult.PENDING,
                        A4sIntentResult.APPLIED);
        realizer.restoreA4sOverrides(
                autoscalerContext, intent.getParallelism(), intent.getResourceProfiles());
        assertThat(realizer.reconcileA4sIntent(autoscalerContext, intent, clock, true))
                .isEqualTo(A4sIntentResult.DISCARDED);
        assertThat(realizer.reconcileA4sIntent(autoscalerContext, intent, clock, true))
                .isEqualTo(A4sIntentResult.PENDING);
        assertThat(deployment.getSpec().getFlinkConfiguration())
                .doesNotContainKeys(
                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                        KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES.key());

        assertThat(realizer.reconcileA4sIntent(autoscalerContext, intent, clock, true))
                .isEqualTo(A4sIntentResult.PENDING);
        var confirmed = reconciliation.deserializeLastReconciledSpec();
        assertThat(
                        new ReflectiveDiffBuilder<>(
                                        KubernetesDeploymentMode.NATIVE,
                                        confirmed,
                                        deployment.getSpec())
                                .build()
                                .getType())
                .isEqualTo(DiffType.IGNORE);

        // A new reconciliation starts with the unchanged user spec and the saved status.
        deployment.getSpec().setFlinkConfiguration(new LinkedHashMap<>(initialConfig.toMap()));
        reconciliation.markReconciledSpecAsStable();
        when(context.getObserveConfig())
                .thenReturn(Configuration.fromMap(confirmed.getFlinkConfiguration()));
        realizer.restoreA4sOverrides(
                autoscalerContext, intent.getParallelism(), intent.getResourceProfiles());
        assertThat(realizer.reconcileA4sIntent(autoscalerContext, intent, clock, true))
                .isEqualTo(A4sIntentResult.APPLIED);
    }

    private void assertOverridesDoNotChange(
            String currentOverrides, LinkedHashMap<String, String> newOverrides) {

        KubernetesJobAutoScalerContext ctx =
                TestingKubernetesAutoscalerUtils.createContext("test", null);
        FlinkDeployment resource = (FlinkDeployment) ctx.getResource();

        // Create resource with existing parallelism overrides
        resource.getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), currentOverrides);
        resource.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(resource.getSpec(), resource);
        resource.getSpec()
                .getFlinkConfiguration()
                .remove(PipelineOptions.PARALLELISM_OVERRIDES.key());

        new KubernetesScalingRealizer().realizeParallelismOverrides(ctx, newOverrides);

        assertThat(
                        ctx.getResource()
                                .getSpec()
                                .getFlinkConfiguration()
                                .get(PipelineOptions.PARALLELISM_OVERRIDES.key()))
                .isEqualTo(currentOverrides);
    }
}
