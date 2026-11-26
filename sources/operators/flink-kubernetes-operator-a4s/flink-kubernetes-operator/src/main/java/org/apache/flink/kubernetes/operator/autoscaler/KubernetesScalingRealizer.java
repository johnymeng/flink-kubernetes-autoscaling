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

import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.state.A4sScalingIntent;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.autoscaler.tuning.MemoryTuning;
import org.apache.flink.configuration.*;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.diff.ReflectiveDiffBuilder;
import org.apache.flink.kubernetes.operator.service.NativeFlinkService;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;

/** The Kubernetes implementation for applying parallelism overrides. */
public class KubernetesScalingRealizer
        implements ScalingRealizer<ResourceID, KubernetesJobAutoScalerContext> {

    public static final ConfigOption<Map<String, String>> RESOURCE_PROFILE_OVERRIDES =
            ConfigOptions.key("pipeline.jobvertex-resourceprofile-overrides")
                    .mapType()
                    .defaultValue(Collections.emptyMap());

    public static final ConfigOption<Map<String, String>> SCALING_CONFIGURATIONS_PERIODS =
            ConfigOptions.key("pipeline.scaling-configurations-periods")
                    .mapType()
                    .defaultValue(Collections.emptyMap());

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesScalingRealizer.class);

    @Override
    public void realizeParallelismOverrides(
            KubernetesJobAutoScalerContext context, Map<String, String> parallelismOverrides) {

        context.getResource()
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                        getOverrideString(
                                context,
                                PipelineOptions.PARALLELISM_OVERRIDES,
                                parallelismOverrides));
    }

    @Override
    public void realizeParallelismOverrides(KubernetesJobAutoScalerContext context, Map<String, String> parallelismOverrides, Map<String, String> justinOverrides) {
        LOG.debug("We are in KubernetesScalingRealizer");
        var config = context.getResource()
                .getSpec()
                .getFlinkConfiguration();
        config.put(
                PipelineOptions.PARALLELISM_OVERRIDES.key(),
                getOverrideString(
                        context, PipelineOptions.PARALLELISM_OVERRIDES, parallelismOverrides));
        config.put(
                RESOURCE_PROFILE_OVERRIDES.key(),
                getOverrideString(context, RESOURCE_PROFILE_OVERRIDES, justinOverrides));
    }

    @Override
    public void restoreA4sOverrides(
            KubernetesJobAutoScalerContext context,
            Map<String, String> parallelism,
            Map<String, String> profiles) {
        if (!parallelism.isEmpty()
                && matchesOverrides(
                        context.getResourceContext().getObserveConfig(), parallelism, profiles)) {
            realizeParallelismOverrides(context, parallelism, profiles);
        }
    }

    @Override
    public A4sIntentResult reconcileA4sIntent(
            KubernetesJobAutoScalerContext context,
            A4sScalingIntent intent,
            Clock clock,
            boolean allowSubmission)
            throws Exception {
        var resourceContext = context.getResourceContext();
        if (!(context.getResource() instanceof FlinkDeployment)
                || !(resourceContext.getFlinkService() instanceof NativeFlinkService)) {
            return A4sIntentResult.DISCARDED;
        }
        var deployment = (FlinkDeployment) context.getResource();
        var reconciliation = deployment.getStatus().getReconciliationStatus();
        var observed = resourceContext.getObserveConfig();
        if (reconciliation.getState() != ReconciliationState.DEPLOYED || observed == null) {
            return A4sIntentResult.PENDING;
        }
        var diff =
                new ReflectiveDiffBuilder<>(
                                resourceContext.getDeploymentMode(),
                                reconciliation.deserializeLastReconciledSpec(),
                                deployment.getSpec())
                        .build();
        if (diff.getType() != DiffType.IGNORE) {
            return A4sIntentResult.DISCARDED;
        }
        if (!reconciliation.isLastReconciledSpecStable()) {
            return A4sIntentResult.PENDING;
        }
        var service = (NativeFlinkService) resourceContext.getFlinkService();
        var result = service.reconcileA4sIntent(resourceContext, intent, clock, allowSubmission);
        if (result != A4sIntentResult.APPLIED
                || matchesOverrides(
                        observed, intent.getParallelism(), intent.getResourceProfiles())) {
            return result;
        }

        realizeParallelismOverrides(context, intent.getParallelism(), intent.getResourceProfiles());
        ReconciliationUtils.updateStatusForDeployedSpec(
                deployment, resourceContext.getDeployConfig(deployment.getSpec()), clock);
        // Keep the intent until the next reconcile observes the persisted status.
        return A4sIntentResult.PENDING;
    }

    private static boolean matchesOverrides(
            Configuration observed, Map<String, String> parallelism, Map<String, String> profiles) {
        return observed != null
                && observed.get(PipelineOptions.PARALLELISM_OVERRIDES).equals(parallelism)
                && observed.get(RESOURCE_PROFILE_OVERRIDES).equals(profiles);
    }

    @Override
    public void realizeConfigOverrides(
            KubernetesJobAutoScalerContext context, ConfigChanges configChanges) {
        if (!(context.getResource() instanceof FlinkDeployment)) {
            // We can't adjust the configuration of non-job deployments.
            return;
        }
        FlinkDeployment flinkDeployment = ((FlinkDeployment) context.getResource());
        // Apply config overrides
        Map<String, String> flinkConf = flinkDeployment.getSpec().getFlinkConfiguration();
        for (String keyToRemove : configChanges.getRemovals()) {
            flinkConf.remove(keyToRemove);
        }
        flinkConf.putAll(configChanges.getOverrides());

        // Update total memory in spec
        var totalMemoryOverride =
                MemoryTuning.getTotalMemory(Configuration.fromMap(flinkConf), context);
        if (totalMemoryOverride.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Total memory override {} is not valid", totalMemoryOverride);
            return;
        }
        Resource tmResource = flinkDeployment.getSpec().getTaskManager().getResource();
        // Make sure to parse in the same way as the original deploy code path.
        var currentMemory =
                MemorySize.parse(
                        FlinkConfigBuilder.parseResourceMemoryString(tmResource.getMemory()));
        if (!totalMemoryOverride.equals(currentMemory)) {
            // Adjust the resource memory to change the total TM memory
            tmResource.setMemory(String.valueOf(totalMemoryOverride.getBytes()));
        }
    }

    @Nullable
    private static String getOverrideString(
            KubernetesJobAutoScalerContext context,
            ConfigOption<Map<String, String>> option,
            Map<String, String> newOverrides) {
        if (context.getResource().getStatus().getReconciliationStatus().isBeforeFirstDeployment()) {
            return ConfigurationUtils.convertValue(newOverrides, String.class);
        }

        var conf = context.getResourceContext().getObserveConfig();
        var currentOverrides = conf.getOptional(option).orElse(Map.of());

        // Check that the overrides actually changed and not just the String representation.
        // This way we prevent reconciling a NOOP config change which would unnecessarily redeploy
        // the pipeline.
        if (currentOverrides.equals(newOverrides)) {
            // If overrides are identical, use the previous string as-is.
            return conf.getValue(option);
        } else {
            return ConfigurationUtils.convertValue(newOverrides, String.class);
        }
    }
}
