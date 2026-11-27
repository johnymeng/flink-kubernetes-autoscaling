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

package org.apache.flink.kubernetes.operator.autoscaler.state;

import org.apache.flink.autoscaler.CheckpointRescaleTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.utils.Constants;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CHECKPOINT_RESCALE_WATCH_ENABLED;

/** Wakes a pending resize when Flink records a completed checkpoint in Kubernetes HA. */
public final class CheckpointCompletionEvents {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCompletionEvents.class);

    private CheckpointCompletionEvents() {}

    public static List<EventSource> sources(
            EventSourceContext<FlinkDeployment> context, Configuration operatorConfig) {
        if (!watchEnabled(operatorConfig)) {
            return List.of();
        }

        // Cache autoscaler transactions without scheduling reconciliation for state writes.
        var stateConfig =
                InformerConfiguration.from(ConfigMap.class, context)
                        .withLabelSelector(
                                Constants.LABEL_COMPONENT_KEY
                                        + "="
                                        + ConfigMapStore.LABEL_COMPONENT_AUTOSCALER)
                        .withSecondaryToPrimaryMapper(map -> Set.of())
                        .withNamespacesInheritedFromController(context)
                        .followNamespaceChanges(true)
                        .build();
        var states = new InformerEventSource<ConfigMap, FlinkDeployment>(stateConfig, context);

        var checkpointConfig =
                InformerConfiguration.from(ConfigMap.class, context)
                        .withLabelSelector(
                                Constants.LABEL_CONFIGMAP_TYPE_KEY
                                        + "="
                                        + Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY)
                        .withSecondaryToPrimaryMapper(Mappers.fromLabel(Constants.LABEL_APP_KEY))
                        .withOnAddFilter(map -> false)
                        .withOnDeleteFilter((map, unknown) -> false)
                        .withOnUpdateFilter(
                                (current, previous) -> shouldWake(current, previous, states::get))
                        .withNamespacesInheritedFromController(context)
                        .followNamespaceChanges(true)
                        .build();
        return List.of(
                states,
                new InformerEventSource<ConfigMap, FlinkDeployment>(checkpointConfig, context));
    }

    static boolean watchEnabled(Configuration operatorConfig) {
        return operatorConfig
                .getOptional(OPERATOR_CHECKPOINT_RESCALE_WATCH_ENABLED)
                .orElseGet(() -> operatorConfig.get(CHECKPOINT_RESCALE_ENABLED));
    }

    static boolean shouldWake(
            ConfigMap current,
            ConfigMap previous,
            Function<ResourceID, Optional<ConfigMap>> lookup) {
        var data = current.getData();
        var oldData = previous.getData();
        if (data == null
                || data.keySet().stream()
                        .noneMatch(
                                key ->
                                        key.startsWith(Constants.CHECKPOINT_ID_KEY_PREFIX)
                                                && (oldData == null
                                                        || !oldData.containsKey(key)))) {
            return false;
        }
        var meta = current.getMetadata();
        var app = meta.getLabels().get(Constants.LABEL_APP_KEY);
        if (app == null || app.isBlank()) {
            return false;
        }

        var stateId = new ResourceID(ConfigMapStore.configMapName(app), meta.getNamespace());
        return lookup.apply(stateId)
                .map(ConfigMap::getData)
                .map(
                        state ->
                                state.get(
                                        KubernetesAutoScalerStateStore
                                                .CHECKPOINT_RESCALE_TRANSACTION_KEY))
                .map(raw -> isPendingJob(raw, app, meta.getName()))
                .orElse(false);
    }

    private static boolean isPendingJob(String raw, String app, String checkpointMapName) {
        try {
            var tx =
                    KubernetesAutoScalerStateStore.YAML_MAPPER.readValue(
                            raw, CheckpointRescaleTransaction.class);
            return tx != null
                    && tx.getPhase() == CheckpointRescaleTransaction.Phase.CHECKPOINT_TRIGGERED
                    && tx.getCheckpointTriggerId() != null
                    && !tx.getCheckpointTriggerId().isBlank()
                    && tx.getJobId() != null
                    && !tx.getJobId().isBlank()
                    && (app + "-" + tx.getJobId() + "-config-map").equals(checkpointMapName);
        } catch (JacksonException e) {
            LOG.debug("Cannot read pending resize for {}", checkpointMapName, e);
            return false;
        }
    }
}
