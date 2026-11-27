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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CHECKPOINT_RESCALE_WATCH_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
class CheckpointCompletionEventsTest {
    private KubernetesClient kubernetesClient;
    private static final String JOB = "0123456789abcdef0123456789abcdef";
    private static final String NAME = "q8-" + JOB + "-config-map";
    private static final String TX =
            "jobId: '" + JOB + "'\ncheckpointTriggerId: 'abcd'\nphase: CHECKPOINT_TRIGGERED\n";
    private static final String KEY = "checkpointID-0009";

    @Test
    void registersNoSourcesByDefaultAndHonorsOperatorOverride() {
        var config = new Configuration();
        assertThat(CheckpointCompletionEvents.sources(null, config)).isEmpty();
        config.set(CHECKPOINT_RESCALE_ENABLED, true);
        assertThat(CheckpointCompletionEvents.watchEnabled(config)).isTrue();
        config.set(OPERATOR_CHECKPOINT_RESCALE_WATCH_ENABLED, false);
        assertThat(CheckpointCompletionEvents.sources(null, config)).isEmpty();
        config.set(CHECKPOINT_RESCALE_ENABLED, false);
        config.set(OPERATOR_CHECKPOINT_RESCALE_WATCH_ENABLED, true);
        assertThat(CheckpointCompletionEvents.watchEnabled(config)).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void sharedInformersDeliverOnlyThePendingDeploymentsCheckpoint() throws Exception {
        var namespace = "work";
        var maps = kubernetesClient.configMaps().inNamespace(namespace);
        var state =
                new ConfigMapBuilder()
                        .withNewMetadata()
                        .withName(ConfigMapStore.configMapName("q8"))
                        .withNamespace(namespace)
                        .addToLabels(
                                Constants.LABEL_COMPONENT_KEY,
                                ConfigMapStore.LABEL_COMPONENT_AUTOSCALER)
                        .endMetadata()
                        .addToData(
                                KubernetesAutoScalerStateStore.CHECKPOINT_RESCALE_TRANSACTION_KEY,
                                TX)
                        .build();
        maps.resource(state).create();
        var other = checkpoint(namespace, "other-" + JOB + "-config-map", Map.of());
        other.getMetadata().getLabels().put(Constants.LABEL_APP_KEY, "other");
        var target = checkpoint(namespace, NAME, Map.of());
        for (var map : new ConfigMap[] {other, target}) {
            map.getMetadata()
                    .getLabels()
                    .put(
                            Constants.LABEL_CONFIGMAP_TYPE_KEY,
                            Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
            maps.resource(map).create();
        }
        var sdk =
                ConfigurationService.newOverriddenConfigurationService(
                        overrides ->
                                overrides
                                        .withKubernetesClient(kubernetesClient)
                                        .withCacheSyncTimeout(Duration.ofSeconds(10)));
        ControllerConfiguration<FlinkDeployment> controller = mock(ControllerConfiguration.class);
        when(controller.getEffectiveNamespaces()).thenReturn(Set.of(namespace));
        when(controller.getConfigurationService()).thenReturn(sdk);
        var context = new EventSourceContext<FlinkDeployment>(null, controller, kubernetesClient);
        var config = new Configuration();
        config.set(CHECKPOINT_RESCALE_ENABLED, true);
        var sources = CheckpointCompletionEvents.sources(context, config);
        var events = new LinkedBlockingQueue<ResourceID>();
        try {
            for (var source : sources) {
                var informer = (InformerEventSource<?, ?>) source;
                informer.setConfigurationService(sdk);
                informer.setEventHandler(event -> events.add(event.getRelatedCustomResourceID()));
                informer.start();
            }
            for (var map : new ConfigMap[] {other, target}) {
                maps.withName(map.getMetadata().getName())
                        .edit(
                                current ->
                                        new ConfigMapBuilder(current)
                                                .addToData(KEY, "handle")
                                                .build());
            }
            assertThat(events.poll(10, TimeUnit.SECONDS))
                    .isEqualTo(new ResourceID("q8", namespace));
            assertThat(events).isEmpty();
        } finally {
            sources.forEach(source -> source.stop());
        }
    }

    @Test
    void ignoresCounterUpdatesExistingCheckpointsAndRemovalsWithoutLookingUpState() {
        var old = checkpoint("work", NAME, Map.of("counter", "8", KEY, "old-handle"));
        @SuppressWarnings("unchecked")
        Function<ResourceID, Optional<ConfigMap>> lookup = mock(Function.class);
        assertThat(
                        CheckpointCompletionEvents.shouldWake(
                                checkpoint("work", NAME, Map.of("counter", "9", KEY, "new-handle")),
                                old,
                                lookup))
                .isFalse();
        assertThat(
                        CheckpointCompletionEvents.shouldWake(
                                checkpoint("work", NAME, Map.of("counter", "9")), old, lookup))
                .isFalse();
        verifyNoInteractions(lookup);
    }

    @Test
    void looksUpOnlyTheMatchingDeploymentAndJobInTheCheckpointNamespace() {
        var previous = checkpoint("work", NAME, Map.of());
        var current = checkpoint("work", NAME, Map.of(KEY, "handle"));
        var lookup = lookup(TX);
        assertThat(CheckpointCompletionEvents.shouldWake(current, previous, lookup)).isTrue();
        assertThat(
                        CheckpointCompletionEvents.shouldWake(
                                checkpoint("elsewhere", NAME, Map.of(KEY, "handle")),
                                previous,
                                lookup))
                .isFalse();
        assertThat(
                        CheckpointCompletionEvents.shouldWake(
                                checkpoint("work", "q8-old-job-config-map", Map.of(KEY, "handle")),
                                previous,
                                lookup))
                .isFalse();
        current.getMetadata().getLabels().clear();
        assertThat(CheckpointCompletionEvents.shouldWake(current, previous, lookup)).isFalse();
    }

    @Test
    void ignoresFinishedMissingAndUnreadableTransactions() {
        var old = checkpoint("work", NAME, Map.of());
        var current = checkpoint("work", NAME, Map.of(KEY, "handle"));
        assertThat(CheckpointCompletionEvents.shouldWake(current, old, id -> Optional.empty()))
                .isFalse();
        for (String state :
                new String[] {
                    TX.replace("CHECKPOINT_TRIGGERED", "APPLYING"),
                    TX.replace("abcd", ""),
                    TX.replace(JOB, ""),
                    "null",
                    "bad: ["
                }) {
            assertThat(CheckpointCompletionEvents.shouldWake(current, old, lookup(state)))
                    .as(state)
                    .isFalse();
        }
    }

    private static ConfigMap checkpoint(String namespace, String name, Map<String, String> data) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .addToLabels(Constants.LABEL_APP_KEY, "q8")
                .endMetadata()
                .withData(data)
                .build();
    }

    private static Function<ResourceID, Optional<ConfigMap>> lookup(String raw) {
        var id = new ResourceID("autoscaler-q8", "work");
        var state =
                new ConfigMapBuilder()
                        .addToData(
                                KubernetesAutoScalerStateStore.CHECKPOINT_RESCALE_TRANSACTION_KEY,
                                raw)
                        .build();
        return requested -> id.equals(requested) ? Optional.of(state) : Optional.empty();
    }
}
