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

package io.a4s.runner;

import static org.junit.jupiter.api.Assertions.*;

import io.a4s.api.EvaluationRequest;
import io.a4s.api.MigrationEstimate;
import io.a4s.api.ScalingPlan;
import io.a4s.core.A4sPolicy;
import io.a4s.http.DecisionClient;
import io.a4s.http.SnapshotRecorder;
import io.a4s.http.Wire;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DecisionServerTest {
    private static final byte[] EXAMPLE = example();
    private static final String EXAMPLE_JSON = new String(EXAMPLE, StandardCharsets.UTF_8);

    @TempDir Path temp;

    private static byte[] example() {
        try {
            return Files.readAllBytes(Path.of("../examples/evaluate.json"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void returnsEquivalentReplayAndRemoteRecommendationsAcrossRestarts() throws Exception {
        var request = Wire.decode(EXAMPLE, EvaluationRequest.class);
        var policy = new A4sPolicy();
        var embedded = policy.evaluate(request);
        var decision = embedded.operators.get("join");
        assertEquals(ScalingPlan.Action.RESCALE, embedded.action);
        assertEquals(6, decision.target.parallelism);
        assertEquals(44_739_243, decision.target.cachePerTaskBytes);
        assertEquals(268_435_458, decision.targetTotalCacheBytes);
        assertTrue(decision.predictedRecordsPerSecond > 60_000);
        assertTrue(embedded.requiresStatePreservation);
        assertNull(embedded.migration);
        assertTrue(policy.isApplicable(request, embedded, 10_002));
        assertFalse(policy.isApplicable(request, embedded, 40_001));

        var snapshots = temp.resolve("snapshots.jsonl");
        var decisions = temp.resolve("decisions.jsonl");
        SnapshotRecorder.append(snapshots, request);
        SnapshotRecorder.append(snapshots, request);
        Main.main(
                new String[] {
                    "replay", "--input", snapshots.toString(), "--output", decisions.toString()
                });
        var expected = new String(Wire.encode(embedded), StandardCharsets.UTF_8);
        assertEquals(java.util.List.of(expected, expected), Files.readAllLines(decisions));

        var client = new DecisionClient();
        for (int i = 0; i < 2; i++) {
            try (var server = new DecisionServer(new InetSocketAddress("127.0.0.1", 0), policy)) {
                server.start();
                var endpoint = endpoint(server);
                var remote = client.evaluate(endpoint, Duration.ofSeconds(2), request);
                assertArrayEquals(Wire.encode(embedded), Wire.encode(remote));
                assertArrayEquals(
                        Wire.encode(embedded),
                        Wire.encode(client.evaluate(endpoint, Duration.ofSeconds(2), request)));
            }
        }
    }

    @Test
    void validatesContractAndFailsClosed() throws Exception {
        var policy = new A4sPolicy();
        assertEquals(
                ScalingPlan.Action.INSUFFICIENT_METRICS,
                policy.evaluate(
                                decode(
                                        EXAMPLE_JSON.replace(
                                                "\"metricsComplete\": true",
                                                "\"metricsComplete\": false")))
                        .action);
        assertEquals(
                ScalingPlan.Action.INSUFFICIENT_METRICS,
                policy.evaluate(decode(EXAMPLE_JSON.replace("10001", "40001"))).action);
        assertEquals(
                ScalingPlan.Action.UNSUPPORTED_ACTION,
                policy.evaluate(decode(EXAMPLE_JSON.replace("CACHE_PER_TASK", "PLACEMENT")))
                        .action);
        assertEquals(
                ScalingPlan.Action.INFEASIBLE_CONFIGURATION,
                policy.evaluate(decode(EXAMPLE_JSON.replace("1073741824", "0"))).action);
        assertEquals(
                ScalingPlan.Action.INFEASIBLE_CONFIGURATION,
                policy.evaluate(
                                decode(
                                        EXAMPLE_JSON.replace(
                                                "\"cpuBudgetCores\": 6", "\"cpuBudgetCores\": 4")))
                        .action);
        assertEquals(
                ScalingPlan.Action.NO_CHANGE,
                policy.evaluate(
                                decode(
                                        EXAMPLE_JSON.replace(
                                                "\"version\": 1",
                                                "\"version\": 1, \"pendingDecisionId\": \"pending\"")))
                        .action);
        var bounded =
                decode(
                        EXAMPLE_JSON.replace(
                                "\"cooldownMillis\": 0",
                                "\"cooldownMillis\": 0, \"maxDowntimeMillis\": 20"));
        assertEquals(ScalingPlan.Action.INFEASIBLE_CONFIGURATION, policy.evaluate(bounded).action);
        var calibrated =
                new A4sPolicy(
                        (current, targets) ->
                                Optional.of(
                                        new MigrationEstimate(
                                                10L, 8L, null, null, null, null, null, 8L)));
        assertEquals(8L, calibrated.evaluate(bounded).migration.downtimeMillis);
        var original = decode(EXAMPLE_JSON);
        var stale = policy.evaluate(decode(EXAMPLE_JSON.replace("generation-7", "generation-6")));
        assertFalse(policy.isApplicable(original, stale, 10002));

        try (var server = new DecisionServer(new InetSocketAddress("127.0.0.1", 0), policy)) {
            server.start();
            var endpoint = endpoint(server);
            assertEquals(
                    400,
                    post(
                            endpoint,
                            EXAMPLE_JSON.replace("\"schemaVersion\": 1", "\"schemaVersion\": 2")));
            assertEquals(400, post(endpoint, EXAMPLE_JSON.replace("\"schemaVersion\": 1,", "")));
            assertEquals(
                    400, post(endpoint, EXAMPLE_JSON.replace("\"windowEndMillis\": 10000,", "")));
            assertEquals(
                    400, post(endpoint, EXAMPLE_JSON.replace("\"metricsComplete\": true,", "")));
            assertEquals(
                    400,
                    post(
                            endpoint,
                            EXAMPLE_JSON.replace("\"missRate\": 0.7", "\"missRate\": \"NaN\"")));
            assertEquals(
                    400,
                    post(endpoint, EXAMPLE_JSON.replace("\"missRate\": 0.7", "\"missRate\": 7")));
            assertEquals(
                    400,
                    post(
                            endpoint,
                            EXAMPLE_JSON.replace(
                                    "\"missRate\": 0.7", "\"missRate\": 0.7,\"unknown\": 0")));
            assertEquals(
                    400,
                    post(
                            endpoint,
                            EXAMPLE_JSON.replace("\"parallelism\": 4", "\"parallelism\": 0")));
            assertEquals(400, post(endpoint, EXAMPLE_JSON + " {}"));
            assertEquals(413, post(endpoint, " ".repeat(Wire.MAX_BODY_BYTES + 1)));
        }
    }

    @Test
    void timeoutAndTerminationFailWithoutProducingAPlan() throws Exception {
        var request = Wire.decode(EXAMPLE, EvaluationRequest.class);
        var client = new DecisionClient();
        URI endpoint;
        try (var server =
                new DecisionServer(
                        new InetSocketAddress("127.0.0.1", 0),
                        input -> {
                            try {
                                TimeUnit.SECONDS.sleep(3);
                                return new A4sPolicy().evaluate(input);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(e);
                            }
                        },
                        100)) {
            server.start();
            endpoint = endpoint(server);
            assertEquals(504, post(endpoint, EXAMPLE_JSON));
            assertThrows(
                    IOException.class,
                    () -> client.evaluate(endpoint, Duration.ofMillis(10), request));
        }
        assertThrows(
                IOException.class,
                () -> client.evaluate(endpoint, Duration.ofMillis(150), request));
    }

    @Test
    void legacyPlacementMatchesOriginalSupportedMpcAndRetry() throws Exception {
        // As on the original A4S frontier, P1 needs 700 MiB and does not fit.
        // P2 needs 100 MiB, plus the operator's 16 MiB base.
        var request = decode(Files.readString(Path.of("../examples/legacy-retry.json")));
        var plan = new A4sPolicy().evaluate(request);
        assertEquals(ScalingPlan.Action.RESCALE, plan.action);
        assertEquals(2, plan.attempts);
        var allocation = plan.operators.get("join");
        assertEquals(2, allocation.target.parallelism);
        assertEquals(116L * 1024 * 1024, allocation.target.managedMemoryPerTaskBytes);
        assertEquals(100L * 1024 * 1024, allocation.modeledMinimumTotalCacheBytes);
        assertNull(allocation.target.cachePerTaskBytes);
        assertNull(allocation.predictedRecordsPerSecond);
    }

    private static URI endpoint(DecisionServer server) {
        return URI.create("http://127.0.0.1:" + server.port());
    }

    private static EvaluationRequest decode(String input) throws IOException {
        return Wire.decode(input.getBytes(StandardCharsets.UTF_8), EvaluationRequest.class);
    }

    private static int post(URI endpoint, String body) throws Exception {
        var request =
                HttpRequest.newBuilder(endpoint.resolve("/v1/evaluate"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        return HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.discarding())
                .statusCode();
    }
}
