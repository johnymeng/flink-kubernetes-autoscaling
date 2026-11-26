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

import io.a4s.api.CachePoint;
import io.a4s.api.EvaluationRequest;
import io.a4s.api.MigrationEstimate;
import io.a4s.api.OperatorConfiguration;
import io.a4s.api.OperatorMetrics;
import io.a4s.api.ScalingPlan;
import io.a4s.api.Worker;
import io.a4s.core.A4sPolicy;
import io.a4s.core.PerformanceModel;
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
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
    void refusesNominalCapacityAndHonorsAttestedWorkerBounds() throws Exception {
        var policy = new A4sPolicy();
        var nominal = decode(EXAMPLE_JSON.replace("MEASURED_CACHE_BYTES", "NOMINAL_BLOCK_BYTES"));
        assertEquals(ScalingPlan.Action.INSUFFICIENT_METRICS, policy.evaluate(nominal).action);
        var workers =
                EXAMPLE_JSON
                        .replace(
                                "\"workers\": []",
                                "\"workers\": [{\"workerId\":\"worker-1\",\"allocatableBytes\":3000000000,\"allocatableCpuCores\":6}]")
                        .replace(
                                "\"upstreamOperators\": []",
                                "\"upstreamOperators\": [], \"telemetry\": {\"allocatedMemoryPerTaskBytes\":500000000}");
        assertEquals(ScalingPlan.Action.RESCALE, policy.evaluate(decode(workers)).action);
        assertEquals(
                ScalingPlan.Action.INFEASIBLE_CONFIGURATION,
                policy.evaluate(decode(workers.replace("3000000000", "2999999999"))).action);
        var historical = decode(Files.readString(Path.of("../examples/q8-archived-curve.json")));
        assertEquals(
                ScalingPlan.Action.INFEASIBLE_CONFIGURATION, policy.evaluate(historical).action);
    }

    @Test
    void verifiesMeasuredSupportAfterApplyingCacheBoundsAndTaskRounding() throws Exception {
        var policy = new A4sPolicy();
        var largerBudget =
                EXAMPLE_JSON
                        .replace(
                                "\"maxCachePerTaskBytes\": 134217728",
                                "\"maxCachePerTaskBytes\": 536870912")
                        .replace(
                                "\"totalCacheBudgetBytes\": 1073741824",
                                "\"totalCacheBudgetBytes\": 4294967296");
        var floorOutsideTheCurve =
                decode(
                        largerBudget.replace(
                                "\"minCachePerTaskBytes\": 0",
                                "\"minCachePerTaskBytes\": 536870912"));
        assertEquals(
                ScalingPlan.Action.INFEASIBLE_CONFIGURATION,
                policy.evaluate(floorOutsideTheCurve).action);

        int start = largerBudget.indexOf("\"missRateCurve\": [");
        int end = largerBudget.indexOf(']', start);
        var rounded =
                decode(
                        largerBudget.substring(0, start)
                                + "\"missRateCurve\": [{\"capacityBytes\":1610612730,\"missRate\":0.55}]"
                                + largerBudget.substring(end + 1));
        // Four subtasks round the minimum above the measured endpoint. Six divide it exactly.
        var plan = policy.evaluate(rounded);
        assertEquals(ScalingPlan.Action.RESCALE, plan.action);
        var join = plan.operators.get("join");
        assertEquals(6, join.target.parallelism);
        assertEquals(1_610_612_730L, join.targetTotalCacheBytes);
        assertTrue(join.predictedRecordsPerSecond >= 60_000);
        assertTrue(policy.isApplicable(rounded, plan, 10_002));
    }

    @Test
    void holdsWhenThePerformanceModelCannotVerifyTheTargetRate() throws Exception {
        var request = decode(EXAMPLE_JSON);
        for (double forecast : new double[] {59_999, Double.NaN, Double.POSITIVE_INFINITY}) {
            var policy =
                    new A4sPolicy(
                            (snapshot, decisions) -> Optional.empty(),
                            "test-model",
                            options ->
                                    new PerformanceModel() {
                                        @Override
                                        public OptionalDouble predict(
                                                OperatorMetrics current,
                                                OperatorConfiguration candidate) {
                                            return OptionalDouble.of(forecast);
                                        }

                                        @Override
                                        public OptionalLong minimumCacheBytes(
                                                OperatorMetrics current,
                                                int parallelism,
                                                List<CachePoint> measuredCurve) {
                                            return OptionalLong.of(
                                                    measuredCurve.get(0).capacityBytes);
                                        }
                                    });
            assertEquals(
                    ScalingPlan.Action.INFEASIBLE_CONFIGURATION, policy.evaluate(request).action);
        }
    }

    @Test
    void provesTheTasksFitTogetherInTheSuppliedWorkers() throws Exception {
        var policy = new A4sPolicy();
        var feasible =
                withWorkers(
                        EXAMPLE_JSON,
                        new Worker("first", 1_500_000_000, 3),
                        new Worker("second", 1_500_000_000, 3));
        var plan = policy.evaluate(feasible);
        assertEquals(ScalingPlan.Action.RESCALE, plan.action);
        assertEquals(6, plan.operators.get("join").target.parallelism);
        assertTrue(policy.isApplicable(feasible, plan, 10_002));

        // Both fleets have spare capacity in aggregate, but can fit only five tasks.
        var fragmentedMemory =
                withWorkers(
                        EXAMPLE_JSON,
                        new Worker("first", 1_499_999_999, 3),
                        new Worker("second", 1_550_000_000, 3));
        var fragmentedCpu =
                withWorkers(
                        EXAMPLE_JSON,
                        new Worker("first", 1_500_000_000, 2.9),
                        new Worker("second", 1_500_000_000, 3.2));
        for (var fragmented : List.of(fragmentedMemory, fragmentedCpu)) {
            assertEquals(
                    ScalingPlan.Action.INFEASIBLE_CONFIGURATION,
                    policy.evaluate(fragmented).action);
            assertFalse(policy.isApplicable(fragmented, plan, 10_002));
        }

        var decimalCpu =
                withWorkers(
                        EXAMPLE_JSON
                                .replace("\"cpuCoresPerTask\": 1", "\"cpuCoresPerTask\": 0.1")
                                .replace("\"cpuBudgetCores\": 6", "\"cpuBudgetCores\": 0.6"),
                        new Worker("first", 1_500_000_000, 0.3),
                        new Worker("second", 1_500_000_000, 0.3));
        assertEquals(ScalingPlan.Action.RESCALE, policy.evaluate(decimalCpu).action);
    }

    private static EvaluationRequest withWorkers(String fixture, Worker... workers)
            throws IOException {
        return decode(
                fixture.replace(
                                "\"workers\": []",
                                "\"workers\": "
                                        + new String(
                                                Wire.encode(List.of(workers)),
                                                StandardCharsets.UTF_8))
                        .replace(
                                "\"upstreamOperators\": []",
                                "\"upstreamOperators\": [], \"telemetry\": {\"allocatedMemoryPerTaskBytes\":500000000}"));
    }

    @Test
    void preservesHistogramProvenanceInTheWireContract() throws Exception {
        var start = EXAMPLE_JSON.indexOf("\"missRateCurve\": [");
        var end = EXAMPLE_JSON.indexOf(']', start);
        var input =
                EXAMPLE_JSON.substring(0, start)
                        + "\"missRateCurve\": [], \"histogram\": {"
                        + "\"estimator\":\"SHARDS\",\"counterKind\":\"INTERVAL\","
                        + "\"population\":\"user-data\",\"cacheScope\":\"isolated\","
                        + "\"counterGeneration\":\"attempt-1\",\"sampling\":\"rate=1/4,seed=2\","
                        + "\"startMillis\":1000,\"endMillis\":10000,\"partitions\":4,"
                        + "\"capacityBytesPerPartition\":[67108864,201326592,402653184],"
                        + "\"frequencies\":[30,15,35],\"missesOutsideBuckets\":20}"
                        + EXAMPLE_JSON.substring(end + 1);
        var decoded = decode(input);
        var roundTrip = Wire.decode(Wire.encode(decoded), EvaluationRequest.class);
        var histogram = roundTrip.snapshot.operators.get(0).histogram;
        assertEquals("rate=1/4,seed=2", histogram.sampling);
        assertEquals("attempt-1", histogram.counterGeneration);
        assertEquals(20, histogram.missesOutsideBuckets);
        var policy = new A4sPolicy();
        var plan = policy.evaluate(roundTrip);
        assertEquals(ScalingPlan.Action.RESCALE, plan.action);
        assertTrue(policy.isApplicable(roundTrip, plan, 10_002));
        var stale = decode(input.replace("\"endMillis\":10000", "\"endMillis\":9000"));
        assertEquals(ScalingPlan.Action.INSUFFICIENT_METRICS, policy.evaluate(stale).action);
        assertFalse(policy.isApplicable(stale, plan, 10_002));
        var expired = decode(input.replace("10001", "40001"));
        assertEquals(ScalingPlan.Action.INSUFFICIENT_METRICS, policy.evaluate(expired).action);
        assertFalse(policy.isApplicable(expired, plan, 10_002));
    }

    @Test
    void validatesTheFirstPermittedParallelismForAnOperatorWithoutCacheScaling() throws Exception {
        var policy = new A4sPolicy();
        var fixture =
                EXAMPLE_JSON
                        .replace("\"cacheSensitive\": true", "\"cacheSensitive\": false")
                        .replace("\"startingParallelism\": 4", "\"startingParallelism\": 5")
                        .replace(
                                "\"maxCachePerTaskBytes\": 134217728",
                                "\"maxCachePerTaskBytes\": 201326592")
                        .replace(
                                "\"totalCacheBudgetBytes\": 1073741824",
                                "\"totalCacheBudgetBytes\": 1610612736")
                        .replace("\"cpuBudgetCores\": 6", "\"cpuBudgetCores\": 8");
        var request = decode(fixture);
        var plan = policy.evaluate(request);
        assertEquals(ScalingPlan.Action.RESCALE, plan.action);
        assertEquals(6, plan.operators.get("join").target.parallelism);
        var fromWire = Wire.decode(Wire.encode(plan), ScalingPlan.class);
        assertTrue(policy.isApplicable(request, fromWire, 10_002));

        var later =
                policy.evaluate(
                        decode(
                                fixture.replace(
                                        "\"startingParallelism\": 5",
                                        "\"startingParallelism\": 7")));
        assertEquals(ScalingPlan.Action.RESCALE, later.action);
        assertEquals(8, later.operators.get("join").target.parallelism);
        assertFalse(policy.isApplicable(request, later, 10_002));
    }

    @Test
    void evaluationAndNetworkTimeoutFailWithoutProducingAPlan() throws Exception {
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
    void runnerProcessDeathDuringEvaluationLeavesRequestRetryable() throws Exception {
        var request = Wire.decode(EXAMPLE, EvaluationRequest.class);
        var client = new DecisionClient();
        var runner =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-cp",
                                System.getProperty(
                                        "surefire.test.class.path",
                                        System.getProperty("java.class.path")),
                                BlockingRunner.class.getName(),
                                temp.toString())
                        .redirectErrorStream(true)
                        .redirectOutput(temp.resolve("runner.log").toFile())
                        .start();
        try {
            var port = temp.resolve("port");
            awaitMarker(runner, port);
            var endpoint = URI.create("http://127.0.0.1:" + Files.readString(port));
            var inFlight =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return client.evaluate(
                                            endpoint, Duration.ofSeconds(20), request);
                                } catch (IOException | InterruptedException e) {
                                    throw new CompletionException(e);
                                }
                            });

            awaitMarker(runner, temp.resolve("evaluating"));
            assertFalse(inFlight.isDone());
            runner.destroyForcibly();
            assertTrue(runner.waitFor(5, TimeUnit.SECONDS));

            var discarded =
                    assertThrows(ExecutionException.class, () -> inFlight.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, discarded.getCause());
        } finally {
            if (runner.isAlive()) {
                runner.destroyForcibly();
                assertTrue(runner.waitFor(5, TimeUnit.SECONDS));
            }
        }

        var policy = new A4sPolicy();
        try (var replacement = new DecisionServer(new InetSocketAddress("127.0.0.1", 0), policy)) {
            replacement.start();
            var recovered = client.evaluate(endpoint(replacement), Duration.ofSeconds(2), request);
            assertEquals(ScalingPlan.Action.RESCALE, recovered.action);
            assertArrayEquals(Wire.encode(policy.evaluate(request)), Wire.encode(recovered));
        }
    }

    private static void awaitMarker(Process runner, Path marker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (Files.notExists(marker) && runner.isAlive() && System.nanoTime() - deadline < 0) {
            TimeUnit.MILLISECONDS.sleep(25);
        }
        assertTrue(Files.exists(marker), () -> "Runner did not reach " + marker.getFileName());
    }

    /** Runs in a child JVM so the test can interrupt the service while it evaluates a request. */
    public static final class BlockingRunner {
        public static void main(String[] args) throws Exception {
            var dir = Path.of(args[0]);
            var hold = new CountDownLatch(1);
            try (var server =
                    new DecisionServer(
                            new InetSocketAddress("127.0.0.1", 0),
                            request -> {
                                try {
                                    Files.createFile(dir.resolve("evaluating"));
                                    hold.await();
                                    return new A4sPolicy().evaluate(request);
                                } catch (IOException | InterruptedException e) {
                                    throw new IllegalStateException(e);
                                }
                            },
                            30_000)) {
                server.start();
                var staging =
                        Files.writeString(
                                dir.resolve("port.pending"), Integer.toString(server.port()));
                Files.move(staging, dir.resolve("port"), StandardCopyOption.ATOMIC_MOVE);
                hold.await();
            }
        }
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
