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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.a4s.api.EvaluationRequest;
import io.a4s.core.A4sPolicy;
import io.a4s.core.ScalingPolicy;
import io.a4s.http.Wire;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

final class DecisionServer implements AutoCloseable {
    private final HttpServer server;
    private final ScalingPolicy policy;
    private final ThreadPoolExecutor http = pool(8, 32);
    private final ThreadPoolExecutor evaluations = pool(4, 8);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final long evaluationTimeoutMillis;

    DecisionServer(InetSocketAddress address, ScalingPolicy policy) throws IOException {
        this(address, policy, 2_000);
    }

    DecisionServer(InetSocketAddress address, ScalingPolicy policy, long evaluationTimeoutMillis)
            throws IOException {
        this.policy = policy;
        this.evaluationTimeoutMillis = evaluationTimeoutMillis;
        server = HttpServer.create(address, 32);
        server.setExecutor(http);
        server.createContext("/", this::dispatch);
    }

    private static ThreadPoolExecutor pool(int size, int queueSize) {
        return new ThreadPoolExecutor(
                size, size, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize));
    }

    int port() {
        return server.getAddress().getPort();
    }

    void start() {
        server.start();
        System.out.println("{\"event\":\"runner_started\",\"port\":" + port() + "}");
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        long start = System.nanoTime();
        try {
            String path = exchange.getRequestURI().getRawPath();
            String method = exchange.getRequestMethod();
            switch (path) {
                case "/health":
                    if (requireMethod(exchange, method, "GET")) {
                        send(exchange, 200, Map.of("status", "ready"));
                    }
                    break;
                case "/v1/version":
                    if (requireMethod(exchange, method, "GET")) {
                        send(
                                exchange,
                                200,
                                Map.of(
                                        "version",
                                        "0.1.0",
                                        "schemaVersion",
                                        EvaluationRequest.SCHEMA_VERSION,
                                        "policies",
                                        List.of("CACHE", "LEGACY_SLOT_RESERVATION"),
                                        "models",
                                        List.of(A4sPolicy.MODEL_VERSION)));
                    }
                    break;
                case "/v1/evaluate":
                    if (requireMethod(exchange, method, "POST")) {
                        evaluate(exchange);
                    }
                    break;
                default:
                    error(exchange, 404, "NOT_FOUND");
            }
        } finally {
            exchange.close();
            System.out.printf(
                    "{\"event\":\"request\",\"status\":%d,\"durationMicros\":%d}%n",
                    exchange.getResponseCode(), (System.nanoTime() - start) / 1_000);
        }
    }

    private static boolean requireMethod(HttpExchange exchange, String supplied, String required)
            throws IOException {
        if (required.equals(supplied)) {
            return true;
        }
        exchange.getResponseHeaders().set("Allow", required);
        error(exchange, 405, "METHOD_NOT_ALLOWED");
        return false;
    }

    private void evaluate(HttpExchange exchange) throws IOException {
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null
                || !contentType.split(";", 2)[0].trim().equalsIgnoreCase("application/json")) {
            error(exchange, 415, "EXPECTED_JSON");
            return;
        }
        byte[] bytes = exchange.getRequestBody().readNBytes(Wire.MAX_BODY_BYTES + 1);
        if (bytes.length > Wire.MAX_BODY_BYTES) {
            error(exchange, 413, "REQUEST_TOO_LARGE");
            return;
        }
        EvaluationRequest request;
        try {
            request = Wire.decode(bytes, EvaluationRequest.class);
        } catch (IOException e) {
            error(exchange, 400, "INVALID_REQUEST_OR_SCHEMA");
            return;
        }
        Future<byte[]> evaluation;
        try {
            evaluation = evaluations.submit(() -> Wire.encode(policy.evaluate(request)));
        } catch (RejectedExecutionException e) {
            error(exchange, 503, "EVALUATOR_BUSY");
            return;
        }
        byte[] body;
        try {
            body = evaluation.get(evaluationTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            error(exchange, 504, "EVALUATION_TIMEOUT");
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error(exchange, 503, "EVALUATION_INTERRUPTED");
            return;
        } catch (ExecutionException e) {
            error(exchange, 500, "EVALUATION_FAILED");
            return;
        } finally {
            if (!evaluation.isDone()) {
                evaluation.cancel(true);
            }
        }
        sendBytes(exchange, 200, body);
    }

    private static void error(HttpExchange exchange, int status, String code) throws IOException {
        send(
                exchange,
                status,
                Map.of("schemaVersion", EvaluationRequest.SCHEMA_VERSION, "error", code));
    }

    private static void send(HttpExchange exchange, int status, Object body) throws IOException {
        sendBytes(exchange, status, Wire.encode(body));
    }

    private static void sendBytes(HttpExchange exchange, int status, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            server.stop(3);
            http.shutdownNow();
            evaluations.shutdownNow();
        }
    }
}
