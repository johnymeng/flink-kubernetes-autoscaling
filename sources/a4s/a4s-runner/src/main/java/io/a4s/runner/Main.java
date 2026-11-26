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

import io.a4s.api.EvaluationRequest;
import io.a4s.api.PolicyConfiguration;
import io.a4s.core.A4sPolicy;
import io.a4s.http.Wire;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws IOException {
        if (args.length > 0 && args[0].equals("replay")) {
            replay(args);
        } else if (args.length == 0 || args.length == 1 && args[0].equals("serve")) {
            String bind = System.getenv().getOrDefault("A4S_BIND", "127.0.0.1");
            int port = Integer.parseInt(System.getenv().getOrDefault("A4S_PORT", "8080"));
            // Bound time spent on the JDK server reading or sending a stalled request.
            System.setProperty("sun.net.httpserver.maxReqTime", "10");
            System.setProperty("sun.net.httpserver.maxRspTime", "10");
            var server = new DecisionServer(new InetSocketAddress(bind, port), new A4sPolicy());
            Runtime.getRuntime().addShutdownHook(new Thread(server::close, "a4s-shutdown"));
            server.start();
        } else {
            throw new IllegalArgumentException(
                    "Expected serve or replay --input FILE --output FILE [--policy MODE]");
        }
    }

    private static void replay(String[] args) throws IOException {
        Path input = null;
        Path output = null;
        String policy = "a4s";
        for (int i = 1; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + args[i]);
            }
            switch (args[i]) {
                case "--input":
                    input = Path.of(args[i + 1]);
                    break;
                case "--output":
                    output = Path.of(args[i + 1]);
                    break;
                case "--policy":
                    policy = args[i + 1];
                    break;
                default:
                    throw new IllegalArgumentException("Unknown replay option " + args[i]);
            }
        }
        if (input == null
                || output == null
                || Files.exists(output) && Files.isSameFile(input, output)) {
            throw new IllegalArgumentException("Replay requires distinct input and output files");
        }
        final String selectedPolicy = policy;
        var engine = new A4sPolicy();
        try (var source = new BufferedInputStream(Files.newInputStream(input));
                var sink = new BufferedOutputStream(Files.newOutputStream(output))) {
            byte[] line;
            long row = 0;
            while ((line = readLine(source)) != null) {
                row++;
                try {
                    var request =
                            withPolicy(Wire.decode(line, EvaluationRequest.class), selectedPolicy);
                    sink.write(Wire.encode(engine.evaluate(request)));
                    sink.write('\n');
                } catch (IllegalArgumentException | IOException e) {
                    throw new IOException("Invalid replay request at line " + row, e);
                }
            }
        }
    }

    private static EvaluationRequest withPolicy(EvaluationRequest request, String policy) {
        if (policy.equals("a4s")) {
            return request;
        }
        var mode = PolicyConfiguration.Mode.valueOf(policy);
        if (mode == request.policy.mode) {
            return request;
        }
        // A replay override changes only the model's output resource. Constraints and
        // capabilities still come from the captured request and can reject that change.
        var options =
                new PolicyConfiguration(
                        mode,
                        request.policy.maxAttempts,
                        request.policy.hitSeconds,
                        request.policy.missSeconds,
                        0,
                        null);
        return new EvaluationRequest(
                request.schemaVersion,
                request.snapshot,
                request.constraints,
                options,
                request.previousState,
                request.evaluatedAtMillis);
    }

    private static byte[] readLine(InputStream input) throws IOException {
        var line = new ByteArrayOutputStream();
        int next;
        while ((next = input.read()) != -1 && next != '\n') {
            if (line.size() >= Wire.MAX_BODY_BYTES) {
                throw new IOException("Replay line exceeds the request size limit");
            }
            line.write(next);
        }
        return next == -1 && line.size() == 0 ? null : line.toByteArray();
    }
}
