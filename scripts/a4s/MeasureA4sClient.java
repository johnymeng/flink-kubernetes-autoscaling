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

import io.a4s.http.DecisionClient;
import io.a4s.http.Wire;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;

/** Measures the production HTTP client against a runner using captured evaluations. */
public final class MeasureA4sClient {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected RUNNER_ORIGIN RECORDINGS_JSONL REPETITIONS");
        }
        var endpoint = URI.create(args[0]);
        var path = Path.of(args[1]);
        int repetitions = Integer.parseInt(args[2]);
        if (repetitions < 1) {
            throw new IllegalArgumentException("At least one measured repetition is required");
        }

        var client = new DecisionClient();
        System.out.println("repetition,line,requestBytes,responseBytes,encodeNanos,exchangeNanos,decodeNanos");
        for (int pass = 0; pass <= repetitions; pass++) {
            try (var input = Files.newBufferedReader(path)) {
                int line = 0;
                for (String body; (body = input.readLine()) != null; ) {
                    line++;
                    var recorded = Wire.decodeRecording(body.getBytes(StandardCharsets.UTF_8));
                    if (recorded.recommendation == null) {
                        throw new IOException("Missing recorded recommendation on line " + line);
                    }
                    var captured = new DecisionClient.Timings[1];
                    var observed = client.evaluate(endpoint, Duration.ofSeconds(3), recorded.request,
                            timing -> captured[0] = timing);
                    if (!Arrays.equals(Wire.encode(observed), Wire.encode(recorded.recommendation))) {
                        throw new IOException("Runner recommendation differs on line " + line);
                    }
                    var timing = captured[0];
                    if (pass > 0) {
                        System.out.printf(Locale.ROOT, "%d,%d,%d,%d,%d,%d,%d%n", pass, line,
                                timing.requestBytes, timing.responseBytes, timing.encodeNanos,
                                timing.exchangeNanos, timing.decodeNanos);
                    }
                }
                if (line == 0) {
                    throw new IOException("No recorded evaluations in the input");
                }
            }
        }
    }
}
