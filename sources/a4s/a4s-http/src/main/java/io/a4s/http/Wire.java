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

package io.a4s.http;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.a4s.api.EvaluationRequest;
import java.io.IOException;

public final class Wire {
    public static final int MAX_BODY_BYTES = 2 * 1024 * 1024;
    // A recording can contain both a complete HTTP request and a complete HTTP response.
    public static final int MAX_RECORD_BYTES = 2 * MAX_BODY_BYTES + 1024;

    private static final JsonMapper JSON =
            JsonMapper.builder()
                    .enable(
                            DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                            DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
                    .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                    .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .build();

    private Wire() {}

    public static byte[] encode(Object value) throws IOException {
        return encodeBounded(value, MAX_BODY_BYTES);
    }

    public static byte[] encodeRecording(RecordedEvaluation recording) throws IOException {
        return encodeBounded(recording, MAX_RECORD_BYTES);
    }

    private static byte[] encodeBounded(Object value, int limit) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(value);
        if (bytes.length > limit) {
            throw new IOException("A4S payload exceeds the size limit");
        }
        return bytes;
    }

    public static <T> T decode(byte[] bytes, Class<T> type) throws IOException {
        if (bytes.length > MAX_BODY_BYTES) {
            throw new IOException("A4S payload exceeds the size limit");
        }
        T result = JSON.readValue(bytes, type);
        if (result == null) {
            throw new IOException("A4S payload must be a JSON object");
        }
        return result;
    }

    /** Reads either a paired evaluation record or a legacy request-only JSONL row. */
    public static RecordedEvaluation decodeRecording(byte[] bytes) throws IOException {
        if (bytes.length > MAX_RECORD_BYTES) {
            throw new IOException("A4S recording exceeds the size limit");
        }
        var root = JSON.readTree(bytes);
        if (root == null || !root.isObject()) {
            throw new IOException("A4S recording must be a JSON object");
        }
        if (root.has("recordingVersion")) {
            return JSON.treeToValue(root, RecordedEvaluation.class);
        }
        if (bytes.length > MAX_BODY_BYTES) {
            throw new IOException("A4S request exceeds the size limit");
        }
        return new RecordedEvaluation(
                RecordedEvaluation.VERSION, JSON.treeToValue(root, EvaluationRequest.class), null);
    }
}
