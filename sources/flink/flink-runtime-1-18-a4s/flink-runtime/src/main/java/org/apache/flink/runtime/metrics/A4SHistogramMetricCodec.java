/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"). You may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Bounded, lossless wire format for cache telemetry transported through writeUTF. */
public final class A4SHistogramMetricCodec {
    private static final Logger LOG = LoggerFactory.getLogger(A4SHistogramMetricCodec.class);
    private static final String PREFIX = "a4s-gzip-v1:";
    private static final String OMITTED = PREFIX + "omitted";
    private static final int MAX_UTF_BYTES = 65_535;
    // About 50 times the observed 84 kB payload, with room for over a million
    // small bucket counts. Bound one expanded gauge independently of its wire size.
    private static final int MAX_JSON_BYTES = 4 * 1024 * 1024;
    private static final long DIAGNOSTIC_INTERVAL_MILLIS = 60_000L;
    private static final AtomicLong NEXT_DIAGNOSTIC = new AtomicLong();

    private A4SHistogramMetricCodec() {}

    public static String encode(String histogramJson) {
        if (histogramJson.length() > MAX_JSON_BYTES) {
            return omit(histogramJson.length(), MAX_JSON_BYTES, "expanded JSON");
        }
        if (modifiedUtfBytes(histogramJson) <= MAX_UTF_BYTES) {
            return histogramJson;
        }
        byte[] jsonBytes = histogramJson.getBytes(StandardCharsets.UTF_8);
        if (jsonBytes.length > MAX_JSON_BYTES) {
            return omit(jsonBytes.length, MAX_JSON_BYTES, "expanded JSON");
        }
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(jsonBytes);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not encode the QuickMRC histogram metric", error);
        }
        String metric = PREFIX + Base64.getEncoder().encodeToString(compressed.toByteArray());
        if (metric.length() <= MAX_UTF_BYTES) {
            return metric;
        }
        return omit(metric.length(), MAX_UTF_BYTES, "encoded metric");
    }

    private static String omit(int bytes, int limit, String representation) {
        long now = System.currentTimeMillis();
        long next = NEXT_DIAGNOSTIC.get();
        if (now >= next && NEXT_DIAGNOSTIC.compareAndSet(next, now + DIAGNOSTIC_INTERVAL_MILLIS)) {
            LOG.warn(
                    "QuickMRC histogram omitted: {} uses {} bytes, above the {}-byte limit.",
                    representation,
                    bytes,
                    limit);
        }
        // Preserve a known omission on the wire so the receiver cannot mistake
        // a partial set of valid subtask histograms for a complete vertex curve.
        return OMITTED;
    }

    public static String decode(String metric) {
        if (!metric.startsWith(PREFIX)) {
            if (metric.length() > MAX_JSON_BYTES) {
                throw new SizeLimitException("QuickMRC histogram exceeds expanded byte limit");
            }
            return metric;
        }
        if (metric.length() > MAX_UTF_BYTES || modifiedUtfBytes(metric) > MAX_UTF_BYTES) {
            throw new SizeLimitException("QuickMRC histogram exceeds encoded byte limit");
        }
        if (isOmitted(metric)) {
            throw new IllegalArgumentException("A4S metric was omitted by its producer");
        }
        byte[] compressed = Base64.getDecoder().decode(metric.substring(PREFIX.length()));
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            ByteArrayOutputStream expanded = new ByteArrayOutputStream();
            byte[] buffer = new byte[8 * 1024];
            for (int count; (count = gzip.read(buffer)) != -1; ) {
                if (count > MAX_JSON_BYTES - expanded.size()) {
                    throw new SizeLimitException(
                            "QuickMRC histogram exceeds expanded byte limit");
                }
                expanded.write(buffer, 0, count);
            }
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .decode(ByteBuffer.wrap(expanded.toByteArray()))
                    .toString();
        } catch (IOException error) {
            throw new IllegalArgumentException("Invalid encoded QuickMRC histogram metric", error);
        }
    }
    public static final class SizeLimitException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        private SizeLimitException(String message) {
            super(message);
        }
    }

    public static boolean isOmitted(String metric) {
        return OMITTED.equals(metric);
    }

    public static int modifiedUtfBytes(String metric) {
        int bytes = 0;
        for (int index = 0; index < metric.length(); index++) {
            char value = metric.charAt(index);
            if (value >= 1 && value <= 0x7f) {
                bytes++;
            } else if (value <= 0x7ff) {
                bytes += 2;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }
}
