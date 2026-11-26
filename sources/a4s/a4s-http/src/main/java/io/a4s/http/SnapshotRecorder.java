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

import io.a4s.api.EvaluationRequest;
import io.a4s.api.ScalingPlan;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Opt-in JSONL capture with a per-file size cap. Use a new filename for each benchmark arm. */
public final class SnapshotRecorder {
    private static final long MAX_FILE_BYTES = 256L * 1024 * 1024;

    private SnapshotRecorder() {}

    public static synchronized void append(Path file, EvaluationRequest request)
            throws IOException {
        appendLine(file, Wire.encode(request));
    }

    public static synchronized void append(
            Path file, EvaluationRequest request, ScalingPlan recommendation) throws IOException {
        appendLine(
                file,
                Wire.encodeRecording(
                        new RecordedEvaluation(
                                RecordedEvaluation.VERSION, request, recommendation)));
    }

    private static void appendLine(Path file, byte[] json) throws IOException {
        var line = ByteBuffer.allocate(json.length + 1).put(json).put((byte) '\n');
        line.flip();
        try (var channel =
                        FileChannel.open(
                                file,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE,
                                LinkOption.NOFOLLOW_LINKS);
                var lock = channel.tryLock()) {
            if (lock == null) {
                throw new IOException("Another process is recording A4S snapshots");
            }
            long start = channel.size();
            if (start > MAX_FILE_BYTES - line.remaining()) {
                throw new IOException("A4S snapshot file has reached its size limit");
            }
            channel.position(start);
            try {
                while (line.hasRemaining()) {
                    channel.write(line);
                }
            } catch (IOException e) {
                channel.truncate(start);
                throw e;
            }
        }
    }
}
