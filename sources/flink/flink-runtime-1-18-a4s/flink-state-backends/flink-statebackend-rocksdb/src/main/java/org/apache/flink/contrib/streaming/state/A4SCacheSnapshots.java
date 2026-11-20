/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.apache.flink.contrib.streaming.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/** Ownership and interval publication for the lifetime of a physical cache. */
final class A4SCacheSnapshots<T> {
    private final long intervalNanos;
    private final Set<Long> owners = new LinkedHashSet<>();
    private long lastOwner;
    private long sequence;
    private long startedAtNanos;
    private Publication<T> latest;
    private boolean closed;

    A4SCacheSnapshots(long intervalNanos) {
        this.intervalNanos = intervalNanos;
    }

    synchronized long attach() {
        if (closed) {
            throw new IllegalStateException("Cache already closed");
        }
        owners.add(++lastOwner);
        invalidateOwnership();
        return lastOwner;
    }

    synchronized void detach(long owner) {
        if (owners.remove(owner)) {
            invalidateOwnership();
        }
    }

    synchronized boolean hasExclusiveOwner() {
        // Both overlapping and sequential users can leave cumulative counters and reuse
        // state behind. A new physical cache is required to re-establish exclusivity.
        return !closed && lastOwner == 1 && owners.size() == 1;
    }

    synchronized Publication<T> poll(long nowNanos, Supplier<T> read) {
        if (closed || owners.isEmpty()) {
            throw new IllegalStateException("No live backend owns the cache");
        }
        if (latest == null || nowNanos - latest.endNanos >= intervalNanos) {
            T value = read.get();
            latest =
                    new Publication<>(
                            ++sequence,
                            startedAtNanos,
                            nowNanos,
                            Collections.unmodifiableList(new ArrayList<>(owners)),
                            hasExclusiveOwner(),
                            value);
            startedAtNanos = nowNanos;
        }
        return latest;
    }

    synchronized void close() {
        closed = true;
        owners.clear();
        latest = null;
    }

    private void invalidateOwnership() {
        if (latest != null) {
            latest =
                    new Publication<>(
                            latest.sequence,
                            latest.startNanos,
                            latest.endNanos,
                            Collections.unmodifiableList(new ArrayList<>(owners)),
                            false,
                            latest.value);
        }
    }

    static final class Publication<T> {
        final long sequence;
        final long startNanos;
        final long endNanos;
        final List<Long> owners;
        final boolean exclusive;
        final T value;

        Publication(
                long sequence,
                long startNanos,
                long endNanos,
                List<Long> owners,
                boolean exclusive,
                T value) {
            this.sequence = sequence;
            this.startNanos = startNanos;
            this.endNanos = endNanos;
            this.owners = owners;
            this.exclusive = exclusive;
            this.value = value;
        }
    }
}
