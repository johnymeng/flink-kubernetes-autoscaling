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
    private final Set<Long> monitors = new LinkedHashSet<>();
    private long lastOwner;
    private long lastMonitor;
    private long ownershipGeneration;
    private long sequence;
    private long startedAtNanos;
    private Publication<T> latest;
    private boolean priorUse;
    private boolean closed;

    A4SCacheSnapshots(long intervalNanos) {
        this.intervalNanos = intervalNanos;
    }

    synchronized long attach(boolean monitor) {
        if (closed) {
            throw new IllegalStateException("Cache already closed");
        }
        long id = monitor ? ++lastMonitor : ++lastOwner;
        (monitor ? monitors : owners).add(id);
        invalidateOwnership();
        return id;
    }

    synchronized void detach(long owner, boolean monitor) {
        if ((monitor ? monitors : owners).remove(owner)) {
            invalidateOwnership();
        }
    }

    private boolean hasExclusiveOwner() {
        // Both overlapping and sequential users can leave cumulative counters and reuse
        // state behind. A new physical cache is required to re-establish exclusivity.
        return !closed && !priorUse && lastOwner == 1 && owners.size() == 1
                && lastMonitor == 1 && monitors.size() == 1;
    }

    synchronized long exclusiveGeneration(long monitor) {
        return hasExclusiveOwner() && monitors.contains(monitor) ? ownershipGeneration : -1;
    }

    synchronized void disqualifyPriorUse() {
        if (!priorUse) {
            priorUse = true;
            invalidateOwnership();
        }
    }

    synchronized Publication<T> poll(long monitor, long nowNanos, Supplier<T> read) {
        if (closed || owners.isEmpty() || !monitors.contains(monitor)) {
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
                            ownershipGeneration,
                            hasExclusiveOwner(),
                            false,
                            value);
            startedAtNanos = nowNanos;
        }
        return latest;
    }

    synchronized void close() {
        closed = true;
        owners.clear();
        monitors.clear();
        latest = null;
    }

    private void invalidateOwnership() {
        ownershipGeneration++;
        if (latest != null) {
            latest =
                    new Publication<>(
                            latest.sequence,
                            latest.startNanos,
                            latest.endNanos,
                            Collections.unmodifiableList(new ArrayList<>(owners)),
                            ownershipGeneration,
                            false,
                            true,
                            latest.value);
        }
    }

    static final class Publication<T> {
        final long sequence;
        final long startNanos;
        final long endNanos;
        final List<Long> owners;
        final long ownershipGeneration;
        final boolean exclusive;
        final boolean ownershipChanged;
        final T value;

        Publication(
                long sequence,
                long startNanos,
                long endNanos,
                List<Long> owners,
                long ownershipGeneration,
                boolean exclusive,
                boolean ownershipChanged,
                T value) {
            this.sequence = sequence;
            this.startNanos = startNanos;
            this.endNanos = endNanos;
            this.owners = owners;
            this.ownershipGeneration = ownershipGeneration;
            this.exclusive = exclusive;
            this.ownershipChanged = ownershipChanged;
            this.value = value;
        }
    }
}
