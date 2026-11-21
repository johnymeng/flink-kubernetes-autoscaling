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

import org.apache.flink.a4s.core.CacheObservation;
import org.apache.flink.a4s.core.CacheObservation.Status;
import org.apache.flink.runtime.metrics.A4SHistogramMetricCodec;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheSnapshotsTest {

    @Test
    void aliasesShareOneImmutableSnapshotInEitherReaderOrder() {
        for (boolean reverse : new boolean[] {false, true}) {
            A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
            AtomicInteger reads = new AtomicInteger();
            AtomicReference<long[]> state = new AtomicReference<>(snapshot(7, 1, 1, 100, 20, 30));
            A4SCacheTelemetry.Source first = source(7, state, reads);
            A4SCacheTelemetry.Source second = source(7, state, reads);
            registry.register(new Object(), first, .1, 1);
            registry.register(new Object(), second, .1, 1);
            try (A4SCacheTelemetry.Lease backendA = registry.attach(first, false);
                    A4SCacheTelemetry.Lease backendB = registry.attach(second, false);
                    A4SCacheTelemetry.Lease monitorA = registry.attach(first, true);
                    A4SCacheTelemetry.Lease monitorB = registry.attach(second, true)) {
                CacheObservation start = read(monitorA, "A", 0);
                assertEquals(Status.SHARED_CACHE, start.status);
                state.set(snapshot(7, 1, 4, 200, 40, 50));
                CacheObservation a = read(reverse ? monitorB : monitorA, "first", 10);
                CacheObservation b = read(reverse ? monitorA : monitorB, "second", 11);
                assertEquals(7, a.window.cacheId);
                assertEquals(4, a.window.sequence);
                assertEquals(a.window.sequence, b.window.sequence);
                assertEquals(100, a.window.references);
                assertEquals(a.window.references, b.window.references);
                assertEquals(Arrays.asList(1L, 2L), b.window.owners);
                assertEquals(Status.SHARED_CACHE, a.status);
                assertEquals(Status.SHARED_CACHE, b.status);
                assertNull(a.histogram);
                assertNull(b.histogram);
                state.set(snapshot(7, 1, 5, 250, 50, 65));
                assertEquals(100, read(monitorB, "B", 12).window.references);
                assertEquals(3, reads.get());
                assertEquals(50, read(monitorA, "A", 20).window.references);
                assertEquals(4, reads.get());
            }
        }
    }

    @Test
    void closingAnAliasReleasesOnlyItsOwnObservationResources() {
        A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
        AtomicReference<long[]> state = new AtomicReference<>(snapshot(7, 1, 1, 100, 20, 30));
        AtomicInteger firstReads = new AtomicInteger();
        AtomicInteger secondReads = new AtomicInteger();
        A4SCacheTelemetry.Source first = source(7, state, firstReads);
        A4SCacheTelemetry.Source second = source(7, state, secondReads);
        Object firstAllocation = new Object();
        registry.register(firstAllocation, first, .1, 1);
        A4SCacheTelemetry.Lease backendA = registry.attach(first, false);
        A4SCacheTelemetry.Lease monitorA = registry.attach(first, true);
        A4SCacheTelemetry.Lease backendB = registry.attach(second, false);
        A4SCacheTelemetry.Lease monitorB = registry.attach(second, true);
        assertEquals(Status.SHARED_CACHE, read(monitorA, "A", 0).status);
        monitorA.close();
        backendA.close();
        registry.release(firstAllocation);
        registry.release(firstAllocation);
        state.set(snapshot(7, 1, 2, 200, 40, 50));
        CacheObservation live = read(monitorB, "B", 10);
        assertEquals(Collections.singletonList(2L), live.window.owners);
        assertEquals(2, live.window.sequence);
        assertEquals(Status.SHARED_CACHE, live.status);
        assertEquals(-1, monitorB.exclusiveGeneration());
        assertEquals(2, firstReads.get());
        assertEquals(1, secondReads.get());
        monitorB.close();
        monitorB.close();
        backendB.close();
        assertNull(registry.attach(second, true));
        assertThrows(IllegalStateException.class, () -> read(monitorB, "B", 20));
    }

    @Test
    void ownershipGenerationChangesAndTwoDbsUnderOneBackendCannotQualify() {
        A4SCacheSnapshots<Integer> snapshots = new A4SCacheSnapshots<Integer>(10);
        long backend = snapshots.attach(false);
        long firstDb = snapshots.attach(true);
        long firstGeneration = snapshots.exclusiveGeneration(firstDb);
        assertTrue(firstGeneration > 0);
        A4SCacheSnapshots.Publication<Integer> exclusive = snapshots.poll(firstDb, 0, () -> 1);
        assertTrue(exclusive.exclusive);
        long secondDb = snapshots.attach(true);
        A4SCacheSnapshots.Publication<Integer> shared = snapshots.poll(secondDb, 1, () -> 2);
        assertSame(shared, snapshots.poll(firstDb, 2, () -> 2));
        assertTrue(shared.ownershipGeneration > exclusive.ownershipGeneration);
        assertEquals(Collections.singletonList(backend), shared.owners);
        assertFalse(shared.exclusive);
        assertTrue(exclusive.exclusive);
        assertEquals(-1, snapshots.exclusiveGeneration(firstDb));
        snapshots.detach(secondDb, true);
        A4SCacheSnapshots.Publication<Integer> after = snapshots.poll(firstDb, 3, () -> 2);
        assertFalse(after.exclusive);
        assertTrue(after.ownershipGeneration > shared.ownershipGeneration);
        snapshots.detach(firstDb, true);
        long reopened = snapshots.attach(true);
        assertFalse(snapshots.poll(reopened, 10, () -> 2).exclusive);
        assertEquals(-1, snapshots.exclusiveGeneration(reopened));
    }

    @Test
    void nativeRecreationAndProcessRestartHaveDifferentIdentity() {
        A4SCacheTelemetry.Registry first = new A4SCacheTelemetry.Registry("first-process");
        A4SCacheTelemetry.Registry second = new A4SCacheTelemetry.Registry("next-process");
        Object reusedWrapper = new Object();
        A4SCacheTelemetry.Source old = source(7, new AtomicReference<>(snapshot(7, 1, 1, 100, 20, 30)), new AtomicInteger());
        A4SCacheTelemetry.Source replacement = source(8, new AtomicReference<>(snapshot(8, 1, 1, 10, 2, 3)), new AtomicInteger());
        assertNull(first.attach(old, true));
        first.register(reusedWrapper, old, .1, 1);
        CacheObservation initial = firstObservation(first, old);
        first.release(reusedWrapper);
        assertNull(first.attach(old, true));
        first.register(reusedWrapper, replacement, .1, 1);
        CacheObservation recreated = firstObservation(first, replacement);
        assertEquals(initial.window.process, recreated.window.process);
        assertNotEquals(initial.window.cacheId, recreated.window.cacheId);
        second.register(reusedWrapper, old, .1, 1);
        CacheObservation restarted = firstObservation(second, old);
        assertEquals(initial.window.cacheId, restarted.window.cacheId);
        assertNotEquals(initial.window.process, restarted.window.process);
    }

    @Test
    void resizeAndCounterResetDoNotPublishAcrossGenerations() {
        A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
        AtomicReference<long[]> state = new AtomicReference<>(snapshot(7, 1, 1, 100, 20, 30));
        A4SCacheTelemetry.Source source = source(7, state, new AtomicInteger());
        registry.register(new Object(), source, .1, 1);
        try (A4SCacheTelemetry.Lease backend = registry.attach(source, false); A4SCacheTelemetry.Lease monitor = registry.attach(source, true)) {
            assertEquals(Status.WARMUP, read(monitor, "A", 0).status);
            state.set(snapshot(7, 1, 4, 200, 40, 50));
            CacheObservation valid = read(monitor, "A", 10);
            assertEquals(Status.VALID, valid.status);
            assertEquals(100, valid.window.references);
            assertEquals(20, valid.histogram.getFrequency(0));
            state.set(snapshot(7, 3, 5, 300, 60, 80));
            assertEquals(Status.CACHE_CHANGED, read(monitor, "A", 20).status);
            state.set(snapshot(7, 3, 6, 10, 2, 3));
            assertEquals(Status.COUNTER_RESET, read(monitor, "A", 30).status);
            state.set(snapshot(7, 3, 7, 100, 10, 20));
            CacheObservation recovered = read(monitor, "A", 40);
            assertEquals(Status.VALID, recovered.status);
            assertEquals(90, recovered.window.references);
            assertEquals(3, recovered.window.epoch);
            state.set(snapshot(8, 3, 8, 200, 20, 40));
            assertEquals(Status.CACHE_CHANGED, read(monitor, "A", 50).status);
        }
    }

    @Test
    void unavailableNativeSnapshotDoesNotFallBackToLegacyDrain() {
        A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
        A4SCacheTelemetry.Source unsupported = new A4SCacheTelemetry.Source(7, 0, () -> {
            throw new UnsupportedOperationException();
        });
        Object token = new Object();
        registry.register(token, unsupported, .1, 1);
        assertEquals(Status.UNSUPPORTED_NATIVE, firstObservation(registry, unsupported).status);
        registry.release(token);
        assertNull(registry.attach(unsupported, false));
    }

    @Test
    void previouslySampledNativeAliasCannotRegainExclusiveAttributionAfterRegistryRetires() {
        A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
        AtomicReference<long[]> state = new AtomicReference<>(snapshot(7, 1, 1, 100, 20, 30));
        A4SCacheTelemetry.Source source = source(7, state, new AtomicInteger());
        Object token = new Object();
        registry.register(token, source, .1, 1);
        assertEquals(Status.WARMUP, firstObservation(registry, source).status);
        registry.release(token);
        state.set(snapshot(7, 1, 2, 200, 40, 55));
        registry.register(new Object(), source, .1, 1);
        state.set(snapshot(7, 1, 3, 200, 40, 55));
        assertEquals(Status.SHARED_CACHE, firstObservation(registry, source).status);
    }

    @Test
    void registrationMarksAPhysicalCacheEvenWhenItsOwnersNeverPoll() {
        A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
        AtomicInteger sequence = new AtomicInteger();
        A4SCacheTelemetry.Source source = new A4SCacheTelemetry.Source(7, 0,
                () -> snapshot(7, 1, sequence.incrementAndGet(), 0, 0, 0));
        Object token = new Object();
        registry.register(token, source, .1, 1);
        try (A4SCacheTelemetry.Lease ignored = registry.attach(source, false)) {
            assertEquals(1, sequence.get());
        }
        registry.release(token);
        registry.register(new Object(), source, .1, 1);
        assertEquals(Status.SHARED_CACHE, firstObservation(registry, source).status);
    }

    @Test
    void membershipChangeAndAliasConfigurationMismatchInvalidateTheCachedInterval() {
        A4SCacheTelemetry.Registry registry = new A4SCacheTelemetry.Registry("process");
        AtomicReference<long[]> state = new AtomicReference<>(snapshot(7, 1, 1, 100, 20, 30));
        AtomicInteger reads = new AtomicInteger();
        A4SCacheTelemetry.Source source = source(7, state, reads);
        registry.register(new Object(), source, .1, 1);
        try (A4SCacheTelemetry.Lease backend = registry.attach(source, false); A4SCacheTelemetry.Lease monitor = registry.attach(source, true)) {
            assertEquals(Status.WARMUP, read(monitor, "A", 0).status);
            state.set(snapshot(7, 1, 2, 200, 40, 50));
            assertEquals(Status.VALID, read(monitor, "A", 10).status);
            try (A4SCacheTelemetry.Lease secondDb = registry.attach(source, true)) {
                CacheObservation pending = read(secondDb, "B", 11);
                assertEquals(Status.SHARED_CACHE, pending.status);
                assertNull(pending.window);
                assertEquals(3, reads.get());
            }
            registry.register(new Object(), source, .2, 1);
            assertEquals(Status.INCOMPATIBLE, read(monitor, "A", 12).status);
        }
    }

    private static CacheObservation firstObservation(
            A4SCacheTelemetry.Registry registry, A4SCacheTelemetry.Source source) {
        try (A4SCacheTelemetry.Lease backend = registry.attach(source, false); A4SCacheTelemetry.Lease monitor = registry.attach(source, true)) {
            return read(monitor, "A", 0);
        }
    }

    private static CacheObservation read(A4SCacheTelemetry.Lease monitor, String vertex, long seconds) {
        return CacheObservation.fromMetricString(A4SHistogramMetricCodec.decode(
                monitor.metric("job", vertex, 0, "attempt", TimeUnit.SECONDS.toNanos(seconds))));
    }

    private static A4SCacheTelemetry.Source source(
            long cacheId, AtomicReference<long[]> state, AtomicInteger reads) {
        return new A4SCacheTelemetry.Source(cacheId, 0, () -> {
            reads.incrementAndGet();
            return state.get();
        });
    }

    private static long[] snapshot(
            long cacheId, long epoch, long sequence, long references, long misses, long bin) {
        return new long[] {1, cacheId, 8192, epoch, 1, sequence, references, misses, misses / 2, 1, bin};
    }
}
