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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class A4SCacheSnapshotsTest {
    @Test
    void legacyReadsDrainPooledReferencesInEitherOrder() {
        AtomicInteger pooled = new AtomicInteger();
        Supplier<Integer> drain = () -> pooled.getAndSet(0);
        for (boolean reverse : new boolean[] {false, true}) {
            pooled.addAndGet(10); // backend A
            pooled.addAndGet(30); // backend B
            int a;
            int b;
            if (reverse) {
                b = drain.get();
                a = drain.get();
            } else {
                a = drain.get();
                b = drain.get();
            }
            assertEquals(reverse ? 0 : 40, a);
            assertEquals(reverse ? 40 : 0, b);
        }
    }

    @Test
    void readersShareOnePublicationAndCannotRelabelPooledHistory() {
        A4SCacheSnapshots<Integer> cache = new A4SCacheSnapshots<>(10);
        long first = cache.attach();
        AtomicInteger reads = new AtomicInteger();
        A4SCacheSnapshots.Publication<Integer> exclusive = cache.poll(0, reads::incrementAndGet);
        assertTrue(exclusive.exclusive);
        long second = cache.attach();
        A4SCacheSnapshots.Publication<Integer> pooled = cache.poll(1, reads::incrementAndGet);
        assertFalse(pooled.exclusive);
        assertEquals(2, pooled.owners.size());
        assertSame(pooled, cache.poll(2, reads::incrementAndGet));
        assertSame(pooled, cache.poll(9, reads::incrementAndGet));
        assertEquals(1, reads.get());
        A4SCacheSnapshots.Publication<Integer> secondInterval = cache.poll(10, reads::incrementAndGet);
        assertSame(secondInterval, cache.poll(11, reads::incrementAndGet));
        assertEquals(2, reads.get());
        cache.detach(second);
        assertFalse(cache.poll(12, reads::incrementAndGet).exclusive);
        cache.detach(first);
        long replacement = cache.attach();
        assertFalse(cache.poll(20, reads::incrementAndGet).exclusive);
        assertTrue(replacement > second);
        A4SCacheSnapshots<Integer> recreated = new A4SCacheSnapshots<>(10);
        recreated.attach();
        assertTrue(recreated.poll(20, reads::incrementAndGet).exclusive);
    }
    @Test
    void resizeAndCounterResetDoNotPublishAcrossGenerations() {
        AtomicReference<long[]> nativeState = new AtomicReference<>(new long[] {1, 7, 8192, 1, 1, 100, 20});
        A4SCacheTelemetry cache = new A4SCacheTelemetry(nativeState::get, () -> new long[] {9, 1}, .1, 1);
        cache.attach();
        assertEquals(Status.WARMUP, read(cache, 10).status);
        nativeState.set(new long[] {1, 7, 8192, 1, 1, 200, 40});
        CacheObservation valid = read(cache, 20);
        assertEquals(Status.VALID, valid.status);
        assertEquals(100, valid.window.references);
        nativeState.set(new long[] {1, 7, 16384, 2, 1, 300, 60});
        assertEquals(Status.CACHE_CHANGED, read(cache, 30).status);
        nativeState.set(new long[] {1, 7, 16384, 2, 1, 10, 2});
        assertEquals(Status.COUNTER_RESET, read(cache, 40).status);
        nativeState.set(new long[] {1, 7, 16384, 2, 1, 100, 10});
        CacheObservation recovered = read(cache, 50);
        assertEquals(Status.VALID, recovered.status);
        assertEquals(90, recovered.window.references);
        assertEquals(2, recovered.window.epoch);
    }

    private CacheObservation read(A4SCacheTelemetry cache, long seconds) {
        return CacheObservation.fromMetricString(
                A4SHistogramMetricCodec.decode(
                        cache.metric("job", "vertex", 0, "attempt", TimeUnit.SECONDS.toNanos(seconds))));
    }
}
