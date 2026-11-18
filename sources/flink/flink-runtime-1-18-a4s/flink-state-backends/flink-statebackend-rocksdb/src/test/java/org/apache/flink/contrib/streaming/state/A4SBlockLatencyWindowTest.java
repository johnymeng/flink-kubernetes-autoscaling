/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.junit.jupiter.api.Test;

import static org.apache.flink.contrib.streaming.state.A4SBlockLatencyWindow.Kind.HIT;
import static org.apache.flink.contrib.streaming.state.A4SBlockLatencyWindow.Kind.MISS_DIRECT;
import static org.apache.flink.contrib.streaming.state.A4SBlockLatencyWindow.Kind.MISS_NON_DIRECT_FILE;
import static org.apache.flink.contrib.streaming.state.A4SBlockLatencyWindow.MAX_GAP_NS;
import static org.apache.flink.contrib.streaming.state.A4SBlockLatencyWindow.WINDOW_NS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

class A4SBlockLatencyWindowTest {
    private static final long[] ZERO = {0, 0, 0, 0};

    @Test
    void poolsMissClassesByCountAndConvertsToSeconds() {
        A4SBlockLatencyWindow window = new A4SBlockLatencyWindow();
        window.observe(0, new long[] {90, 10, 10, 0}, new long[] {900, 800, 800, 0});
        window.observe(
                WINDOW_NS,
                new long[] {190, 30, 90, 0},
                new long[] {10_900, 20_800, 8_800, 0});
        A4SBlockLatencyWindow.Sample sample = window.latest(WINDOW_NS);
        assertThat(sample.count(HIT)).isEqualTo(100);
        assertThat(sample.count(MISS_DIRECT)).isEqualTo(20);
        assertThat(sample.meanSeconds(MISS_DIRECT)).isNaN();
        assertThat(sample.meanSeconds(MISS_NON_DIRECT_FILE)).isCloseTo(1e-7, offset(1e-15));
        assertThat(sample.missPenaltySeconds()).isCloseTo(1.8e-7, offset(1e-15));
    }

    @Test
    void neverPublishesSparseNegativeOrInvalidWindows() {
        A4SBlockLatencyWindow window = new A4SBlockLatencyWindow();
        window.observe(0, ZERO, ZERO);
        window.observe(WINDOW_NS, new long[] {63, 64, 0, 0}, new long[] {126, 128, 0, 0});
        assertThat(window.latest(WINDOW_NS).missPenaltySeconds()).isNaN();
        window.observe(
                2 * WINDOW_NS, new long[] {127, 128, 0, 0}, new long[] {382, 256, 0, 0});
        assertThat(window.latest(2 * WINDOW_NS).missPenaltySeconds()).isNaN();
        window.observe(
                3 * WINDOW_NS, new long[] {191, 128, 0, 0}, new long[] {446, 260, 0, 0});
        assertThat(window.latest(3 * WINDOW_NS).count(HIT)).isZero();
        window.observe(4 * WINDOW_NS, new long[] {255, 192, 0, 0}, new long[] {-1, 388, 0, 0});
        assertThat(window.latest(4 * WINDOW_NS).missPenaltySeconds()).isNaN();
    }

    @Test
    void detectsResetAgainstIntermediatePollEvenIfAboveWindowBaseline() {
        A4SBlockLatencyWindow window = new A4SBlockLatencyWindow();
        window.observe(0, new long[] {100, 100, 0, 0}, new long[] {100, 200, 0, 0});
        window.observe(
                WINDOW_NS / 2, new long[] {200, 200, 0, 0}, new long[] {200, 400, 0, 0});
        window.observe(
                WINDOW_NS, new long[] {180, 180, 0, 0}, new long[] {180, 360, 0, 0});
        assertThat(window.latest(WINDOW_NS).missPenaltySeconds()).isNaN();
    }

    @Test
    void resetsOrExpiresInsteadOfReusingOldPenalty() {
        A4SBlockLatencyWindow window = new A4SBlockLatencyWindow();
        long[] counts = {64, 64, 0, 0};
        long[] times = {64, 128, 0, 0};
        window.observe(0, ZERO, ZERO);
        window.observe(WINDOW_NS, counts, times);
        assertThat(window.latest(WINDOW_NS).missPenaltySeconds()).isPositive();
        window.observe(WINDOW_NS + 1, ZERO, ZERO);
        assertThat(window.latest(WINDOW_NS + 1).missPenaltySeconds()).isNaN();
        window.observe(2 * WINDOW_NS + 1, counts, times);
        assertThat(window.latest(2 * WINDOW_NS + 1).missPenaltySeconds()).isPositive();
        assertThat(window.latest(2 * WINDOW_NS + 2 + MAX_GAP_NS).missPenaltySeconds()).isNaN();
        window.clear();
        assertThat(window.latest(2 * WINDOW_NS + 1).missPenaltySeconds()).isNaN();
    }
}
