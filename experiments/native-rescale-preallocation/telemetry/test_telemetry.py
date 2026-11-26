"""Regression checks for the benchmark adapters' evidence boundaries."""
import unittest

from score_broker_offsets import PARTITIONS, observe
from score_q8_latency import score, summarize_cgroups


def snapshot(epoch, end, committed):
    return {
        "before_ms": epoch,
        "after_ms": epoch,
        "partitions": [
            {"topic": topic, "partition": partition, "end": end, "committed": committed}
            for topic, partition in sorted(PARTITIONS)
        ],
    }


class KafkaOffsetsTest(unittest.TestCase):
    def test_unknown_offsets_are_gaps_and_observed_zeros_are_retained(self):
        state, initial = observe(None, snapshot(0, 0, None))
        self.assertIsNone(initial["kafka_committed_consumer_lag_offsets"])
        state, unchanged = observe(state, snapshot(5_000, 0, 0))
        self.assertEqual(0, unchanged["kafka_log_end_increments_per_second"])
        self.assertEqual(0, unchanged["kafka_committed_consumer_lag_offsets"])
        _, later = observe(state, snapshot(10_000, 20, 12))
        self.assertEqual(288, later["kafka_log_end_increments_per_second"])
        self.assertEqual(384, later["kafka_committed_consumer_lag_offsets"])

    def test_decreasing_and_after_end_offsets_are_not_interpreted_as_zero(self):
        state, _ = observe(None, snapshot(0, 10, 10))
        _, row = observe(state, snapshot(5_000, 9, 12))
        self.assertIsNone(row["kafka_log_end_increments_per_second"])
        self.assertIsNone(row["kafka_committed_consumer_lag_offsets"])

    def test_duplicate_partition_is_rejected(self):
        sample = snapshot(0, 0, 0)
        sample["partitions"][-1] = sample["partitions"][0]
        with self.assertRaises(ValueError):
            observe(None, sample)


class Q8LatencyTest(unittest.TestCase):
    def test_replays_and_missing_emissions_preserve_coverage(self):
        records = [
            {"kind": "output", "auction": 512, "person": 7, "epoch_ms": 1700},
            {"kind": "input", "auction": 512, "epoch_ms": 1200},
            {"kind": "input", "auction": 512, "epoch_ms": 1100},
            {"kind": "output", "auction": 512, "person": 7, "epoch_ms": 1500},
            {"kind": "output", "auction": 512, "person": 8, "epoch_ms": 1600},
            {"kind": "output", "auction": 1024, "person": 7, "epoch_ms": 1700},
            {"kind": "output", "auction": 512, "person": 9, "epoch_ms": 61000},
        ]
        rows, result = score(records, 1000, 61000)
        self.assertEqual(2, result["matched_results"])
        self.assertEqual(1, result["unmatched_results"])
        self.assertEqual(400, result["p50_ms"])
        self.assertIsNone(result["p95_ms"])
        self.assertIsNone(result["p99_ms"])
        self.assertIsNone(rows[1]["auction_to_result_p50_ms"])

    def test_p99_uses_sample_size_gate(self):
        records = [
            row
            for i in range(100)
            for row in (
                {"kind": "input", "auction": i, "epoch_ms": 1000},
                {"kind": "output", "auction": i, "person": i, "epoch_ms": 1000 + i},
            )
        ]
        _, result = score(records, 1000, 61000)
        self.assertEqual((49, 94, 98), (result["p50_ms"], result["p95_ms"], result["p99_ms"]))

    def test_container_counters_do_not_bridge_pod_changes_or_gaps(self):
        records = [
            {"epoch_ms": epoch, "pod_uid": uid, "cpu_usage_usec": cpu,
             "memory_current_bytes": memory}
            for epoch, uid, cpu, memory in [(1000, "a", 0, 5), (6000, "a", 2_000_000, 8),
                                            (16000, "a", 9_000_000, 9), (16900, "b", 0, 7)]
        ]
        result = summarize_cgroups(records, 1000, 20000)
        self.assertEqual(5, result["cpu_covered_taskmanager_seconds"])
        self.assertEqual(2, result["observed_taskmanager_container_cpu_core_seconds"])
        self.assertEqual(16, result["peak_observed_taskmanager_memory_current_bytes_5s_bin"])


if __name__ == "__main__":
    unittest.main()
