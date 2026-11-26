import copy
import runpy
import unittest
from pathlib import Path


summarize = runpy.run_path(str(Path(__file__).with_name("q8-latency.py")))["summarize"]


def histogram(counts, invalid=0):
    return {"attempt": 0, "binMs": 10, "overflowAtMs": 120_000, "counts": counts, "invalid": invalid}


class LatencySummaryTest(unittest.TestCase):
    def setUp(self):
        self.before = {
            "job": "job",
            "vertex": "join",
            "observedAtMs": 1,
            "roster": {"0": [0, 1, "tm0"], "1": [0, 1, "tm1"]},
            "histograms": {"0": histogram({"1": 100}), "1": histogram({}, 1)},
        }
        self.after = copy.deepcopy(self.before)
        self.after["observedAtMs"] = 2

    def test_merges_counts_before_calculating_percentiles(self):
        self.after["histograms"] = {"0": histogram({"1": 190}),
                                    "1": histogram({"10": 7, "12000": 3}, 2)}
        self.assertEqual(summarize(self.before, self.after), {
            "samples": 100,
            "invalidClockSamples": 1,
            "overflowSamples": 3,
            "p50": {"lowerMs": 10, "upperMs": 19},
            "p95": {"lowerMs": 100, "upperMs": 109},
            "p99": {"lowerMs": 120_000, "upperMs": None},
        })

    def test_empty_interval_has_unavailable_percentiles(self):
        summary = summarize(self.before, self.after)
        self.assertEqual(summary["samples"], 0)
        self.assertIsNone(summary["p99"])

    def test_q20_latency_uses_its_own_overflow_bucket(self):
        for snapshot in (self.before, self.after):
            snapshot["metric"] = "q20KafkaLatencyV1"
            for gauge in snapshot["histograms"].values():
                gauge["overflowAtMs"] = 900_000
        self.after["histograms"]["0"]["counts"].update({"80000": 10, "90000": 1})
        summary = summarize(self.before, self.after)
        self.assertEqual(summary["samples"], 11)
        self.assertEqual(summary["overflowSamples"], 1)
        self.assertEqual(summary["p50"], {"lowerMs": 800_000, "upperMs": 800_009})
        self.assertEqual(summary["p99"], {"lowerMs": 900_000, "upperMs": None})
        self.after.pop("metric")
        with self.assertRaises(ValueError):
            summarize(self.before, self.after)

    def test_rejects_resets_and_incomplete_or_changed_attempts(self):
        changes = [
            ("reset", lambda window: window["histograms"]["0"]["counts"].clear()),
            ("missing", lambda window: window["histograms"].pop("1")),
            ("stale", lambda window: window["histograms"]["0"].update(attempt=1)),
            ("changed", lambda window: window["roster"].update({"0": [1, 2, "tm2"]})),
        ]
        for label, change in changes:
            with self.subTest(label=label):
                window = copy.deepcopy(self.after)
                change(window)
                with self.assertRaises(ValueError):
                    summarize(self.before, window)


if __name__ == "__main__":
    unittest.main()
