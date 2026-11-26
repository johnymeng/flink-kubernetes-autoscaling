import copy
import runpy
import unittest
from pathlib import Path
from unittest import mock


collector = runpy.run_path(str(Path(__file__).with_name("q8-latency.py")))
summarize = collector["summarize"]
timed_interval = collector["timed_interval"]


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

    def test_q20_v2_resolves_bucket_boundaries_and_overflow(self):
        for snapshot in (self.before, self.after):
            snapshot["metric"] = "q20KafkaLatencyV2"
            for gauge in snapshot["histograms"].values():
                gauge.pop("binMs")
                gauge["overflowAtMs"] = 900_000
                gauge["segments"] = [[2_000, 10], [60_000, 100], [900_000, 1_000]]
        for bucket, lower, upper in (
            (199, 1_990, 1_999), (200, 2_000, 2_099), (779, 59_900, 59_999),
            (780, 60_000, 60_999), (1_619, 899_000, 899_999), (1_620, 900_000, None),
        ):
            with self.subTest(bucket=bucket):
                self.after["histograms"]["0"]["counts"] = {"1": 100, str(bucket): 1}
                result = summarize(self.before, self.after)
                self.assertEqual(result["p99"], {"lowerMs": lower, "upperMs": upper})
        self.after["histograms"]["0"]["counts"]["1621"] = 1
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

    def make_timed(self):
        for snapshot in (self.before, self.after):
            snapshot["metric"] = "q20KafkaLatencyV2"
            for sample in snapshot["histograms"].values():
                sample.pop("binMs")
                sample["overflowAtMs"] = 900_000
                sample["segments"] = [[2_000, 10], [60_000, 100], [900_000, 1_000]]
                sample["sampledAtMs"] = 100

    def test_timed_interval_distinguishes_cached_reads_from_zero_output(self):
        self.make_timed()
        self.assertIsNone(timed_interval(self.before, self.after))
        self.after["histograms"]["0"]["sampledAtMs"] = 200
        self.assertIsNone(timed_interval(self.before, self.after))
        self.after["histograms"]["1"]["sampledAtMs"] = 220
        result = timed_interval(self.before, self.after)
        self.assertEqual(result["subtaskIntervalsMs"], {"0": [100, 200], "1": [100, 220]})
        self.assertEqual(result["latency"]["samples"], 0)
        self.after["histograms"]["0"]["counts"]["9"] = 1
        self.assertEqual(timed_interval(self.before, self.after)["latency"]["p99"]["lowerMs"], 90)

    def test_timed_interval_rejects_invalid_sample_times_and_attempts(self):
        self.make_timed()
        changes = [
            lambda window: window["histograms"]["0"].pop("sampledAtMs"),
            lambda window: window["histograms"]["0"].update(sampledAtMs=99),
            lambda window: window["histograms"]["0"]["counts"].update({"1": 101}),
            lambda window: window["roster"].update({"0": [1, 2, "tm2"]}),
        ]
        for change in changes:
            window = copy.deepcopy(self.after)
            change(window)
            with self.assertRaises(ValueError):
                timed_interval(self.before, window)

    def test_failed_poll_starts_new_baseline(self):
        self.make_timed()
        later = copy.deepcopy(self.after)
        later["observedAtMs"] += 1
        later["histograms"]["0"].update(sampledAtMs=200, counts={"1": 102})
        later["histograms"]["1"].update(sampledAtMs=200)
        trace = collector["capture_series"]
        clock = [0]

        def advance(seconds):
            clock[0] += seconds

        responses = mock.Mock(side_effect=[self.before, OSError("fetch failed"), self.after, later])
        with mock.patch.dict(trace.__globals__, {"capture": responses}), \
                mock.patch("time.monotonic", side_effect=lambda: clock[0]), \
                mock.patch("time.sleep", side_effect=advance):
            events = list(trace("http://127.0.0.1:8081", "job", "join", 4, 1))
        self.assertEqual([event["event"] for event in events], ["baseline", "gap", "baseline", "interval"])
        self.assertEqual(events[-1]["latency"]["samples"], 2)


if __name__ == "__main__":
    unittest.main()
