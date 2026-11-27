import json
import runpy
import tempfile
import unittest
from pathlib import Path


score = runpy.run_path(str(Path(__file__).with_name("score-live-a4s-client.py")))["score"]


class LiveClientTimingTest(unittest.TestCase):
    base_time = 1_700_000_000_000

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.bundle = Path(temporary.name)
        self.write(self.bundle / "execution-spec.json", {
            "run": "run", "job_id": "job", "crossover_order": ["remote", "embedded"],
        })
        crossover = self.bundle / "scout/crossover"
        crossover.mkdir(parents=True)
        (crossover / "complete").touch()
        self.window(crossover / "01-remote", "remote", 100, 200,
                    [self.record("warmup", 75), self.record("measured", 150), self.record("failed", 160, False)])
        self.window(crossover / "02-embedded", "embedded", 300, 400, [self.record("embedded", 350)])
        self.operator_log = self.bundle / "scout/operator.log"

    def write(self, path, payload):
        path.write_text(json.dumps(payload) + "\n")

    def record(self, snapshot, elapsed, has_recommendation=True):
        return {
            "request": {"snapshot": {"jobId": "job", "snapshotId": snapshot},
                        "evaluatedAtMillis": self.base_time + elapsed},
            "recommendation": {"action": "NO_CHANGE"} if has_recommendation else None,
        }

    def window(self, path, mode, start, end, records):
        path.mkdir()
        self.write(path / "planned.json", {"mode": mode})
        for name, elapsed in (("before", start), ("after", end)):
            self.write(path / f"{name}.json", {"latency": {"observedAtMs": self.base_time + elapsed}})
        (path / "a4s-requests.jsonl").write_text("\n".join(map(json.dumps, records)) + "\n")

    def line(self, elapsed, snapshot, job="job"):
        return (f"2023-11-14T22:13:20.{elapsed:03d}Z INFO A4S runner timing job={job} snapshot={snapshot} "
                "requestBytes=10 responseBytes=5 encodeNanos=1000000 exchangeNanos=2000000 decodeNanos=3000000\n")

    def test_uses_recorded_remote_start_and_reports_completion_after_window(self):
        self.operator_log.write_text(self.line(40, "measured", "other-job")
                                     + self.line(90, "warmup") + self.line(220, "measured"))
        result = score(self.bundle)
        self.assertEqual(result["measuredRemoteRecommendations"], 1)
        self.assertEqual(result["remoteEvaluationsWithoutRecommendation"], 1)
        self.assertEqual(result["completedAfterWindow"], 1)
        self.assertEqual(result["firstClientTiming"]["snapshotId"], "warmup")
        self.assertEqual(result["phasesMs"]["totalMs"], {"p50": 6, "p95": 6, "p99": 6})
        self.assertEqual(result["samples"][0]["completedAtMs"], self.base_time + 220)

    def test_rejects_missing_duplicate_and_embedded_measurements(self):
        valid = self.line(220, "measured")
        for log, detail in (("", "Missing or reused"), (valid * 2, "Duplicate client timing"),
                            (valid + self.line(360, "embedded"), "Embedded snapshot")):
            with self.subTest(detail=detail), self.assertRaisesRegex(ValueError, detail):
                self.operator_log.write_text(log)
                score(self.bundle)


if __name__ == "__main__":
    unittest.main()
