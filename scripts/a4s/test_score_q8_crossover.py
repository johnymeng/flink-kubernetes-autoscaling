import copy
import json
import runpy
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


scorer = runpy.run_path(str(Path(__file__).with_name("score-q8-crossover.py")))
pod_resources_by_period = scorer["pod_resources_by_period"]
latency_tools = runpy.run_path(str(Path(__file__).with_name("q8-latency.py")))


class MatchedWorkloadTest(unittest.TestCase):
    def test_keeps_pod_settings_when_normalizing_run_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory)
            (bundle / "scout").mkdir()
            manifest = bundle / "scout/deployment.json"

            def configuration(run, release, node):
                spec = {
                    "job": {"args": ["--q8-release-epoch-ms", release]},
                    "flinkConfiguration": {"$internal.pipeline.job-id": run},
                    "taskManager": {"podTemplate": {"spec": {
                        "nodeSelector": {"kubernetes.io/hostname": node},
                        "volumes": [{"name": "state", "hostPath": {"path": f"/state/q8-pr2-{run}/scout"}}],
                    }}},
                }
                manifest.write_text(json.dumps({"spec": spec}))
                return scorer["workload_configuration"](bundle, run)

            first = configuration("live-01", "100", "c167")
            self.assertEqual(first, configuration("live-02", "200", "c167"))
            self.assertNotEqual(first, configuration("live-02", "200", "c182"))


class TimedLatencyTest(unittest.TestCase):
    def frame(self, observed, sampled, samples):
        return {
            "job": "job", "vertex": "join", "observedAtMs": observed,
            "roster": {"0": [0, 1, "tm0"], "1": [0, 1, "tm1"]},
            "histograms": {
                task: {"attempt": 0, "binMs": 10, "overflowAtMs": 120_000, "invalid": 0,
                       "sampledAtMs": stamp, "counts": {"10": samples}}
                for task, stamp in zip(("0", "1"), sampled)
            },
        }

    def test_excludes_cached_start_and_samples_past_the_end(self):
        frames = [self.frame(105, (98, 102), 2), self.frame(115, (110, 112), 3),
                  self.frame(135, (130, 132), 5), self.frame(145, (138, 142), 7)]
        result = scorer["score_timed_latency"](frames, 100, 140, latency_tools["sample_times"],
                                               latency_tools["timed_interval"])
        self.assertEqual(result["subtaskIntervalsMs"], {"0": [110, 130], "1": [112, 132]})
        self.assertEqual(result["latency"]["samples"], 4)

    def test_reports_unavailable_until_every_sink_gauge_advances(self):
        frames = [self.frame(105, (100, 102), 2), self.frame(115, (100, 112), 2)]
        result = scorer["score_timed_latency"](frames, 100, 140, latency_tools["sample_times"],
                                               latency_tools["timed_interval"])
        self.assertIsNone(result)


class PacedSourceTest(unittest.TestCase):
    start = 1_700_000_000_000

    def setUp(self):
        self.spec = {"job_id": "job", "collection_seconds": 600,
                     "vertices": {role: role for role in ("person", "auction", "join")}}
        self.roster = {role: [(task, 0, self.start, "tm") for task in range(count)]
                       for role, count in (("person", 2), ("auction", 2), ("join", 1))}

    def snapshot(self, offset, backlog):
        responses = {"job": {"jid": "job", "state": "RUNNING"}, "subtask_metrics": {}}
        for role, tasks in self.roster.items():
            responses[role] = {"id": role, "subtasks": [
                {"status": "RUNNING", "subtask": subtask, "attempt": attempt,
                 "start-time": start, "taskmanager-id": worker}
                for subtask, attempt, start, worker in tasks
            ]}
            if role != "join":
                rate = 1111 if role == "person" else 3889
                for subtask, *_ in tasks:
                    responses["subtask_metrics"][f"{role}/{subtask}"] = {"values": [
                        {"id": "source.q8Offered", "value": str(offset * rate)},
                        {"id": "source.q8Backlog", "value": str(backlog)},
                    ]}
        return {"started": datetime.fromtimestamp(self.start / 1000 + offset, timezone.utc).isoformat(),
                "responses": responses}

    def qualify(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory)
            scout = bundle / "scout"
            scout.mkdir()
            (scout / "deployment.json").write_text(json.dumps({
                "spec": {"job": {"args": ["--q8-input-plan", "600:1111:3889"]}},
            }))
            (scout / "operational-snapshots.jsonl").write_text("\n".join(map(json.dumps, rows)))
            return scorer["qualify_paced_input"](bundle, self.spec,
                                                 [(self.start, self.start + 20_000, "period")],
                                                 self.roster, 1000)["period"]

    def test_uses_all_four_sources_and_reports_peak_inside_window(self):
        result = self.qualify([self.snapshot(0, 0), self.snapshot(10, 35), self.snapshot(20, 10)])
        self.assertEqual(result, {"plannedOfferedRecordsPerSecond": 10_000,
                                  "observedOfferedRecordsPerSecond": 10_000,
                                  "peakObservedSourceBacklogRecords": 140,
                                  "sourceObservations": 3, "observedSeconds": 20})

    def test_rejects_peak_backlog_and_missing_source_gauges(self):
        with self.assertRaisesRegex(ValueError, "observed 1200 backlogged records"):
            self.qualify([self.snapshot(0, 0), self.snapshot(10, 300), self.snapshot(20, 0)])
        incomplete = self.snapshot(10, 0)
        incomplete["responses"]["subtask_metrics"].pop("person/0")
        with self.assertRaisesRegex(ValueError, "complete source gauges"):
            self.qualify([self.snapshot(0, 0), incomplete, self.snapshot(20, 0)])

    def test_rejects_changed_attempt_and_offered_counter_reset(self):
        changed = self.snapshot(20, 0)
        changed["responses"]["person"]["subtasks"][0]["attempt"] = 1
        with self.assertRaisesRegex(ValueError, "complete source gauges"):
            self.qualify([self.snapshot(0, 0), changed])
        reset = self.snapshot(20, 0)
        reset["responses"]["subtask_metrics"]["person/0"]["values"][0]["value"] = "0"
        with self.assertRaisesRegex(ValueError, "reset offered counter"):
            self.qualify([self.snapshot(10, 0), reset])


class PodResourcesTest(unittest.TestCase):
    def setUp(self):
        self.start = 1_700_000_000_000
        self.finish = self.start + 20_000
        self.before = self.sample(2, "2023-11-14T22:13:22Z", 1_000_000_000, 2_000)
        self.after = self.sample(12, "2023-11-14T22:13:32Z", 6_000_000_000, 5_000)

    def sample(self, seconds, timestamp, cpu, memory):
        return {
            "observedAtMs": self.start + seconds * 1000,
            "containers": [{
                "container": "runner",
                "pod": {"uid": "runner-1"},
                "startTime": "2023-11-14T22:12:00Z",
                "cpu": {"time": timestamp, "usageCoreNanoSeconds": cpu},
                "memory": {"workingSetBytes": memory},
            }],
        }

    def test_uses_counter_deltas_and_peak_working_set(self):
        outside = self.sample(18, "2023-11-14T22:13:10Z", 50_000_000_000, 4_000)
        result = pod_resources_by_period([self.before, self.after, outside], self.start, self.finish)
        self.assertEqual(result["runner"], {
            "meanCpuCores": 0.5,
            "minimumCpuSampleSeconds": 10.0,
            "peakWorkingSetBytes": 5_000,
        })
        self.assertIsNone(result["flink"])

    def test_keeps_resource_metrics_unavailable_after_a_restart_or_reset(self):
        tests = (
            {"pod": {"uid": "runner-2"}},
            {"cpu": {"time": "2023-11-14T22:13:32Z", "usageCoreNanoSeconds": 1}},
        )
        for changes in tests:
            with self.subTest(changes=changes):
                after = copy.deepcopy(self.after)
                after["containers"][0].update(changes)
                result = pod_resources_by_period([self.before, after], self.start, self.finish)
                self.assertIsNone(result["runner"])


if __name__ == "__main__":
    unittest.main()
