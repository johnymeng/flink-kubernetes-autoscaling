import copy
import runpy
import unittest
from pathlib import Path


pod_resources_by_period = runpy.run_path(
    str(Path(__file__).with_name("score-q8-crossover.py"))
)["pod_resources_by_period"]


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
