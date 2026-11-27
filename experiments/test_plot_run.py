import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

from matplotlib.figure import Figure

import plot_run


class StandardPlotTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def write(self, path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def test_plot_overlays_runs_without_filling_missing_samples(self):
        first_path = self.write(
            self.root / "first.csv",
            f"timestamp_epoch,source_records_per_second,{plot_run.MEMORY_USED},{plot_run.MEMORY_CAPACITY}\n"
            "100,0,1048576,4194304\n105,,2097152,4194304\n110,8000,2097152,4194304\n",
        )
        second_path = self.write(
            self.root / "second.csv",
            f"timestamp_epoch,source_records_per_second,{plot_run.MEMORY_USED},{plot_run.MEMORY_CAPACITY}\n"
            "200,NaN,1048576,4194304\n207,3500,2097152,4194304\n215,10000,3145728,4194304\n",
        )
        first = plot_run.load_samples(first_path)
        second = plot_run.load_samples(second_path)
        selected = [plot_run.MEMORY_CAPACITY, "source_records_per_second", plot_run.MEMORY_USED]
        with patch.object(Figure, "savefig", autospec=True) as savefig:
            plot_run.render("q20", [first, second], ["Main", "Online"], selected, self.root / "out.png", None)
        memory, throughput = savefig.call_args.args[0].axes

        for axis in (memory, throughput):
            self.assertEqual(axis.get_xlim()[0], 0)
            self.assertEqual(axis.get_ylim()[0], 0)
        self.assertEqual(memory.get_ylabel(), "Managed memory (MiB)")
        self.assertEqual([list(line.get_ydata()) for line in memory.lines], [[1, 2, 2], [4, 4, 4], [1, 2, 3], [4, 4, 4]])
        measured_first, gap, measured_second = throughput.lines
        self.assertEqual(list(measured_first.get_xdata()), [0, 5, 10])
        self.assertEqual(measured_first.get_ydata()[0], 0)
        self.assertTrue(math.isnan(measured_first.get_ydata()[1]))
        self.assertEqual(list(gap.get_xdata()), [0, 10])
        self.assertEqual(gap.get_linestyle(), ":")
        self.assertEqual(list(measured_second.get_xdata()), [0, 7, 15])
        self.assertTrue(math.isnan(measured_second.get_ydata()[0]))

    def test_cli_selects_latest_run_and_indexed_comparison_from_another_root(self):
        first_root, second_root = self.root / "first", self.root / "second"
        first_query, second_query = first_root / "nexmark/q4", second_root / "nexmark/q4"
        index_header = "run_id,environment,autoscaler,run_commit\n"
        self.write(first_query / "runs.csv", index_header + "older,lab,ds2,b1\nnewer,lab,a4s,b2\n")
        self.write(second_query / "runs.csv", index_header + "baseline,lab,ds2,a1\n")
        self.write(first_query / "newer_lab_a4s/samples.csv", "time,task_slots_used,extra\n10,10,2\n")
        self.write(second_query / "baseline_lab_ds2/samples.csv", "time,task_slots_used\n20,8\n")
        args = [
            "plot.py", "--latest", "--results-root", str(first_root), "--compare-run-id", "baseline",
            "--compare-results-root", str(second_root), "--metrics", "task_slots_used", "--label", "Online",
        ]
        with patch.object(sys, "argv", args), patch.object(plot_run, "render") as render:
            plot_run.main(default_query="q4")
        query, runs, labels, metrics, output, _ = render.call_args.args
        self.assertEqual(query, "q4")
        self.assertEqual([run.name for run in runs], ["newer", "baseline"])
        self.assertEqual(labels, ["Online", "ds2"])
        self.assertEqual(metrics, ["task_slots_used"])
        self.assertEqual(output, first_query / "newer_lab_a4s/comparison-baseline.png")

    def test_export_preserves_window_origin_and_distinguishes_memory_level_from_bytes(self):
        for autoscaler in ("a4s", "ds2"):
            with self.subTest(autoscaler=autoscaler):
                directory = self.root / autoscaler
                metadata = {"autoscaler": autoscaler, "time_window": {"start": 100.0004}}
                self.write(directory / "metadata.json", json.dumps(metadata))
                self.write(directory / "throughput_total_source.csv", "timestamp,value\n105,500\n110,0\n")
                self.write(directory / "memory_timeline.csv", "timestamp,memory_bytes,memory_level\n100,1048576,1\n")
                run = plot_run.load_run(None, self.root, None, directory, None)
                self.assertEqual(run.metrics["throughput_total_source"].seconds, [5, 10])
                if autoscaler == "a4s":
                    self.assertEqual(run.metrics["memory_level"].values, [1])
                else:
                    self.assertEqual(run.metrics["configured_memory_bytes"].values, [1048576])
