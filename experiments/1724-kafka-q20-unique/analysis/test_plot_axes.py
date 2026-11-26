import json
import runpy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.figure import Figure


SCRIPT_DIR = Path(__file__).resolve().parent


class PlotAxesTest(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.addCleanup(plt.close, "all")
        self.root = Path(temp_dir.name)

    def make_run(self, name, start, *, include_time_window=True):
        directory = self.root / name
        directory.mkdir()
        metadata = {"job_name": "Q20", "autoscaler": name}
        if include_time_window:
            metadata["time_window"] = {"start": start}
        (directory / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

        samples = {
            "throughput_total_source": [(0, 50_000), (60, 0), (180, 60_000)],
            "tm_count": [(0, 4), (180, 5)],
            "join_parallelism": [(0, 4), (60, 5)],
            "join_busy_pct": [(0, "NaN"), (30, 70), (180, 60)],
            "join_cache_hit_rate_pct": [(60, 90), (180, 95)],
            "kafka_lag_bid": [(90, 500), (180, 400)],
        }
        for metric, points in samples.items():
            rows = "".join(f"{start + offset:.3f},{value}\n" for offset, value in points)
            (directory / f"{metric}.csv").write_text("timestamp,value\n" + rows, encoding="utf-8")
        (directory / "memory_timeline.csv").write_text(
            f"timestamp,memory_mb,memory_level\n{start},512,1\n{start + 60},1024,2\n",
            encoding="utf-8",
        )
        return directory

    def run_renderer(self, script, *directories):
        path = SCRIPT_DIR / script
        output = self.root / "plot.png"
        argv = [str(path), *(str(directory) for directory in directories), "--output", str(output)]
        with patch.object(sys, "argv", argv), patch.object(Figure, "savefig", autospec=True) as savefig:
            runpy.run_path(str(path), run_name="__main__")
        savefig.assert_called_once()
        return savefig.call_args.args[0]

    def assert_zero_axes(self, fig, expected_axes):
        self.assertEqual(len(fig.axes), expected_axes)
        for ax in fig.axes:
            self.assertEqual(ax.get_xlim()[0], 0)
            self.assertGreater(ax.get_xlim()[1], 0)
            self.assertEqual(ax.get_ylim()[0], 0)
            self.assertGreater(ax.get_ylim()[1], 0)

    def test_single_run_uses_elapsed_time_on_primary_and_secondary_axes(self):
        run = self.make_run("justin", 1_700_000_000.0004)
        fig = self.run_renderer("plot-experiment.py", run)

        self.assert_zero_axes(fig, 6)
        throughput, cpu, lag, busy, memory, cache = fig.axes
        self.assertEqual(list(throughput.lines[0].get_xdata()), [0, 1, 3])
        self.assertEqual(list(throughput.lines[0].get_ydata()), [50_000, 0, 60_000])
        self.assertEqual(list(lag.lines[1].get_xdata()), [1.5, 3])
        self.assertEqual(list(busy.lines[0].get_xdata()), [0.5, 3])
        self.assertEqual(list(memory.lines[0].get_xdata()), [0, 1])
        self.assertEqual(list(cache.lines[0].get_xdata()), [1, 3])
        for ax in (throughput, cpu, lag, busy):
            self.assertEqual(list(ax.lines[-1].get_xdata()), [1, 1])
        self.assertEqual([text.get_position()[0] for text in throughput.texts], [0.5, 2])

    def test_comparison_keeps_metric_offsets_for_both_runs(self):
        left = self.make_run("justin", 1_700_000_000)
        right = self.make_run("ds2", 1_800_000_000, include_time_window=False)
        (left / "throughput_total_source.csv").write_text(
            "timestamp,value\n1700000060,0\n1700000180,61000\n",
            encoding="utf-8",
        )
        fig = self.run_renderer("compare-justin-ds2.py", left, right)

        self.assert_zero_axes(fig, 3)
        expected = [([1, 3], [0, 1, 3]), ([0.5, 3], [0.5, 3]), ([1.5, 3], [1.5, 3])]
        for ax, (left_times, right_times) in zip(fig.axes, expected):
            self.assertEqual([list(line.get_xdata()) for line in ax.lines], [left_times, right_times])
