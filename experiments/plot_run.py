#!/usr/bin/env python3
"""Render benchmark samples as stacked panels, optionally overlaying a second run."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path


MEMORY_USED = "total_managed_memory_used_bytes"
MEMORY_CAPACITY = "total_managed_memory_total_bytes"
EXPORTED_METRICS = (
    "throughput_total_source",
    "tm_count",
    "join_parallelism",
    "join_busy_pct",
    "join_cache_hit_rate_pct",
    "kafka_lag_bid",
    "kafka_lag_auction",
)
METRIC_LABELS = {
    "source_records_per_second": ("Source throughput", "records/s"),
    "throughput_total_source": ("Source throughput", "events/s"),
    "task_slots_used": ("Occupied task slots", ""),
    "tm_count": ("TaskManagers", ""),
    "join_parallelism": ("Join parallelism", ""),
    "join_busy_pct": ("Join busy time", "%"),
    "join_cache_hit_rate_pct": ("Join cache hit rate", "%"),
    "kafka_lag_bid": ("Bid Kafka lag", "events"),
    "kafka_lag_auction": ("Auction Kafka lag", "events"),
    MEMORY_USED: ("Managed memory used", "MiB"),
    MEMORY_CAPACITY: ("Managed memory capacity", "MiB"),
}


@dataclass
class Series:
    seconds: list[float]
    values: list[float]
    step: bool = False


@dataclass
class Run:
    directory: Path
    name: str
    metrics: dict[str, Series]
    autoscaler: str = ""
    commit: str = ""


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)
    if any(None in row for row in rows):
        raise ValueError(f"{path}: a row has more fields than the header")
    return rows


def numeric_column(rows: list[dict[str, str]], column: str, path: Path) -> list[float]:
    values = []
    for line, row in enumerate(rows, start=2):
        value = row.get(column)
        try:
            number = float(value) if value and value.strip() else math.nan
        except ValueError as exc:
            raise ValueError(f"{path}:{line}: {column} must be numeric, got {value!r}") from exc
        values.append(number if math.isfinite(number) else math.nan)
    return values


def timestamps(rows: list[dict[str, str]], column: str, path: Path) -> list[float]:
    times = numeric_column(rows, column, path)
    if any(not math.isfinite(time) for time in times):
        raise ValueError(f"{path}: {column} requires a finite timestamp on every row")
    return times


def load_samples(path: Path, metadata: dict[str, str] | None = None) -> Run:
    rows = read_rows(path)
    if not rows:
        raise ValueError(f"{path}: no samples")
    columns = rows[0]
    timestamp = next(
        (name for name in ("timestamp_epoch", "timestamp", "elapsed_seconds", "time", "t") if name in columns),
        None,
    )
    if timestamp is None:
        raise ValueError(f"{path}: expected a numeric timestamp column such as timestamp_epoch")
    times = timestamps(rows, timestamp, path)
    if any(left > right for left, right in zip(times, times[1:])):
        raise ValueError(f"{path}: samples must be ordered by timestamp")
    seconds = [time - times[0] for time in times]
    metrics = {
        column: Series(seconds, numeric_column(rows, column, path))
        for column in columns
        if column not in (timestamp, "timestamp_iso", "ts_iso")
    }
    metadata = metadata or {}
    return Run(
        directory=path.parent,
        name=metadata.get("run_id") or (path.parent.name if path.name == "samples.csv" else path.stem),
        metrics=metrics,
        autoscaler=metadata.get("autoscaler", ""),
        commit=metadata.get("run_commit", ""),
    )


def load_export(directory: Path) -> Run:
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    autoscaler = metadata.get("autoscaler", "")
    sources = {
        name: (directory / f"{name}.csv", "value", name == "join_parallelism")
        for name in EXPORTED_METRICS
    }
    memory_metric = "memory_level" if autoscaler in ("justin", "a4s") else "configured_memory_bytes"
    memory_column = "memory_level" if memory_metric == "memory_level" else "memory_bytes"
    sources[memory_metric] = (directory / "memory_timeline.csv", memory_column, True)
    metrics = {}
    for name, (path, column, step) in sources.items():
        if path.is_file() and (rows := read_rows(path)):
            if column not in rows[0]:
                raise ValueError(f"{path}: missing {column} column")
            times = timestamps(rows, "timestamp", path)
            values = numeric_column(rows, column, path)
            ordered = sorted(zip(times, values), key=lambda point: point[0])
            metrics[name] = Series([t for t, _ in ordered], [v for _, v in ordered], step)

    start = metadata.get("time_window", {}).get("start")
    if start is None:
        start = min((series.seconds[0] for series in metrics.values()), default=0)
    start = round(float(start), 3)
    for series in metrics.values():
        series.seconds = [round(time - start, 3) for time in series.seconds]
    return Run(directory, directory.name, metrics, autoscaler)


def load_run(
    query: str | None,
    results_root: Path,
    run_id: str | None,
    run_dir: Path | None,
    samples: Path | None,
) -> Run:
    if samples is not None:
        return load_samples(samples.resolve())
    if run_dir is not None:
        run_dir = run_dir.resolve()
        samples = run_dir / "samples.csv"
        return load_samples(samples) if samples.is_file() else load_export(run_dir)

    if not query:
        raise ValueError("--query is required for indexed runs")
    query_dir = results_root.resolve() / "nexmark" / query
    index = query_dir / "runs.csv"
    rows = read_rows(index)
    if not rows:
        raise ValueError(f"{index}: no runs")
    row = next((row for row in rows if row.get("run_id") == run_id), None) if run_id else rows[-1]
    if row is None:
        raise ValueError(f"{index}: run {run_id!r} not found")
    parts = [row.get(key, "").strip() for key in ("run_id", "environment", "autoscaler")]
    if not all(parts):
        raise ValueError(f"{index}: selected run needs run_id, environment and autoscaler")
    return load_samples(query_dir / "_".join(parts) / "samples.csv", row)


def metric_label(metric: str) -> tuple[str, str]:
    if metric in METRIC_LABELS:
        return METRIC_LABELS[metric]
    for suffix, unit in (("_bytes", "MiB"), ("_per_second", "/s"), ("_gib", "GiB"), ("_pct", "%")):
        if metric.endswith(suffix):
            return metric.removesuffix(suffix).replace("_", " ").capitalize(), unit
    return metric.replace("_", " ").capitalize(), ""


def plot_series(ax, series: Series, values: list[float], *, color: str, marker: str, linestyle: str, label: str) -> None:
    ax.plot(
        series.seconds, values, color=color, marker=marker, markersize=3,
        linestyle=linestyle, label=label, drawstyle="steps-post" if series.step else "default",
    )
    # A dotted connector identifies a bounded gap without adding measured points.
    previous = None
    for index, value in enumerate(values):
        if not math.isfinite(value):
            continue
        if previous is not None and index > previous + 1:
            ax.plot(
                [series.seconds[previous], series.seconds[index]], [values[previous], value],
                color=color, linestyle=":", zorder=1,
            )
        previous = index


def render(
    query: str | None,
    runs: list[Run],
    labels: list[str],
    metrics: list[str],
    output: Path,
    figsize: list[float] | None,
) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    panels = {}
    memory_panel = [MEMORY_USED, MEMORY_CAPACITY]
    combine_memory = all(metric in metrics for metric in memory_panel)
    for metric in metrics:
        if combine_memory and metric in memory_panel:
            panels[MEMORY_USED] = memory_panel
        else:
            panels[metric] = [metric]

    style = {
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.edgecolor": "#b0b0b0",
        "axes.grid": True,
        "grid.color": "#e6e6e6",
        "grid.linewidth": 0.7,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "legend.frameon": False,
        "lines.linewidth": 1.8,
    }
    colors = ("#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#17becf")
    comparing = len(runs) > 1
    with plt.rc_context(style):
        fig, axes = plt.subplots(
            len(panels), 1, sharex=True, squeeze=False, layout="constrained",
            figsize=figsize or (12, max(3.1 * len(panels), 5.5)),
        )
        try:
            duration = max(
                (series.seconds[-1] for run in runs for series in run.metrics.values() if series.seconds),
                default=0.0,
            )
            for panel_number, (ax, panel) in enumerate(zip(axes[:, 0], panels.values())):
                label, unit = metric_label(panel[0])
                if len(panel) > 1:
                    label = "Managed memory"
                ax.set_ylabel(f"{label} ({unit})" if unit else label)
                peak = 0.0
                for run_number, (run, run_label) in enumerate(zip(runs, labels)):
                    for metric_number, metric in enumerate(panel):
                        series = run.metrics[metric]
                        values = series.values
                        if metric.endswith("_bytes"):
                            values = [value / 1024**2 for value in values]
                        peak = max(peak, max((value for value in values if math.isfinite(value)), default=0))
                        suffix = {MEMORY_USED: "Used", MEMORY_CAPACITY: "Capacity"}.get(metric, "")
                        legend = " ".join(part for part in (run_label if comparing else "", suffix) if part)
                        color = ("#1f77b4", "#d55e00")[run_number] if comparing else colors[
                            (panel_number + metric_number) % len(colors)
                        ]
                        plot_series(
                            ax, series, values, color=color, marker=("o", "s")[run_number],
                            linestyle="--" if metric_number else "-", label=legend,
                        )
                ax.set_ylim(0, peak * 1.08 if peak > 0 else 1)
                if panel[0] == "task_slots_used":
                    ax.yaxis.set_major_locator(MaxNLocator(integer=True, min_n_ticks=1))
                if comparing or len(panel) > 1:
                    ax.legend(loc="best")

            axes[-1, 0].set_xlim(0, duration * 1.02 if duration > 0 else 1)
            axes[-1, 0].set_xlabel("Elapsed time (s)")
            names = [
                " ".join(part for part in (label, run.name, f"({run.commit})" if run.commit else "") if part)
                for run, label in zip(runs, labels)
            ]
            fig.suptitle(f"{query.upper() if query else 'Benchmark'}\n" + " · ".join(names))
            output.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output, dpi=300)
        finally:
            plt.close(fig)


def main(default_query: str | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--query", default=default_query,
        help=f"Query directory under nexmark. Default: {default_query}." if default_query else
        "Query directory under nexmark. Required for indexed runs.",
    )
    parser.add_argument(
        "--results-root", type=Path, default=Path(__file__).resolve().parents[1] / "results",
        help="Indexed results root. Default: <repository>/results.",
    )
    primary = parser.add_mutually_exclusive_group()
    primary.add_argument("--run-id", help="Run ID from the query's runs.csv.")
    primary.add_argument("--latest", action="store_true", help="Use the last indexed run (default).")
    primary.add_argument("--run-dir", type=Path, help="Directory containing samples.csv or a Prometheus export.")
    primary.add_argument("--samples", type=Path, help="Path to a standalone wide samples CSV.")
    comparison = parser.add_mutually_exclusive_group()
    comparison.add_argument("--compare-run-id", help="Overlay another indexed run for the same query.")
    comparison.add_argument("--compare-run-dir", type=Path, help="Overlay a run directory.")
    comparison.add_argument("--compare-samples", type=Path, help="Overlay a standalone samples CSV.")
    parser.add_argument("--compare-results-root", type=Path, help="Results root for --compare-run-id.")
    parser.add_argument("--label", help="Display label for the first run.")
    parser.add_argument("--compare-label", help="Display label for the comparison run.")
    parser.add_argument("--metrics", nargs="+", metavar="COLUMN", help="Metric names to plot in order.")
    parser.add_argument("--list-metrics", action="store_true", help="List available metric names without rendering.")
    parser.add_argument("--figsize", nargs=2, type=float, metavar=("WIDTH", "HEIGHT"), help="Figure size in inches.")
    parser.add_argument("--output", type=Path, help="Image path. Defaults to the first run directory.")
    args = parser.parse_args()

    if args.compare_results_root and not args.compare_run_id:
        parser.error("--compare-results-root requires --compare-run-id")
    if args.figsize and any(size <= 0 or not math.isfinite(size) for size in args.figsize):
        parser.error("--figsize requires two positive, finite dimensions")

    try:
        first = load_run(args.query, args.results_root, args.run_id, args.run_dir, args.samples)
        runs = [first]
        labels = [args.label or first.autoscaler]
        available = list(first.metrics)
        output = args.output or first.directory / "plot.png"

        if args.compare_run_id or args.compare_run_dir or args.compare_samples:
            second = load_run(
                args.query, args.compare_results_root or args.results_root,
                args.compare_run_id, args.compare_run_dir, args.compare_samples,
            )
            runs.append(second)
            if first.autoscaler == second.autoscaler:
                labels = [args.label or "Run 1", args.compare_label or "Run 2"]
            else:
                labels = [labels[0] or "Run 1", args.compare_label or second.autoscaler or "Run 2"]
            available = [metric for metric in available if metric in second.metrics]
            output = args.output or first.directory / f"comparison-{second.name}.png"

        if not available:
            raise ValueError("No metrics available in the selected input(s)")
        if args.list_metrics:
            print("\n".join(available))
            return
        metrics = list(dict.fromkeys(args.metrics or available))
        missing = [metric for metric in metrics if metric not in available]
        if missing:
            raise ValueError(f"Unavailable metrics: {', '.join(missing)}. Available: {', '.join(available)}")
        render(args.query, runs, labels, metrics, output.resolve(), args.figsize)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(output.resolve())


if __name__ == "__main__":
    main()
