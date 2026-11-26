"""Read exported metric samples and align them to the run's time origin."""

from __future__ import annotations

import csv
from pathlib import Path


def load_series(path: Path) -> tuple[list[float], list[float]]:
    timestamps: list[float] = []
    values: list[float] = []
    if not path.exists():
        return timestamps, values
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            value = row.get("value", "")
            if value in ("", "NaN", "nan"):
                continue
            timestamps.append(float(row["timestamp"]))
            values.append(float(value))
    return timestamps, values


def run_start(metadata: dict, *time_series: list[float]) -> float:
    start = metadata.get("time_window", {}).get("start")
    if start is not None:
        return float(start)
    return min((timestamp for times in time_series for timestamp in times), default=0.0)


def elapsed_minutes(timestamp: float, start: float) -> float:
    # The exporter writes Prometheus timestamps at millisecond precision.
    return round(timestamp - start, 3) / 60.0
