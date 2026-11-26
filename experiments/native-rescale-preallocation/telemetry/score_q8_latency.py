#!/usr/bin/env python3
"""Score sampled auction emission to first Q8 result and write Figure 10 input."""

import argparse
from collections import defaultdict
import csv
import json
import math
from pathlib import Path

WINDOW_MS = 30_000
METRICS = {"p50_ms": (0.50, 2), "p95_ms": (0.95, 20), "p99_ms": (0.99, 100)}


def quantiles(values):
    ordered = sorted(values)
    return {
        name: ordered[math.ceil(fraction * len(ordered)) - 1] if len(ordered) >= minimum else None
        for name, (fraction, minimum) in METRICS.items()
    }


def score(records, start, cutoff):
    first_emission = {}
    first_result = {}
    inputs = outputs = 0
    for row in records:
        epoch = row["epoch_ms"]
        if not start <= epoch < cutoff:
            continue
        auction = row["auction"]
        if row["kind"] == "input":
            inputs += 1
            first_emission[auction] = min(first_emission.get(auction, epoch), epoch)
        elif row["kind"] == "output":
            outputs += 1
            pair = auction, row["person"]
            first_result[pair] = min(first_result.get(pair, epoch), epoch)
    matched = []
    windows = defaultdict(list)
    unmatched = 0
    for (auction, _), result in first_result.items():
        emission = first_emission.get(auction)
        if emission is None or emission > result:
            unmatched += 1
            continue
        latency = result - emission
        matched.append(latency)
        windows[(result - start) // WINDOW_MS].append(latency)
    rows = []
    for index, window in enumerate(range(start, cutoff, WINDOW_MS)):
        observations = windows[index]
        rows.append({
            "timestamp_epoch": window / 1000,
            **{f"auction_to_result_{key}": value for key, value in quantiles(observations).items()},
        })
    return rows, {
        "input_observations": inputs,
        "output_observations": outputs,
        "unique_auction_emissions": len(first_emission),
        "unique_results": len(first_result),
        "matched_results": len(matched),
        "unmatched_results": unmatched,
        "window_ms": WINDOW_MS,
        "pairs_per_window": [len(windows[index]) for index in range(len(rows))],
        **quantiles(matched),
    }


def convert(lab, output):
    spec = json.loads((lab / "execution-spec.json").read_text())
    capture = json.loads((lab / "scout/q8-record-latency-capture.json").read_text())
    if capture["run"] != spec["run"]:
        raise ValueError("latency capture belongs to a different trial")
    with (lab / "scout/q8-record-latency.jsonl").open() as raw:
        rows, summary = score(
            map(json.loads, raw),
            spec["prefix_or_timed_release_ms"],
            spec["run_deadline_ms"],
        )
    index = {"run_id": spec["run"], "environment": spec["worker"],
             "autoscaler": "preallocation" if spec["arm"] == "true" else "immediate"}
    query = output / "nexmark/q8"
    trial = query / "_".join(index.values())
    trial.mkdir(parents=True, exist_ok=False)
    with (query / "runs.csv").open("x", newline="") as file:
        writer = csv.DictWriter(file, [*index, "run_commit"])
        writer.writeheader()
        writer.writerow({**index, "run_commit": spec["source"][:8]})
    with (trial / "samples.csv").open("x", newline="") as file:
        writer = csv.DictWriter(file, rows[0])
        writer.writeheader()
        for row in rows:
            writer.writerow({key: value if value is not None else "nan" for key, value in row.items()})
    summary.update(run=spec["run"], pods=capture["pods"], listing_errors=capture["listing_errors"])
    (trial / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lab", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(convert(args.lab, args.output), indent=2))
