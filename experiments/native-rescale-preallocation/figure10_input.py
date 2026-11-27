#!/usr/bin/env python3
"""Adapt one measured Q8 arm to the supplied Figure 10 renderer's input schema."""

import argparse
from bisect import bisect_right
import csv
from hashlib import sha256
import json
from pathlib import Path

from score_resources import backlog_samples, pod_samples, read_lines


COLUMNS = {
    "source_records_per_second": "source_records_per_second",
    "total_managed_memory_used_bytes": "managed_memory_used_bytes",
    "total_managed_memory_total_bytes": "managed_memory_allocated_bytes",
    "task_slots_used": "task_slots_used",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lab_receipt", type=Path)
    parser.add_argument("results_root", type=Path)
    parser.add_argument("--supplemental", action="store_true",
                        help="Include observed pod requests, TaskManagers, and DataStream backlog.")
    args = parser.parse_args()
    run = json.loads((args.lab_receipt / "execution-spec.json").read_text())
    raw = (args.lab_receipt / "scout/resource-series.jsonl").read_bytes()
    index = {
        "run_id": run["run"],
        "environment": run["worker"],
        "autoscaler": "preallocation" if run["arm"] == "true" else "immediate",
        "run_commit": run["source"][:8],
    }
    window = run["collection_seconds"]
    raw_rows = list(map(json.loads, raw.splitlines()))
    samples = [
        row for row in raw_rows
        if row["type"] == "sample" and 0 <= row["scheduled_elapsed_sec"] <= window
    ]
    if not samples or any(row["job_id"] != run["job_id"] for row in samples):
        raise ValueError("the sampler has no data or contains data for a different job")
    offsets = [row["scheduled_elapsed_sec"] for row in samples]
    if offsets != sorted(set(offsets)):
        raise ValueError("sample times are inconsistent")

    query = args.results_root / "nexmark/q8"
    output = query / "_".join(index[key] for key in ("run_id", "environment", "autoscaler"))
    output.mkdir(parents=True, exist_ok=False)
    runs_file = query / "runs.csv"
    new_index = not runs_file.exists()
    with runs_file.open("a", newline="") as file:
        writer = csv.DictWriter(file, index)
        if new_index:
            writer.writeheader()
        writer.writerow(index)
    measurements = [
        {column: row["values"].get(key) for column, key in COLUMNS.items()} for row in samples
    ]
    if args.supplemental:
        pod_rows = list(pod_samples(read_lines(args.lab_receipt / "cluster-resources.jsonl"),
                                    run["worker"]))
        pod_times = [row[0] for row in pod_rows]
        backlog = dict(backlog_samples(raw_rows))
        for row, values in zip(samples, measurements):
            instant = row["capture_epoch"] * 1000
            index = bisect_right(pod_times, instant) - 1
            cpu = memory = taskmanagers = None
            if index >= 0 and instant - pod_times[index] <= 14_000:
                _, cpu, memory, taskmanagers = pod_rows[index]
            values.update(
                datastream_pending_records=backlog.get(instant),
                scheduled_flink_cpu_cores=cpu,
                scheduled_flink_memory_gib=memory,
                running_taskmanager_pods=taskmanagers,
            )
    columns = list(measurements[0])
    with (output / "samples.csv").open("x", newline="") as file:
        writer = csv.DictWriter(file, ["timestamp_epoch", *columns])
        writer.writeheader()
        for row, values in zip(samples, measurements):
            writer.writerow({
                "timestamp_epoch": row["capture_epoch"],
                **{column: "nan" if value is None else value for column, value in values.items()},
            })
    (output / "provenance.json").write_text(json.dumps({
        "resource_series_sha256": sha256(raw).hexdigest(),
        "sample_count": len(samples),
        "scheduled_offsets_seconds": offsets,
        "input_release_epoch_ms": run["prefix_or_timed_release_ms"],
        "cutoff_epoch_ms": run["run_deadline_ms"],
        "missing_values": {column: sum(row[column] is None for row in measurements)
                           for column in columns},
    }, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
