#!/usr/bin/env python3
"""Summarize pod-request time, stateful phase overhead, and DataStream backlog."""

import argparse
import json
from pathlib import Path
import re
from statistics import median


MEMORY_MULTIPLIERS = {
    "": 1, "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3,
    "k": 1000, "M": 1000**2, "G": 1000**3,
}


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def cores(quantity):
    return float(quantity[:-1]) / 1000 if quantity.endswith("m") else float(quantity)


def gibibytes(quantity):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|k|M|G)?", quantity)
    if match is None:
        raise ValueError(f"unsupported memory quantity {quantity!r}")
    return float(match[1]) * MEMORY_MULTIPLIERS[match[2] or ""] / 1024**3


def pod_samples(rows, worker):
    for sample in rows:
        if "pods" not in sample:
            continue
        cpu = memory = taskmanagers = 0
        for pod in sample["pods"]:
            if pod["node"] != worker or pod["phase"] in ("Succeeded", "Failed"):
                continue
            containers = [
                container for container in pod["containers"]
                if container["name"] == "flink-main-container" and container.get("requirements")
            ]
            if containers and pod["name"].startswith("flink-taskmanager-"):
                taskmanagers += int(pod["phase"] == "Running")
            for container in containers:
                cpu += cores(container["requirements"]["cpu"])
                memory += gibibytes(container["requirements"]["memory"])
        yield sample["captured_ms"], cpu, memory, taskmanagers


def request_time(samples, start, end, reference=(0, 0)):
    cpu_seconds = memory_seconds = covered = 0
    for before, after in zip(samples, samples[1:]):
        if after[0] - before[0] > 14_000:
            continue
        seconds = (min(after[0], end) - max(before[0], start)) / 1000
        if seconds <= 0:
            continue
        covered += seconds
        cpu_seconds += max(0, before[1] - reference[0]) * seconds
        memory_seconds += max(0, before[2] - reference[1]) * seconds
    return {"covered_seconds": covered, "cpu_core_seconds": cpu_seconds if covered else None,
            "memory_gib_seconds": memory_seconds if covered else None}


def backlog_samples(sampler):
    for sample in sampler:
        if sample.get("type") != "sample":
            continue
        sources = sample.get("sources", [])
        counts = []
        for source in sources:
            counters = [
                int(row["value"]) for row in source.get("values", [])
                if row["id"].endswith(".pendingRecords") and re.fullmatch(r"[0-9]+", row["value"])
            ]
            if len(counters) != 2:
                break
            counts.extend(counters)
        else:
            if len(sources) == 2:
                yield sample["capture_epoch"] * 1000, sum(counts)


def backlog_result(samples, start, end):
    pre = [count for time, count in samples if start - 30_000 <= time < start]
    selected = [(time, count) for time, count in samples if start <= time < end]
    if not selected:
        return {"peak_records": None, "clearance_seconds": None}
    peak_time, peak = max(selected, key=lambda item: item[1])
    threshold = 1.1 * median(pre) if len(pre) >= 4 else None
    cleared = None
    if threshold is not None:
        after_peak = [(time, count) for time, count in selected if time > peak_time]
        for left, middle, right in zip(after_peak, after_peak[1:], after_peak[2:]):
            if (right[0] - left[0] >= 9000
                    and max(middle[0] - left[0], right[0] - middle[0]) <= 8000
                    and max(left[1], middle[1], right[1]) <= threshold):
                cleared = (left[0] - start) / 1000
                break
    return {"peak_records": peak, "predecision_clearance_threshold_records": threshold,
            "clearance_seconds": cleared, "last_observed_records": selected[-1][1],
            "samples": len(selected)}


def jvm_summary(rows):
    samples = [
        (row["captured_ms"], {
            worker: {metric["id"]: float(metric["value"]) for metric in metrics}
            for worker, metrics in row["workers"].items() if metrics
        })
        for row in rows if row.get("workers")
    ]
    cpu_time = "Status.JVM.CPU.Time"
    heap = "Status.JVM.Memory.Heap.Used"
    nonheap = "Status.JVM.Memory.NonHeap.Used"
    cpu_seconds = covered = 0
    for (start, old), (end, new) in zip(samples, samples[1:]):
        seconds = (end - start) / 1000
        if not 0 < seconds <= 14:
            continue
        deltas = [
            new[worker][cpu_time] - old[worker][cpu_time]
            for worker in old.keys() & new.keys()
            if 0 <= old[worker].get(cpu_time, -1) <= new[worker].get(cpu_time, -1)
        ]
        if deltas:
            cpu_seconds += sum(deltas) / 1_000_000_000
            covered += seconds
    memory = [
        sum(metrics[heap] + metrics[nonheap] for metrics in workers.values())
        for _, workers in samples if workers
        and all(heap in values and nonheap in values for values in workers.values())
    ]
    return {
        "comparable_cpu_counter_seconds": covered,
        "observed_taskmanager_jvm_cpu_core_seconds": cpu_seconds if covered else None,
        "peak_observed_taskmanager_jvm_heap_and_nonheap_bytes": max(memory, default=None),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lab_receipt", type=Path)
    parser.add_argument("timings", type=Path)
    args = parser.parse_args()
    directory = args.lab_receipt
    run = json.loads((directory / "execution-spec.json").read_bytes())
    cluster = read_lines(directory / "cluster-resources.jsonl")
    pods = list(pod_samples(cluster, run["worker"]))
    sampler = read_lines(directory / "scout/resource-series.jsonl")
    backlog = list(backlog_samples(sampler))
    decisions = json.loads(args.timings.read_bytes())["decisions"]
    release = run["prefix_or_timed_release_ms"]
    cutoff = run["run_deadline_ms"]
    for decision, following in zip(decisions, decisions[1:] + [None]):
        start = decision["decision_epoch_ms"]
        end = min(cutoff, following["decision_epoch_ms"]) if following else cutoff
        decision["datastream_pending_records"] = backlog_result(backlog, start, end)
        timestamps = decision.get("stage_timestamps_ms", {})
        preparation = timestamps.get("PREPARATION_STARTED", [])
        restart = timestamps.get("RESTART_START", [])
        if preparation and restart and preparation[0] < restart[0]:
            previous = [row for row in pods if row[0] <= preparation[0]]
            if previous and preparation[0] - previous[-1][0] <= 14_000:
                decision["extra_requests_during_preparation"] = request_time(
                    pods, preparation[0], restart[0], previous[-1][1:3])
    summary = {
        "run": run["run"],
        "configured_offered_records_per_second": 80_000,
        "scheduled_flink_pod_requests": request_time(pods, release, cutoff),
        "peak_requested_cpu_cores": max((row[1] for row in pods), default=None),
        "peak_requested_memory_gib": max((row[2] for row in pods), default=None),
        "peak_running_taskmanager_pods": max((row[3] for row in pods), default=None),
        "container_utilization_samples": sum(bool(row.get("utilization_available")) for row in cluster),
        "datastream_backlog_samples": len(backlog),
        "end_to_end_latency_percentiles": None,
        "decisions": decisions,
    }
    jvm_path = directory / "jvm-resources.jsonl"
    if jvm_path.exists():
        summary["taskmanager_jvms"] = jvm_summary(read_lines(jvm_path))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
