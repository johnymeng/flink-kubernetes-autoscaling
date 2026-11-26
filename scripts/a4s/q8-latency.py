#!/usr/bin/env python3
"""Capture and combine the opt-in DataStream Q8 sink latency gauges."""

import argparse
import json
import re
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


METRIC = "q8AuctionLatencyV1"
BIN_MS = 10
OVERFLOW_AT_MS = 120_000
OVERFLOW_BIN = OVERFLOW_AT_MS // BIN_MS


def capture(rest_url, job, vertex):
    if any(not re.fullmatch(r"[0-9a-f]{32}", value) for value in (job, vertex)):
        raise ValueError("Job and vertex IDs must be 32 lowercase hexadecimal digits")

    def fetch(path):
        with urlopen(rest_url.rstrip("/") + path, timeout=5) as response:
            return json.load(response)

    def roster():
        details = fetch(vertex_path)
        if details["id"] != vertex or details["parallelism"] < 1:
            raise ValueError("Unexpected vertex details")
        subtasks = details["subtasks"]
        if (
            any(
                task["status"] != "RUNNING" or task.get("other-concurrent-attempts")
                or task["start-time"] <= 0 or task["attempt"] < 0
                or not task["taskmanager-id"] or task["taskmanager-id"] == "(unassigned)"
                for task in subtasks
            )
            or sorted(task["subtask"] for task in subtasks) != list(range(details["parallelism"]))
        ):
            raise ValueError("Q8 latency requires a complete running sink roster")
        return {
            str(task["subtask"]): [task["attempt"], task["start-time"], task["taskmanager-id"]]
            for task in subtasks
        }

    vertex_path = f"/jobs/{job}/vertices/{vertex}"
    metrics_path = vertex_path + "/metrics"
    before = roster()
    ids = [metric["id"] for metric in fetch(metrics_path) if metric["id"].endswith("." + METRIC)]
    if not ids:
        raise ValueError("The Q8 latency gauge is unavailable for this vertex")
    response = fetch(metrics_path + "?" + urlencode({"get": ",".join(ids)}))
    observed = {}
    for metric in response:
        task = metric["id"].partition(".")[0]
        if metric["id"] not in ids or task in observed or task not in before:
            raise ValueError("Ambiguous Q8 latency gauge identity")
        value = json.loads(metric["value"])
        validate_histogram(value, before[task][0])
        observed[task] = value
    observed_at = time.time_ns() // 1_000_000
    if before != roster() or observed.keys() != before.keys():
        raise ValueError("Q8 sink attempt changed or gauge coverage was incomplete")
    return {
        "job": job, "vertex": vertex, "observedAtMs": observed_at,
        "roster": before, "histograms": observed,
    }


def validate_histogram(value, attempt):
    if (
        type(value["attempt"]) is not int or value["attempt"] != attempt
        or value["binMs"] != BIN_MS or value["overflowAtMs"] != OVERFLOW_AT_MS
        or type(value["invalid"]) is not int or value["invalid"] < 0
    ):
        raise ValueError("Q8 latency gauge does not match the running attempt or v1 layout")
    for bin_id, count in value["counts"].items():
        bin_index = int(bin_id)
        if (str(bin_index) != bin_id or not 0 <= bin_index <= OVERFLOW_BIN
                or type(count) is not int or count <= 0):
            raise ValueError("Invalid Q8 latency bucket")


def summarize(before, after):
    if (any(before[key] != after[key] for key in ("job", "vertex", "roster"))
            or not before["roster"] or before["observedAtMs"] >= after["observedAtMs"]):
        raise ValueError("The two Q8 snapshots must cover one ordered, unchanged sink roster")
    if any(snapshot["histograms"].keys() != before["roster"].keys() for snapshot in (before, after)):
        raise ValueError("The two snapshots must cover every running subtask")

    totals = Counter()
    invalid = 0
    for task, (attempt, _, _) in before["roster"].items():
        old, new = (snapshot["histograms"][task] for snapshot in (before, after))
        for value in (old, new):
            validate_histogram(value, attempt)
        if new["invalid"] < old["invalid"]:
            raise ValueError("The Q8 clock error counter reset")
        invalid += new["invalid"] - old["invalid"]
        bins = old["counts"] | new["counts"]
        for bin_id in bins:
            delta = new["counts"].get(bin_id, 0) - old["counts"].get(bin_id, 0)
            if delta < 0:
                raise ValueError("The Q8 latency histogram reset")
            totals[int(bin_id)] += delta

    count = sum(totals.values())

    def percentile(p):
        if not count:
            return None
        rank = (count * p + 99) // 100
        for index, population in sorted(totals.items()):
            rank -= population
            if rank <= 0:
                return {
                    "lowerMs": index * BIN_MS,
                    "upperMs": None if index == OVERFLOW_BIN else (index + 1) * BIN_MS - 1,
                }
        raise AssertionError("The latency rank exceeds the recorded sample count")

    return {
        "samples": count, "invalidClockSamples": invalid, "overflowSamples": totals[OVERFLOW_BIN],
        "p50": percentile(50), "p95": percentile(95), "p99": percentile(99),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    capture_command = commands.add_parser("capture")
    capture_command.add_argument("rest_url")
    capture_command.add_argument("job_id")
    capture_command.add_argument("vertex_id")
    summarize_command = commands.add_parser("summarize")
    summarize_command.add_argument("before", type=Path)
    summarize_command.add_argument("after", type=Path)
    args = parser.parse_args()
    try:
        if args.mode == "capture":
            result = capture(args.rest_url, args.job_id, args.vertex_id)
        else:
            result = summarize(json.loads(args.before.read_text()), json.loads(args.after.read_text()))
    except (KeyError, OSError, TypeError, ValueError) as error:
        parser.exit(2, f"Q8 latency measurement failed: {error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
