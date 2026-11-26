#!/usr/bin/env python3
"""Capture and combine the opt-in Q8 and Q20 sink latency gauges."""

import argparse
import json
import re
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


METRIC = "q8AuctionLatencyV1"
Q20_METRIC = "q20KafkaLatencyV2"
BUCKET_LAYOUTS = {
    METRIC: ((120_000, 10),),
    "q20KafkaLatencyV1": ((900_000, 10),),
    Q20_METRIC: ((2_000, 10), (60_000, 100), (900_000, 1_000)),
}


def bucket_segments(layout):
    start = 0
    for end, width in layout:
        yield start, width, (end - start) // width
        start = end


def capture(rest_url, job, vertex, gauge=METRIC):
    layout = BUCKET_LAYOUTS[gauge]
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
            raise ValueError("Latency capture requires a complete running sink roster")
        return {
            str(task["subtask"]): [task["attempt"], task["start-time"], task["taskmanager-id"]]
            for task in subtasks
        }

    vertex_path = f"/jobs/{job}/vertices/{vertex}"
    metrics_path = vertex_path + "/metrics"
    before = roster()
    ids = [metric["id"] for metric in fetch(metrics_path) if metric["id"].endswith("." + gauge)]
    if not ids:
        raise ValueError(f"The latency gauge {gauge} is unavailable for this vertex")
    response = fetch(metrics_path + "?" + urlencode({"get": ",".join(ids)}))
    observed = {}
    for metric in response:
        task = metric["id"].partition(".")[0]
        if metric["id"] not in ids or task in observed or task not in before:
            raise ValueError("Ambiguous latency gauge identity")
        value = json.loads(metric["value"])
        validate_histogram(value, before[task][0], layout)
        observed[task] = value
    observed_at = time.time_ns() // 1_000_000
    if before != roster() or observed.keys() != before.keys():
        raise ValueError("Sink attempt changed or gauge coverage was incomplete")
    result = {
        "job": job, "vertex": vertex, "observedAtMs": observed_at,
        "roster": before, "histograms": observed,
    }
    if gauge != METRIC:
        result["metric"] = gauge
    return result


def validate_histogram(value, attempt, layout):
    expected_layout = ({"binMs": layout[0][1]} if len(layout) == 1
                       else {"segments": [list(row) for row in layout]})
    if (
        type(value["attempt"]) is not int or value["attempt"] != attempt
        or value["overflowAtMs"] != layout[-1][0]
        or any(value.get(key) != expected for key, expected in expected_layout.items())
        or type(value["invalid"]) is not int or value["invalid"] < 0
    ):
        raise ValueError("Latency gauge does not match the running attempt or expected layout")
    overflow_bin = sum(count for _, _, count in bucket_segments(layout))
    for bin_id, count in value["counts"].items():
        bin_index = int(bin_id)
        if (str(bin_index) != bin_id or not 0 <= bin_index <= overflow_bin
                or type(count) is not int or count <= 0):
            raise ValueError("Invalid latency bucket")


def summarize(before, after):
    gauge = before.get("metric", METRIC)
    if gauge not in BUCKET_LAYOUTS or after.get("metric", METRIC) != gauge:
        raise ValueError("The latency snapshots must describe the same supported gauge")
    layout = BUCKET_LAYOUTS[gauge]
    overflow_bin = sum(count for _, _, count in bucket_segments(layout))
    if (any(before[key] != after[key] for key in ("job", "vertex", "roster"))
            or not before["roster"] or before["observedAtMs"] >= after["observedAtMs"]):
        raise ValueError("The two snapshots must cover one ordered, unchanged sink roster")
    if any(snapshot["histograms"].keys() != before["roster"].keys() for snapshot in (before, after)):
        raise ValueError("The two snapshots must cover every running subtask")

    totals = Counter()
    invalid = 0
    for task, (attempt, _, _) in before["roster"].items():
        old, new = (snapshot["histograms"][task] for snapshot in (before, after))
        for value in (old, new):
            validate_histogram(value, attempt, layout)
        if new["invalid"] < old["invalid"]:
            raise ValueError("The clock error counter reset")
        invalid += new["invalid"] - old["invalid"]
        bins = old["counts"] | new["counts"]
        for bin_id in bins:
            delta = new["counts"].get(bin_id, 0) - old["counts"].get(bin_id, 0)
            if delta < 0:
                raise ValueError("The latency histogram reset")
            totals[int(bin_id)] += delta

    count = sum(totals.values())

    def percentile(p):
        if not count:
            return None
        rank = (count * p + 99) // 100
        for index, population in sorted(totals.items()):
            rank -= population
            if rank <= 0:
                for start, width, bins in bucket_segments(layout):
                    if index < bins:
                        lower = start + index * width
                        return {"lowerMs": lower, "upperMs": lower + width - 1}
                    index -= bins
                return {"lowerMs": layout[-1][0], "upperMs": None}
        raise AssertionError("The latency rank exceeds the recorded sample count")

    return {
        "samples": count, "invalidClockSamples": invalid, "overflowSamples": totals[overflow_bin],
        "p50": percentile(50), "p95": percentile(95), "p99": percentile(99),
    }


def sample_times(snapshot):
    times = {task: value.get("sampledAtMs") for task, value in snapshot["histograms"].items()}
    if not times or any(type(stamp) is not int or stamp <= 0 for stamp in times.values()):
        raise ValueError("The latency gauge must provide TaskManager sample times")
    return times


def timed_interval(before, after):
    """Describe an interval only when every sink gauge has advanced."""
    if any(before[key] != after[key] for key in ("job", "vertex", "roster", "metric")):
        raise ValueError("The latency capture changed identity or task attempts")
    starts, ends = sample_times(before), sample_times(after)
    if starts.keys() != ends.keys() or starts.keys() != before["roster"].keys():
        raise ValueError("The sample times must cover the running sink roster")
    for task, start in starts.items():
        if ends[task] < start:
            raise ValueError("A TaskManager sample time regressed")
        if ends[task] == start and before["histograms"][task] != after["histograms"][task]:
            raise ValueError("A latency gauge changed at the same sample time")
    if any(starts[task] == ends[task] for task in starts):
        return None
    return {
        "event": "interval", "job": after["job"], "vertex": after["vertex"], "metric": after["metric"],
        "roster": after["roster"], "subtaskIntervalsMs": {task: [start, ends[task]] for task, start in starts.items()},
        "observedAtMs": after["observedAtMs"], "latency": summarize(before, after),
    }


def capture_series(rest_url, job, vertex, duration_sec, interval_sec):
    deadline = time.monotonic() + duration_sec
    baseline = None
    while (started := time.monotonic()) < deadline:
        try:
            current = capture(rest_url, job, vertex, Q20_METRIC)
            times = sample_times(current)
            if baseline is None or current["roster"] != baseline["roster"]:
                baseline = current
                yield {
                    "event": "baseline", "job": job, "vertex": vertex, "metric": Q20_METRIC,
                    "roster": current["roster"], "sampledAtMs": times, "observedAtMs": current["observedAtMs"],
                }
            elif (interval := timed_interval(baseline, current)) is not None:
                baseline = current
                yield interval
        except (KeyError, OSError, TypeError, ValueError) as error:
            baseline = None
            yield {"event": "gap", "job": job, "vertex": vertex, "metric": Q20_METRIC,
                   "observedAtMs": time.time_ns() // 1_000_000, "error": str(error)}
        time.sleep(max(0, min(deadline, started + interval_sec) - time.monotonic()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    capture_command = commands.add_parser("capture")
    capture_command.add_argument("rest_url")
    capture_command.add_argument("job_id")
    capture_command.add_argument("vertex_id")
    capture_command.add_argument("--metric", choices=BUCKET_LAYOUTS, default=METRIC)
    series_command = commands.add_parser("capture-series", help="record time-bounded Q20 latency intervals")
    series_command.add_argument("rest_url")
    series_command.add_argument("job_id")
    series_command.add_argument("vertex_id")
    series_command.add_argument("--duration-sec", required=True, type=int)
    series_command.add_argument("--interval-sec", default=5, type=int)
    summarize_command = commands.add_parser("summarize")
    summarize_command.add_argument("before", type=Path)
    summarize_command.add_argument("after", type=Path)
    args = parser.parse_args()
    if args.mode == "capture-series":
        if args.duration_sec < 1 or args.interval_sec < 1:
            parser.error("latency capture duration and interval must be positive seconds")
        if any(not re.fullmatch(r"[0-9a-f]{32}", value) for value in (args.job_id, args.vertex_id)):
            parser.error("job and vertex IDs must be 32 lowercase hexadecimal digits")
        has_interval = False
        for event in capture_series(args.rest_url, args.job_id, args.vertex_id, args.duration_sec, args.interval_sec):
            print(json.dumps(event, sort_keys=True), flush=True)
            has_interval |= event["event"] == "interval"
        if not has_interval:
            parser.exit(2, "No timestamped latency intervals were recorded\n")
        return
    try:
        if args.mode == "capture":
            result = capture(args.rest_url, args.job_id, args.vertex_id, args.metric)
        else:
            result = summarize(json.loads(args.before.read_text()), json.loads(args.after.read_text()))
    except (KeyError, OSError, TypeError, ValueError) as error:
        parser.exit(2, f"Latency measurement failed: {error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
