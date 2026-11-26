#!/usr/bin/env python3
"""Score manual native decisions against scheduler events and compatible task counters."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
from statistics import median


EVENT = re.compile(
    r"NATIVE_RESCALE_(\w+) jobId=([0-9a-f]{32}) generation=(\d+) timestampMs=(\d+)"
)


def records(path):
    with path.open() as file:
        return [json.loads(line) for line in file if line.strip()]


def native_events(path, job):
    generations = defaultdict(dict)
    with path.open(errors="replace") as file:
        for line in file:
            match = EVENT.search(line)
            if not match or match[2] != job:
                continue
            event, _, generation, timestamp = match.groups()
            generations[int(generation)].setdefault(event, []).append(int(timestamp))
    return generations


def task_samples(telemetry, vertex):
    for row in telemetry:
        response = row["responses"].get(vertex, {})
        subtasks = (response.get("body") or {}).get("subtasks", [])
        roster = []
        count = 0
        for task in subtasks:
            counters = task["metrics"]
            if (task["status"] != "RUNNING" or not counters.get("read-records-complete")
                    or counters.get("read-records") is None):
                break
            roster.append((task["subtask"], task["attempt"], task["start-time"]))
            count += counters["read-records"]
        else:
            if roster:
                yield response["ended_ns"] / 1_000_000, tuple(sorted(roster)), count


def rate_intervals(samples, max_gap):
    for previous, current in zip(samples, samples[1:]):
        start, old_roster, old_count = previous
        end, roster, count = current
        seconds = (end - start) / 1000
        if roster == old_roster and 0 < seconds <= max_gap and count >= old_count:
            yield start, end, roster, (count - old_count) / seconds


def difference(start, end):
    return None if start is None or end is None or end < start else (end - start) / 1000


def recovery(intervals, threshold, duration=10):
    streak_start = last_end = None
    for start, end, _, rate in intervals:
        if rate < threshold:
            streak_start = last_end = None
            continue
        if start != last_end:
            streak_start = start
        last_end = end
        if difference(streak_start, end) >= duration:
            return streak_start
    return None


def score_decision(decision, generations, intervals):
    response = decision["response"]
    decision_ms = response["started_ns"] / 1_000_000
    ack_ms = response["ended_ns"] / 1_000_000
    matches = [
        (generation, events) for generation, events in generations.items()
        if any(decision_ms <= timestamp <= ack_ms
               for timestamp in events.get("REQUEST_RECEIVED", []))
    ]
    row = {
        "scheduled_offset_seconds": decision["scheduled_offset_seconds"],
        "decision_epoch_ms": decision_ms,
        "acknowledgement_status": response["status"],
        "acknowledgement_seconds": difference(decision_ms, ack_ms),
    }
    if len(matches) != 1:
        return {**row, "generation": None, "result": "request_event_unmatched"}
    generation, events = matches[0]

    def first(event):
        return next(iter(events.get(event, [])), None)

    preparation_start = first("PREPARATION_STARTED")
    restart = first("RESTART_START")
    running = first("ALL_TARGET_TASKS_RUNNING")
    ready = first("PREPARATION_READY")
    old_terminated = first("OLD_TASKS_TERMINATED")
    graph_created = first("REPLACEMENT_GRAPH_CREATED")
    reference = [row for row in intervals if decision_ms - 30_000 <= row[0]
                 and row[1] <= decision_ms]
    before_restart = [row for row in intervals
                      if restart is not None and row[1] <= restart and row[3] > 0]
    replacement = [row for row in intervals
                   if restart is not None and row[0] >= restart
                   and all(started >= restart for _, _, started in row[2])]
    first_positive = next((row for row in replacement if row[3] > 0), None)
    recovery_time = None
    if sum((end - start) / 1000 for start, end, _, _ in reference) >= 24:
        threshold = 0.9 * median(row[3] for row in reference)
        if threshold > 0:
            recovery_time = recovery(replacement, threshold)
    progress_bounds = list(first_positive[:2]) if first_positive else None
    outcome = ("applied" if running is not None else
               "deferred" if first("PREPARATION_DEFERRED") is not None else
               "superseded" if first("PREPARATION_SUPERSEDED") is not None else
               "incomplete")
    return {
        **row,
        "generation": generation,
        "result": outcome,
        "stage_timestamps_ms": events,
        "preparation_seconds": difference(preparation_start, first("PREPARATION_COMMITTED")),
        "old_graph_positive_intervals_during_preparation": sum(
            1 for start, end, _, rate in intervals
            if preparation_start is not None and restart is not None
            and preparation_start <= start < end <= restart and rate > 0
        ),
        "time_to_ready_seconds": difference(first("RESOURCES_DECLARED"), ready),
        "decision_to_all_running_seconds": difference(decision_ms, running),
        "old_task_termination_seconds": difference(restart, old_terminated),
        "replacement_graph_creation_seconds": difference(first("GRAPH_CREATE_REQUESTED"), graph_created),
        "checkpoint_restore_seconds": difference(first("CHECKPOINT_RESTORE_START"),
                                                 first("CHECKPOINT_RESTORE_END")),
        "first_observed_replacement_progress_bounds_ms": progress_bounds,
        "execution_interruption_upper_seconds": difference(restart, first_positive[1]) if first_positive else None,
        "observed_positive_progress_gap_seconds": difference(before_restart[-1][1], first_positive[1])
        if before_restart and first_positive else None,
        "sustained_throughput_recovery_seconds": difference(restart, recovery_time),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observer_directory", type=Path)
    parser.add_argument("jobmanager_log", type=Path)
    args = parser.parse_args()
    directory = args.observer_directory
    run = json.loads((directory / "run.json").read_text())
    events = native_events(args.jobmanager_log, run["job"])
    samples = list(task_samples(records(directory / "telemetry.jsonl"), run["stateful_vertex"]))
    intervals = list(rate_intervals(samples, 2.5 * run["sample_interval_seconds"]))
    output = {
        "job": run["job"], "revision": run["revision"],
        "stateful_attempt_samples": len(samples), "comparable_counter_intervals": len(intervals),
        "decisions": [score_decision(decision, events, intervals)
                      for decision in records(directory / "decisions.jsonl")],
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
