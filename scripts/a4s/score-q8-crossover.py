#!/usr/bin/env python3
"""Score complete observation periods from the fixed-allocation Q8 crossover."""

import argparse
import json
import runpy
from datetime import datetime
from pathlib import Path
from statistics import median


def read(path):
    return json.loads(path.read_text())


def cumulative_count(snapshot, roles, field):
    tasks = [task for role in roles for task in snapshot["vertices"][role]["subtasks"]]
    assert tasks and all(task["metrics"][field + "-complete"] is True for task in tasks)
    values = [task["metrics"][field] for task in tasks]
    assert all(type(value) is int and value >= 0 for value in values)
    return sum(values)


def select_metrics(snapshot, roles, suffix):
    return [int(metric["value"]) for role in roles for metric in snapshot["cacheAndBacklog"][role]
            if metric["id"].replace("_", "-").endswith(suffix) and str(metric["value"]).isdigit()]


def read_runner(path):
    for line in path.read_text().splitlines():
        stamp, separator, body = line.partition(" ")
        if separator and body.startswith("{"):
            event = json.loads(body)
            if event.get("route") == "/v1/evaluate" and event.get("event") == "request":
                yield int(datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp() * 1000), event


def score(bundle, latency_script):
    arm = bundle / "scout"
    paths = sorted((arm / "crossover").glob("[0-9][0-9]-*"))
    events = list(read_runner(arm / "runner.log"))
    spec = read(bundle / "execution-spec.json")
    assert (arm / "crossover/complete").is_file() and len(paths) == len(spec["crossover_order"])
    merge_latency = runpy.run_path(str(latency_script))["summarize"]
    decisions, all_periods = set(), []
    for mode, path in zip(spec["crossover_order"], paths):
        planned = read(path / "planned.json")
        assert planned["mode"] == mode
        before, after = (read(path / f"{name}.json") for name in ("before", "after"))
        start, finish = (sample["latency"]["observedAtMs"] for sample in (before, after))
        assert planned["observeFromMs"] <= start < finish and planned["observeToMs"] <= finish
        assert finish < planned["observeToMs"] + 5000
        assert before["profiles"] == after["profiles"]
        for frame in (before, after):
            assert frame["overview"]["slots-total"] == 8 and frame["overview"]["slots-available"] == 3
        elapsed = (finish - start) / 1000

        def counter_rate(roles, field):
            records = cumulative_count(after, roles, field) - cumulative_count(before, roles, field)
            assert records >= 0
            return records / elapsed

        def metric_delta(roles, suffix, expected):
            values = [select_metrics(frame, roles, suffix) for frame in (before, after)]
            if any(len(v) != expected for v in values):
                return None
            difference = sum(values[1]) - sum(values[0])
            return difference if difference >= 0 else None

        sink_outputs = metric_delta(["join"], ".TumblingEventTimeWindows.numRecordsOut", 1)
        hits = metric_delta(["join"], "block-cache-hit", 1)
        misses = metric_delta(["join"], "block-cache-miss", 1)
        actual_cache = select_metrics(after, ["join"], "block-cache-capacity")
        backlog = [select_metrics(frame, ["person", "auction"], ".q8Backlog") for frame in (before, after)]
        record = [json.loads(line) for line in (path / "a4s-requests.jsonl").read_text().splitlines()]
        during = [item for item in record if start <= item["request"]["evaluatedAtMillis"] <= finish]
        assert during and all(row["recommendation"] is not None for row in during)
        decisions.update(item["request"]["snapshot"]["executionGeneration"] for item in during)
        routed = [event for stamp, event in events if start <= stamp <= finish]
        assert bool(routed) == (mode == "remote") and all(event["status"] == 200 for event in routed)
        latency_summary = merge_latency(before["latency"], after["latency"])
        assert latency_summary == read(path / "latency-summary.json")
        all_periods.append({
            "period": path.name, "mode": mode, "measuredSeconds": elapsed,
            "joinInputRecordsPerSecond": counter_rate(["join"], "read-records"),
            "sourceOutputRecordsPerSecond": counter_rate(["person", "auction"], "write-records"),
            "joinOutputRecordsPerSecond": None if sink_outputs is None else sink_outputs / elapsed,
            "latency": latency_summary, "backlogChange": sum(backlog[1]) - sum(backlog[0])
            if all(len(v) == 4 for v in backlog) else None,
            "runnerRequests": len(routed), "recordedEvaluations": len(during),
            "cacheCapacityBytes": actual_cache[0] if actual_cache and len(set(actual_cache)) == 1 else None,
            "cacheMissFraction": misses / (hits + misses)
            if hits is not None and misses is not None and hits + misses > 0 else None,
        })

    assert len(decisions) == 1, "Multiple execution generations in measured periods"
    relative = []
    for pair in (all_periods[i:i + 2] for i in range(0, len(all_periods), 2)):
        rates = {item["mode"]: item["joinInputRecordsPerSecond"] for item in pair}
        assert len(rates) == 2 and all(rate > 0 for rate in rates.values())
        relative.append(100 * (rates["remote"] / rates["embedded"] - 1))
    return {"run": spec["run"], "periods": all_periods, "pairedRemoteJoinInputChangePct": relative,
            "medianRemoteJoinInputChangePct": median(relative), "experimentUnits": 1}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    print(json.dumps(score(args.bundle, Path(__file__).with_name("q8-latency.py")), indent=2))
