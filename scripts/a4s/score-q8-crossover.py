#!/usr/bin/env python3
"""Score complete observation periods from the fixed-allocation Q8 crossover."""

import argparse
import json
import runpy
from datetime import datetime
from math import sqrt
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


def integer_gauge(gauges, name):
    value = gauges.get(name)
    return int(value) if str(value).isdigit() else None


def join_timeline(path, spec, windows, events):
    """Read complete, same-attempt intervals from the optional ten-second capture."""
    if not path.is_file():
        return []
    release = spec["prefix_or_timed_release_ms"]
    prefix = "TumblingEventTimeWindows.column_family.window-contents.rocksdb_"
    intervals = []
    previous = None
    with path.open() as stream:
        for row in map(json.loads, stream):
            stamp = int(datetime.fromisoformat(row["started"]).timestamp() * 1000)
            job = row["responses"].get("job") or {}
            join = row["responses"].get("join") or {}
            tasks = join.get("subtasks") or []
            if (stamp < release or job.get("jid") != spec["job_id"] or job.get("state") != "RUNNING"
                    or join.get("id") != spec["vertices"]["join"] or len(tasks) != 1):
                previous = None
                continue
            task = tasks[0]
            counters = task.get("metrics") or {}
            if (task["status"] != "RUNNING" or task.get("other-concurrent-attempts")
                    or counters.get("read-records-complete") is not True):
                previous = None
                continue
            identity = (job["start-time"], task["attempt"], task["start-time"], task["taskmanager-id"])
            records = counters["read-records"]
            if previous:
                since, old_identity, old_records = previous
                if identity == old_identity and 0 < stamp - since <= 15_000 and records >= old_records:
                    data = (row["responses"].get("subtask_metrics") or {}).get("join/0") or {}
                    gauges = {m["id"]: m["value"] for m in data.get("values", [])}
                    period = next((name for start, end, name in windows if start <= since < stamp <= end), None)
                    intervals.append({
                        "fromSeconds": (since - release) / 1000, "toSeconds": (stamp - release) / 1000,
                        "period": period, "joinInputRecordsPerSecond": (records - old_records) * 1000 / (stamp - since),
                        "pendingCompactionBytesAtEnd": integer_gauge(gauges, prefix + "estimate-pending-compaction-bytes"),
                        "compactionPendingAtEnd": integer_gauge(gauges, prefix + "compaction-pending"),
                        "runnerRequests": sum(since < time <= stamp for time, _ in events),
                    })
            previous = (stamp, identity, records)
    return intervals


def pod_resources_by_period(samples, start, end):
    """Use cumulative container counters captured inside the observed interval."""
    roles = {"flink": ("flink-main-container", 3),
             "operator": ("flink-kubernetes-operator", 1), "runner": ("runner", 1)}
    result = {}
    for role, (container_name, expected_count) in roles.items():
        endpoints = {}
        peak_working_set = None
        stable_ids = None
        for sample in samples:
            if not start <= sample["observedAtMs"] <= end:
                continue
            containers = [row for row in sample.get("containers", [])
                          if row["container"] == container_name]
            if len(containers) != expected_count:
                continue
            ids = {(row["pod"]["uid"], row["startTime"]) for row in containers}
            if len(ids) != expected_count or (stable_ids is not None and stable_ids != ids):
                endpoints.clear()
                break
            stable_ids = ids
            memory = []
            for row in containers:
                identity = row["pod"]["uid"], row["startTime"]
                cpu = row.get("cpu") or {}
                memory.append((row.get("memory") or {}).get("workingSetBytes"))
                counter = cpu.get("usageCoreNanoSeconds")
                if "time" not in cpu or type(counter) is not int or counter < 0:
                    continue
                timestamp = datetime.fromisoformat(cpu["time"].replace("Z", "+00:00")).timestamp() * 1000
                if start <= timestamp <= end:
                    endpoints.setdefault(identity, {})[timestamp] = counter
            if all(type(value) is int and value >= 0 for value in memory):
                peak_working_set = max(peak_working_set or 0, sum(memory))

        observations = [sorted(values.items()) for values in endpoints.values()]
        if len(observations) != expected_count or any(
                len(series) < 2 or any(before[1] > after[1] for before, after in zip(series, series[1:]))
                for series in observations):
            result[role] = None
            continue
        durations_ms = [series[-1][0] - series[0][0] for series in observations]
        cores = sum((series[-1][1] - series[0][1]) / (duration * 1_000_000)
                    for series, duration in zip(observations, durations_ms))
        result[role] = {"meanCpuCores": cores, "minimumCpuSampleSeconds": min(durations_ms) / 1000,
                        "peakWorkingSetBytes": peak_working_set}
    return result


def running_roster(frame):
    expected = {"person": 2, "auction": 2, "join": 1}
    roster = {}
    for role, count in expected.items():
        subtasks = frame["vertices"][role]["subtasks"]
        assert sorted(task["subtask"] for task in subtasks) == list(range(count))
        assert all(task["status"] == "RUNNING" and not task.get("other-concurrent-attempts")
                   for task in subtasks)
        roster[role] = sorted((task["subtask"], task["attempt"], task["start-time"], task["taskmanager-id"])
                              for task in subtasks)
    return roster


def score_timed_latency(frames, start, end, sample_times, timed_interval):
    """Use the widest histogram interval entirely inside the measurement window."""
    eligible = []
    for index, frame in enumerate(frames):
        if index:
            timed_interval(frames[index - 1], frame)
        if all(start <= stamp <= end for stamp in sample_times(frame).values()):
            eligible.append(frame)
    if len(eligible) < 2:
        return None
    return timed_interval(eligible[0], eligible[-1])


def score(bundle, latency_script):
    arm = bundle / "scout"
    paths = sorted((arm / "crossover").glob("[0-9][0-9]-*"))
    events = list(read_runner(arm / "runner.log"))
    resources = arm / "pod-resources.jsonl"
    pod_samples = []
    if resources.is_file():
        with resources.open() as stream:
            pod_samples = list(map(json.loads, stream))
    spec = read(bundle / "execution-spec.json")
    assert (arm / "crossover/complete").is_file() and len(paths) == len(spec["crossover_order"])
    latency_tools = runpy.run_path(str(latency_script))
    merge_latency = latency_tools["summarize"]
    decisions, all_periods = set(), []
    observed_intervals = []
    admission = arm / "admission/confirmed-applied-profiles.json"
    startup_profiles = read(admission) if admission.is_file() else None
    roster = None
    for mode, path in zip(spec["crossover_order"], paths):
        planned = read(path / "planned.json")
        assert planned["mode"] == mode
        before, after = (read(path / f"{name}.json") for name in ("before", "after"))
        start, finish = (sample["latency"]["observedAtMs"] for sample in (before, after))
        assert planned["observeFromMs"] <= start < finish and planned["observeToMs"] <= finish
        assert finish < planned["observeToMs"] + 5000
        observed_intervals.append((start, finish, path.name))
        for frame in (before, after):
            if startup_profiles is None:
                startup_profiles = frame["profiles"]
            assert frame["profiles"] == startup_profiles
            current_roster = running_roster(frame)
            if roster is None:
                roster = current_roster
            assert current_roster == roster, "Task attempts changed during the crossover"
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
        summary = {
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
        }
        timed_path = path / "latency-timed-snapshots.json"
        if timed_path.is_file():
            frames = read(timed_path)
            if len(frames) < 2 or frames[0] != before["latency"] or frames[-1] != after["latency"]:
                raise ValueError(f"Latency series endpoints differ from the Q8 window {path.name}")
            summary["timestampAlignedLatency"] = score_timed_latency(
                frames, planned["observeFromMs"], planned["observeToMs"],
                latency_tools["sample_times"], latency_tools["timed_interval"],
            )
        if pod_samples:
            summary["podResources"] = pod_resources_by_period(pod_samples, start, finish)
        all_periods.append(summary)

    assert len(decisions) == 1, "Multiple execution generations in measured periods"
    relative = []
    for pair in (all_periods[i:i + 2] for i in range(0, len(all_periods), 2)):
        rates = {item["mode"]: item["joinInputRecordsPerSecond"] for item in pair}
        assert len(rates) == 2 and all(rate > 0 for rate in rates.values())
        relative.append(100 * (rates["remote"] / rates["embedded"] - 1))
    return {"run": spec["run"], "periods": all_periods, "pairedRemoteJoinInputChangePct": relative,
            "medianRemoteJoinInputChangePct": median(relative), "experimentUnits": 1,
            "joinIntervals": join_timeline(arm / "operational-snapshots.jsonl", spec, observed_intervals, events)}


def workload_configuration(bundle, run):
    deployment = read(bundle / "scout/deployment.json")["spec"]
    args = deployment["job"]["args"]
    args[args.index("--q8-release-epoch-ms") + 1] = "<release>"
    deployment["flinkConfiguration"].pop("$internal.pipeline.job-id")
    return json.dumps(deployment, sort_keys=True).replace(f"q8-pr2-{run}", "q8-pr2-<run>")


def compare_counterbalanced(first_bundle, second_bundle, latency_script):
    """Compare the same fixed allocation and treatment at opposite positions in time."""
    bundles = [first_bundle, second_bundle]
    specs = [read(bundle / "execution-spec.json") for bundle in bundles]
    first, second = specs
    if first["run"] == second["run"] or first["job_id"] == second["job_id"]:
        raise ValueError("Expected two different Q8 deployments")
    for name in ("image", "operator", "vertices", "worker", "effective_gate_values",
                 "collection_seconds", "crossover_first_offset_seconds", "crossover_period_seconds",
                 "crossover_guard_seconds", "crossover_trailing_seconds"):
        if first[name] != second[name]:
            raise ValueError(f"The two Q8 deployments differ in {name}")
    first_order, second_order = (spec["crossover_order"] for spec in specs)
    reversed_modes = {"embedded": "remote", "remote": "embedded"}
    if second_order != [reversed_modes[mode] for mode in first_order]:
        raise ValueError("Expected the opposite mode at each position")
    if read(first_bundle / "release-receipt.json") != read(second_bundle / "release-receipt.json"):
        raise ValueError("The pinned Q8 releases differ")
    if workload_configuration(first_bundle, first["run"]) != workload_configuration(second_bundle, second["run"]):
        raise ValueError("The two initial workload configurations differ")
    if (read(first_bundle / "scout/admission/confirmed-applied-profiles.json")
            != read(second_bundle / "scout/admission/confirmed-applied-profiles.json")):
        raise ValueError("The two startup allocations differ")

    deployments = [score(bundle, latency_script) for bundle in bundles]
    for deployment in deployments:
        rates = {}
        for mode in reversed_modes:
            periods = [row for row in deployment["periods"] if row["mode"] == mode]
            seconds = sum(row["measuredSeconds"] for row in periods)
            rates[mode] = sum(row["joinInputRecordsPerSecond"] * row["measuredSeconds"]
                              for row in periods) / seconds
        deployment["measuredJoinInputByMode"] = rates
        deployment["measuredRemoteChangePct"] = 100 * (rates["remote"] / rates["embedded"] - 1)

    paired_changes = [deployment["pairedRemoteJoinInputChangePct"] for deployment in deployments]
    return {
        "experimentUnits": len(deployments),
        "deployments": deployments,
        "counterbalancedPairChangePct": [
            100 * (sqrt((1 + normal / 100) * (1 + reverse / 100)) - 1)
            for normal, reverse in zip(*paired_changes)
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--counterbalanced-bundle", type=Path, help="A second deployment with opposite mode order")
    args = parser.parse_args()
    latency_script = Path(__file__).with_name("q8-latency.py")
    if args.counterbalanced_bundle:
        result = compare_counterbalanced(args.bundle, args.counterbalanced_bundle, latency_script)
    else:
        result = score(args.bundle, latency_script)
    print(json.dumps(result, indent=2))
