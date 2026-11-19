"""Measure native operator scaling reports, acknowledgements, and observed physical placement."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re

from analyze import allocation, input_spec, JOIN, read, rows
from native_observe import P_KEY, RP_KEY

MIB = 1024 ** 2
ID = r"[0-9a-f]{32}"
REQUIRED = ("job.autoscaler.enabled", "job.autoscaler.scaling.enabled", "job.autoscaler.justin.enabled",
            "job.autoscaler.a4s.enabled", "kubernetes.operator.job.upgrade.inplace-scaling.enabled")


def event_times(samples, start, end):
    # Core/v1 aggregates repeating events. Never invent timestamps for repeats it coalesced.
    found = {}
    for row in samples:
        for event in row.get("events", []):
            series = event.get("series") or {}
            timestamps = (event.get("eventTime"), event.get("firstTimestamp"),
                          event.get("lastTimestamp"), series.get("lastObservedTime"))
            for stamp in filter(None, timestamps):
                epoch = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
                if start <= epoch <= end:
                    found[event["uid"], epoch] = (epoch, event["reason"], event.get("message") or "")
    return sorted(found.values())


def targets(overrides, join_id=JOIN):
    counts = {vertex: int(value) for vertex, value in
              re.findall(f"({ID}):([0-9]+)", overrides.get(P_KEY) or "")}
    memory = {}
    for vertex, resource in re.findall(f"({ID}):ResourceProfile\\{{([^}}]+)\\}}", overrides.get(RP_KEY) or ""):
        amount = re.search(r"managedMemory=[^,]*?\((\d+) bytes\)", resource)
        if amount:
            memory[vertex] = int(amount[1])
    if join_id in counts and join_id in memory:
        return {"parallelism": counts, "join_bytes_per_slot": memory[join_id], "managed_bytes_by_vertex": memory}
    return None


def verified(row, target):
    profile = allocation(row)
    wanted = target["parallelism"]
    if (not profile or row.get("state") != "RUNNING" or row.get("running_tasks") != row.get("parallelism")
            or any(row["parallelism"].get(key) != p for key, p in wanted.items())
            or any(row.get("managed_bytes_by_vertex", {}).get(key) != size
                   for key, size in target.get("managed_bytes_by_vertex", {}).items())
            or profile["join_managed_slot_bytes"] != target["join_bytes_per_slot"]):
        return False
    locations = row.get("join_locations", {})
    if set(locations) != {str(i) for i in range(wanted[row.get("join_id", JOIN)])}:
        return False
    matching = {tm["id"]: sum(size * MIB == target["join_bytes_per_slot"] for size in tm["job_slot_managed_mib"])
                for tm in row.get("physical_slots", [])}
    return all(matching.get(tm, 0) >= count for tm, count in Counter(locations.values()).items())


def kafka_evidence(folder, release, end):
    """Measure broker arrivals within single topic generations; resets restart offsets at zero."""
    events = rows(folder / "kafka-generations.jsonl")
    offsets = rows(folder / "kafka-offsets.jsonl")
    starts = [e for e in events if e["event"] == "start" and e["epoch"] <= end]
    issues = []
    if not starts or any(e.get("offered_records_per_second") != 80000 or e.get("first_event_id") != 1 for e in starts):
        issues.append("Missing 80k mixed-event producer start receipt from event ID one")
    generations = []
    previous_start = None
    for start in starts:
        generation = start["generation"]
        if previous_start is not None and not any(e["event"] == "reset" and previous_start <= e["epoch"] <= start["epoch"] for e in events):
            issues.append("Producer generation restarted without a topic reset receipt")
        previous_start = start["epoch"]
        stop = min([end, *(e.get("started_epoch", e["epoch"]) for e in events
                            if e["generation"] == generation and e["event"] in ("pause", "stop")
                            and e["epoch"] >= start["epoch"])])
        points = []
        for row in offsets:
            if row.get("generation") != generation or not max(start["epoch"], release) <= row["epoch"] <= stop:
                continue
            topics = row.get("topics") or {}
            if len(topics) != 3 or not all(topic and partitions for topic, partitions in topics.items()):
                continue
            counters = {(topic, partition): offset for topic, partitions in topics.items()
                        for partition, offset in partitions.items()}
            if any(not isinstance(offset, int) or offset < 0 for offset in counters.values()):
                continue
            points.append((row["epoch"], counters))
        points.sort(key=lambda item: item[0])
        reset = any(t2 <= t1 or b.keys() != a.keys() or any(b[k] < a[k] for k in a)
                    for (t1, a), (t2, b) in zip(points, points[1:]))
        scope = {"generation": generation, "producer_start_from_release_s": start["epoch"] - release,
                 "samples": len(points), "measured_s": 0, "records": 0}
        if reset:
            issues.append(f"Broker offsets reset or partition membership changed within generation {generation}")
        elif len(points) >= 2:
            t0, first = points[0]
            t1, last = points[-1]
            counts = {topic: sum(last[k] - first[k] for k in first if k[0] == topic) for topic, _ in first}
            scope.update(measured_s=t1 - t0, records=sum(counts.values()), per_topic_records=counts,
                         records_per_second=sum(counts.values()) / (t1 - t0))
        generations.append(scope)
    seconds = sum(g["measured_s"] for g in generations)
    actual = sum(g["records"] for g in generations) / seconds if seconds > 0 else None
    if actual is None:
        issues.append("Broker samples did not establish a mixed-event delivery rate within a producer generation")
    return generations, actual, issues


def analyze_arm(folder):
    plan = read(folder / "plan.json")
    release = plan["release_ms"] / 1000
    window = read(folder / "window-ended.json")
    finish = read(folder / "finished.json")
    end = window.get("deadline_epoch") or finish.get("epoch")
    if end is None:
        return {"issues": ["No completed measurement window"], "transactions": []}
    created = read(folder / "deployment-created.json")
    conf = created["spec"]["flinkConfiguration"]
    kafka = plan.get("source_mode") == "kafka"
    issues = []
    if any(str(conf.get(key)).lower() != "true" for key in REQUIRED):
        issues.append("The deployed job did not enable the built-in Justin/A4S autoscaler with in-place scaling")
    if (plan.get("arm") == "calibrated") != (str(conf.get("job.autoscaler.a4s.online-cache-costs.enabled")).lower() == "true"):
        issues.append("Deployed online cost option differs from arm assignment")
    expected = {}
    if kafka:
        args = created["spec"]["job"]
        if not args.get("entryClass", "").endswith("SqlQueryJob") or "q8_unique" not in args.get("args", []):
            issues.append("The deployed job is not the Kafka Q8 SQL job")
    else:
        expected, flags = input_spec(created)
        stages = [tuple(map(int, stage.split(":"))) for stage in flags["--q8-input-plan"].split(",")]
        if any(int(flags["--p-person-source"]) * p + int(flags["--p-auction-source"]) * a != 80000
               for _, p, a in stages) or sum(s[0] for s in stages) < end - release - .01:
            issues.append("Scheduled input is not 80,000 records/s for the observed window")
        if int(flags["--q8-release-epoch-ms"]) != plan["release_ms"]:
            issues.append("Manifest and plan release clocks differ")

    progress = [row for row in rows(folder / "progress.jsonl") if row["capture_end_epoch"] <= end]
    resources = [row for row in rows(folder / "resource-series.jsonl") if row.get("type") == "sample"
                 and row["capture_epoch"] <= end]
    snapshots = rows(folder / "native-operator-series.jsonl")
    events = event_times(snapshots, created_time(created), end)
    decisions = [t for t, reason, message in events if reason == "ScalingReport" and "scaling execution enabled" in message.lower()]
    actions = [t for t, reason, message in events if reason == "Scaling" and "in-place scaling triggered" in message.lower()]
    suspended = [t for t, reason, _ in events if reason == "Suspended"]
    snapshots = [s for s in snapshots if s["epoch"] <= end]
    pods = {(p["uid"], p["restarts"]) for s in snapshots for p in s.get("jobmanagers", [])}
    starts = sorted({r["job_start_ms"] for r in progress if r.get("job_start_ms", -1) > 0})
    same_jm = len(pods) == 1
    # An adaptive execution-graph restart can change Flink timestamps in the same
    # JobManager. A replacement pod/container or operator suspension is stronger evidence.
    reissued = len(pods) > 1 or bool(suspended)
    join_ids = {s["join_id"] for s in progress if s.get("join_id")}
    join_id = (next(iter(join_ids)) if len(join_ids) == 1 else None) if kafka else JOIN
    if not join_id:
        issues.append("A unique Q8 join could not be identified in the running job graph")
    changes = []
    for snap in snapshots:
        resource = snap.get("resource", {})
        # A reconciled spec may contain native overrides even when the CR's persisted spec does not.
        current = targets(resource.get("reconciled_overrides", {}), join_id) or targets(resource.get("submitted_overrides", {}), join_id)
        if current is not None and (not changes or current != changes[-1][1]):
            changes.append((snap["capture_end_epoch"], current))
    first_action = min(actions, default=end)
    initial_rows = [r for r in progress if r["epoch"] < first_action and r.get("state") == "RUNNING"
                    and r.get("running_tasks") == r.get("parallelism") and r.get("physical_slots")]
    initial = initial_rows[0] if initial_rows else {}
    workers = initial.get("physical_slots", [])
    initial_slots = {"total": sum(tm["total_slots"] for tm in workers),
                     "occupied_by_job": sum(len(tm["job_slot_managed_mib"]) for tm in workers)}
    baseline = next((p for row in initial_rows if row.get("parallelism") == initial.get("parallelism")
                     and (p := allocation(row)) and p["occupied_slots"] == initial_slots["occupied_by_job"]), None)
    if initial_slots != {"total": 2, "occupied_by_job": 2}:
        issues.append("Initial physical allocation of two occupied slots was not confirmed")
    filled = [s for s in resources if s["values"]["task_slots_used"] is not None]
    if not filled or filled[0]["values"]["task_slots_used"] != 2:
        issues.append("The aligned Q8 samples did not confirm the two-slot start")
    if not resources or resources[0]["capture_epoch"] - release > 10 or end - resources[-1]["capture_epoch"] > 10:
        issues.append("Resource sampling did not cover the measurement window")
    if not snapshots or not any("events" in snap and "resource" in snap for snap in snapshots):
        issues.append("Missing Kubernetes operator event or resource observations")
    if not same_jm:
        issues.append("Continuous JobManager pod identity was not established")
    if reissued:
        issues.append("JobManager replacement, container restart, or operator suspension observed")
    if len(starts) > 1 and not actions:
        issues.append("Flink job start changed without a native scale acknowledgement")

    spans = []
    for i, ack in enumerate(actions):
        next_ack = actions[i + 1] if i + 1 < len(actions) else end + .000001
        previous_ack = actions[i - 1] if i else created_time(created)
        reports = [t for t in decisions if previous_ack < t <= ack]
        report = max(reports) if reports else None
        # The operator may commit the target just before the RPC or in the following poll.
        candidates = [(t, target) for t, target in changes if (report or previous_ack) <= t < next_ack]
        target = candidates[0][1] if len(candidates) == 1 else None
        running = next((s for s in progress if target and ack <= s["epoch"] and s["capture_end_epoch"] < next_ack
                        and s.get("state") == "RUNNING" and s.get("running_tasks") == s.get("parallelism")
                        and all(s["parallelism"].get(k) == v for k, v in target["parallelism"].items())), None)
        snapshot = next((s for s in progress if ack <= s["epoch"] and s["capture_end_epoch"] < next_ack
                         and target and verified(s, target)), None)
        complete = snapshot["capture_end_epoch"] if snapshot else None
        before_slots = [s["values"]["task_slots_used"] for s in filled if s["capture_epoch"] < ack]
        previous_slots = before_slots[-1] if before_slots else None
        changed_slots = next((s for s in filled if ack <= s["capture_epoch"] < next_ack
                              and s["values"]["task_slots_used"] > 0
                              and previous_slots is not None and s["values"]["task_slots_used"] != previous_slots), None)
        spans.append({"decision_from_release_s": report - release if report is not None else None,
                      "in_place_ack_from_release_s": ack - release, "requested": target,
                      "first_changed_occupied_slots": changed_slots["values"]["task_slots_used"] if changed_slots else None,
                      "ack_to_first_changed_slots_s": changed_slots["capture_epoch"] - ack if changed_slots else None,
                      "ack_to_running_requested_parallelism_s": running["capture_end_epoch"] - ack if running else None,
                      "observed_from_release_s": complete - release if complete else None,
                      "decision_to_observed_s": complete - report if complete and report is not None else None,
                      "ack_to_observed_s": complete - ack if complete else None,
                      "observed_profile": allocation(snapshot) if snapshot else None})
    if reissued:
        outcome = "restart_observed"
    elif spans:
        outcome = "in_place_verified" if same_jm and all(s["observed_profile"] for s in spans) else "in_place_acknowledged_only"
    elif changes or (baseline and any(allocation(r) and allocation(r) != baseline for r in progress)):
        outcome = "target_or_resource_change_without_in_place_ack"
        issues.append("Observed target or physical allocation changed without a native acknowledgement")
    else:
        outcome = "no_in_place_resize_observed"
    if spans and outcome != "in_place_verified":
        issues.append("An acknowledged resize lacks complete in-place placement verification")
    observations = [(r["epoch"], sum(r[k]["q8Offered"] for k in expected)) for r in progress if not kafka and r["epoch"] >= release
                    and all(r.get(k, {}).get("q8Offered", -1) >= 0 for k in expected)]
    live_rate = None
    if len(observations) >= 2:
        t0, n0 = observations[0]
        t1, n1 = observations[-1]
        if t1 - t0 >= 30:
            live_rate = (n1 - n0) / (t1 - t0)
            if abs(n1 - n0 - 80000 * (t1 - t0)) > 2 * 80000:
                issues.append("Source offer counters differ from the configured rate by over two seconds of input")
    generations = []
    if kafka:
        generations, live_rate, broker_issues = kafka_evidence(folder, release, end)
        issues.extend(broker_issues)
    if not kafka and live_rate is None:
        issues.append("Insufficient live source offer counters to verify sustained 80k input")
    scoped_log = folder / "operator.log"
    lines = scoped_log.read_text(errors="replace").splitlines() if scoped_log.exists() else []
    online = [line for line in lines if f"A4S online cache costs for vertex {join_id}" in line]
    empty_curve = [line for line in lines if "MissRateCurve has no points" in line]
    errors = sorted({message for _, reason, message in events if reason.lower() == "autoscalererror"})
    if outcome == "no_in_place_resize_observed" and errors:
        outcome = "no_resize_with_autoscaler_errors"
    fallbacks = fallback_failures(lines, release, end, decisions)
    if fallbacks:
        issues.append("The native request failed and the operator entered its regular upgrade path")
        outcome = "in_place_failed_then_upgrade"
    if not lines:
        issues.append("Scoped operator logs are missing")
    if plan["arm"] == "calibrated" and not online:
        issues.append("Operator logs did not confirm a qualified online cache cost for the Q8 join")
    return {"outcome": outcome, "window_s": end - release,
            "source_mode": plan.get("source_mode", "replay"), "join_id": join_id,
            "sampling_period_s": 5, "initial_profile": baseline, "initial_slots": initial_slots,
            "last_observed_occupied_slots": filled[-1]["values"]["task_slots_used"] if filled else None,
            "observed_occupied_slots": sorted({s["values"]["task_slots_used"] for s in filled}),
            "failed_in_place_requests": fallbacks,
            "kafka_producer_generations": generations, "observed_offered_records_per_second": live_rate, "job_start_epochs_ms": starts,
            "jobmanager_pods": sorted(pods), "scaling_reports_from_release_s": [t-release for t in decisions],
            "suspended_from_release_s": [t-release for t in suspended], "transactions": spans,
            "observed_targets": [{"observed_from_release_s": t-release, **target} for t, target in changes],
            "qualified_online_log_lines": len(online), "empty_curve_error_lines": len(empty_curve),
            "autoscaler_errors": errors,
            "issues": issues}


def created_time(created):
    return datetime.fromisoformat(created["metadata"]["creationTimestamp"].replace("Z", "+00:00")).timestamp()


def fallback_failures(lines, release, end, reports):
    failures = []
    active = None
    for line in lines:
        stamp, _, message = line.partition(" ")
        if "Error while rescaling, falling back to regular upgrade" in message:
            try:
                epoch = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
            except ValueError:
                active = None
                continue
            if epoch > end:
                active = None
                continue
            decision = max((t for t in reports if t <= epoch), default=None)
            active = {"native_failure_from_release_s": epoch - release,
                      "report_to_native_failure_s": epoch - decision if decision is not None else None,
                      "cause": None}
            failures.append(active)
        elif active and "Caused by:" in message:
            cause = message.split("Caused by:", 1)[1].strip()
            if not active["cause"] or "RestClientException:" not in cause:
                active["cause"] = cause[:600]
        elif re.match(r"\d{4}-\d\d-\d\d[ T]", message):
            active = None
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pair", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = {name: analyze_arm(args.pair / name) for name in ("hardcoded", "calibrated")}
    operators = [read(args.pair / arm / "operator.json") for arm in result]
    plans = [read(args.pair / arm / "plan.json") for arm in result]
    pair_issues = []
    for field in ("revision", "jar_sha256"):
        values = [op.get(field) for op in operators]
        if not all(values) or len(set(values)) != 2:
            pair_issues.append(f"Distinct operator {field} values were not established")
    if not plans[0].get("artifacts") or plans[0]["artifacts"] != plans[1].get("artifacts"):
        pair_issues.append("The two arms do not share identical worker and workload artifacts")
    if all(plan.get("source_mode") == "kafka" for plan in plans):
        brokers = [plan.get("kafka_bootstrap_servers") for plan in plans]
        if not all(brokers) or len(set(brokers)) != 2:
            pair_issues.append("Isolated Kafka broker endpoints were not established")
    for arm, operator in zip(result, operators):
        result[arm]["revision"] = operator.get("revision")
        result[arm]["operator_jar_sha256"] = operator.get("jar_sha256")
        result[arm]["issues"].extend(pair_issues)
    output = args.output or args.pair / "native-results.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({arm: {"outcome": run.get("outcome"), "issues": run["issues"]} for arm, run in result.items()}))
    return int(any(run["issues"] for run in result.values()))


if __name__ == "__main__":
    raise SystemExit(main())
