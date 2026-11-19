"""Check that two Q8 controllers ran the same workload and applied different join resources."""
import argparse
import json
from math import isfinite
from statistics import median
from pathlib import Path

import analyze


def input_at_cutoff(folder, arm, seconds):
    release = arm["release_ms"] / 1000
    cutoff = release + seconds
    ends = arm["input_end_offsets"]
    if arm["completed_input"] is not None and arm["wall_seconds_from_release"] <= seconds:
        return {"records": arm["completed_input"], "sample_seconds": arm["wall_seconds_from_release"],
                "next_sample_records": arm["completed_input"], "next_sample_seconds": None, "finished_before_cutoff": True}
    points = []
    last_restore = max((r["verified_s"] for r in arm["applied"] if r["verified_s"] <= seconds), default=0)
    series = analyze.rows(folder / "progress.jsonl")
    receipt = analyze.read(folder / "window-ended.json")
    if receipt.get("snapshot") and receipt.get("snapshot_end_epoch"):
        series.append(dict(receipt["snapshot"], epoch=receipt["observed_epoch"],
                           capture_end_epoch=receipt["snapshot_end_epoch"]))
    for row in sorted(series, key=lambda sample: sample["epoch"]):
        if row["epoch"] < release + last_restore or row.get("state") != "RUNNING":
            continue
        if all(row.get(key, {}).get("q8InputEnd") == end and
               0 <= row[key].get("q8NextOffset", -1) <= end for key, end in ends.items()):
            points.append((row["epoch"], sum(row[k]["q8NextOffset"] for k in ends),
                           row.get("capture_end_epoch", row["epoch"])))
    before = [point for point in points if point[2] <= cutoff]
    after = [point for point in points if point[0] >= cutoff]
    if not before or cutoff - before[-1][0] > 12:
        return None
    following = after[0] if after and after[0][0] - cutoff <= 12 else None
    # The per-partition offsets are sampled during the REST poll. Preserve the timestamp
    # and the next complete poll rather than interpolating a precise cutoff count.
    return {"records": before[-1][1], "sample_seconds": before[-1][0] - release,
            "next_sample_records": following[1] if following else None,
            "next_sample_seconds": following[0] - release if following else None, "finished_before_cutoff": False}


def demand_evidence(folder, arm, meta, seconds):
    """Check the requested load and physical slot count against the live receipts."""
    issues = []
    rate = meta["offered_records_per_second"]
    expected_slots = meta["expected_initial_occupied_slots"]
    manifest = analyze.read(folder / "deployment-created.json")
    expected, flags = analyze.input_spec(manifest)
    stages = [tuple(map(int, stage.split(":"))) for stage in flags["--q8-input-plan"].split(",")]
    # A full-drain run continues draining after the last scheduled arrival.
    if not seconds:
        seconds = sum(length for length, _, _ in stages)
    persons, auctions = (int(flags["--p-" + source + "-source"]) for source in ("person", "auction"))
    if (rate <= 0 or expected_slots <= 0 or sum(length for length, _, _ in stages) < seconds
            or any(length <= 0 or p < 0 or a < 0 or persons * p + auctions * a != rate
                   for length, p, a in stages)):
        issues.append("Source input plan does not sustain the requested aggregate rate throughout the window")

    initial = arm.get("matched_start_profile") or {}
    if initial.get("occupied_slots") != expected_slots:
        issues.append("Flink's cold-start occupied task slots differ from the requested allocation")
    expected_join = meta.get("expected_initial_join_parallelism")
    if expected_join is not None and initial.get("join_parallelism") != expected_join:
        issues.append("Cold-start join parallelism differs from the requested parallelism")

    release = arm["release_ms"] / 1000
    progress = analyze.rows(folder / "progress.jsonl")
    cold = arm.get("cold_start_evidence") or {}
    physical = next((r["physical_slots"] for r in progress
                     if r["epoch"] == cold.get("native_sample_epoch") and r.get("physical_slots")), [])
    slot_mib = [v for tm in physical for v in tm["job_slot_managed_mib"]]
    total_slots = sum(tm["total_slots"] for tm in physical)
    if (len(slot_mib) != expected_slots
            or sum(tm["total_slots"] - tm["free_slots"] for tm in physical) != expected_slots):
        issues.append("Physical TaskManager allocations do not confirm cold-start occupancy")
    if "expected_initial_total_slots" in meta and total_slots != meta["expected_initial_total_slots"]:
        issues.append("Cold-start physical slot capacity differs from the requested slot capacity")
    if ("expected_initial_job_slot_managed_mib" in meta
            and any(v != meta["expected_initial_job_slot_managed_mib"] for v in slot_mib)):
        issues.append("Physical starting slot memory differs from the requested per-slot reservation")
    if ("expected_initial_managed_pool_bytes_per_taskmanager" in meta
            and any(tm["pool_bytes"] != meta["expected_initial_managed_pool_bytes_per_taskmanager"]
                    for tm in physical)):
        issues.append("Cold-start TaskManager managed pool differs from the requested pool")
    samples = []
    lag_errors = []
    per_source_bounds = {key: max(stage[1 if key.startswith("Persons/") else 2]
                                  for stage in stages) for key in expected}
    for row in progress:
        elapsed = row["epoch"] - release
        if row.get("state") != "RUNNING" or not 0 <= elapsed <= seconds:
            continue
        partitions = [row.get(key, {}) for key in expected]
        fields = ("q8Offered", "q8Backlog", "pendingRecords", "q8NextOffset")
        if not all(all(isinstance(part.get(field), int) and part[field] >= 0 for field in fields)
                   for part in partitions):
            continue
        samples.append((elapsed, sum(part["q8Offered"] for part in partitions),
                        sum(part["pendingRecords"] for part in partitions)))
        for key, part in zip(expected, partitions):
            # Gauges have independent callbacks and the Flink metrics cache refreshes each second.
            # Check agreement to one second of scheduled arrivals per partition.
            delta = abs(part["q8Backlog"] - part["pendingRecords"])
            lag_errors.append(delta / max(1, per_source_bounds[key]))
    offered_rate = None
    if len(samples) >= 2:
        t0, offered0, _ = samples[0]
        t1, offered1, _ = samples[-1]
        if t1 > t0:
            offered_rate = (offered1 - offered0) / (t1 - t0)
            # Allow two one-second metric refresh intervals across the endpoint polls.
            if t1 - t0 < 30 or abs((offered1 - offered0) - rate * (t1 - t0)) > 2 * rate:
                issues.append("Live source offers do not corroborate the configured sustained rate")
    if offered_rate is None or not lag_errors:
        issues.append("Missing live offered and standard pending-record gauges for all source partitions")
    elif median(lag_errors) > 1:
        issues.append("Standard pending-record gauges disagree with replay backlog")

    policy = analyze.rows(folder / "policy.jsonl")
    targets = []
    failed_input_rates = []
    for row in policy:
        elapsed = row["epoch"] - release
        if not 0 <= elapsed <= seconds:
            continue
        ingress = row.get("source_demand_records_s")
        if (row.get("type") == "literal_main_empty_curve" and isinstance(ingress, (int, float))
                and isfinite(ingress) and ingress >= 0):
            failed_input_rates.append({"elapsed_seconds": elapsed, "source_demand_records_per_second": ingress})
        value = row.get("target_records_s")
        if isinstance(value, (int, float)) and isfinite(value) and value >= 0 and row.get("type") == "selection":
            targets.append({"elapsed_seconds": elapsed, "join_target_records_per_second": value,
                                "online_fit_qualified": bool(row.get("online_fit_qualified"))})
    resources = [r for r in analyze.rows(folder / "resource-series.jsonl")
                 if r.get("type") == "sample" and 0 <= r["capture_epoch"] - release <= seconds
                 and r.get("job_state") == "RUNNING" and r["values"].get("task_slots_used") is not None]
    if not resources or resources[0]["values"]["task_slots_used"] != expected_slots:
        issues.append("TaskManager slot series does not confirm the requested start")
    if not arm["applied"]:
        events = analyze.rows(folder / "rescale-events.jsonl")
        stable_profiles = [a for row in progress if (a := analyze.allocation(row)) is not None
                           and a["join_managed_slot_bytes"] is not None]
        keys = ("occupied_slots", "join_parallelism", "join_managed_slot_bytes", "native_bytes_per_join")
        if (not stable_profiles or any(a[k] != initial.get(k) for a in stable_profiles for k in keys)
                or any(r["values"]["task_slots_used"] != expected_slots for r in resources)
                or any(row["type"] == "rescale_start" for row in events)
                or (not targets and arm.get("raw_main_empty_curve_evaluations", 0) < 2)):
            issues.append("An unchanged allocation lacks live placement and evaluation evidence")
    stats = {"offered_records_per_second": rate, "measured_offered_records_per_second": offered_rate,
             "full_lag_samples": len(samples), "peak_observed_pending_records": max((s[2] for s in samples), default=None),
             "median_lag_gauge_disagreement_seconds_of_input": median(lag_errors) if lag_errors else None,
             "initial_occupied_slots": initial.get("occupied_slots"), "initial_total_slots": total_slots,
             "initial_taskmanagers": physical,
             "last_observed_occupied_slots": resources[-1]["values"]["task_slots_used"] if resources else None,
             "controller_join_targets": targets, "source_demand_on_main_failure": failed_input_rates}
    return stats, issues


def validate(output, analysis_seconds=None):
    pair, = output.glob("pair-*")
    meta = analyze.read(pair / "pair.json")
    controllers = analyze.read(output / "controllers.json")
    sustained = "offered_records_per_second" in meta
    configured_window = meta.get("observation_seconds") or 0
    seconds = configured_window if analysis_seconds is None else analysis_seconds
    if seconds < 0 or (configured_window and seconds > configured_window):
        raise ValueError("Observation must be within the recorded window")
    names = ("hardcoded", "calibrated")
    arms = {}
    issues = []
    classes = ("ScalingMetricEvaluator", "ScalingExecutor", "a4s.A4S")
    handling = meta.get("main_empty_curves")
    if handling is None:
        # The first diagnostic run predates the selectable literal driver.
        first_diagnostic = "b3defac2e9129cd81c35746a36a27aaf3c565bbfc18564de67040f901973469f"
        if controllers["hardcoded"]["policy_sha256"] == first_diagnostic:
            handling = "omit-empty"
    if handling not in ("literal", "omit-empty"):
        issues.append("Cannot establish how the main driver treated empty miss curves")
    if controllers["hardcoded"].get("empty_curve_handling", handling) != handling:
        issues.append("Main driver mode differs from run plan")
    for name in names:
        arm = analyze.analyze(pair / name, require_shadow=False, require_finish=not seconds)
        mode = controllers[name]["mode"]
        arms[mode] = arm
        arm["revision"] = controllers[name]["revision"]
        controller = controllers[name]
        expected = arm["provenance"]
        if (expected["policy_sha256"] != controller["policy_sha256"] or
                expected["controller_classpath_sha256"] != analyze.digest(controller["jars"])):
            issues.append(f"{mode}: controller provenance mismatch")
        origins = controller["loaded"]["origins"]
        arm["controller_origins"] = {
            key: origins["org.apache.flink.autoscaler." + key]["sha256"] for key in classes}
        issues.extend(f"{mode}: {err}" for err in arm["issues"])
        if arm["unexpected_restores"] or arm["unexpected_job_start_epochs_ms"]:
            issues.append(f"{mode}: unexpected job restart")
        applied = arm["applied"]
        if seconds:
            arm["input_at_cutoff"] = input_at_cutoff(pair / name, arm, seconds)
            if not arm["input_at_cutoff"]:
                issues.append(f"{mode}: missing complete source offsets near the cutoff")
            stop = analyze.read(pair / name / "window-ended.json")
            if configured_window and not arm["finished"]:
                if (not stop or stop.get("observation_seconds") != configured_window
                        or stop.get("release_ms") != arm["release_ms"]
                        or stop.get("deadline_epoch") != arm["release_ms"] / 1000 + configured_window
                        or stop.get("state") != "RUNNING" or stop.get("active_transaction")):
                    issues.append(f"{mode}: incomplete window or resize still in progress")
            if any(d["verified_s"] > seconds for d in applied):
                issues.append(f"{mode}: resize verification came after the measurement cutoff")
        if mode == "main":
            evidence = analyze.rows(pair / name / "policy.jsonl")
            failures = [row for row in evidence if row.get("type") == "literal_main_empty_curve"
                        and "MissRateCurve has no points" in str(row.get("exception"))]
            with_join_curve = [row for row in failures
                               if row.get("curve_points_by_vertex", {}).get(analyze.JOIN, 0) > 0
                               and any(row["curve_points_by_vertex"].get(source) == 0
                                       for source in analyze.SOURCE_IDS.values())]
            arm["raw_main_empty_curve_evaluations"] = len({row["snapshot"] for row in with_join_curve})
            arm["first_raw_main_empty_curve_error"] = next(iter(with_join_curve), None)
        if sustained:
            arm["demand"], errors = demand_evidence(pair / name, arm, meta, seconds)
            issues.extend(f"{mode}: {message}" for message in errors)
            if applied and not arm["standard_triggers"]:
                issues.append(f"{mode}: resize without a recorded standard autoscaler trigger")
        elif mode == "main" and handling == "literal" and not applied:
            progress = analyze.rows(pair / name / "progress.jsonl")
            profiles = [a for row in progress if (a := analyze.allocation(row)) is not None]
            start = arm["matched_start_profile"]
            stable = [p for p in profiles if p["join_managed_slot_bytes"] is not None]
            join_fields = ("join_parallelism", "join_managed_slot_bytes", "native_bytes_per_join")
            if (not start or not stable or any(p[k] != start[k] for p in stable for k in join_fields)
                    or arm["raw_main_empty_curve_evaluations"] < 2
                    or any(row["type"] == "rescale_start" for row in analyze.rows(pair / name / "rescale-events.jsonl"))):
                issues.append("No verified evidence explaining an unchanged literal main allocation")
        elif not applied or not arm["standard_triggers"]:
            issues.append(f"{mode}: no verified ordinary autoscaler resize")
        if not sustained and mode == "branch" and not any(d["applied_model"] == "online" for d in applied):
            issues.append("Branch never applied a qualified online estimate")
        for decision in applied:
            if arm["completed_input"] is not None and all(x is not None for x in decision["offsets"].values()):
                decision["records_remaining_at_savepoint"] = arm["completed_input"] - sum(decision["offsets"].values())
                if decision["records_remaining_at_savepoint"] <= 0:
                    issues.append(f"{mode}: no sealed input left at restore")
    main, branch = arms["main"], arms["branch"]
    matched_fields = ["input_plan", "input_end_offsets", "matched_start_profile"]
    if not seconds:
        matched_fields.append("completed_input")
    for field in matched_fields:
        if not main.get(field) or main[field] != branch.get(field):
            issues.append(f"Unmatched {field}")
    for field in ("artifacts", "normalized_manifest_sha256", "deployed_spec_sha256"):
        if not main["provenance"].get(field) or main["provenance"][field] != branch["provenance"].get(field):
            issues.append(f"Unmatched {field}")
    if meta["execution_order"] == "parallel" and main["release_ms"] != branch["release_ms"]:
        issues.append("Concurrent arms had different release times")
    if main["controller_origins"] == branch["controller_origins"] or main["revision"] == branch["revision"]:
        issues.append("Both arms used the same autoscaler binaries or revision")

    def profile(decision):
        return decision["p"], decision["managed_bytes_per_slot"], tuple(sorted(decision["native_bytes"].values()))

    decisions = {mode: [profile(d) for d in arm["applied"]] for mode, arm in arms.items()}
    for mode, history in decisions.items():
        initial = arms[mode]["matched_start_profile"]
        if initial:
            history.insert(0, (initial["join_parallelism"], initial["join_managed_slot_bytes"],
                               tuple(sorted(initial["native_bytes_per_join"]))))
    different = all(decisions.values()) and decisions["main"][-1] != decisions["branch"][-1]
    if not sustained and not different:
        issues.append("The corresponding verified join allocations did not diverge")
    throughput = [arm["completed_input_per_wall_second"] for arm in (main, branch)]
    matched_records = [arm.get("input_at_cutoff") for arm in (main, branch)]
    sampled_change = None
    if seconds and all(matched_records) and matched_records[0]["records"] and not issues:
        sampled_change = 100 * (matched_records[1]["records"] / matched_records[0]["records"] - 1)
    return {"passed": not issues, "issues": issues, "execution_order": meta["execution_order"],
            "main_empty_curve_handling": handling, "observation_seconds": seconds,
            "sampled_branch_input_change_percent_at_cutoff": sampled_change,
            "wall_seconds_including_setup_and_cleanup": meta.get("total_wall_seconds_including_setup_and_cleanup"),
            "different_verified_join_profile": different,
            "branch_throughput_change_percent": (100 * (throughput[1] / throughput[0] - 1)
                                                    if all(throughput) and not issues else None),
            "arms": arms}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Run root containing controllers.json and pair-*")
    parser.add_argument("--analysis-seconds", type=int,
                        help="Retrospectively summarize this many seconds of an archived run")
    args = parser.parse_args()
    report = validate(args.output, args.analysis_seconds)
    (args.output / "quick-results.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "arms"}, indent=2))
    raise SystemExit(0 if report["passed"] else 1)
