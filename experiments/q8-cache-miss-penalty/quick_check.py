"""Check that two Q8 controllers ran the same workload and applied different join resources."""
import argparse
import json
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


def validate(output, analysis_seconds=None):
    pair, = output.glob("pair-*")
    meta = analyze.read(pair / "pair.json")
    controllers = analyze.read(output / "controllers.json")
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
        if mode == "main" and handling == "literal" and not applied:
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
        if mode == "branch" and not any(d["applied_model"] == "online" for d in applied):
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
    if not different:
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
