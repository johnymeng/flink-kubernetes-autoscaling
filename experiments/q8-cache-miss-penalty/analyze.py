"""Validate archived Q8 arms and compare wall-clock progress, including savepoint recovery."""
import argparse
from collections import defaultdict
from hashlib import sha256
import json
from pathlib import Path
import re
from statistics import median

JOIN = "11ffd1a2f5905feab358b0af9a75fe4e"
SOURCE_IDS = {"Persons": "cbc357ccb763df2852fee8c4fc7d55f2", "Auctions": "6cdc5bb954874d922eaee11a8e7b5dd5"}
OFFSET = re.compile(r"Q8_INPUT_OFFSET action=(\w+) source=(Persons|Auctions) partition=(\d+) parallelism=(\d+) checkpoint=(-?\d+) next=(\d+) end=(\d+)")


def read(path):
    return json.loads(path.read_text()) if path.is_file() else {}


def rows(path):
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def read_offsets(folder):
    values = defaultdict(set)
    restores = defaultdict(set)
    for path in sorted(folder.glob("*.offsets")):
        for line in path.read_text(errors="replace").splitlines():
            match = OFFSET.search(line)
            if not match:
                continue
            action, role, index, p, checkpoint, offset, end = match.groups()
            key = f"{role}/{index}"
            values[action, int(checkpoint), key].add((int(offset), int(end), int(p)))
            if action == "restore":
                restores[int(checkpoint), key].add(line)
    return values, restores


def input_spec(manifest):
    args = manifest["spec"]["job"]["args"]
    flags = dict(zip(args[::2], args[1::2]))
    stages = [tuple(map(int, step.split(":"))) for step in flags["--q8-input-plan"].split(",")]
    sizes = {"Persons": int(flags["--p-person-source"]), "Auctions": int(flags["--p-auction-source"])}
    expected = {}
    for role, n in sizes.items():
        end = sum(seconds * (persons if role == "Persons" else auctions) for seconds, persons, auctions in stages)
        for i in range(n):
            expected[f"{role}/{i}"] = (end, n)
    return expected, flags


def normalized(manifest, owner, arm):
    # Canonicalize the four arm-dependent values also varied by the renderer.
    text = json.dumps(manifest).replace(owner + "-" + arm, "OWNER-ARM").replace(owner, "OWNER")
    result = json.loads(text)
    config = result["spec"]["flinkConfiguration"]
    key = "job.autoscaler.a4s.online-cache-costs.enabled"
    if str(config[key]).lower() != str(arm == "calibrated").lower():
        raise ValueError("Deployed cost mode disagrees with assigned arm")
    config[key] = "MODE"
    config["$internal.pipeline.job-id"] = "JOBID"
    args = result["spec"]["job"]["args"]
    args[args.index("--q8-release-epoch-ms") + 1] = "RELEASE"
    return result


def digest(value):
    return sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def budgets(comparison):
    p = comparison.get("trigger_parallelism", {}).get(JOIN)
    target = comparison.get("target_records_s")
    rates = [point["missRate"] for point in comparison.get("mrc") or [] if isinstance(point, dict)]
    if not p or not target or not rates:
        return None
    minimum = min(rates)
    result = {"trigger_join_parallelism": p, "lowest_curve_miss_fraction": minimum}
    for label, base, slope in (("fixed", "fixed_baseline_s", "fixed_slope_s"),
                               ("online", "online_baseline_s", "online_slope_s"),
                               ("slope_only", "fixed_baseline_s", "online_slope_s")):
        if not comparison.get(slope) or base not in comparison:
            continue
        budget = (p / target - comparison[base]) / comparison[slope]
        result[label] = {"raw_miss_budget": budget, "unconstrained": budget >= 1,
                         "achievable_on_curve": budget >= minimum}
    return result


def allocation(row):
    p = row.get("parallelism", {}).get(JOIN)
    caps = row.get("native") or {}
    overview = row.get("overview") or {}
    if not p or not all(caps.get(str(i), 0) > 0 for i in range(p)):
        return None
    if any(overview.get(key) is None for key in ("slots-total", "slots-available", "taskmanagers")):
        return None
    return {"join_parallelism": p, "native_bytes_per_join": [caps[str(i)] for i in range(p)],
            "join_managed_slot_bytes": row.get("join_managed_slot_bytes"),
            "occupied_slots": overview["slots-total"] - overview["slots-available"],
            "total_slots": overview["slots-total"], "workers": overview["taskmanagers"]}


def analyze(folder):
    plan = read(folder / "plan.json")
    manifest = read(folder / "deployment-created.json")
    issues = []
    if not manifest:
        return {"arm": plan.get("arm"), "path": folder.name, "issues": ["Missing initial manifest"]}
    expected, flags = input_spec(manifest)
    stages = flags["--q8-input-plan"]
    release = int(flags["--q8-release-epoch-ms"]) / 1000
    if str(plan.get("plan")) != stages or plan.get("release_ms") != int(flags["--q8-release-epoch-ms"]):
        issues.append("Input schedule or release time differs from the recorded plan")
    full = sum(end for end, _ in expected.values())
    progress = rows(folder / "progress.jsonl")
    events = rows(folder / "rescale-events.jsonl")
    policy = rows(folder / "policy.jsonl")
    triggers = [r for r in policy if r.get("type") == "decision"]
    starts = {e["transaction"]: e for e in events if e["type"] == "rescale_start"}
    verified = {e["transaction"]: e for e in events if e["type"] == "rescale_end"}
    cutoff = min((e["epoch"] for e in starts.values()), default=float("inf"))
    initial = next(((row, profile) for row in progress if row["epoch"] < cutoff
                    and (profile := allocation(row)) is not None and profile["join_managed_slot_bytes"] is not None),
                   (None, None))
    cold = next((row for row in progress if row["epoch"] <= release and allocation(row) is not None
                 and all(row.get(key, {}).get("q8NextOffset") == 0
                         and row[key].get("q8RestoredCheckpointId") == -1 for key in expected)), None)
    if not initial[1] or not cold:
        issues.append("Missing complete cold allocation and partition offsets before release")
    if initial[1] and initial[1]["join_managed_slot_bytes"] is None:
        issues.append("Missing initial join managed-memory profile")
    offset_log, restore_lines = read_offsets(folder)
    applied = []
    for transaction, start in starts.items():
        end = verified.get(transaction)
        proof = read(folder / (transaction + "-applied.json"))
        if not end or not proof:
            issues.append(f"Unverified resize {transaction}")
            continue
        restored = (proof.get("checkpoint_status", {}).get("latest", {}).get("restored") or {})
        checkpoint = restored.get("id")
        source_evidence = {}
        for key, (size, p) in expected.items():
            saved = offset_log["snapshot", checkpoint, key]
            resumed = offset_log["restore", checkpoint, key]
            # The post-restore gauge is emitted by the restored source attempt itself.
            # It can corroborate a saved partition offset if the old log follower closed.
            resumed = resumed | {(r[key]["q8RestoredOffset"], r[key]["q8InputEnd"], p) for r in progress
                                 if r.get("job_start_ms") == end.get("restored_job_start_epoch_ms")
                                 and r.get(key, {}).get("q8RestoredCheckpointId") == checkpoint
                                 and "q8RestoredOffset" in r[key] and "q8InputEnd" in r[key]}
            matched = len(saved) == 1 and saved == resumed
            source_evidence[key] = next(iter(saved))[0] if matched and next(iter(saved))[1:] == (size, p) else None
        if checkpoint is None or any(value is None for value in source_evidence.values()):
            issues.append(f"Missing or inconsistent source offset at restore for {transaction}")
        p = int(end["observed_parallelism"][JOIN])
        native = proof.get("native_cache_bytes", {})
        if set(native) != {str(i) for i in range(p)} or any(value <= 0 for value in native.values()):
            issues.append(f"Missing live join cache capacity for {transaction}")
        off = {e["type"]: e["epoch"] for e in events if e.get("transaction") == transaction}
        offline = (off["source_offline_end"] - off["source_offline_start"]
                   if {"source_offline_start", "source_offline_end"} <= off.keys() else None)
        proposal = read(folder / (transaction + "-proposal.json"))
        cf = proposal.get("comparison") or {}
        # Omit raw curves and subtask arrays from aggregate output. Raw proposal retains them.
        placement = {k: v for k, v in cf.items() if k not in ("mrc", "subtasks")}
        if not cf or not cf.get("mrc"):
            issues.append(f"Resize {transaction} lacks a retained decision-time miss-rate curve")
        other = "fixed" if plan["arm"] == "calibrated" else "online"
        for mode in (other, "slope_only"):
            if "proposed" not in (cf.get(mode) or {}):
                issues.append(f"Missing {mode} counterfactual at resize {transaction}")
        applied.append({"transaction": transaction, "checkpoint": checkpoint, "offsets": source_evidence,
                        "request_s": start["epoch"] - release, "verified_s": end["epoch"] - release,
                        "request_to_verified_s": end["epoch"] - start["epoch"],
                        "observed_offline_s": offline, "p": p,
                        "managed_bytes_per_slot": end["join_managed_bytes"], "native_bytes": native,
                        "savepoint": end["savepoint"], "counterfactual": placement,
                        "budgets_at_standard_trigger": budgets(cf),
                        "other_model_would_apply_same_profile": (
                            cf[other].get("parallelism") == proposal.get("parallelism")
                            and cf[other].get("profiles") == proposal.get("resource_profiles"))
                            if (cf.get(other) or {}).get("proposed") else None,
                        "applied_model": ("online" if cf.get("online_fit_qualified") else "fixed_fallback")
                        if plan["arm"] == "calibrated" else "fixed",
                        "active_parallelism": proposal.get("parallelism"), "active_profiles": proposal.get("resource_profiles")})
    drained = {k for k, (size, p) in expected.items() if offset_log["end", -1, k] == {(size, size, p)}}
    known_checkpoints = {tx["checkpoint"] for tx in applied}
    extra_restores = []
    for (checkpoint, key), receipts in sorted(restore_lines.items()):
        count = len(receipts) - int(checkpoint in known_checkpoints)
        if count > 0:
            saved = offset_log["snapshot", checkpoint, key]
            resumed = offset_log["restore", checkpoint, key]
            extra_restores.append({"checkpoint": checkpoint, "partition": key, "extra_restore_count": count,
                                   "offsets": sorted(s[0] for s in resumed),
                                   "matches_snapshot": len(saved) == 1 and saved == resumed})
    job_starts = sorted({r["job_start_ms"] for r in progress if r.get("job_start_ms", 0) > 0})
    expected_generations = {r["restored_job_start_epoch_ms"] for r in verified.values()
                            if "restored_job_start_epoch_ms" in r}
    if job_starts:
        expected_generations.add(job_starts[0])
    extra_generations = sorted(set(job_starts) - expected_generations)
    finish = read(folder / "finished.json")
    job, status = finish.get("job") or {}, finish.get("status") or {}
    finished = job.get("state") == "FINISHED" or status.get("state") == "FINISHED"
    end_epoch = job.get("end-time", -1) / 1000 if job.get("end-time", -1) > 0 else finish.get("epoch")
    elapsed = end_epoch - release if finished and end_epoch and end_epoch > release else None
    if not finished or drained != set(expected):
        issues.append("Terminal FINISHED and every partition end offset were not both verified")
    completed = full if finished and drained == set(expected) else None
    samples = [(r["epoch"], sum(r[k]["q8Backlog"] for k in expected)) for r in progress
               if r["epoch"] >= release and all(r.get(k, {}).get("q8Backlog") is not None for k in expected)]
    timeline = [release] + [epoch for epoch, _ in samples if end_epoch is None or epoch < end_epoch]
    if elapsed:
        timeline.append(end_epoch)
    gaps = [b - a for a,b in zip(timeline, timeline[1:])]
    baseline = {k: plan.get(k) for k in ("artifacts", "policy_sha256", "controller_classpath_sha256", "normalized_manifest_sha256")}
    if any(not val for val in baseline.values()):
        issues.append("Incomplete binary or normalized-manifest provenance")
    owner = manifest.get("metadata", {}).get("annotations", {}).get("ece496.org/benchmark-owner")
    submitted = read(folder / "manifest.json")
    cp = read(folder.parent.parent / "classpath.json")
    if cp and digest(cp) != baseline["controller_classpath_sha256"]:
        issues.append("Controller classpath file list does not match its recorded hash")
    if owner and submitted:
        try:
            if digest(normalized(submitted, owner, plan["arm"])) != baseline["normalized_manifest_sha256"]:
                issues.append("Submitted manifest differs from recorded normalized template")
            baseline["deployed_spec_sha256"] = digest(normalized({"spec": manifest["spec"]}, owner, plan["arm"]))
        except (ValueError, KeyError) as error:
            issues.append(str(error))
    else:
        issues.append("Submitted manifest or deployment owner unavailable for verification")
    return {"arm": plan["arm"], "path": folder.name, "release_ms": int(flags["--q8-release-epoch-ms"]),
            "input_plan": stages, "input_end_offsets": {k: v[0] for k, v in expected.items()}, "provenance": baseline,
            "matched_start_profile": initial[1], "pre_release_zero_offsets_observed": bool(cold),
            "finished": finished, "completed_input": completed, "wall_seconds_from_release": elapsed,
            "finish_clock": "Flink end-time" if job.get("end-time", -1) > 0 else "FINISHED poll upper bound",
            "completed_input_per_wall_second": completed / elapsed if elapsed and completed is not None else None,
            "sampled_backlog_peak": max((b for _, b in samples), default=None),
            "complete_backlog_samples": len(samples), "longest_backlog_sample_gap_seconds": max(gaps, default=None),
            "standard_triggers": len(triggers),
            "unexpected_restores": extra_restores, "unexpected_job_start_epochs_ms": extra_generations,
            "job_start_epochs_ms": job_starts,
            "qualified_online_at_triggers": sum(bool(row.get("online_fit_qualified")) for row in triggers),
            "trigger_comparisons": [{k: v for k, v in row.items() if k not in ("mrc", "subtasks")} for row in triggers],
            "applied": applied, "issues": issues}


def compare(arms):
    arms.sort(key=lambda arm: arm.get("release_ms", 0))
    order = [a.get("arm") for a in arms]
    checks = {}
    for field in ("provenance", "input_plan", "input_end_offsets", "matched_start_profile"):
        checks[field] = bool(arms) and all(a.get(field) and a[field] == arms[0].get(field) for a in arms)
    abba = (len(order) == 4 and order[0] == order[3] and order[1] == order[2]
            and set(order) == {"hardcoded", "calibrated"})
    exercised = any(a.get("applied") for a in arms)
    matched = abba and all(checks.values()) and all(not a["issues"] for a in arms)
    pairs = []
    for i in range(0, len(arms) - 1, 2):
        by_mode = {a["arm"]: a for a in arms[i:i+2]}
        if set(by_mode) == {"hardcoded", "calibrated"}:
            rates = [by_mode[k].get("completed_input_per_wall_second") for k in ("hardcoded", "calibrated")]
            if all(rates):
                both_triggered = all(a.get("standard_triggers", 0) > 0 for a in by_mode.values())
                both_scaled = all(a.get("applied") for a in by_mode.values())
                used_online = any(t["applied_model"] == "online" for t in by_mode["calibrated"]["applied"])
                pairs.append({"pair": i // 2 + 1, "policy_rate_change_percent": 100 * (rates[1] / rates[0] - 1),
                              "both_triggered": both_triggered, "both_applied": both_scaled,
                              "online_applied_with_qualified_fit": used_online,
                              "exercised_online_resize": both_triggered and both_scaled and used_online})
    other_recoveries = any(a.get("unexpected_restores") or a.get("unexpected_job_start_epochs_ms") for a in arms)
    qualified = matched and not other_recoveries and len(pairs) == 2 and all(p["exercised_online_resize"] for p in pairs)
    change = median(p["policy_rate_change_percent"] for p in pairs) if pairs else None
    return {"abba_order": order, "complete_abba": abba, "match": checks, "any_verified_resize": exercised,
            "additional_observed_recoveries": other_recoveries,
            "matched_policy_comparison_pass": matched, "online_resize_comparison_pass": qualified,
            "qualification_pass": qualified, "pairs": pairs,
            "matched_policy_paired_median_rate_change_percent": change if matched else None,
            "paired_median_rate_change_percent": change if qualified else None, "arms": arms}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", type=Path, nargs="+", help="Arm directories, or a parent containing them")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    paths = {p for d in args.directories for p in ([d] if (d / "plan.json").exists() else [x.parent for x in d.rglob("plan.json")])}
    if not paths:
        parser.error("No arm with plan.json found")
    report = compare([analyze(path) for path in sorted(paths)])
    data = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(data)
    else:
        print(data, end="")


if __name__ == "__main__":
    main()
