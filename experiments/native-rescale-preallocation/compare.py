#!/usr/bin/env python3
"""Compare only matched native rescale arms, retaining incomplete decisions."""

import argparse
import json
from pathlib import Path
from statistics import median


METRICS = (
    "decision_to_all_running_seconds",
    "execution_interruption_upper_seconds",
    "observed_positive_progress_gap_seconds",
    "sustained_throughput_recovery_seconds",
    "old_task_termination_seconds",
    "replacement_graph_creation_seconds",
    "checkpoint_restore_seconds",
)


def arm(path):
    data = json.loads(path.read_text())
    lab = path.parent / "lab"
    spec = json.loads((lab / "execution-spec.json").read_text())
    observer = json.loads((lab / "scout/native-observer/run.json").read_text())
    release = json.loads((lab / "release-receipt.json").read_text())
    deployment = json.loads((lab / "scout/deployment.json").read_text().replace(spec["run"], "RUN"))
    seed = json.loads((lab / "scout/admission/seed/target.json").read_text())
    config = deployment["spec"]["flinkConfiguration"]
    flag = config.pop("job.autoscaler.native-rescale.preallocation.enabled")
    config.pop("$internal.pipeline.job-id")
    args = deployment["spec"]["job"]["args"]
    args.pop(args.index("--q8-release-epoch-ms") + 1)
    return {
        "summary": data, "spec": spec, "flag": flag,
        "comparison_identity": (deployment, release, seed, observer["transitions"]),
    }


def difference(a, b):
    return b - a if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None


def wait_after_termination(row):
    stages = row.get("stage_timestamps_ms", {})
    if not stages.get("OLD_TASKS_TERMINATED") or not stages.get("GRAPH_CREATE_REQUESTED"):
        return None
    return (stages["GRAPH_CREATE_REQUESTED"][0] - stages["OLD_TASKS_TERMINATED"][0]) / 1000


def initialization_after_restore(row):
    stages = row.get("stage_timestamps_ms", {})
    restored = stages.get("CHECKPOINT_RESTORE_END", [])
    running = stages.get("ALL_TARGET_TASKS_RUNNING", [])
    return (running[0] - restored[0]) / 1000 if restored and running else None


def compare_pair(baseline, treatment):
    if (baseline["flag"], treatment["flag"]) != ("false", "true"):
        raise ValueError("provide a disabled arm followed by an enabled arm")
    if baseline["comparison_identity"] != treatment["comparison_identity"]:
        raise ValueError("release, starting profiles, schedule, or deployment settings differ")
    a, b = (entry["summary"] for entry in (baseline, treatment))
    a_decisions = {row["scheduled_offset_seconds"]: row for row in a["decisions"]}
    b_decisions = {row["scheduled_offset_seconds"]: row for row in b["decisions"]}
    planned = [offset for offset, _ in baseline["comparison_identity"][-1]]
    rows = []
    for offset in planned:
        old, new = a_decisions.get(offset, {}), b_decisions.get(offset, {})
        old_metrics = {key: old.get(key) for key in METRICS}
        new_metrics = {key: new.get(key) for key in METRICS}
        old_metrics.update(
            wait_after_termination_seconds=wait_after_termination(old),
            task_initialization_after_restore_seconds=initialization_after_restore(old),
            peak_datastream_pending_records=old.get("datastream_pending_records", {}).get("peak_records"),
        )
        new_metrics.update(
            wait_after_termination_seconds=wait_after_termination(new),
            task_initialization_after_restore_seconds=initialization_after_restore(new),
            peak_datastream_pending_records=new.get("datastream_pending_records", {}).get("peak_records"),
        )
        rows.append({
            "offset_seconds": offset,
            "disabled_result": old.get("result", "unobserved"),
            "enabled_result": new.get("result", "unobserved"),
            "disabled": old_metrics,
            "enabled": new_metrics,
            "enabled_minus_disabled": {key: difference(old_metrics[key], new_metrics[key])
                                      for key in old_metrics},
            "enabled_preparation_requests": new.get("extra_requests_during_preparation"),
        })
    return {
        "disabled_run": a["run"], "enabled_run": b["run"],
        "disabled_resource_time": a["scheduled_flink_pod_requests"],
        "enabled_resource_time": b["scheduled_flink_pod_requests"],
        "decisions": rows,
    }


def summarize(pairs):
    offsets = sorted({row["offset_seconds"] for pair in pairs for row in pair["decisions"]})
    aggregate = []
    for offset in offsets:
        rows = [row for pair in pairs for row in pair["decisions"] if row["offset_seconds"] == offset]
        statistics = {}
        for key in rows[0]["enabled_minus_disabled"]:
            values = [row["enabled_minus_disabled"][key] for row in rows
                      if row["enabled_minus_disabled"][key] is not None
                      and row["enabled_result"] == row["disabled_result"] == "applied"]
            arm_values = {
                arm: [row[arm][key] for row in rows
                      if row[f"{arm}_result"] == "applied" and row[arm][key] is not None]
                for arm in ("disabled", "enabled")
            }
            statistics[key] = {
                **{f"median_{arm}": median(observed) if observed else None
                   for arm, observed in arm_values.items()},
                "pairs_with_timings": len(values),
                "median_enabled_minus_disabled": median(values) if values else None,
                "range": [min(values), max(values)] if values else None,
            }
        aggregate.append({
            "offset_seconds": offset,
            "matched_pairs": len(rows),
            "enabled_applied": sum(row["enabled_result"] == "applied" for row in rows),
            "disabled_applied": sum(row["disabled_result"] == "applied" for row in rows),
            "paired_observations": statistics,
        })
    return {"pairs": pairs, "summary": aggregate}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", action="append", nargs=2, type=Path, required=True,
                        metavar=("DISABLED_JSON", "ENABLED_JSON"))
    args = parser.parse_args()
    pairs = [compare_pair(arm(disabled), arm(enabled)) for disabled, enabled in args.pair]
    print(json.dumps(summarize(pairs), indent=2))


if __name__ == "__main__":
    main()
