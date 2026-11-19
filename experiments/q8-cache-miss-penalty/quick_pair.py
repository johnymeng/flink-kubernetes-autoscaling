"""Run two source-pinned Q8 autoscalers against short identical bounded inputs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import time
from zipfile import ZipFile

import run


# Preserve the original warmup so the live estimator can qualify before scaling.
# Both sources continue through the expected savepoint, then drain finite input.
PLAN = "95:1800:6200,26:8000:32000,100:200:700,5:3000:12000"
CONFIG = {"job.autoscaler.a4s.memory-base.mb": "32"}
MODES = {"hardcoded": "main", "calibrated": "branch"}


def value_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def controller(args, arm):
    mode = MODES[arm]
    classpath = getattr(args, mode + "_classpath")
    policy = getattr(args, mode + "_policy").resolve()
    revision = getattr(args, mode + "_revision")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError(f"Expected full source commit for {mode}, got {revision!r}")
    marker = "a4s-quick-literal-main"
    if arm == "hardcoded":
        with ZipFile(policy) as jar:
            has_marker = marker in jar.namelist()
        if has_marker != (args.main_empty_curves == "literal"):
            raise ValueError(f"Main driver marker {marker!r} disagrees with --main-empty-curves "
                             f"{args.main_empty_curves}; check --main-policy")
    else:
        has_marker = None
    directory = args.output / "controllers" / arm
    directory.mkdir(parents=True)
    jars = run.classpath_hashes(classpath)
    run.save(directory / "classpath.json", jars)
    loaded = run.verify_classpath(directory, classpath, policy, legacy=arm == "hardcoded")
    origins = loaded["origins"]
    if Path(origins["org.apache.flink.autoscaler.RyanQ8Policy"]["path"]) != policy:
        raise ValueError(f"{mode} controller loaded an unexpected policy driver")
    empty_curves = args.main_empty_curves if arm == "hardcoded" else "branch-native"
    return {"mode": mode, "revision": revision, "empty_curve_handling": empty_curves,
            "literal_main_marker": has_marker, "classpath": classpath,
            "controller_classpath_sha256": value_hash(jars), "jars": jars,
            "policy": str(policy), "policy_sha256": run.digest(policy), "loaded": loaded}


def configure_manifest(manifest, scenario):
    spec = manifest["spec"]
    config = spec["flinkConfiguration"]
    config.update(CONFIG)
    if scenario != "two-slot-80k":
        return
    config.update({
        "benchmark.q8.initial-slot-sharing.enabled": "true",
        # The Justin scheduler needs every proposed slot before it can place the graph.
        "jobmanager.adaptive-scheduler.resource-stabilization-timeout": "300 s",
        "taskmanager.numberOfTaskSlots": "2",
        "taskmanager.cpu.cores": "2",
        "taskmanager.memory.managed.size": "316m",
        "job.autoscaler.vertex.max-parallelism": config["pipeline.max-parallelism"],
    })
    spec["taskManager"]["resource"]["cpu"] = 2
    containers = spec["taskManager"]["podTemplate"]["spec"]["containers"]
    container = next(item for item in containers if item["name"] == "flink-main-container")
    container["env"].append({"name": "FLINK_Q8_REPORT_PENDING_RECORDS", "value": "1"})


def check_shared_start_artifacts(artifacts):
    required = {
        "savepoint-profiles.jar": ("org/apache/flink/runtime/scheduler/adaptive/AdaptiveScheduler.class",
                                   b"benchmark.q8.initial-slot-sharing.enabled"),
        "Query8.jar": ("be/uclouvain/gepiciad/sources/Q8ReplaySource.class", b"FLINK_Q8_REPORT_PENDING_RECORDS"),
    }
    for jar, (entry, flag) in required.items():
        with ZipFile(artifacts / jar) as archive:
            if entry not in archive.namelist() or flag not in archive.read(entry):
                raise ValueError(f"{jar} needs the two-slot benchmark build; see QUICK.md")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    for mode in ("main", "branch"):
        parser.add_argument("--" + mode + "-classpath", required=True)
        parser.add_argument("--" + mode + "-policy", type=Path, required=True)
        parser.add_argument("--" + mode + "-revision", required=True)
    parser.add_argument("--main-empty-curves", choices=("literal", "omit-empty"), default="literal",
                        help="Run raw main on the real telemetry; omit-empty compares placement after filtering empty source curves")
    parser.add_argument("--worker", choices=("c167", "c182"), default="c167")
    parser.add_argument("--order", choices=("parallel", "AB", "BA"), default="parallel",
                        help="Simultaneous on one worker by default, AB or BA for sequential timing")
    parser.add_argument("--scenario", choices=("burst", "two-slot-80k"), default="burst")
    parser.add_argument("--plan", help="Override the input phases for the burst scenario")
    parser.add_argument("--release-lead-seconds", type=int, default=125)
    parser.add_argument("--observation-seconds", type=int, default=360,
                        help="Observe from each source release for this many seconds; 0 drains all sealed input")
    parser.add_argument("--timeout", type=int, default=550, help="Seconds from each job becoming RUNNING")
    args = parser.parse_args()
    if min(args.release_lead_seconds, args.timeout) <= 0 or args.observation_seconds < 0:
        parser.error("Release lead and timeout must be positive; observation seconds must be nonnegative")
    if args.scenario == "two-slot-80k":
        if args.plan:
            parser.error("The two-slot scenario sets its sustained 80,000 records/s schedule")
        args.plan = f"{args.observation_seconds or 360}:8000:32000"
        check_shared_start_artifacts(args.artifacts)
    elif args.plan is None:
        args.plan = PLAN
    args.output = args.output.resolve()
    args.output.mkdir()
    started = time.time()
    hashes = {name: run.digest(args.artifacts / name) for name in run.NAMES}
    controllers = {arm: controller(args, arm) for arm in MODES}
    run.save(args.output / "controllers.json", controllers)
    evaluator = "org.apache.flink.autoscaler.ScalingMetricEvaluator"
    if (controllers["hardcoded"]["loaded"]["origins"][evaluator]["sha256"]
            == controllers["calibrated"]["loaded"]["origins"][evaluator]["sha256"]):
        raise ValueError("The main and branch JVMs loaded the same autoscaler JAR")
    template = run.render("deployment.json", {"WORKER": args.worker, "PLAN": args.plan,
                          **{"HASH_" + key: value for key, value in hashes.items()}})
    configure_manifest(template, args.scenario)
    template_hash = value_hash(template)
    order = "BA" if args.order == "BA" else "AB"
    arms = ["hardcoded", "calibrated"] if order == "AB" else ["calibrated", "hardcoded"]
    pair = args.output / ("pair-1-" + order)
    pair.mkdir()
    created = []
    owner = "r8pen-" + datetime.now(timezone.utc).strftime("%H%M%S")
    result = {"execution_order": args.order, "arm_names": arms, "owner": owner, "worker": args.worker,
              "input_plan": args.plan, "observation_seconds": args.observation_seconds, "main_empty_curves": args.main_empty_curves, "started_epoch": started}
    if args.scenario == "two-slot-80k":
        result.update(scenario=args.scenario, offered_records_per_second=80000,
                      expected_initial_occupied_slots=2, expected_initial_total_slots=2,
                      expected_initial_join_parallelism=1, expected_initial_job_slot_managed_mib=158,
                      expected_initial_managed_pool_bytes_per_taskmanager=316 * 1024**2)
    run.save(pair / "pair.json", result)
    try:
        run.stage_objects(owner, args.worker, args.artifacts, hashes, pair, created)
        release_ms = round(time.time() * 1000) + args.release_lead_seconds * 1000 if args.order == "parallel" else None
        result["common_release_ms"] = release_ms
        run.save(pair / "pair.json", result)

        def run_arm(arm):
            record = controllers[arm]
            options = argparse.Namespace(**vars(args), policy=Path(record["policy"]), classpath=record["classpath"])
            run.arm_run(options, owner, arm, hashes, record["controller_classpath_sha256"],
                        template_hash, pair, release_ms=release_ms,
                        configure=lambda manifest: configure_manifest(manifest, args.scenario),
                        extra_observer=run.HERE / "quick_series.py", observation_seconds=args.observation_seconds,
                        main_empty_curves=args.main_empty_curves if arm == "hardcoded" else None)

        if args.order == "parallel":
            with ThreadPoolExecutor(max_workers=2) as executor:
                list(executor.map(run_arm, arms))
        else:
            for arm in arms:
                run_arm(arm)
    finally:
        run.cleanup_infra(owner, created, pair)
        result["finished_epoch"] = time.time()
        result["total_wall_seconds_including_setup_and_cleanup"] = result["finished_epoch"] - started
        run.save(pair / "pair.json", result)


if __name__ == "__main__":
    main()
