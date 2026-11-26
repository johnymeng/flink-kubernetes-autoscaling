#!/usr/bin/env python3
"""Capture an isolated Nexmark Q8 run and optionally send scheduled native targets."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PREALLOCATION = "job.autoscaler.native-rescale.preallocation.enabled"
SCALING = "job.autoscaler.scaling.enabled"


def request(rest, path, payload=None):
    started = time.time_ns()
    body = json.dumps(payload).encode() if payload is not None else None
    http_request = Request(
        rest + path,
        data=body,
        headers={"Content-Type": "application/json"} if body is not None else {},
        method="PUT" if body is not None else "GET",
    )
    try:
        with urlopen(http_request, timeout=3) as response:
            data = response.read()
            return {"started_ns": started, "ended_ns": time.time_ns(),
                    "status": response.status, "body": json.loads(data) if data else None}
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        return {"started_ns": started, "ended_ns": time.time_ns(),
                "status": getattr(error, "code", None), "error": str(error)}


def checked(request_result):
    if "error" in request_result:
        raise RuntimeError(request_result)
    return request_result["body"]


def transition(value):
    offset, parallelism = map(int, value.split(":"))
    if offset < 0 or parallelism < 1:
        raise ValueError("expected a non-negative offset and positive parallelism")
    return offset, parallelism


def target_for(seed, stateful_vertex, parallelism):
    target = {}
    for vertex, requirement in seed.items():
        bounds = dict(requirement["parallelism"])
        if vertex == stateful_vertex:
            bounds["lowerBound"] = min(bounds["lowerBound"], parallelism)
            bounds["upperBound"] = parallelism
        target[vertex] = {"parallelism": bounds,
                          "resourceProfile": requirement["resourceProfile"]}
    return target


def job_is_running(job):
    return job["state"] == "RUNNING" and all(
        vertex["tasks"]["RUNNING"] == vertex["parallelism"] for vertex in job["vertices"]
    )


def execute(args):
    job_path = "/jobs/" + args.job
    native_path = job_path + "/justin"
    configuration = {
        row["key"]: row["value"]
        for row in checked(request(args.rest, "/jobmanager/config"))
    }
    if configuration.get(PREALLOCATION, "").lower() != args.preallocation:
        raise RuntimeError("JobManager pre-allocation setting differs from the requested arm")
    if args.transition and configuration.get(SCALING, "").lower() != "false":
        raise RuntimeError("disable automatic scaling before submitting manual decisions")
    job = checked(request(args.rest, job_path))
    if job["jid"] != args.job or not job["name"].startswith("Nexmark Query8"):
        raise RuntimeError("unexpected job for the Q8 protocol")
    if not job_is_running(job):
        raise RuntimeError("wait for all initial subtasks to run before starting the timed window")
    vertices = {vertex["id"] for vertex in job["vertices"]}
    if args.stateful_vertex not in vertices or not set(args.source_vertex) < vertices:
        raise RuntimeError("vertex IDs do not match the verified Q8 graph")

    requirements = checked(request(args.rest, native_path + "?includeAppliedProfile=true"))
    if set(requirements) != vertices:
        raise RuntimeError("native profile response is incomplete")
    for _, parallelism in args.transition:
        vertex = next(v for v in job["vertices"] if v["id"] == args.stateful_vertex)
        if parallelism > vertex["maxParallelism"]:
            raise RuntimeError("requested parallelism exceeds the job's max parallelism")
    if args.transition and any(
        requirement.get("appliedResourceProfile") != requirement["resourceProfile"]
        for requirement in requirements.values()
    ):
        raise RuntimeError("establish concrete requested and applied profiles before timing")

    args.output.mkdir(parents=True, exist_ok=False)
    protocol = {
        "job": args.job,
        "revision": args.revision,
        "stateful_vertex": args.stateful_vertex,
        "source_vertices": args.source_vertex,
        "effective_configuration": {key: configuration.get(key) for key in (PREALLOCATION, SCALING)},
        "offered_rate_per_second": args.offered_rate,
        "duration_seconds": args.duration,
        "sample_interval_seconds": args.interval,
        "transitions": sorted(args.transition),
        "initial_requirements": requirements,
    }
    (args.output / "run.json").write_text(json.dumps(protocol, indent=2) + "\n")
    paths = {"job": job_path, "workers": "/taskmanagers", "overview": "/overview",
             "native": native_path + "?includeAppliedProfile=true"}
    paths.update({vertex: job_path + "/vertices/" + vertex for vertex in vertices})

    if args.start_epoch is not None:
        delay = args.start_epoch - time.time()
        if delay <= 0:
            raise RuntimeError("the sealed input release timestamp has already passed")
        time.sleep(delay)

    started = time.monotonic()
    transitions = iter(sorted(args.transition))
    pending = next(transitions, None)
    sample_number = 0
    with (args.output / "telemetry.jsonl").open("x", buffering=1) as samples, (
        args.output / "decisions.jsonl").open("x", buffering=1) as decisions, ThreadPoolExecutor(
        max_workers=len(paths) + 1
    ) as executor:
        while started + sample_number * args.interval < started + args.duration:
            sample_time = started + sample_number * args.interval
            time.sleep(max(0, sample_time - time.monotonic()))
            if time.monotonic() >= started + args.duration:
                break
            elapsed = time.monotonic() - started
            while pending is not None and pending[0] <= elapsed:
                offset, parallelism = pending
                target = target_for(requirements, args.stateful_vertex, parallelism)
                result = request(args.rest, native_path, target)
                decisions.write(json.dumps({"scheduled_offset_seconds": offset,
                                            "target": target, "response": result}) + "\n")
                pending = next(transitions, None)
            current_paths = dict(paths)
            if sample_number % 5 == 0:
                current_paths["checkpoints"] = job_path + "/checkpoints"
            round_start = time.time_ns()
            futures = {name: executor.submit(request, args.rest, path)
                       for name, path in current_paths.items()}
            samples.write(json.dumps({"started_ns": round_start, "offset_seconds": elapsed,
                                      "responses": {name: future.result()
                                                    for name, future in futures.items()}}) + "\n")
            sample_number += 1
            # Keep gaps when requests overrun. Do not take catch-up samples.
            sample_number = max(sample_number, int((time.monotonic() - started) / args.interval) + 1)
    (args.output / "window-ended.json").write_text(json.dumps({
        "cutoff_ns": time.time_ns(), "elapsed_seconds": time.monotonic() - started,
        "next_unsent_transition": pending,
    }) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rest", required=True, help="Flink REST URL for the isolated job")
    parser.add_argument("--job", required=True, help="verified 32-character Flink JobID")
    parser.add_argument("--stateful-vertex", required=True)
    parser.add_argument("--source-vertex", required=True, action="append")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--preallocation", choices=("true", "false"), required=True)
    parser.add_argument("--offered-rate", type=int, default=80_000)
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--start-epoch", type=float, help="sealed input release time, in seconds")
    parser.add_argument("--transition", type=transition, action="append", default=[],
                        metavar="ELAPSED_SECONDS:PARALLELISM")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.rest = args.rest.rstrip("/")
    if not re.fullmatch(r"[0-9a-f]{32}", args.job):
        parser.error("invalid Flink JobID")
    if args.duration <= 0 or args.interval <= 0 or args.offered_rate <= 0:
        parser.error("duration, interval and offered rate must be positive")
    if any(offset >= args.duration for offset, _ in args.transition):
        parser.error("transitions must be scheduled within the timed window")
    execute(args)


if __name__ == "__main__":
    main()
