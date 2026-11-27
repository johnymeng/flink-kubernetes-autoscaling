#!/usr/bin/env python3
"""Score operator-side A4S HTTP phases for archived crossover windows."""

import argparse
import json
import re
from datetime import datetime
from math import ceil
from pathlib import Path


TIMING = re.compile(
    r"A4S runner timing job=(\S+) snapshot=(\S+) requestBytes=(\d+) responseBytes=(\d+) "
    r"encodeNanos=(\d+) exchangeNanos=(\d+) decodeNanos=(\d+)"
)


def read(path):
    return json.loads(path.read_text())


def client_timings(path, job):
    timings = {}
    with path.open() as stream:
        for line in stream:
            match = TIMING.search(line)
            if not match or match[1] != job:
                continue
            snapshot = match[2]
            if snapshot in timings:
                raise ValueError(f"Duplicate client timing for snapshot {snapshot}")
            stamp = datetime.fromisoformat(line.split(maxsplit=1)[0].replace("Z", "+00:00"))
            request, response, encode, exchange, decode = map(int, match.groups()[2:])
            timings[snapshot] = {
                "snapshotId": snapshot,
                "completedAtMs": int(stamp.replace(microsecond=0).timestamp()) * 1000 + stamp.microsecond // 1000,
                "requestBytes": request,
                "responseBytes": response,
                "encodeMs": encode / 1_000_000,
                "exchangeMs": exchange / 1_000_000,
                "decodeMs": decode / 1_000_000,
                "totalMs": (encode + exchange + decode) / 1_000_000,
            }
    return timings


def score(bundle):
    scout = bundle / "scout"
    spec = read(bundle / "execution-spec.json")
    job = spec["job_id"]
    crossover = scout / "crossover"
    windows = sorted(crossover.glob("[0-9][0-9]-*"))
    if not (crossover / "complete").is_file() or len(windows) != len(spec["crossover_order"]):
        raise ValueError("The archived crossover is incomplete")
    timings = client_timings(scout / "operator.log", job)
    measured = {}
    no_recommendation = 0
    for window, mode in zip(windows, spec["crossover_order"]):
        if mode not in ("embedded", "remote") or read(window / "planned.json")["mode"] != mode:
            raise ValueError(f"Unexpected mode in window {window.name}")
        start, end = (
            read(window / f"{name}.json")["latency"]["observedAtMs"]
            for name in ("before", "after")
        )
        if start >= end:
            raise ValueError(f"Invalid observation bounds for {window.name}")
        with (window / "a4s-requests.jsonl").open() as stream:
            for record in map(json.loads, stream):
                request = record["request"]
                if not start <= request["evaluatedAtMillis"] <= end:
                    continue
                if request["snapshot"]["jobId"] != job:
                    raise ValueError(f"Unexpected job in window {window.name}")
                snapshot = request["snapshot"]["snapshotId"]
                timing = timings.get(snapshot)
                if mode == "embedded":
                    if timing is not None:
                        raise ValueError(f"Embedded snapshot has a client timing: {snapshot}")
                    continue
                if record["recommendation"] is None:
                    no_recommendation += 1
                    continue
                if timing is None or snapshot in measured:
                    raise ValueError(f"Missing or reused remote client timing: {snapshot}")
                measured[snapshot] = {**timing, "window": window.name, "windowEndMs": end,
                                      "evaluatedAtMs": request["evaluatedAtMillis"]}
    if not measured:
        raise ValueError("No measured remote recommendations with client timings")
    samples = list(measured.values())
    summary = {}
    for phase in ("encodeMs", "exchangeMs", "decodeMs", "totalMs"):
        values = sorted(sample[phase] for sample in samples)
        summary[phase] = {f"p{rank}": values[ceil(len(values) * rank / 100) - 1]
                          for rank in (50, 95, 99)}
    return {
        "run": spec["run"],
        "measuredRemoteRecommendations": len(samples),
        "remoteEvaluationsWithoutRecommendation": no_recommendation,
        "completedAfterWindow": sum(s["completedAtMs"] > s["windowEndMs"] for s in samples),
        "firstClientTiming": min(timings.values(), key=lambda s: s["completedAtMs"]),
        "bytes": {key: {"min": min(s[key] for s in samples), "max": max(s[key] for s in samples)}
                  for key in ("requestBytes", "responseBytes")},
        "phasesMs": summary,
        "samples": samples,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundles", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps([score(bundle) for bundle in args.bundles], indent=2))
    except ValueError as error:
        parser.exit(1, f"{error}\n")
