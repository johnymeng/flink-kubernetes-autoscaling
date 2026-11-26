#!/usr/bin/env python3
"""Convert Q20 broker observations to synchronized Figure 10 input and explicit coverage."""

import argparse
import csv
import json
from pathlib import Path

CONSUMED = {"nexmark-auction", "nexmark-bid"}
PRODUCED = CONSUMED | {"nexmark-person"}
PARTITIONS = {(topic, partition) for topic in PRODUCED for partition in range(24)}


def observe(previous, sample):
    keys = [(row["topic"], row["partition"]) for row in sample["partitions"]]
    if len(keys) != len(PARTITIONS) or set(keys) != PARTITIONS:
        raise ValueError("Q20 offset snapshot does not contain exactly the declared partitions")

    midpoint = (sample["before_ms"] + sample["after_ms"]) / 2
    ends = {key: row["end"] for key, row in zip(keys, sample["partitions"], strict=True)}
    rate = None
    if any(offset is None or offset < 0 for offset in ends.values()):
        state = None
    else:
        state = midpoint, ends
        if previous and 0 < midpoint - previous[0] <= 10_000:
            changes = [ends[key] - previous[1][key] for key in PARTITIONS]
            if all(change >= 0 for change in changes):
                rate = sum(changes) / ((midpoint - previous[0]) / 1000)

    gaps = [
        row["end"] - row["committed"]
        if row["end"] is not None and row["committed"] is not None and row["committed"] >= 0
        else None
        for row in sample["partitions"] if row["topic"] in CONSUMED
    ]
    lag = sum(gaps) if all(gap is not None and gap >= 0 for gap in gaps) else None
    return state, {
        "timestamp_epoch": midpoint / 1000,
        "kafka_log_end_increments_per_second": rate,
        "kafka_committed_consumer_lag_offsets": lag,
    }


def convert(lab, output):
    spec = json.loads((lab / "execution-spec.json").read_text())
    if spec["query"] != "q20_unique":
        raise ValueError("expected the isolated Q20 benchmark")
    clock = json.loads((lab / "scout/broker-clock.json").read_text())
    before = clock["controller_before_ms"]
    after = clock["controller_after_ms"]
    if not before - 250 <= clock["broker_ms"] <= after + 250:
        raise ValueError("broker clock is outside the alignment tolerance")
    release = spec["prefix_or_timed_release_ms"]
    cutoff = spec["run_deadline_ms"]
    rows = []
    previous = None
    last = None
    for line in (lab / "scout/broker-offsets.jsonl").read_text().splitlines():
        sample = json.loads(line)
        observed = sample["before_ms"]
        if not release <= sample["scheduled_ms"] <= observed <= sample["after_ms"] or observed >= cutoff:
            raise ValueError("Kafka offset sample is outside the timed window")
        if last is not None and observed <= last:
            raise ValueError("Kafka offset sample timestamps are out of order")
        last = observed
        previous, row = observe(previous, sample)
        rows.append(row)
    if not rows:
        raise ValueError("no Kafka offset samples")
    index = {"run_id": spec["run"], "environment": spec["worker"],
             "autoscaler": "preallocation" if spec["arm"] == "true" else "immediate"}
    query = output / "nexmark/q20"
    run = query / "_".join(index.values())
    run.mkdir(parents=True, exist_ok=False)
    with (query / "runs.csv").open("x", newline="") as file:
        writer = csv.DictWriter(file, [*index, "run_commit"])
        writer.writeheader()
        writer.writerow({**index, "run_commit": spec["source"][:8]})
    with (run / "samples.csv").open("x", newline="") as file:
        writer = csv.DictWriter(file, rows[0])
        writer.writeheader()
        for row in rows:
            writer.writerow({key: value if value is not None else "nan" for key, value in row.items()})
    coverage = {
        "samples": len(rows),
        "observed_arrival_rate_samples": sum(row["kafka_log_end_increments_per_second"] is not None for row in rows),
        "observed_committed_lag_samples": sum(row["kafka_committed_consumer_lag_offsets"] is not None for row in rows),
        "clock_check": clock,
        "run": spec["run"],
    }
    (run / "coverage.json").write_text(json.dumps(coverage, indent=2) + "\n")
    return coverage


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lab", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(convert(args.lab, args.output), indent=2))
