#!/usr/bin/env python3
"""Independently verify the lookup-weighted MRC and its byte units."""
import argparse
import json
from fractions import Fraction
from pathlib import Path


def reference_points(fixture):
    samples = fixture["observations"]
    actual = {str(task["subtask"]): task["attempt"] for task in samples}
    if len(samples) != len(actual) or actual != fixture["expectedAttempts"]:
        raise ValueError("Missing, duplicate or replaced subtask")
    counts = [task["histogram"] for task in samples]
    widths = {hist["bucketSizeScaling"] * hist["cacheItemSizeBytes"] for hist in counts}
    if len(widths) != 1 or any(hist["numPartitions"] != 1 for hist in counts):
        raise ValueError("Incompatible byte bins")
    width, = widths
    weights = [task["window"]["references"] for task in samples]
    for bound in range(1, min(len(hist["bucketCounts"]) for hist in counts) + 1):
        weighted = sum(
            weight * Fraction(
                hist["completeMisses"] + sum(hist["bucketCounts"][bound:]),
                hist["completeMisses"] + sum(hist["bucketCounts"]),
            )
            for weight, hist in zip(weights, counts)
        )
        yield len(weights) * bound * width, weighted / sum(weights)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    data = json.loads(parser.parse_args().fixture.read_text())
    points = list(reference_points(data))
    expected = data["curve"]
    if len(points) != len(expected):
        raise SystemExit("Curve support differs")
    for (capacity, miss_rate), point in zip(points, expected):
        if capacity != point["cacheSizeBytes"] or abs(float(miss_rate) - point["missRate"]) > 1e-12:
            raise SystemExit(f"Curve differs at {capacity} bytes")
        print(f"{capacity} bytes, miss rate {miss_rate} ({float(miss_rate):.9f})")
    incomplete = {**data, "observations": data["observations"][:-1]}
    try:
        list(reference_points(incomplete))
    except ValueError:
        print("Incomplete subtask set refused")
    else:
        raise SystemExit("Incomplete subtask set accepted")


if __name__ == "__main__":
    main()
