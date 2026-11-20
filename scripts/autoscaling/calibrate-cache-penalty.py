#!/usr/bin/env python3
r"""Calibrate one A4S vertex from matched fixed-parallelism cache trials.

Run from the repository root.
  python3 scripts/autoscaling/calibrate-cache-penalty.py windows.csv \
    --holdout-capacity-bytes 16777216 --vertex-id 0123456789abcdef0123456789abcdef

Provide one CSV row per non-overlapping measurement window.
  capacity_bytes,records,lookups,misses,busy_seconds[,predicted_miss_rate]
Optional evidence columns: trial_id,forecast_at_ms,window_start_ms.
When trial IDs are present, each entire trial must belong to one capacity.
Provide both timestamps for dated forecasts and freeze the prediction before the window.

Counts and busy seconds are deltas summed across the same vertex's subtasks.
Use exact resident counters from the physical LRU lookups that feed the MRC, and hold
parallelism, input, slot layout and storage fixed while varying per-cache
capacity. Busy seconds exclude idle and backpressure time. For each held-out
window supply a miss fraction from an independently computed MRC at that
capacity, without using the observed miss counter to derive the prediction.

Training pools counters within each capacity and weights capacities equally.
It fits busy_seconds / records = hit cost + miss penalty * (misses / lookups).
Both fitted coefficients are expressed in seconds per input record.
An interior holdout tests interpolation. At least five percentage points of
training miss-rate spread are required to avoid fitting from nearly identical
observations. The script emits a flinkConfiguration entry only when calibrated
service-time predictions have strictly lower record-weighted RMSE than legacy
over the held-out windows evaluated with their MRC predictions. Both observed
and predicted holdout miss rates must lie within the training miss-rate range.
Holdout errors are signed as predicted minus observed.
"""

import argparse
import csv
import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REQUIRED_COLUMNS = {"capacity_bytes", "records", "lookups", "misses", "busy_seconds"}
MIN_TRAINING_MISS_RATE_SPREAD = 0.05
LEGACY_HIT_SEC = 5e-6
LEGACY_PENALTY_SEC = 5e-3 - LEGACY_HIT_SEC
CONFIG_KEY = "job.autoscaler.a4s.calibrated-cache-costs"


@dataclass(frozen=True)
class Window:
    capacity_bytes: int
    records: int
    lookups: int
    misses: int
    busy_seconds: float
    predicted_miss_rate: Optional[float] = None
    trial_id: Optional[str] = None
    forecast_at_ms: Optional[int] = None
    window_start_ms: Optional[int] = None

    @property
    def miss_rate(self):
        return self.misses / self.lookups

    @property
    def service_sec_per_record(self):
        return self.busy_seconds / self.records

    @property
    def lookups_per_record(self):
        return self.lookups / self.records


def read_windows(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        rows = csv.DictReader(source, strict=True)
        missing = REQUIRED_COLUMNS - set(rows.fieldnames or ())
        if missing:
            raise ValueError("missing CSV columns: " + ", ".join(sorted(missing)))
        if len(set(rows.fieldnames)) != len(rows.fieldnames):
            raise ValueError("duplicate CSV column names")
        for row in rows:
            try:
                if None in row or any(row[column] is None for column in REQUIRED_COLUMNS):
                    raise ValueError("wrong number of CSV fields")
                predicted = (row.get("predicted_miss_rate") or "").strip()
                forecast_at = (row.get("forecast_at_ms") or "").strip()
                window_start = (row.get("window_start_ms") or "").strip()
                window = Window(
                    capacity_bytes=int(row["capacity_bytes"]),
                    records=int(row["records"]),
                    lookups=int(row["lookups"]),
                    misses=int(row["misses"]),
                    busy_seconds=float(row["busy_seconds"]),
                    predicted_miss_rate=float(predicted) if predicted else None,
                    trial_id=(row.get("trial_id") or "").strip() or None,
                    forecast_at_ms=int(forecast_at) if forecast_at else None,
                    window_start_ms=int(window_start) if window_start else None,
                )
                if window.capacity_bytes < 0 or min(window.records, window.lookups) <= 0:
                    raise ValueError("capacity must be nonnegative and records/lookups positive")
                if not 0 <= window.misses <= window.lookups:
                    raise ValueError("misses must be between zero and lookups")
                if not math.isfinite(window.busy_seconds) or window.busy_seconds <= 0:
                    raise ValueError("busy_seconds must be positive and finite")
                if (
                    window.predicted_miss_rate is not None
                    and not 0 <= window.predicted_miss_rate <= 1
                ):
                    raise ValueError("predicted_miss_rate must be a fraction between zero and one")
                if window.forecast_at_ms is not None or window.window_start_ms is not None:
                    if (
                        window.predicted_miss_rate is None
                        or window.forecast_at_ms is None
                        or window.window_start_ms is None
                        or not 0 <= window.forecast_at_ms <= window.window_start_ms
                    ):
                        raise ValueError("dated forecasts must precede the holdout window")
            except ValueError as error:
                raise ValueError(f"CSV line {rows.line_num}: {error}") from error
            yield window


def pool(capacity, windows):
    return Window(
        capacity,
        sum(window.records for window in windows),
        sum(window.lookups for window in windows),
        sum(window.misses for window in windows),
        math.fsum(window.busy_seconds for window in windows),
    )


def fit(training):
    mean_miss_rate = math.fsum(window.miss_rate for window in training) / len(training)
    mean_service_sec = (
        math.fsum(window.service_sec_per_record for window in training) / len(training)
    )
    min_miss_rate = min(window.miss_rate for window in training)
    max_miss_rate = max(window.miss_rate for window in training)
    spread = max_miss_rate - min_miss_rate
    if spread < MIN_TRAINING_MISS_RATE_SPREAD and not math.isclose(
        spread, MIN_TRAINING_MISS_RATE_SPREAD
    ):
        raise ValueError("training miss-rate spread is less than five percentage points")
    variance = math.fsum((window.miss_rate - mean_miss_rate) ** 2 for window in training)
    covariance = math.fsum(
        (window.miss_rate - mean_miss_rate) * (window.service_sec_per_record - mean_service_sec)
        for window in training
    )
    penalty_sec = covariance / variance
    hit_sec = mean_service_sec - penalty_sec * mean_miss_rate
    if not all(math.isfinite(value) and value > 0 for value in (hit_sec, penalty_sec)):
        raise ValueError("fitted hit cost and miss penalty must be positive and finite")
    return hit_sec, penalty_sec, (min_miss_rate, max_miss_rate)


def weighted_rmse(errors, weights):
    total_weight = sum(weights)
    return math.sqrt(
        math.fsum(weight / total_weight * error**2 for weight, error in zip(weights, errors))
    )


def service_rmse(holdout, miss_rates, hit_sec, penalty_sec):
    errors = (
        hit_sec + penalty_sec * miss_rate - window.service_sec_per_record
        for window, miss_rate in zip(holdout, miss_rates)
    )
    return weighted_rmse(errors, [window.records for window in holdout])


def compare(holdout, miss_rates, hit_sec, penalty_sec):
    return {
        "calibrated_rmse_sec_per_record": service_rmse(holdout, miss_rates, hit_sec, penalty_sec),
        "legacy_rmse_sec_per_record": service_rmse(
            holdout, miss_rates, LEGACY_HIT_SEC, LEGACY_PENALTY_SEC
        ),
    }


def describe_holdout(window, hit_sec, penalty_sec):
    observed_sec = window.service_sec_per_record
    predicted_rate = window.predicted_miss_rate
    details = {
        "records": window.records,
        "lookups": window.lookups,
        "lookups_per_record": window.lookups_per_record,
        "observed_miss_rate": window.miss_rate,
        "observed_service_sec_per_record": observed_sec,
        "predicted_miss_rate": predicted_rate,
        "trial_id": window.trial_id,
        "forecast_at_ms": window.forecast_at_ms,
        "window_start_ms": window.window_start_ms,
        "actual_miss_plug_in_error_sec_per_record": (
            hit_sec + penalty_sec * window.miss_rate - observed_sec
        ),
    }
    if predicted_rate is not None:
        calibrated_sec = hit_sec + penalty_sec * predicted_rate
        legacy_sec = LEGACY_HIT_SEC + LEGACY_PENALTY_SEC * predicted_rate
        details.update(
            mrc_miss_rate_error=predicted_rate - window.miss_rate,
            calibrated_mrc_service_sec_per_record=calibrated_sec,
            calibrated_mrc_error_sec_per_record=calibrated_sec - observed_sec,
            legacy_mrc_service_sec_per_record=legacy_sec,
            legacy_mrc_error_sec_per_record=legacy_sec - observed_sec,
        )
    return details


def calibrate(windows, holdout_capacity, vertex):
    by_capacity = defaultdict(list)
    trial_capacities = {}
    unidentified_trials = False
    for window in windows:
        by_capacity[window.capacity_bytes].append(window)
        if window.trial_id is None:
            unidentified_trials = True
        else:
            capacity = trial_capacities.setdefault(window.trial_id, window.capacity_bytes)
            if capacity != window.capacity_bytes:
                raise ValueError("a trial spans multiple capacities and cannot be split safely")
    if trial_capacities and unidentified_trials:
        raise ValueError("provide trial IDs for every window or use the legacy capacity-only format")
    holdout = by_capacity.pop(holdout_capacity, None)
    if not holdout:
        raise ValueError("no windows found at the requested holdout capacity")
    if not by_capacity or not min(by_capacity) < holdout_capacity < max(by_capacity):
        raise ValueError("holdout must be strictly between two training capacities")

    training = [pool(capacity, values) for capacity, values in sorted(by_capacity.items())]
    hit_sec, penalty_sec, training_range = fit(training)
    min_miss_rate, max_miss_rate = training_range
    observed_rates = [window.miss_rate for window in holdout]
    observed_within_range = all(min_miss_rate <= rate <= max_miss_rate for rate in observed_rates)
    observed_comparison = compare(holdout, observed_rates, hit_sec, penalty_sec)
    observed_comparison["within_training_range"] = observed_within_range
    training_lookup_densities = [
        window.lookups_per_record
        for capacity_windows in by_capacity.values()
        for window in capacity_windows
    ]
    training_receipt = [
        (window.capacity_bytes, window.records, window.lookups, window.misses, window.busy_seconds)
        for window in training
    ]
    model_fingerprint = hashlib.sha256(
        json.dumps([hit_sec, penalty_sec, training_receipt], separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    evidence = {
        "vertex_id": vertex,
        "model_sha256": model_fingerprint,
        "training_split": "whole_trial" if trial_capacities else "capacity_only",
        "training_trial_ids": sorted(
            trial for trial, capacity in trial_capacities.items() if capacity != holdout_capacity
        ),
        "holdout_trial_ids": sorted(
            trial for trial, capacity in trial_capacities.items() if capacity == holdout_capacity
        ),
        "holdout_forecasts_dated": all(window.forecast_at_ms is not None for window in holdout),
        "hit_sec_per_record": hit_sec,
        "miss_penalty_sec_per_record": penalty_sec,
        "legacy_hit_sec_per_record": LEGACY_HIT_SEC,
        "legacy_miss_penalty_sec_per_record": LEGACY_PENALTY_SEC,
        "training_miss_rate_range": training_range,
        "training_lookups_per_record_range": (
            min(training_lookup_densities),
            max(training_lookup_densities),
        ),
        "training": [
            {
                "capacity_bytes": window.capacity_bytes,
                "windows": len(by_capacity[window.capacity_bytes]),
                "records": window.records,
                "lookups": window.lookups,
                "lookups_per_record": window.lookups_per_record,
                "miss_rate": window.miss_rate,
                "service_sec_per_record": window.service_sec_per_record,
            }
            for window in training
        ],
        "held_out_capacity_bytes": holdout_capacity,
        "held_out_windows": len(holdout),
        "held_out_records": sum(window.records for window in holdout),
        "held_out_lookups_per_record_range": (
            min(window.lookups_per_record for window in holdout),
            max(window.lookups_per_record for window in holdout),
        ),
        "held_out_window_details": [
            describe_holdout(window, hit_sec, penalty_sec) for window in holdout
        ],
        "held_out_observed_misses": observed_comparison,
        "held_out_mrc": {"status": "missing_predictions"},
    }
    predictions = [window.predicted_miss_rate for window in holdout]
    if all(prediction is not None for prediction in predictions):
        comparison = compare(holdout, predictions, hit_sec, penalty_sec)
        improved = (
            comparison["calibrated_rmse_sec_per_record"]
            < comparison["legacy_rmse_sec_per_record"]
        )
        predicted_within_range = all(min_miss_rate <= rate <= max_miss_rate for rate in predictions)
        comparison["within_training_range"] = predicted_within_range
        comparison["miss_rate_rmse"] = weighted_rmse(
            (prediction - window.miss_rate for window, prediction in zip(holdout, predictions)),
            [window.lookups for window in holdout],
        )
        evidence["held_out_mrc"] = comparison
        if not (observed_within_range and predicted_within_range):
            comparison["status"] = "outside_training_range"
        elif not improved:
            comparison["status"] = "no_improvement"
        else:
            comparison["status"] = "improved"
            evidence["config_entry"] = {CONFIG_KEY: f"{vertex}:{hit_sec}/{penalty_sec}"}
    return evidence


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("csv", type=Path, help="CSV of aligned fixed-parallelism windows")
    parser.add_argument("--holdout-capacity-bytes", type=int, required=True)
    parser.add_argument("--vertex-id", required=True, help="32 hexadecimal characters")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-fA-F]{32}", args.vertex_id):
        parser.error("--vertex-id must be a Flink JobVertexID with 32 hexadecimal characters")
    try:
        evidence = calibrate(
            read_windows(args.csv), args.holdout_capacity_bytes, args.vertex_id.lower()
        )
        print(json.dumps(evidence, indent=2, allow_nan=False))
    except (OSError, csv.Error, ValueError, OverflowError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
