"""Offline telemetry, miss-rate, cost, and control validation. No policy actuation."""
import argparse
import gzip
import hashlib
import json
from itertools import zip_longest
from math import isfinite, sqrt
from pathlib import Path
from statistics import mean, median

from analyze import compare


CAPACITY = "native_cache_capacity_bytes"
LOOKUP_POPULATIONS = ("all_cache_lookups", "foreground_cache_lookups")
FIXED = (5e-6, 4_995e-6)
# Existing CacheObservation acquisition and cross-process wall clock gates.
ACQUISITION_SKEW_NANOS = 100_000_000
CLOCK_SKEW_SECONDS = 5
COUNTERS = ("lookups", "misses", "complete_misses", "compaction_hits",
            "compaction_misses", "records_in", "busy_nanos")
TEXT_IDENTITY = ("job_id", "vertex_id", "attempt", "process", "instrumentation")
NUMERIC_IDENTITY = ("cache_id", "capacity_generation", "ownership_generation", CAPACITY)
IDENTITY_FIELDS = (*TEXT_IDENTITY, *NUMERIC_IDENTITY, "owners", "observation_schema", "timing_abi")
WINDOW_KINDS = ("current_anchor", "held_out_capacity", "later_phase")
NATIVE_TESTS = (
    "A4SSnapshotsConserveLookupsAcrossBarrierAndLegacyReset",
    "A4SObservationDeltasAreIndependentOfPollingPositions",
    "A4SLookupSnapshotsAreCoherentDuringLookups",
    "A4SSnapshotIdentityAndCapacityGeneration",
)


def finite(value):
    return type(value) in (int, float) and isfinite(value)


def fraction(value):
    return finite(value) and 0 <= value <= 1


def natural(value, minimum=0):
    return type(value) is int and value >= minimum


def read(path):
    return json.loads(path.read_text())


def file_hash(path):
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            checksum.update(block)
    return checksum.hexdigest()


def native_trace_evidence(paths):
    """Consume Prompt 03's recorded tests independently of any fitted model."""
    evidence = []
    for path in paths:
        lines = path.read_text().splitlines()
        outcomes = {
            name: (any(line.startswith("[       OK ] LRUCacheTest." + name + " ") for line in lines)
                   and not any(line.startswith("[  FAILED  ] LRUCacheTest." + name) for line in lines))
            for name in NATIVE_TESTS
        }
        evidence.append({"transcript": path.name, "sha256": file_hash(path), "checks": outcomes,
                         "scope": "historical native test transcript; current process identity unavailable"})
    return {"trace_and_concurrency_checks_observed": all(all(row["checks"].values()) for row in evidence)
            if evidence else None, "transcripts": evidence}


def indexed_samples(samples, expected):
    if not isinstance(samples, list):
        return None
    if any(not isinstance(sample, dict) or not natural(sample.get("subtask")) for sample in samples):
        return None
    result = {str(sample["subtask"]): sample for sample in samples}
    return result if len(result) == len(samples) and set(result) == expected else None


def cache_identity(sample):
    owners = sample.get("owners")
    if (any(not isinstance(sample.get(field), str) or not sample[field] for field in TEXT_IDENTITY)
            or any(not natural(sample.get(field)) for field in NUMERIC_IDENTITY)
            or sample[CAPACITY] <= 0 or sample["cache_id"] <= 0
            or not isinstance(owners, list) or len(owners) != 1
            or owners[0] in (None, "") or type(owners[0]) not in (int, str)
            or sample.get("observation_schema") != 2 or sample.get("timing_abi") != 3):
        return None
    return {name: sample[name] for name in IDENTITY_FIELDS}


def paired_counts(before, after, start, end):
    identity = cache_identity(before)
    if identity is None or identity != cache_identity(after):
        return None, "identity_or_ownership_changed"
    points = [(before, start), (after, end)]
    for sample, boundary in points:
        epoch = sample.get("acquired_epoch")
        if (not finite(epoch) or abs(epoch - boundary) > CLOCK_SKEW_SECONDS
                or not natural(sample.get("acquisition_skew_nanos"))
                or sample["acquisition_skew_nanos"] > ACQUISITION_SKEW_NANOS
                or not natural(sample.get("native_monotonic_nanos"), 1)
                or not natural(sample.get("sequence"), 1)):
            return None, "incomplete_or_unaligned_endpoints"
    if (after["sequence"] <= before["sequence"] or after["acquired_epoch"] <= before["acquired_epoch"]
            or after["native_monotonic_nanos"] <= before["native_monotonic_nanos"]):
        return None, "nonmonotone_observation"
    a, b = (sample.get("counters") for sample, _ in points)
    if any(not isinstance(row, dict) or any(not natural(row.get(key)) for key in COUNTERS) for row in (a, b)):
        return None, "missing_cumulative_counts"
    delta = {key: b[key] - a[key] for key in COUNTERS}
    bins_a, bins_b = (sample.get("bins") for sample, _ in points)
    if any(not isinstance(bins, list) or any(not natural(value) for value in bins) for bins in (bins_a, bins_b)):
        return None, "missing_histogram_bins"
    bin_deltas = [right - left for left, right in zip_longest(bins_a, bins_b, fillvalue=0)]
    if min([*delta.values(), *bin_deltas]) < 0:
        return None, "counter_reset"
    for counts in (a, b, delta):
        if (counts["misses"] > counts["lookups"]
                or counts["compaction_misses"] > counts["misses"]
                or counts["compaction_hits"] > counts["lookups"] - counts["misses"]):
            return None, "invalid_exact_event_accounting"

    histogram_mass = sum(bin_deltas) + delta["complete_misses"]
    estimate_a, estimate_b = (counter.get("histogram_events") for counter in (a, b))
    if natural(estimate_a) and natural(estimate_b):
        # These are estimator-event totals in the same native bin-count units,
        # not a claim that sampled histogram mass equals exact lookups.
        conservation = before.get("histogram_count_unit")
        if (not conservation or conservation != after.get("histogram_count_unit")
                or estimate_b - estimate_a != histogram_mass):
            return None, "histogram_event_conservation_mismatch"
    else:
        conservation = None
    return {"identity": identity, "delta": delta, "histogram_event_mass": histogram_mass,
            "histogram_conservation_unit": conservation}, None


def telemetry(window):
    start, end = window.get("start_epoch"), window.get("end_epoch")
    subtasks = window.get("expected_subtasks")
    population = window.get("lookup_population")
    if (not finite(start) or not finite(end) or end <= start
            or not isinstance(subtasks, list) or not subtasks
            or any(not natural(index) for index in subtasks)
            or set(subtasks) != set(range(len(subtasks)))
            or window.get("capacity_definition") != CAPACITY
            or population not in LOOKUP_POPULATIONS):
        return {"status": "unavailable", "reason": "invalid_window_population_or_capacity_definition"}
    expected = {str(index) for index in subtasks}
    a, b = (indexed_samples(window.get(key), expected) for key in ("before", "after"))
    if a is None or b is None:
        return {"status": "unavailable", "reason": "incomplete_or_duplicate_subtasks"}

    rows, physical = {}, set()
    for index in sorted(expected, key=int):
        row, reason = paired_counts(a[index], b[index], start, end)
        if reason:
            return {"status": "unavailable", "reason": reason, "subtask": int(index)}
        ident, counts = row["identity"], row["delta"]
        cache = ident["process"], ident["cache_id"]
        if (cache in physical or any(ident[key] != window.get(key) for key in
                                     ("job_id", "vertex_id", "instrumentation"))):
            return {"status": "unavailable", "reason": "duplicate_cache_or_incompatible_window_identity"}
        physical.add(cache)
        all_misses = counts["misses"]
        foreground_misses = all_misses - counts["compaction_misses"]
        foreground_lookups = counts["lookups"] - counts["compaction_hits"] - counts["compaction_misses"]
        lookups, misses = ((counts["lookups"], all_misses) if population == LOOKUP_POPULATIONS[0]
                           else (foreground_lookups, foreground_misses))
        if counts["records_in"] <= 0 or lookups <= 0:
            return {"status": "unavailable", "reason": "measured_zero_population", "subtask": int(index)}
        row.update(observed_miss_fraction=misses / lookups,
                   observed_busy_seconds_per_input=counts["busy_nanos"] / 1e9 / counts["records_in"],
                   lookups_per_input=counts["lookups"] / counts["records_in"],
                   foreground_miss_share=foreground_misses / all_misses if all_misses else None)
        rows[index] = row
    return {"status": "valid_counters", "lookup_population": population, "capacity_definition": CAPACITY,
            "subtasks": rows, "all_histogram_mass_checks_observed":
                all(row["histogram_conservation_unit"] is not None for row in rows.values())}


def forecast_is_prior(forecast, start):
    issued = forecast.get("available_epoch")
    begin, end = forecast.get("training_start_epoch"), forecast.get("training_end_epoch")
    return all(finite(t) for t in (begin, end, issued, start)) and begin < end <= issued <= start


def capacity_forecast(window, measured):
    forecast = window.get("mrc_prediction")
    if not forecast or measured["status"] != "valid_counters":
        return {"status": "unavailable", "reason": "forecast_or_measured_counters_unavailable"}
    if not forecast_is_prior(forecast, window["start_epoch"]):
        return {"status": "unavailable", "reason": "forecast_not_available_before_evaluation"}
    if any(forecast.get(field) != window.get(field) for field in
           ("lookup_population", "capacity_definition", "instrumentation")):
        return {"status": "unavailable", "reason": "forecast_population_or_units_mismatch"}
    observed = measured["subtasks"]
    if forecast.get("prediction_parallelism") != len(observed):
        return {"status": "unavailable", "reason": "forecast_targets_different_parallelism"}
    forecasts = forecast.get("by_subtask") or {}
    if (set(observed) != set(forecasts)
            or any(not isinstance(row, dict) or not fraction(row.get("miss_fraction"))
                   for row in forecasts.values())):
        return {"status": "unavailable", "reason": "incomplete_per_cache_forecast"}
    results = {}
    for index, row in observed.items():
        candidate = forecasts[index]
        capacity = row["identity"][CAPACITY]
        if candidate.get(CAPACITY) != capacity:
            return {"status": "unavailable", "reason": "forecast_targets_different_capacity"}
        predicted, actual = candidate["miss_fraction"], row["observed_miss_fraction"]
        support = candidate.get("validated_support_bytes")
        supported = (support[0] <= capacity <= support[1] if isinstance(support, list) and len(support) == 2
                     and all(natural(value, 1) for value in support) and support[0] <= support[1] else None)
        ceiling = candidate.get("feasible_miss_ceiling")
        boundary = None
        if fraction(ceiling):
            boundary = ("false_feasible" if predicted <= ceiling < actual else
                        "false_infeasible" if actual <= ceiling < predicted else "same_side")
        error = 100 * abs(actual - predicted)
        results[index] = {"predicted_miss_fraction": predicted, "observed_miss_fraction": actual,
                          "absolute_error_percentage_points": error,
                          "anchor_error_percentage_points": error if window["case"] == "current_anchor" else None,
                          "capacity_in_validated_support": supported, "feasibility_boundary": boundary}
    return {"status": "scored", "case": window["case"], "subtasks": results,
            "macro_mean_absolute_error_percentage_points":
                mean(row["absolute_error_percentage_points"] for row in results.values())}


def record_weighted_cost(rows):
    """Score paired per-cache models by represented input records."""
    records = sum(row["records"] for row in rows)
    return sum(row["records"] * row["seconds_per_input"] for row in rows) / records


def cost_value(coefficients, miss_fraction):
    return coefficients[0] + coefficients[1] * miss_fraction


def cost_report(window, measured, mrc):
    fit = window.get("fit")
    if not fit or measured["status"] != "valid_counters":
        return {"status": "unavailable", "reason": "fit_or_observation_unavailable"}
    if not forecast_is_prior(fit, window["start_epoch"]):
        return {"status": "unavailable", "reason": "fit_overlaps_or_follows_evaluation"}
    if any(fit.get(field) != window.get(field) for field in ("lookup_population", "capacity_definition", "instrumentation")):
        return {"status": "unavailable", "reason": "fit_population_units_or_instrumentation_mismatch"}
    observed = measured["subtasks"]
    coefficients = fit.get("by_subtask") or {}
    if set(coefficients) != set(observed):
        return {"status": "unavailable", "reason": "unmatched_subtask_coefficients"}
    rows = {}
    phase = window.get("phase")
    same_context = isinstance(phase, str) and bool(phase) and fit.get("phase") == phase
    for index, observation in observed.items():
        params = coefficients[index]
        base = params.get("residual_seconds_per_input")
        slope = params.get("slope_seconds_per_input_per_unit_miss_fraction")
        if not finite(base) or base < 0 or not finite(slope) or slope <= 0:
            return {"status": "unavailable", "reason": "invalid_normalized_cost"}
        same_context &= params.get("fit_identity") == observation["identity"]
        miss = observation["observed_miss_fraction"]
        forecast = (mrc.get("subtasks") or {}).get(index, {}).get("predicted_miss_fraction")
        actual = observation["observed_busy_seconds_per_input"]
        observed_baseline = (window.get("observed_zero_miss_seconds_per_input_by_subtask") or {}).get(index)
        prior = params.get("recent_busy_input_baseline") or {}
        prior_rate = prior.get("seconds_per_input")
        recent = (prior_rate if finite(prior_rate) and prior_rate >= 0
                  and prior.get("fit_identity") == observation["identity"]
                  and forecast_is_prior(prior, window["start_epoch"]) else None)
        rows[index] = {
            "records": observation["delta"]["records_in"], "actual_busy_seconds_per_input": actual,
            "actual_miss_oracle_seconds_per_input": cost_value((base, slope), miss),
            "forecast_mrc_combined_seconds_per_input": cost_value((base, slope), forecast)
                if fraction(forecast) else None,
            "fixed_actual_miss_seconds_per_input": cost_value(FIXED, miss),
            "fixed_forecast_mrc_seconds_per_input": cost_value(FIXED, forecast) if fraction(forecast) else None,
            "recent_busy_baseline_seconds_per_input": recent,
            "baseline_drift_seconds_per_input": observed_baseline - base if finite(observed_baseline) else None,
            "lookups_per_input": observation["lookups_per_input"],
            "foreground_miss_share": observation["foreground_miss_share"],
        }
    fields = ("actual_busy_seconds_per_input", "actual_miss_oracle_seconds_per_input",
              "forecast_mrc_combined_seconds_per_input", "fixed_actual_miss_seconds_per_input",
              "fixed_forecast_mrc_seconds_per_input", "recent_busy_baseline_seconds_per_input")
    aggregate = {field: record_weighted_cost([{"records": row["records"], "seconds_per_input": row[field]}
                                              for row in rows.values()])
                 if all(row[field] is not None for row in rows.values()) else None for field in fields}
    actual = aggregate[fields[0]]
    errors = {field: value - actual if value is not None else None for field, value in aggregate.items()
              if field != fields[0]}
    uncertainty = fit.get("uncertainty")
    bounds = uncertainty.get("bounds_seconds_per_input") if isinstance(uncertainty, dict) else None
    if (not isinstance(bounds, list) or len(bounds) != 2 or any(not finite(value) for value in bounds)
            or bounds[0] > bounds[1] or not uncertainty.get("method") or not uncertainty.get("evidence")):
        uncertainty = None
    support = [row.get("capacity_in_validated_support") for row in (mrc.get("subtasks") or {}).values()]
    supported = False if False in support else True if support and all(value is True for value in support) else None
    return {"status": "scored", "fit_scope": "same_observed_context" if same_context else "outside_fit_scope_diagnostic",
            "subtasks": rows, "record_weighted_scores": aggregate, "signed_prediction_errors": errors,
            "uncertainty": uncertainty, "candidate_support_recorded": supported}


def dependence(windows):
    trials = {}
    unavailable = []
    for index, window in enumerate(windows):
        trial = window.get("trial_id")
        a, b = window.get("start_epoch"), window.get("end_epoch")
        if not isinstance(trial, str) or not trial or not finite(a) or not finite(b) or b <= a:
            unavailable.append(index)
        else:
            trials.setdefault(trial, []).append((a, b, index))
    overlaps = []
    for trial, spans in trials.items():
        ordered = sorted(spans)
        for pos, (start, end, index) in enumerate(ordered):
            for next_start, _, following in ordered[pos + 1:]:
                if next_start >= end:
                    break
                overlaps.append({"trial_id": trial, "window_indices": [index, following]})
    return {"unit_of_repetition": "trial", "distinct_recorded_trials": len(trials),
            "overlapping_windows": overlaps, "unclassified_window_indices": unavailable,
            "independence_established_by_identifiers_alone": False}


def validate_windows(windows):
    result = {"telemetry": [], "mrc": [], "cost": [], "dependence": dependence(windows)}
    for index, window in enumerate(windows):
        key = {"window_index": index, "trial_id": window.get("trial_id"), "case": window.get("case")}
        if window.get("case") not in WINDOW_KINDS:
            window = {**window, "capacity_definition": None}
        measured = telemetry(window)
        forecast = capacity_forecast(window, measured)
        result["telemetry"].append({**key, **measured})
        result["mrc"].append({**key, **forecast})
        result["cost"].append({**key, **cost_report(window, measured, forecast)})
    return result


ARCHIVE_COUNTERS = ("lookups", "misses", "compactionHits", "compactionMisses", "busyMillis", "recordsIn")
ARCHIVE_IDENTITY = ("attempt", "cacheId", "capacityBytes", "cacheEpoch", "timingAbi")


def selected_lines(stream, indices):
    rows = {}
    for line_number, line in enumerate(stream, 1):
        if line_number in indices:
            rows[line_number] = json.loads(line)
            if rows.keys() == indices:
                break
    if rows.keys() != indices:
        raise ValueError("Recorded archive lines are missing")
    return rows


def archived_endpoint_values(row):
    snapshot = row["snapshot"]
    return {
        "elapsed_seconds": row["elapsed_sec"], "capture_epoch_seconds": row["capture_epoch"],
        "job_id": snapshot["job"]["jid"], "job_start_epoch_ms": snapshot["job"]["start-time"],
        "parallelism": snapshot["join_parallelism"],
        "native_samples": sorted(snapshot["native_cache_response"]["cacheCostSamples"],
                                 key=lambda sample: sample["subtask"]),
    }


def q8_measurement_history(snapshot_path, references, starts):
    """Verify referenced endpoints and retain the last two compatible prior samples."""
    wanted = {reference["line"]: key for key, reference in references.items()}
    prior = {key: [] for key in starts}
    endpoints = {}
    with gzip.open(snapshot_path, "rt") as stream:
        for number, line in enumerate(stream, 1):
            row = json.loads(line)
            if number in wanted:
                key = wanted[number]
                endpoint = archived_endpoint_values(row)
                if any(references[key][field] != value for field, value in endpoint.items()) or row.get("late"):
                    raise ValueError("Archived reference does not match its raw snapshot")
                endpoints[key] = endpoint
                if endpoints.keys() == references.keys():
                    break
            if row.get("type") != "sample" or row.get("late") or row.get("arm") != "calibrated":
                continue
            try:
                observed = archived_endpoint_values(row)
            except (KeyError, TypeError):
                continue
            for key in starts:
                target = references[key]
                a, b = observed["native_samples"], target["native_samples"]
                target_time = min(target["capture_epoch_seconds"],
                                  *[sample["observedAtMillis"] / 1000 for sample in b])
                if (observed["capture_epoch_seconds"] >= target_time or len(a) != len(b)
                        or any(observed[field] != target[field] for field in ("job_id", "job_start_epoch_ms", "parallelism"))
                        or any(first.get("subtask") != last["subtask"]
                               or any(first.get(field) != last[field] for field in ARCHIVE_IDENTITY)
                               or not finite(first.get("observedAtMillis"))
                               or first["observedAtMillis"] / 1000 >= target_time
                               for first, last in zip(a, b))):
                    continue
                points = prior[key]
                if any(first["observedAtMillis"] <= last["observedAtMillis"] for first, last in zip(
                        a, points[-1]["native_samples"] if points else [])):
                    continue
                points.append(observed)
                if len(points) > 2:
                    points.pop(0)
    if endpoints.keys() != references.keys():
        raise ValueError("Recorded Q8 archive endpoints are missing")
    return endpoints, prior


def archived_recent_busy_input(points):
    if len(points) != 2:
        return None
    before, after = points
    try:
        counts = archived_deltas(before, after)
    except ValueError:
        return None
    return {"seconds_per_input": counts["busyMillis"] / 1000 / counts["recordsIn"],
            "observed_start_epoch": before["capture_epoch_seconds"], "observed_end_epoch": after["capture_epoch_seconds"],
            "records_in": counts["recordsIn"]}


def archived_deltas(before, after):
    if any(before[key] != after[key] for key in ("job_id", "job_start_epoch_ms", "parallelism")):
        raise ValueError("Archived measurement crosses a topology or execution transition")
    first, last = (row["native_samples"] for row in (before, after))
    expected = list(range(before["parallelism"]))
    if any([row["subtask"] for row in samples] != expected for samples in (first, last)):
        raise ValueError("Incomplete archived measurement population")
    totals = dict.fromkeys(ARCHIVE_COUNTERS, 0)
    for a, b in zip(first, last):
        if (any(a[key] != b[key] for key in ARCHIVE_IDENTITY) or not a["attempt"]
                or not natural(a["cacheId"], 1) or not natural(a["capacityBytes"], 1)
                or a["timingAbi"] != 3 or b["observedAtMillis"] <= a["observedAtMillis"]):
            raise ValueError("Archived endpoints have incompatible cache identity or timestamps")
        delta = {key: b[key] - a[key] for key in ARCHIVE_COUNTERS}
        if (any(not finite(value) or value < 0 for value in delta.values())
                or not 0 <= delta["compactionMisses"] <= delta["misses"] <= delta["lookups"]
                or not 0 <= delta["compactionHits"] <= delta["lookups"] - delta["misses"]
                or not delta["recordsIn"] or not delta["lookups"]):
            raise ValueError("Archived measured deltas are unavailable or inconsistent")
        for key, value in delta.items():
            totals[key] += value
    return totals


def replay_q8(reference_path, snapshot_path=None, policy_path=None):
    if snapshot_path is None or policy_path is None:
        return {"status": "unavailable", "missing": [field for field, path in (
            ("original_snapshot_archive", snapshot_path), ("original_policy_log", policy_path)) if path is None]}
    reference = read(reference_path)
    receipts = {"resource-snapshots.jsonl.gz": file_hash(snapshot_path), "policy.jsonl": file_hash(policy_path)}
    if receipts != reference.get("input_sha256"):
        raise ValueError("Q8 raw archive hashes differ from the extraction receipt")
    starts = {str(window["start_tick_seconds"]) for window in reference["windows"]}
    endpoints, prior = q8_measurement_history(snapshot_path, reference["native_endpoints"], starts)
    with policy_path.open() as stream:
        models = selected_lines(stream, {int(index) for index in reference["policy_selections"]})

    results = []
    for window in reference["windows"]:
        before, after = (endpoints[str(window[field])] for field in ("start_tick_seconds", "end_tick_seconds"))
        counts = archived_deltas(before, after)
        actual = counts["busyMillis"] / 1000 / counts["recordsIn"]
        miss = counts["misses"] / counts["lookups"]
        recent = archived_recent_busy_input(prior[str(window["start_tick_seconds"])])
        predictions = []
        for comparison in window["comparisons"]:
            line = comparison["policy_line"]
            model = models[line]
            if model != reference["policy_selections"][str(line)] or model["selected_model"] != "online":
                raise ValueError("Q8 model receipt differs from the archived prior selection")
            # Wall chronology and the saved native endpoint both precede the
            # evaluation. Process-local monotonic clocks are absent here.
            start = min(before["capture_epoch_seconds"],
                        *[row["observedAtMillis"] / 1000 for row in before["native_samples"]])
            if model["epoch"] > start or any(row["observedAtMillis"] / 1000 > start for row in model["subtasks"]):
                raise ValueError("Q8 model was not available before the archived evaluation")
            coeff = (model["zero_miss_seconds_per_record"], model["miss_penalty_seconds_per_record"])
            prediction = cost_value(coeff, miss)
            same = (model["job_id"] == before["job_id"] and model["parallelism"] == before["parallelism"]
                    and model["job_start_epoch_ms"] == before["job_start_epoch_ms"]
                    and len(model["subtasks"]) == before["parallelism"] and all(
                        all(a[key] == b[key] for key in ARCHIVE_IDENTITY)
                        for a, b in zip(sorted(model["subtasks"], key=lambda row: row["subtask"]),
                                        before["native_samples"])))
            retrospect = window.get("retrospective_fit") or {}
            zero_miss_us = retrospect.get("zero_miss_us_per_record")
            predictions.append({
                "policy_line": line, "available_epoch": model["epoch"],
                "training_parallelism": model["parallelism"],
                "fit_scope": "same_recorded_context" if same else "out_of_scope_for_active_same_p_control",
                "actual_miss_oracle_seconds_per_input": prediction,
                "oracle_error_seconds_per_input": prediction - actual,
                "relative_error": (prediction - actual) / actual if actual else None,
                "baseline_drift_seconds_per_input": zero_miss_us * 1e-6 - coeff[0] if finite(zero_miss_us) else None,
                "forecast_mrc_combined_seconds_per_input": None,
                "recent_busy_baseline_seconds_per_input": recent["seconds_per_input"] if recent else None,
                "uncertainty": None,
            })
        results.append({
            "window": window["window"], "evaluation_parallelism": before["parallelism"],
            "lookup_population": "all_cache_lookups", "capacity_definition": CAPACITY,
            "start_epoch": before["capture_epoch_seconds"], "end_epoch": after["capture_epoch_seconds"],
            "observed_busy_seconds_per_input": actual,
            "observed_miss_fraction": miss, "lookups_per_input": counts["lookups"] / counts["recordsIn"],
            "foreground_miss_share": (counts["misses"] - counts["compactionMisses"]) / counts["misses"]
                if counts["misses"] else None,
            "fixed_actual_miss_seconds_per_input": cost_value(FIXED, miss), "oracle_diagnostics": predictions,
            "recent_busy_input_baseline": recent,
        })
    return {"status": "historical_oracle_scored", "raw_input_sha256": receipts, "windows": results,
            "missing_for_current_validation": ["native_process_and_owner_identity", "aligned_histogram_mass",
                                               "before_window_mrc_forecast",
                                               "counterfactual_capacity_support", "forecast_uncertainty"]}


def q20_bracket(sample):
    return {"rest_before": sample["rest_before"], "rest_after": sample["rest_after"],
            "native_before_ms": sample["native"]["before_ms"], "native_after_ms": sample["native"]["after_ms"],
            "counters": {"records": sample["task"]["records"], "busy_total_ms": sample["task"]["busy_total_ms"],
                         "lookups": sample["native"]["lookups"], "misses": sample["native"]["misses"]}}


def q20_archive_provenance(archive, progress_path, raw_root):
    """Check the archived measured brackets and the earlier MRC source points."""
    if progress_path is None or raw_root is None:
        return {"status": "unavailable", "reason": "progress_receipt_or_original_traces_missing"}
    progress = read(progress_path)
    cases = progress["cases"]
    root = raw_root.resolve()
    measurements, forecasts, hashes = {}, {}, {}
    for case_id, case in cases.items():
        samples = {}
        forecast_points = {}
        for trace in case["source_traces"]:
            parts = Path(trace["path"]).parts
            if "runs" not in parts:
                raise ValueError("Q20 trace receipt lacks the raw runs path")
            path = root.joinpath(*parts[parts.index("runs"):]).resolve()
            if not path.is_relative_to(root):
                raise ValueError("Q20 trace receipt points outside its raw archive")
            hashes[str(path.relative_to(root))] = file_hash(path)
            count = 0
            with path.open() as stream:
                for line in stream:
                    sample = json.loads(line)
                    if sample.get("type") != "sample":
                        continue
                    if sample["job_id"] != case["job_id"] or sample["owner"] != case["owner"]:
                        raise ValueError("Q20 raw sample has a different job or cache owner")
                    samples[sample["rest_before"], sample["rest_after"]] = q20_bracket(sample)
                    mrc = sample.get("mrc") or {}
                    if finite(mrc.get("after")):
                        forecast_points[mrc["after"]] = {(point["cacheSizeBytes"], point["missRate"])
                                                         for point in mrc.get("points", [])}
                    count += 1
            if count != trace["raw_samples"]:
                raise ValueError("Q20 raw trace sample count differs from the progress receipt")
        brackets = [item for label in ("start", "end") for item in case[label]["bracket"]]
        if any(samples.get((item["rest_before"], item["rest_after"])) != item for item in brackets):
            raise ValueError("Q20 progress bracket does not match original sampled counters")
        measurements[case_id] = {"job_id": case["job_id"],
                                  "start_bracket_epoch": [item["rest_before"] for item in case["start"]["bracket"]],
                                  "end_bracket_epoch": [item["rest_after"] for item in case["end"]["bracket"]]}
        forecasts[case_id] = forecast_points
    heldout = progress["holdout_cases"]
    if len(heldout) != len(archive["held_out_window_details"]):
        raise ValueError("Q20 holdout population differs from the calibration receipt")
    training = [case for case in cases if case not in heldout]
    training_end = max(measurements[key]["end_bracket_epoch"][-1] for key in training)
    for key, row in zip(heldout, archive["held_out_window_details"]):
        candidate = cases[key]
        if (candidate["records"] != row["records"] or candidate["lookups"] != row["lookups"]
                or candidate["miss_fraction"] != row["observed_miss_rate"]
                or candidate["busy_seconds_per_record"] != row["observed_service_sec_per_record"]):
            raise ValueError("Q20 calibration window differs from the recorded progress scoring")
        source = candidate["forecast_case"]
        points = candidate["forecast_points"]
        first = measurements[key]["start_bracket_epoch"][0]
        if (source == key or cases[source]["job_id"] != candidate["forecast_job_id"]
                or any((capacity, miss) not in forecasts[source].get(epoch, set())
                       for capacity, miss, epoch in points)
                or not points or median(point[1] for point in points) != row["predicted_miss_rate"]):
            raise ValueError("Q20 forecast point receipt differs from its archived MRC inputs")
        measurements[key]["all_forecast_inputs_before_holdout"] = (
            max(point[2] for point in points) < first and training_end < first)
    return {"status": "archive_endpoint_brackets_verified",
            "window_convention": "recorded historical interpolation; no new interpolation",
            "cases": measurements, "raw_trace_sha256": hashes,
            "progress_receipt_sha256": file_hash(progress_path),
            "fit_or_forecast_issued_at_epoch": None}


def replay_q20(path, progress_path=None, raw_root=None):
    archive = read(path)
    coeff = (archive["hit_sec_per_record"], archive["miss_penalty_sec_per_record"])
    rows = []
    for record in archive["held_out_window_details"]:
        actual = record["observed_service_sec_per_record"]
        miss, forecast = record["observed_miss_rate"], record["predicted_miss_rate"]
        if not finite(actual) or not fraction(miss) or not fraction(forecast):
            raise ValueError("Held-out Q20 scoring inputs are incomplete")
        oracle, combined = (cost_value(coeff, value) for value in (miss, forecast))
        rows.append({
            "actual_seconds_per_input": actual,
            "observed_miss_fraction": miss, "recorded_forecast_miss_fraction": forecast,
            "absolute_mrc_error_percentage_points": 100 * abs(miss - forecast),
            "actual_miss_oracle_error_seconds_per_input": oracle - actual,
            "forecast_mrc_combined_error_seconds_per_input": combined - actual,
            "mrc_contribution_seconds_per_input": combined - oracle,
            "fixed_actual_miss_error_seconds_per_input": cost_value(FIXED, miss) - actual,
            "fixed_forecast_mrc_error_seconds_per_input": cost_value(FIXED, forecast) - actual,
            "lookups_per_input": record.get("lookups_per_record"),
        })
    if not rows:
        raise ValueError("No Q20 held-out scoring inputs")
    rmses = {field.replace("_error_", "_rmse_"): sqrt(mean(row[field] ** 2 for row in rows))
             for field in rows[0] if "_error_seconds_per_input" in field}
    cross_term = 2 * mean(row["actual_miss_oracle_error_seconds_per_input"] *
                          row["mrc_contribution_seconds_per_input"] for row in rows)
    provenance = q20_archive_provenance(archive, progress_path, raw_root)
    missing = ["fit_and_prediction_availability_times", "native_event_mass", "current_observation_schema"]
    if provenance["status"] == "unavailable":
        missing += ["raw_counter_endpoint_brackets", "training_and_holdout_timestamps"]
    return {"status": "historical_window_diagnostic", "windows": rows, **rmses, "provenance": provenance,
            "twice_mean_oracle_times_mrc_error_seconds_squared_per_input_squared": cross_term,
            "combined_error_cancellation_observed": cross_term < 0 and
                rmses["forecast_mrc_combined_rmse_seconds_per_input"] < rmses["actual_miss_oracle_rmse_seconds_per_input"],
            "prior_forecast_chronology_verified": None, "independent_trial_count": None,
            "recent_busy_baseline_seconds_per_input": None, "foreground_miss_share": None, "uncertainty": None,
            "missing_for_current_validation": missing}


def resource_integral(intervals, start, end, name):
    """Accept only measured integrals over explicit intervals with full coverage."""
    if not finite(start) or not finite(end) or end <= start:
        return None
    if not intervals or any(not finite(row.get("start_epoch")) or not finite(row.get("end_epoch"))
                            or row["end_epoch"] <= row["start_epoch"]
                            or not finite(row.get(name)) or row[name] < 0 for row in intervals):
        return None
    ordered = sorted(intervals, key=lambda row: row["start_epoch"])
    if (ordered[0]["start_epoch"] != start or ordered[-1]["end_epoch"] != end
            or any(a["end_epoch"] != b["start_epoch"] for a, b in zip(ordered, ordered[1:]))):
        return None
    return sum(row[name] for row in ordered)


def control_report(archived=None, serialized=None):
    result = {"allocation_superiority_established": False,
              "policy_replay_method": "consume recorded actions or active checker outputs"}
    if archived is not None:
        replay = compare(archived["arms"])
        result["historical"] = {
            "classification": replay["classification"],
            "matched_policy_comparison": replay["matched_policy_comparison_pass"],
            "same_observed_actions": replay["same_observed_actions"],
            "matched_policy_paired_median_rate_change_percent":
                replay["matched_policy_paired_median_rate_change_percent"],
            "decisions_by_arm": replay["decisions_by_arm"],
            "completed_input_records_by_arm": [arm.get("completed_input") for arm in replay["arms"]],
            "observed_backlog_peak_by_arm": [arm.get("sampled_backlog_peak") for arm in replay["arms"]],
            "memory_bytes_seconds": None, "cpu_seconds": None, "slo_outcome": None,
        }
    current = {}
    for name, arm in (serialized or {}).items():
        policy = arm.get("decision_evidence") or {}
        resources = arm.get("measured_resource_intervals") or []
        start, end = arm.get("measurement_start_epoch"), arm.get("measurement_end_epoch")
        current[name] = {
            "decision_evidence": policy or None, "comparison_windows": arm.get("comparison_windows"),
            "paired_window_rankings": arm.get("paired_window_rankings"),
            "same_observed_actions": arm.get("same_observed_actions"),
            "transition_timing": arm.get("timeline"), "completed_records": arm.get("completed_records"),
            "observed_backlog": arm.get("observed_backlog"), "slo_outcome": arm.get("slo_outcome"),
            "memory_bytes_seconds": resource_integral(resources, start, end, "memory_bytes_seconds"),
            "cpu_seconds": resource_integral(resources, start, end, "cpu_seconds"),
        }
    result["current"] = current or None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--windows", type=Path, help="Version 1 window observations and prior predictions")
    parser.add_argument("--native-trace", type=Path, action="append", default=[])
    parser.add_argument("--archive-q8", type=Path)
    parser.add_argument("--q8-snapshots", type=Path)
    parser.add_argument("--q8-policy", type=Path)
    parser.add_argument("--archive-q20", type=Path)
    parser.add_argument("--q20-progress", type=Path)
    parser.add_argument("--q20-raw-root", type=Path)
    parser.add_argument("--archive-abba", type=Path)
    parser.add_argument("--serialized-controller", type=Path)
    parser.add_argument("--output", type=Path, required=True, help="New validation receipt")
    args = parser.parse_args()
    try:
        window_data = read(args.windows) if args.windows else {"schema_version": 1, "windows": []}
        if window_data.get("schema_version") != 1 or not isinstance(window_data.get("windows"), list):
            raise ValueError("Expected version 1 window data")
        paths = [args.windows, args.archive_q8, args.q8_snapshots, args.q8_policy, args.archive_q20,
                 args.q20_progress, args.archive_abba, args.serialized_controller, *args.native_trace]
        report = {
            "schema_version": 1, "scope": "offline_validation", "live_validation_verified": False,
            "units": {"cost": "seconds/input", "miss_fraction": "fraction", "miss_error": "percentage_points",
                      "memory_integral": "byte_seconds", "cpu_integral": "cpu_seconds"},
            "input_sha256": {path.name: file_hash(path) for path in paths if path},
            "native_telemetry_evidence": native_trace_evidence(args.native_trace),
            "current_windows": validate_windows(window_data["windows"]),
            "q8_archive": replay_q8(args.archive_q8, args.q8_snapshots, args.q8_policy) if args.archive_q8 else None,
            "q20_archive": replay_q20(args.archive_q20, args.q20_progress, args.q20_raw_root)
                    if args.archive_q20 else None,
            "control": control_report(read(args.archive_abba) if args.archive_abba else None,
                                      read(args.serialized_controller) if args.serialized_controller else None),
        }
        content = json.dumps(report, allow_nan=False, sort_keys=True, indent=2)
        with args.output.open("x") as stream:
            stream.write(content + "\n")
    except (KeyError, OSError, ValueError, TypeError, AttributeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
