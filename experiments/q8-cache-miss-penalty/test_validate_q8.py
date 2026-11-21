import copy
import json
import os
from pathlib import Path
import unittest

import validate_q8 as validation


ARCHIVES = os.environ.get("Q8_VALIDATION_ARCHIVES")


def sample(subtask, epoch, final, misses=500):
    events = 1000 if final else 0
    misses = misses if final else 0
    return {"subtask": subtask, "job_id": "job", "vertex_id": "join", "attempt": f"attempt-{subtask}",
            "process": f"worker-{subtask}", "instrumentation": "ABI3-latency",
            "cache_id": 1, "capacity_generation": 0, "ownership_generation": 0,
            "owners": [1], "observation_schema": 2, "timing_abi": 3,
            validation.CAPACITY: 8 << 20, "sequence": 2 if final else 1,
            "native_monotonic_nanos": 61_000_000_000 if final else 1_000_000_000,
            "acquired_epoch": epoch, "acquisition_skew_nanos": 1000,
            "histogram_count_unit": "test_estimator_events", "bins": [events - misses],
            "counters": {"lookups": events, "misses": misses, "complete_misses": misses,
                         "compaction_hits": 0, "compaction_misses": 100 if final and misses >= 100 else 0,
                         "records_in": events, "busy_nanos": 200_000 if final else 0,
                         "histogram_events": events}}


def example():
    before, after = [sample(0, 200, False)], [sample(0, 260, True)]
    context = {"lookup_population": "all_cache_lookups", "capacity_definition": validation.CAPACITY,
               "instrumentation": "ABI3-latency"}
    training = {"training_start_epoch": 100, "training_end_epoch": 180, "available_epoch": 190}
    prior = {**training, "fit_identity": validation.cache_identity(before[0]), "seconds_per_input": 2.1e-7}
    return {
        "trial_id": "trial-a", "case": "current_anchor", "phase": "steady",
        "job_id": "job", "vertex_id": "join", **context,
        "start_epoch": 200, "end_epoch": 260, "expected_subtasks": [0],
        "before": before, "after": after,
        "mrc_prediction": {**training, **context, "prediction_parallelism": 1,
                           "by_subtask": {"0": {validation.CAPACITY: 8 << 20, "miss_fraction": .25,
                                                "validated_support_bytes": [4 << 20, 16 << 20],
                                                "feasible_miss_ceiling": .3}}},
        "fit": {**training, **context, "phase": "steady",
                "by_subtask": {"0": {"fit_identity": validation.cache_identity(before[0]),
                                     "residual_seconds_per_input": 1e-7,
                                     "slope_seconds_per_input_per_unit_miss_fraction": 1e-7,
                                     "recent_busy_input_baseline": prior}}},
    }


class OfflineValidationTest(unittest.TestCase):
    def test_units_and_oracle_are_independent_of_forecast(self):
        report = validation.validate_windows([example()])
        result, = report["cost"]
        scores = result["record_weighted_scores"]
        self.assertAlmostEqual(scores["actual_busy_seconds_per_input"], 2e-7)
        self.assertAlmostEqual(scores["actual_miss_oracle_seconds_per_input"], 1.5e-7)
        self.assertAlmostEqual(scores["forecast_mrc_combined_seconds_per_input"], 1.25e-7)
        self.assertAlmostEqual(scores["fixed_actual_miss_seconds_per_input"], 5e-6 + .004995 * .5)
        self.assertAlmostEqual(scores["recent_busy_baseline_seconds_per_input"], 2.1e-7)
        self.assertEqual(result["subtasks"]["0"]["lookups_per_input"], 1)
        self.assertAlmostEqual(result["subtasks"]["0"]["foreground_miss_share"], .8)
        self.assertIsNone(result["uncertainty"])

    def test_no_future_training_or_forecast_available_after_start(self):
        window = example()
        window["fit"]["training_end_epoch"] = 210
        window["fit"]["available_epoch"] = 220
        window["mrc_prediction"]["available_epoch"] = 201
        report = validation.validate_windows([window])
        self.assertEqual(report["cost"][0]["reason"], "fit_overlaps_or_follows_evaluation")
        self.assertEqual(report["mrc"][0]["reason"], "forecast_not_available_before_evaluation")
        self.assertEqual(report["telemetry"][0]["status"], "valid_counters")

    def test_sample_count_cannot_supply_uncertainty(self):
        window = example()
        window["fit"]["uncertainty"] = {"sample_count": 64}
        self.assertIsNone(validation.validate_windows([window])["cost"][0]["uncertainty"])

    def test_current_anchor_error_and_false_feasibility_stay_visible(self):
        report = validation.validate_windows([example()])
        anchor = report["mrc"][0]["subtasks"]["0"]
        self.assertEqual(anchor["anchor_error_percentage_points"], 25)
        self.assertEqual(anchor["feasibility_boundary"], "false_feasible")
        window = example()
        window["mrc_prediction"]["lookup_population"] = "foreground_cache_lookups"
        self.assertEqual(validation.validate_windows([window])["mrc"][0]["reason"],
                         "forecast_population_or_units_mismatch")

    def test_trace_independent_from_fit_and_ownership_or_histogram_errors(self):
        window = example()
        window["after"][0]["bins"] = [1]
        self.assertEqual(validation.telemetry(window)["reason"], "histogram_event_conservation_mismatch")
        window = example()
        window["after"][0]["ownership_generation"] += 1
        self.assertEqual(validation.telemetry(window)["reason"], "identity_or_ownership_changed")
        window = example()
        window["after"].append(copy.deepcopy(window["after"][0]))
        self.assertEqual(validation.telemetry(window)["reason"], "incomplete_or_duplicate_subtasks")
        self.assertIsNone(validation.native_trace_evidence([])["trace_and_concurrency_checks_observed"])

    def test_later_phase_or_new_cache_remains_a_diagnostic(self):
        window = example()
        window["case"], window["phase"] = "later_phase", "write-sensitive"
        result = validation.validate_windows([window])
        self.assertEqual(result["cost"][0]["fit_scope"], "outside_fit_scope_diagnostic")
        self.assertIsNone(result["mrc"][0]["subtasks"]["0"]["anchor_error_percentage_points"])

    def test_overlapping_windows_are_not_trial_repetitions(self):
        first = example()
        overlapping = {**first, "start_epoch": 240, "end_epoch": 300}
        another_trial = {**first, "trial_id": "trial-b"}
        result = validation.dependence([first, overlapping, another_trial])
        self.assertEqual(result["distinct_recorded_trials"], 2)
        self.assertEqual(result["overlapping_windows"], [{"trial_id": "trial-a", "window_indices": [0, 1]}])
        self.assertFalse(result["independence_established_by_identifiers_alone"])

    def test_heterogeneous_pooled_132_us_is_not_the_paired_150_us(self):
        local = [validation.cost_value((99e-6, 10e-6), .1), validation.cost_value((110e-6, 100e-6), .9)]
        pooled = validation.cost_value((104.5e-6, 55e-6), .5)
        scored = validation.record_weighted_cost([{"records": 1_000_000, "seconds_per_input": cost}
                                                  for cost in local])
        self.assertAlmostEqual(pooled * 1e6, 132)
        self.assertAlmostEqual(scored * 1e6, 150)

    def test_measured_resource_coverage_is_metric_specific(self):
        intervals = [{"start_epoch": 10, "end_epoch": 20, "memory_bytes_seconds": 300,
                      "cpu_seconds": 5},
                     {"start_epoch": 20, "end_epoch": 30, "memory_bytes_seconds": 360}]
        self.assertEqual(validation.resource_integral(intervals, 10, 30, "memory_bytes_seconds"), 660)
        self.assertIsNone(validation.resource_integral(intervals, 10, 30, "cpu_seconds"))
        self.assertIsNone(validation.resource_integral(intervals, 10, 40, "memory_bytes_seconds"))
        self.assertIsNone(validation.resource_integral([*intervals, intervals[0]], 10, 30, "memory_bytes_seconds"))

    def test_identical_action_abba_remains_a_control(self):
        archive = Path(__file__).parent / "results/2026-11-19.json"
        report = validation.control_report(validation.read(archive))
        self.assertEqual(report["historical"]["classification"], "same_action_control")
        self.assertFalse(report["allocation_superiority_established"])
        self.assertIsNone(report["historical"]["cpu_seconds"])

    def test_controller_consumes_recorded_timing_and_measured_resources(self):
        observed = {"measurement_start_epoch": 30, "measurement_end_epoch": 40,
                    "decision_evidence": {"request_composition": "non_learned"},
                    "timeline": {"linked": [{"decision_id": "stable", "acknowledged_epoch": 35}]},
                    "completed_records": 17, "observed_backlog": 4, "slo_outcome": "missed",
                    "measured_resource_intervals": [{"start_epoch": 30, "end_epoch": 40,
                                                     "memory_bytes_seconds": 1000, "cpu_seconds": 2}]}
        arm = validation.control_report(serialized={"online": observed})["current"]["online"]
        self.assertEqual(arm["memory_bytes_seconds"], 1000)
        self.assertEqual(arm["cpu_seconds"], 2)
        self.assertEqual(arm["completed_records"], 17)
        self.assertEqual(arm["transition_timing"], observed["timeline"])
        self.assertEqual(arm["decision_evidence"], observed["decision_evidence"])

    def test_missing_archived_counter_inputs_are_unavailable(self):
        self.assertEqual(validation.replay_q8(None)["status"], "unavailable")


@unittest.skipUnless(ARCHIVES, "archived raw inputs were not supplied")
class ArchivedReplayTest(unittest.TestCase):
    root = Path(ARCHIVES) if ARCHIVES else None

    def test_q8_exact_counter_endpoint_and_prior_fit_replay(self):
        result = validation.replay_q8(self.root / "q8-transfer.json",
                                      self.root / "resource-snapshots.jsonl.gz", self.root / "policy.jsonl")
        for row, actual, prediction in zip(result["windows"], (40.5275, 307.7621, 10263.7882),
                                           (41.4691, 45.2425, 322.8179)):
            self.assertAlmostEqual(row["observed_busy_seconds_per_input"] * 1e6, actual, delta=.00005)
            self.assertAlmostEqual(row["oracle_diagnostics"][0]["actual_miss_oracle_seconds_per_input"] * 1e6,
                                   prediction, delta=.00005)
            self.assertIsNone(row["oracle_diagnostics"][0]["forecast_mrc_combined_seconds_per_input"])
        first = result["windows"][0]
        self.assertLess(first["recent_busy_input_baseline"]["observed_end_epoch"], first["start_epoch"])
        self.assertGreater(first["recent_busy_input_baseline"]["records_in"], 0)
        self.assertEqual(result["windows"][0]["oracle_diagnostics"][0]["fit_scope"], "same_recorded_context")
        self.assertEqual(result["windows"][1]["oracle_diagnostics"][0]["fit_scope"],
                         "out_of_scope_for_active_same_p_control")

    def test_q20_error_cancellation_and_archived_input_provenance(self):
        result = validation.replay_q20(self.root / "q20-calibration.json", self.root / "q20-progress.json",
                                       self.root / "q20-raw")
        self.assertAlmostEqual(result["forecast_mrc_combined_rmse_seconds_per_input"] * 1e6, 4.4954, delta=.00005)
        self.assertAlmostEqual(result["actual_miss_oracle_rmse_seconds_per_input"] * 1e6, 8.3800, delta=.00005)
        self.assertTrue(result["combined_error_cancellation_observed"])
        self.assertTrue(result["provenance"]["cases"]["h08a"]["all_forecast_inputs_before_holdout"])
        self.assertIsNone(result["prior_forecast_chronology_verified"])

    def test_prompt_03_trace_evidence_has_conservation_and_concurrency(self):
        report = validation.native_trace_evidence([self.root / "native-green.log", self.root / "native-disabled.log"])
        self.assertTrue(report["trace_and_concurrency_checks_observed"])
        self.assertEqual(len(report["transcripts"]), 2)


if __name__ == "__main__":
    unittest.main()
