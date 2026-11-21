"""Regression tests for the distinction between an accepted native request and applied placement."""
from concurrent.futures import ThreadPoolExecutor
import json
import re
from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import unittest
from unittest.mock import patch

from analyze import JOIN, compare
from native_check import decision_evidence, event_times, fallback_failures, kafka_evidence, targets, verified, MIB
from q8_vertices import topology
from quick_series import capture
from native_observe import P_KEY, RP_KEY


class NativeTimingTest(unittest.TestCase):
    def test_rpc_failure_has_a_timestamp_and_preserves_root_cause(self):
        stamp = "1970-01-01T00:00:03.042Z "
        records = fallback_failures([
            stamp + "2026-11-19 NativeFlinkService Error while rescaling, falling back to regular upgrade",
            stamp + "Caused by: java.io.NotSerializableException: JustinVertexResourceRequirements",
            stamp + "Caused by: org.apache.flink.runtime.rest.util.RestClientException: outer wrapper",
            stamp + "2026-11-19 Reconciler Suspending existing deployment"], 1, 9, [2])
        self.assertAlmostEqual(records[0]["report_to_native_failure_s"], 1.042)
        self.assertIn("NotSerializableException", records[0]["cause"])

    def test_reconciled_target_parses_flink_profile_with_commas(self):
        overrides = {P_KEY: f"{JOIN}:2", RP_KEY:
                     f"'{JOIN}:ResourceProfile{{cpuCores=1, managedMemory=32.000mb (33554432 bytes), networkMemory=0 bytes}}'"}
        self.assertEqual(targets(overrides), {"parallelism": {JOIN: 2}, "join_bytes_per_slot": 32 * MIB,
                                             "managed_bytes_by_vertex": {JOIN: 32 * MIB}})

    def test_reset_uses_per_generation_broker_offset_deltas(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            def emit(file, records):
                (folder / file).write_text("".join(json.dumps(r) + "\n" for r in records))
            emit("kafka-generations.jsonl", [{"epoch": t, "generation": g, "event": e,
                                              "offered_records_per_second": 80000, "first_event_id": 1}
                                             for t, g, e in [(0, 0, "start"), (40, 0, "stop"),
                                                             (50, 1, "reset"), (60, 1, "start")]])
            emit("kafka-offsets.jsonl", [{"epoch": t, "generation": g, "topics":
                                       {name: {"0": v * n} for name, n in (("person", 1), ("auction", 3), ("bid", 46))}}
                                       for t, g, v in [(2, 0, 3200), (22, 0, 35200), (61, 1, 1600), (71, 1, 17600)]])
            gens, rate, issues = kafka_evidence(folder, 0, 80)
            self.assertEqual(issues, [])
            self.assertEqual([g["measured_s"] for g in gens], [20, 10])
            self.assertEqual(rate, 80000)

    def test_kafka_source_meter_fits_flink_request_line_with_24_partitions(self):
        prefix = "0.Source__person_kafka[1]."
        meter = prefix + "numRecordsOutPerSecond"
        offsets = [prefix + f"KafkaSourceReader.topic.nexmark-person.partition.{p}.{kind}Offset"
                   for p in range(24) for kind in ("committed", "current")]
        def get(path):
            if path == "/job":
                return {"state": "RUNNING", "vertices": [{"id": "p", "name": "Source: person_kafka", "parallelism": 1}]}
            if path.endswith("/metrics"):
                return [{"id": metric} for metric in [meter, prefix + "numRecordsOut", *offsets]]
            if "?get=" in path:
                self.assertLess(len(path), 500)
                return [{"id": meter, "value": "123.5"}, {"id": prefix + "numRecordsOut", "value": "82"}]
            return {}
        with ThreadPoolExecutor(2) as pool:
            sample = capture(pool, get, "/job", kafka=True)
        self.assertEqual(sample["values"]["source_records_per_second"], 123.5)

    def test_stale_kafka_meters_after_native_resize_leave_a_chart_gap(self):
        from quick_series import confirmed_kafka_rates
        def row(t, persons, auctions, start=1):
            sources = []
            for vid, count in (("p", persons), ("a", auctions)):
                prefix = f"0.Source__{vid}."
                sources.append({"vertex_id": vid, "values": [
                    {"id": prefix + "numRecordsOut", "value": str(count)},
                    {"id": prefix + "numRecordsOutPerSecond", "value": "10"}]})
            return {"elapsed_sec": t, "job_state": "RUNNING", "job_start_epoch_ms": start,
                    "values": {"source_records_per_second": 20}, "sources": sources}
        rows = [row(0, 3, 5), row(5, 3, 5), row(10, 3, 5), row(15, 3, 5),
                row(20, 4, 6), row(25, 5, 6), row(30, 6, 7, start=2), row(35, 7, 8, start=2),
                row(40, 8, 9, start=2)]
        transactions = [{"in_place_ack_from_release_s": 5, "ack_to_running_requested_parallelism_s": 5},
                        {"in_place_ack_from_release_s": 40, "ack_to_running_requested_parallelism_s": None}]
        self.assertEqual(list(confirmed_kafka_rates(rows, transactions)),
                         [20, None, None, None, 20, None, None, 20, None])

    def test_sql_q8_identifies_join_without_datastream_uid(self):
        vertices = [{"id": "p", "name": "Source: person_kafka[1] -> Calc[2]"},
                    {"id": "a", "name": "Source: auction_kafka[7] -> Calc[8]"},
                    {"id": "j", "name": "WindowJoin[13] -> Sink: nexmark_q8[15]"}]
        nodes = [{"id": v["id"], "inputs": [] if v["id"] != "j" else [{"id": "p"}, {"id": "a"}]} for v in vertices]
        sources, join = topology({"vertices": vertices, "plan": {"nodes": nodes}})
        self.assertEqual([v["id"] for v in sources], ["p", "a"])
        self.assertEqual(join, "j")

    def test_observed_tasks_and_slots_distinguish_ack_from_placement(self):
        target = {"parallelism": {JOIN: 2}, "join_bytes_per_slot": 32 * MIB,
                  "managed_bytes_by_vertex": {JOIN: 32 * MIB}}
        sample = {"state": "RUNNING", "parallelism": {JOIN: 2}, "running_tasks": {JOIN: 1},
                  "native": {"0": 1024, "1": 1024}, "join_managed_slot_bytes": 32 * MIB,
                  "managed_bytes_by_vertex": {JOIN: 32 * MIB},
                  "overview": {"slots-total": 4, "slots-available": 1, "taskmanagers": 2},
                  "join_locations": {"0": "tm1", "1": "tm2"},
                  "physical_slots": [{"id": "tm1", "total_slots": 2, "free_slots": 0, "job_slot_managed_mib": [32, 32]},
                                     {"id": "tm2", "total_slots": 2, "free_slots": 1, "job_slot_managed_mib": [32]}]}
        self.assertFalse(verified(sample, target))
        sample["running_tasks"][JOIN] = 2
        self.assertTrue(verified(sample, target))
        sample["native"] = {}
        self.assertTrue(verified(sample, target))
        self.assertFalse(verified(sample | {"join_locations": {"0": "tm1"}}, target))
        self.assertFalse(verified(sample | {"managed_bytes_by_vertex": {}}, target))
        self.assertFalse(verified(sample | {"physical_slots": sample["physical_slots"][:1]}, target))
        # Flink recorded the join's TaskManager, but not its specific slot ID.
        sample["physical_slots"][0]["job_slot_managed_mib"] = [32, 158]
        self.assertFalse(verified(sample, target))
        sample["physical_slots"][0]["job_slot_managed_mib"] = [32, 32]
        sample["physical_slots"][1]["job_slot_managed_mib"] = [158]
        self.assertFalse(verified(sample, target))

    def test_incomplete_requested_memory_cannot_verify_a_resize(self):
        other_vertex = "0" * 32
        overrides = {P_KEY: f"{JOIN}:2,{other_vertex}:1", RP_KEY:
                     f"'{JOIN}:ResourceProfile{{managedMemory=32.000mb (33554432 bytes)}}'"}
        self.assertIsNone(targets(overrides))

    def test_legacy_scaling_reports_do_not_inherit_nearby_fits(self):
        from native_check import join_cost_at_reports
        reports = join_cost_at_reports([
            f"t1 Evaluating metrics for vertex {JOIN}", f"t2 A4S online cache costs for vertex {JOIN}: fit",
            f"t3 Evaluating metrics for vertex {JOIN}", f"t4 No usable miss rate curve found for vertex {JOIN}",
            "t5 SCALINGREPORT Scaling execution enabled",
            "t6 SCALINGREPORT Scaling execution enabled"], JOIN, True)
        self.assertEqual([item["join_cost"] for item in reports], ["unknown", "unknown"])

    def test_duplicate_kubernetes_event_polls_and_aggregate_repeats(self):
        event = {"uid": "evt", "reason": "Scaling", "message": "In-place scaling triggered",
                 "firstTimestamp": "2026-01-01T00:00:00Z", "lastTimestamp": "2026-01-01T00:00:10Z"}
        received = event_times([{"events": [event]}, {"events": [event]}], 0, float("inf"))
        self.assertEqual(len(received), 2)
        self.assertEqual(received[1][0] - received[0][0], 10)



JOB = "f" * 32


def profile(mib):
    return f"ResourceProfile{{cpuCores=1, managedMemory={mib}.000mb ({mib * MIB} bytes)}}"


def requested(mib=192):
    return {"parallelism": {JOIN: 2}, "profiles": {JOIN: profile(mib)}}


def logged_decision(eid, request_id=None, second=20, action="POLICY_REQUEST", mib=192, native_mib=160):
    p, resources = (f"{{{JOIN}=2}}", f"{{{JOIN}={profile(mib)}}}") if request_id else ("null", "null")
    memory = ("{" + JOIN + "=EvaluatedMetrics.ManagedMemoryDecision("
              f"trustedCurrentManagedBytes={mib * MIB}, nominalNativeCacheBytes={native_mib * MIB})" + "}")
    return (f"1970-01-01T00:00:{second:02d}Z A4S cache decision job={JOB} evaluationId={eid} action={action} "
            f"cachePlacementEnabled=true holdReason=null observationHolds={{}} managedMemoryDecisions={memory} "
            f"requestedParallelismOverrides={p} requestedResourceProfileOverrides={resources} "
            f"requestedDecisionId={request_id or 'null'} appliedOverrides=unavailable")


def logged_model(eid, second=20, mode="ONLINE_QUALIFIED", reason="QUALIFIED", fit_reason="QUALIFIED",
                 selected="ONLINE_QUALIFIED", fit_end_ms=19_000, fallback="null",
                 scope="OBSERVED_ALLOCATION"):
    return (f"1970-01-01T00:00:{second:02d}Z A4S cache cost vertex={JOIN} mode={mode} reason={reason} "
            f"evaluationId={eid} selectedCostPolicy={selected} fallbackReason={fallback} "
            "observation=EvaluatedMetrics.ObservationEvidence(status=VALID, expectedSubtasks=2) "
            f"fitReason={fit_reason} measuredFit=OnlineCacheCost.Fit(windowEndMillis={fit_end_ms}) "
            "residualBusySecondsPerInput=4.0E-5 slopeSecondsPerInputPerUnitMissFraction=2.0E-5 "
            f"recommendation=QuickMrcCostProjection.Recommendation(totalNativeCacheBytes={320 * MIB}, "
            f"predictionScope={scope}) fixedCosts={{hitSecondsPerAccess=5.0E-6}}")


def receipt(kind, decision_id="request-1", epoch=21, **data):
    return dict(schema_version=1, kind=kind, job_id=JOB, decision_id=decision_id, epoch=epoch, **data)


def observed_placement(decision_id="request-1", native_mib=160):
    physical = {"join_id": JOIN, "state": "RUNNING", "parallelism": {JOIN: 2}, "running_tasks": {JOIN: 2},
                "join_managed_slot_bytes": 192 * MIB, "managed_bytes_by_vertex": {JOIN: 192 * MIB},
                "join_locations": {"0": "tm1", "1": "tm2"}, "native": {"0": native_mib * MIB, "1": native_mib * MIB},
                "epoch": 24, "capture_end_epoch": 25,
                "physical_slots": [{
                    "id": worker, "total_slots": 1, "free_slots": 0, "job_slot_managed_mib": [192]}
                    for worker in ("tm1", "tm2")],
                "overview": {"taskmanagers": 2, "slots-total": 2, "slots-available": 0}}
    identities = {str(i): {"attempt": f"attempt-{i}", "process": f"process-{i}",
                           "cache_id": 1, "capacity_generation": 0} for i in range(2)}
    return receipt("placement", decision_id=decision_id, epoch=25, observation=physical,
                   cache_identities=identities)


class DecisionEvidenceTest(unittest.TestCase):
    def classify(self, lines, receipts=()):
        return decision_evidence(lines, receipts, job_id=JOB, join_id=JOIN, online_configured=True,
                                 start=0, end=59)

    def test_later_fit_does_not_explain_earlier_request(self):
        lines = [logged_decision("early", "request-1"), logged_decision("later", second=30, action="POLICY_NO_CHANGE"),
                 logged_model("later", second=30, fit_end_ms=29_000),
                 "1970-01-01T00:00:31Z A4S online cache costs for vertex " + JOIN + ": legacy"]
        result = self.classify(lines, [receipt("acknowledgement", accepted=True, request=requested())])
        earlier, later = result["evaluations"]
        self.assertIsNone(earlier["model_used"])
        self.assertIsNone(earlier["model_qualified"])
        self.assertTrue(later["model_qualified"])
        self.assertEqual(result["proven_learned_requests"], 0)
        self.assertIsNone(result["learned_decision_fraction"])

    def test_future_fit_endpoint_in_same_record_is_invalid(self):
        result = self.classify([logged_decision("eval-1", "request-1"), logged_model("eval-1", fit_end_ms=21_000)])
        self.assertFalse(result["evaluations"][0]["model_qualified"])

    def test_first_learned_request_and_second_fixed_fallback_remain_mixed(self):
        lines = [logged_decision("eval-1", "request-1"), logged_model("eval-1"),
                 logged_decision("eval-2", "request-2", second=40),
                 logged_model("eval-2", second=40, mode="ONLINE_SHADOW", fit_reason="MISSING_TIMING",
                              selected="LEGACY_FIXED", fallback="MISSING_TIMING")]
        records = [receipt("acknowledgement", accepted=True, request=requested()), observed_placement(),
                   receipt("outcome", epoch=30, valid=True, evidence="outcome.json")]
        result = self.classify(lines, records)
        learned, fallback = result["evaluations"]
        self.assertTrue(learned["proven_learned_control"])
        self.assertTrue(learned["valid_outcome_observed"])
        self.assertEqual(fallback["model_used"], "fixed_fallback")
        self.assertFalse(fallback["proven_learned_control"])
        self.assertEqual(result["request_composition"], "mixed")
        self.assertEqual(result["learned_decision_fraction"], .5)

    def test_same_snapshot_floors_can_erase_a_recommendation_difference(self):
        digest = "a" * 64
        comparison = receipt("comparison", evaluation_id="eval-1", epoch=28,
                             source="active_policy_replay", decision_input_sha256=digest,
                             fixed={"input_sha256": digest, "recommendation": {"parallelism": 2,
                                    "managed_bytes_per_slot": 32 * MIB}, "final_request": requested(),
                                    "miss_fraction": .4, "slope_seconds_per_input_per_unit_miss_fraction": .004995},
                             online={"input_sha256": digest, "recommendation": {"parallelism": 2,
                                     "managed_bytes_per_slot": 79 * MIB}, "final_request": requested(),
                                     "miss_fraction": .4, "slope_seconds_per_input_per_unit_miss_fraction": .000049})
        result = self.classify([logged_decision("eval-1", "request-1"), logged_model("eval-1")],
                               [receipt("acknowledgement", accepted=True, request=requested()),
                                observed_placement(), comparison])
        row = result["evaluations"][0]
        self.assertTrue(row["recommendation_differs"])
        self.assertFalse(row["final_action_differs"])
        self.assertEqual(row["classification"], "same_action_control")
        self.assertEqual(row["same_snapshot_comparison"]["fixed"]["miss_fraction"], .4)
        comparison["online"]["recommendation"] = dict(comparison["fixed"]["recommendation"], miss_fraction=.3)
        result = self.classify([logged_decision("eval-1", "request-1"), logged_model("eval-1")],
                               [comparison])["evaluations"][0]
        self.assertFalse(result["recommendation_differs"])
        comparison["online"]["input_sha256"] = "b" * 64
        self.assertIsNone(self.classify([logged_decision("eval-1", "request-1"), logged_model("eval-1")],
                                       [comparison])["evaluations"][0]["final_action_differs"])

    def test_reported_realizer_return_and_native_ack_are_separate_from_application(self):
        lines = [logged_decision("eval-1", "request-1"), logged_model("eval-1"),
                 f"1970-01-01T00:00:21Z A4S realizer returned job={JOB} decisionId=request-1 appliedOverrides=unavailable"]
        row = self.classify(lines)["evaluations"][0]
        self.assertEqual(row["request_status"], "realizer_returned")
        self.assertIsNone(row["applied_resources_verified"])
        ack = receipt("acknowledgement", accepted=True, request=requested())
        row = self.classify(lines, [ack])["evaluations"][0]
        self.assertEqual(row["classification"], "acknowledged_unverified")
        self.assertFalse(row["proven_learned_control"])
        wrong_cache = self.classify(lines, [ack, observed_placement(native_mib=159)])["evaluations"][0]
        self.assertTrue(wrong_cache["managed_placement_verified"])
        self.assertFalse(wrong_cache["applied_resources_verified"])
        self.assertFalse(wrong_cache["native_capacity_verified"])
        self.assertFalse(wrong_cache["proven_learned_control"])
        no_cache = observed_placement()
        no_cache["observation"].pop("native")
        unknown = self.classify(lines, [ack, no_cache])["evaluations"][0]
        self.assertTrue(unknown["managed_placement_verified"])
        self.assertIsNone(unknown["applied_resources_verified"])
        self.assertIsNone(unknown["native_capacity_verified"])

    def test_a_second_log_for_the_same_request_does_not_double_count_or_relabel_it(self):
        lines = [logged_decision("eval-1", "request-1"), logged_model("eval-1"),
                 logged_decision("retry", "request-1", second=22), logged_model("retry", second=22)]
        records = [receipt("acknowledgement", accepted=False, request=requested()),
                   receipt("acknowledgement", epoch=23, accepted=True, request=requested()), observed_placement()]
        result = self.classify(lines, records)
        self.assertEqual(result["request_count"], 1)
        self.assertEqual(result["proven_learned_requests"], 1)
        conflict = [*lines, logged_decision("bad-retry", "request-1", second=25, mib=200)]
        self.assertEqual(self.classify(conflict, records)["proven_learned_requests"], 0)

    def test_typed_holds_are_distinct_and_safe_throughput_is_never_learned(self):
        reasons = {"MISSING_OBSERVATIONS": "missing_curve_or_observations", "MISSING_TIMING": "missing_timing_samples",
                   "INSUFFICIENT_HISTORY": "warm_up", "CAPACITY_UNSUPPORTED": "unsupported_capacity"}
        for reason, category in reasons.items():
            with self.subTest(reason=reason):
                result = self.classify([logged_decision("eval-1", action="POLICY_HOLD"),
                                        logged_model("eval-1", reason=reason, fit_reason=reason, selected="null")])
                self.assertEqual(result["evaluations"][0]["classification"], category)
        lines = [logged_decision("eval-1", "request-1", action="THROUGHPUT_SCALE_UP_RETAINED_PROFILES"),
                 logged_model("eval-1")]
        result = self.classify(lines, [receipt("acknowledgement", accepted=True, request=requested()),
                                       observed_placement()])
        row = result["evaluations"][0]
        self.assertEqual(row["model_used"], "throughput_recovery")
        self.assertFalse(row["proven_learned_control"])
        self.assertEqual(result["learned_decision_fraction"], 0)

    def test_advisory_and_shadow_candidates_do_not_become_learned_actions(self):
        for model in (logged_model("eval-1", scope="ADVISORY_CAPACITY_CHANGE"),
                      logged_model("eval-1", mode="ONLINE_SHADOW", selected="LEGACY_FIXED")):
            with self.subTest(model=model):
                row = self.classify([logged_decision("eval-1", "request-1"), model],
                                    [receipt("acknowledgement", accepted=True, request=requested()),
                                     observed_placement()])["evaluations"][0]
                self.assertTrue(row["model_qualified"])
                self.assertNotEqual(row["model_used"], "online")
                self.assertFalse(row["proven_learned_control"])

    def test_actuator_unavailable_is_a_qualified_hold_without_a_request(self):
        result = self.classify([logged_decision("eval-1", action="ONLINE_CACHE_ONLY_ACTUATION_UNAVAILABLE"),
                                logged_model("eval-1", scope="ADVISORY_CAPACITY_CHANGE")])
        row = result["evaluations"][0]
        self.assertEqual(row["classification"], "actuator_unavailable")
        self.assertTrue(row["model_qualified"])
        self.assertIsNone(row["model_used"])
        self.assertEqual(result["request_count"], 0)

    def test_stale_placement_and_duplicate_cache_identities_never_prove_application(self):
        ack = receipt("acknowledgement", accepted=True, request=requested())
        samples = [observed_placement(), observed_placement()]
        samples[0]["observation"]["epoch"] = 19
        samples[1]["cache_identities"]["0"] = samples[1]["cache_identities"]["1"]
        for sample in samples:
            row = self.classify([logged_decision("eval-1", "request-1"), logged_model("eval-1")],
                                [ack, sample])["evaluations"][0]
            self.assertFalse(row["proven_learned_control"])
            self.assertIsNone(row["native_capacity_verified"])

    def test_typed_hold_and_floor_no_op_can_be_read_without_a_request(self):
        typed = f"1970-01-01T00:00:20Z A4S hold job={JOB} vertex={JOIN} reason=ABSENT evaluationId=eval"
        hold = self.classify([logged_decision("eval", action="POLICY_HOLD"), typed])["evaluations"][0]
        self.assertEqual(hold["classification"], "missing_curve_or_observations")
        floor = logged_decision("eval", action="POLICY_NO_CHANGE").replace(
            f"nominalNativeCacheBytes={160 * MIB}", f"nominalNativeCacheBytes={160 * MIB}, "
            f"configuredFloorBytes={192 * MIB}, floorLimitedManagedBytes={192 * MIB}, "
            f"finalManagedBytes={192 * MIB}, unconstrainedManagedBytes={72 * MIB}")
        no_op = self.classify([floor])
        self.assertEqual(no_op["request_count"], 0)
        self.assertEqual(no_op["evaluations"][0]["classification"], "floor_or_retention_bound_no_op")

    def test_same_build_pair_does_not_require_distinct_operator_hashes(self):
        from native_check import main
        with TemporaryDirectory() as directory:
            pair = Path(directory)
            for index, name in enumerate(("hardcoded", "calibrated")):
                side = pair / name
                side.mkdir()
                (side / "operator.json").write_text(json.dumps({"revision": "a" * 40, "jar_sha256": "b" * 64}))
                (side / "plan.json").write_text(json.dumps({"source_mode": "kafka",
                    "kafka_bootstrap_servers": f"broker{index}", "artifacts": {"runtime": "c" * 64}}))
            (pair / "pair.json").write_text(json.dumps({"comparison": "same-build"}))
            report = pair / "new.json"
            with patch.object(sys, "argv", ["native_check.py", str(pair), "--output", str(report)]), \
                    patch("native_check.analyze_arm", side_effect=lambda path: {"issues": [], "transactions": []}):
                self.assertEqual(main(), 0)
            self.assertFalse(json.loads(report.read_text())["calibrated"]["same_action_control"])

    def test_archived_abba_preserves_identical_allocations_and_lower_confidence(self):
        archive = Path(__file__).resolve().parent / "results/2026-11-19.json"
        old = json.loads(archive.read_text())
        result = compare(old["arms"])
        self.assertTrue(result["matched_policy_comparison_pass"])
        self.assertFalse(result["online_resize_comparison_pass"])
        self.assertEqual(result["classification"], "same_action_control")
        self.assertTrue(all(arm["confidence"] == "historical" for arm in result["decisions_by_arm"]))
        for arm in result["decisions_by_arm"]:
            decision, = arm["requests"]
            self.assertEqual(decision["observed_allocation"][:2], (2, 79 * MIB))
            self.assertTrue(decision["archive_alternate_selected_same_request"])
            self.assertIsNone(decision["model_used"])
            self.assertIn("fixed_slope_s", decision["counterfactual"])


class ProgressReportingTest(unittest.TestCase):
    @staticmethod
    def sample(when, count, attempt=1, state="RUNNING", rate=None):
        from quick_series import SOURCE_POPULATION
        return {"capture_epoch": when, "capture_end_epoch": when, "elapsed_sec": when,
                "job_id": JOB, "job_start_epoch_ms": 100, "job_state": state,
                "values": {"source_records_per_second": rate},
                "counter_endpoints": {SOURCE_POPULATION: {
                    "status": "complete" if count is not None else "counter_unavailable",
                    "attempts": {"source/0": [attempt, 1_000, "worker"]},
                    "counts": {"source/0": count} if count is not None else None}}}

    def test_capture_reads_counts_when_operator_gauges_do_not_exist(self):
        from quick_series import SOURCE_POPULATION, JOIN_POPULATION
        vertices = [{"id": "p", "name": "Source: person_kafka", "parallelism": 1, "inputs": []},
                    {"id": "a", "name": "Source: auction_kafka", "parallelism": 1, "inputs": []},
                    {"id": "j", "name": "WindowJoin[13]", "parallelism": 1,
                     "inputs": [{"id": "p"}, {"id": "a"}]}]

        def get(path):
            if path == "/job":
                return {"state": "RUNNING", "start-time": 100, "vertices": vertices, "plan": {"nodes": vertices}}
            if re.fullmatch(r"/job/vertices/[paj]", path):
                return {"subtasks": [{"subtask": 0, "attempt": 1, "start-time": 1000,
                                      "taskmanager-id": "tm1", "status": "RUNNING",
                                      "metrics": {"read-records-complete": True, "read-records": 75,
                                                  "write-records-complete": True, "write-records": 79}}]}
            if path.endswith("/metrics"):
                return [{"id": "0.Source__q8_kafka.numRecordsOut"}]
            if "?get=" in path:
                return [{"id": "0.Source__q8_kafka.numRecordsOut", "value": "82"}]
            return {}

        with ThreadPoolExecutor(2) as pool:
            sample = capture(pool, get, "/job", kafka=True)
        self.assertIsNone(sample["values"]["source_records_per_second"])
        self.assertEqual([source["record_total"] for source in sample["sources"]], [82, 82])
        self.assertEqual(sample["counter_endpoints"][SOURCE_POPULATION]["counts"], {"p/0": 79, "a/0": 79})
        self.assertEqual(sample["counter_endpoints"][JOIN_POPULATION]["counts"], {"j/0": 75})

    def test_gap_has_endpoint_progress_but_no_guessed_rate_inside_it(self):
        from quick_series import SOURCE_POPULATION, confirmed_kafka_rates, counter_intervals
        rows = [self.sample(0, 100), self.sample(5, None), self.sample(10, 180)]
        interval, = counter_intervals(rows, SOURCE_POPULATION)
        self.assertEqual(interval["records"], 80)
        self.assertEqual(interval["interval_average_records_per_second"], 8)
        self.assertTrue(interval["contains_observation_gap"])
        self.assertIsNone(interval["within_interval_trajectory"])
        self.assertEqual(list(confirmed_kafka_rates(rows, [])), [None, None, None])

    def test_missing_endpoints_are_unscored_and_zero_progress_is_valid(self):
        from quick_series import SOURCE_POPULATION, counter_intervals
        rows = [self.sample(0, None), self.sample(5, 180), self.sample(10, 180), self.sample(15, None)]
        intervals = counter_intervals(rows, SOURCE_POPULATION)
        self.assertEqual([row["records"] for row in intervals], [None, 0, None])
        self.assertEqual(intervals[1]["interval_average_records_per_second"], 0)
        self.assertEqual(intervals[0]["unscored_reason"], "missing_counter_endpoint")
        self.assertEqual(intervals[2]["unscored_reason"], "missing_counter_endpoint")

    def test_replacement_and_counter_reset_invalidate_only_the_crossing_intervals(self):
        from quick_series import SOURCE_POPULATION, counter_intervals
        rows = [self.sample(0, 50), self.sample(5, 200, attempt=2),
                self.sample(10, 10, attempt=2), self.sample(15, 12, attempt=2)]
        before_change, reset, measured = counter_intervals(rows, SOURCE_POPULATION)
        self.assertEqual(before_change["unscored_reason"], "execution_changed")
        self.assertEqual(reset["unscored_reason"], "counter_decreased")
        self.assertEqual(measured["records"], 2)
        stopped = [self.sample(0, 50), self.sample(5, None, state="RESTARTING"), self.sample(10, 60)]
        self.assertEqual([row["records"] for row in counter_intervals(stopped, SOURCE_POPULATION)], [None, None])

    def test_tail_and_aligned_rankings_survive_without_a_selected_winner(self):
        from native_check import compare_windows, scoring_windows
        from quick_series import SOURCE_POPULATION, counter_intervals
        settings = {"population": SOURCE_POPULATION, "tail_seconds": 10, "post_placement_delay_seconds": 0,
                    "post_placement_seconds": 10, "matched_progress_start_records": 0,
                    "matched_progress_end_records": 500}
        fixed = [self.sample(t, v) for t, v in ((0, 0), (10, 100), (20, 600), (30, 1500))]
        online = [self.sample(t, v) for t, v in ((0, 0), (10, 100), (15, 600), (20, 620), (30, 1020))]
        reports = [scoring_windows(counter_intervals(rows, SOURCE_POPULATION),
                                   settings, 0, 30, 10, [(1, 10)]) for rows in (fixed, online)]
        fixed_score, online_score = reports
        result = compare_windows(*reports)
        self.assertEqual(result["tail"]["ranking"], "fixed")
        self.assertEqual(result["post_placement"]["ranking"], "online")
        self.assertEqual(result["matched_progress"]["ranking"], "online")
        self.assertEqual(fixed_score["whole_horizon"]["records"], 1500)
        self.assertEqual(online_score["whole_horizon"]["records"], 1020)

    def test_counter_thresholds_between_observations_are_bracketed_without_interpolation(self):
        from native_check import matched_progress_window, score_window
        from quick_series import SOURCE_POPULATION, counter_intervals
        intervals = counter_intervals([self.sample(0, 100), self.sample(10, 200)], SOURCE_POPULATION)
        score = matched_progress_window(intervals, 0, 0, 75)
        self.assertEqual(score["end_boundary_bracket_epoch"], [0, 10])
        self.assertIsNone(score["rate_records_per_second"])
        sliced = score_window(intervals, 5, 10)
        self.assertIsNone(sliced["records"])
        self.assertEqual(sliced["observed_coverage_seconds"], 0)

    def test_ack_running_placement_recovery_and_backlog_are_separate(self):
        from native_check import transition_timeline
        from quick_series import SOURCE_POPULATION, counter_intervals
        rows = [self.sample(t, v) for t, v in ((24, 20), (29, 30))]
        rows[0]["vertex_attempts"] = {"join": {0: [1, 22_000, "tm"]}}
        span = {"decision_from_release_s": 18, "in_place_ack_from_release_s": 20,
                "ack_to_running_requested_parallelism_s": 4, "observed_from_release_s": 27}
        backlog = [{"capture_end_epoch": stamp, "state": "RUNNING", "Persons/0": {"q8Backlog": amount}}
                   for stamp, amount in ((26, 50), (30, 0))]
        timeline = transition_timeline({}, [span], rows, backlog, {"Persons/0": 100},
                                       counter_intervals(rows, SOURCE_POPULATION), 0, 60)
        legacy, = timeline["legacy_event_spans"]
        self.assertEqual(legacy["ack_to_all_running_seconds"], 4)
        self.assertEqual(legacy["ack_to_managed_placement_seconds"], 7)
        self.assertEqual(legacy["replacement_deploy_first_observed_task_epoch"], 22)
        self.assertEqual(legacy["counter_rate_recovery_observed_epoch"], 29)
        self.assertEqual(legacy["backlog_caught_up_observed_epoch"], 30)
        self.assertIsNone(legacy["request_sent_epoch"])
        self.assertIsNone(legacy["native_placement_epoch"])

    def test_scoring_window_changes_after_plan_receipt_are_unscored(self):
        from benchmark_plan import digest_json
        from native_check import declared_windows
        with TemporaryDirectory() as tmp:
            pair = Path(tmp)
            side = pair / "hardcoded"
            side.mkdir()
            comparison = {"arms": {"A": {"horizon": {"scoring_windows": {"tail_seconds": 10}}}}}
            (pair / "pair.json").write_text(json.dumps({"comparison_plan_sha256": digest_json(comparison)}))
            plan_path = side / "comparison-plan.json"
            plan_path.write_text(json.dumps(comparison))
            self.assertEqual(declared_windows(side, {"arm": "hardcoded"}), ({"tail_seconds": 10}, "paired_plan"))
            comparison["arms"]["A"]["horizon"]["scoring_windows"]["tail_seconds"] = 5
            plan_path.write_text(json.dumps(comparison))
            self.assertEqual(declared_windows(side, {"arm": "hardcoded"}), ({}, "comparison_plan_identity_unverified"))

    def test_plot_separates_missed_poll_without_synthesizing_a_rate(self):
        # The drawing preparation is independent of Matplotlib availability.
        from types import SimpleNamespace
        fake_axes = SimpleNamespace(MaxNLocator=None, StrMethodFormatter=None)
        with patch.dict(sys.modules, {"matplotlib": SimpleNamespace(pyplot=None), "matplotlib.ticker": fake_axes}):
            from plot_quick import split_plot_gaps
        rows = [self.sample(0, 1, rate=4), self.sample(10, 50, rate=8)]
        for row in rows:
            row["scheduled_elapsed_sec"] = row["elapsed_sec"]
        observed = split_plot_gaps(rows)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(observed), 3)
        self.assertIsNone(observed[1]["values"]["source_records_per_second"])

    def test_exact_request_time_and_managed_placement_survive_missing_native_bytes(self):
        from native_check import transition_timeline
        observed = observed_placement()
        observed["observation"].pop("native")
        policy = decision_evidence([logged_decision("a", "request-1"), logged_model("a")], [
            receipt("request", epoch=20.5, request=requested()),
            receipt("acknowledgement", epoch=21, accepted=True, request=requested()), observed],
            job_id=JOB, join_id=JOIN, online_configured=True, start=0, end=60)
        timeline = transition_timeline(policy, [], [], [], {}, [], 0, 60)
        linked, = timeline["linked"]
        self.assertEqual(linked["request_to_ack_seconds"], .5)
        self.assertEqual(linked["ack_to_managed_placement_seconds"], 4)
        self.assertIsNone(linked["native_placement_epoch"])



if __name__ == "__main__":
    unittest.main()
