import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import benchmark_plan
import quick_pair


HERE = Path(__file__).resolve().parent


def arm(label, worker="c167", hashes=None):
    mode, selector = ("fixed", "hardcoded") if label == "A" else ("online", "calibrated")
    args = SimpleNamespace(arm=selector, namespace="q8-trial-" + label.lower(),
                           worker=worker, label=[], bootstrap_servers=f"broker-{label.lower()}:9092")
    if hashes is None:
        hashes = {name: "1" * 64 for name in quick_pair.LIBRARIES}
    manifest = quick_pair.render_trial_manifest(args, "q8-owner-" + label.lower(),
                                                label.lower() * 32, hashes, 0)
    requested = {key: "2" * 64 for key in benchmark_plan.ARTIFACT_KEYS}
    requested["operator_image"] = "operator@sha256:" + "3" * 64
    requested["runtime_image"] = manifest["spec"]["image"]
    requested.update({key: hashes[name] for name, key in quick_pair.ARTIFACT_IDENTITIES.items()})
    requested["benchmark_dependencies_sha256"] = {name: hashes[name] for name in quick_pair.KAFKA_JARS}
    return {
        "trial_id": "q8-" + label.lower(), "mode": mode,
        "source": {"revision": "a" * 40, "tree": "b" * 40, "native_gitlink": "c" * 40,
                   "native_patches": [{"path": "native.patch", "sha256": "4" * 64}]},
        "artifacts": {"requested": requested, "deployed": None, "observed_loaded": None},
        "manifest": manifest,
        "operator_configuration": {"kubernetes.operator.reconcile.interval": "5 s"},
        "runtime_policy": {"estimator": "quickmrc", "timing_abi": 3,
                           "timing_instrumentation": "block-latency ABI 3",
                           "metadata_policy": "shared cache",
                           "memory_policy": {"managed": True, "write_buffer_ratio": 0.5,
                                             "floor_mib_by_vertex": {}},
                           "resource_limits": {"max_parallelism": 24, "max_managed_mib_per_slot": 600}},
        "placement": {"flink_worker": worker, "broker_worker": "broker-host", "worker_ids": [worker, "broker-host"],
                      "storage_device_ids": ["worker-disk", "broker-disk"],
                      "configuration": {"worker_class": "fixed-spec", "storage_class": "ssd"}},
        "actuation": benchmark_plan.NATIVE_ACTUATOR,
        "workload": {"query": "q8_unique", "source_mode": "kafka", "source_implementation": "com.github.nexmark.flink.sql.SqlQueryJob",
                     "generator_implementation": "insert_kafka_unique", "seed": 1,
                     "first_event_id": 1, "topic_partitions": 24, "total_event_budget": 300_000_000,
                     "producer_image": "producer@sha256:" + "3" * 64,
                     "broker_image": "broker@sha256:" + "4" * 64,
                     "population": "all-generated-events", "target_events_per_second": 80_000,
                     "event_mix": {"person": 1, "auction": 3, "bid": 46},
                     "topics": dict(quick_pair.TOPICS), "start_offsets": {name: 0 for name in quick_pair.TOPICS.values()},
                     "start_state": "empty topics and fresh Flink state", "reset_policy": "owned generation reset",
                     "completion_criterion": "observation deadline", "producer_hook_sha256": "5" * 64},
        "horizon": {"duration_seconds": 360},
    }


def pairing():
    return {"schema_version": 1, "comparison": "same-build", "claim": "measurement",
            "orders": ["AB", "BA"], "arms": {"A": arm("A"), "B": arm("B")}}


class BenchmarkPlanTest(unittest.TestCase):
    def test_same_build_accepts_only_intended_and_trial_specific_differences(self):
        plan = benchmark_plan.prepare_plan(pairing())
        self.assertEqual(plan["validation"]["plan_status"], "VALID", plan["validation"])
        self.assertEqual(plan["validation"]["undeclared_differences"], [])
        self.assertEqual(plan["execution"], "sequential")
        self.assertEqual(plan["orders"], ["AB", "BA"])
        quick_pair.check_plan_order(plan, 1, "AB", 360)
        quick_pair.check_plan_order(plan, 2, "BA", 360)
        for pair_index, order, seconds in ((2, "AB", 360), (1, "parallel", 360), (1, "AB", 361), (3, "AB", 360)):
            with self.subTest(pair_index=pair_index, order=order, seconds=seconds):
                with self.assertRaises(ValueError):
                    quick_pair.check_plan_order(plan, pair_index, order, seconds)

    def test_non_treatment_changes_fail_and_report_the_exact_field(self):
        changes = (
            ("artifacts.requested.jni_jar_sha256",
             lambda arm: arm["artifacts"]["requested"].update(jni_jar_sha256="8" * 64)),
            ("runtime_policy.memory_policy.floor_mib_by_vertex",
             lambda arm: arm["runtime_policy"]["memory_policy"]["floor_mib_by_vertex"].update(join=192)),
            ("manifest.spec.flinkConfiguration.job.autoscaler.metrics.window",
             lambda arm: arm["manifest"]["spec"]["flinkConfiguration"].update(
                 {"job.autoscaler.metrics.window": "60 s"})),
            ("workload.source_implementation",
             lambda arm: arm["workload"].update(source_implementation="unmatched source")),
            ("runtime_policy.metadata_policy",
             lambda arm: arm["runtime_policy"].update(metadata_policy="outside cache")),
            ("actuation", lambda arm: arm.update(actuation="external-savepoint-redeployment")),
            ("manifest.spec.flinkConfiguration.state.savepoints.dir",
             lambda arm: arm["manifest"]["spec"]["flinkConfiguration"].update(
                 {"state.savepoints.dir": "file:///other-disk/q8-owner-b-calibrated-ssd-v2/savepoints"})),
        )
        for field, change in changes:
            with self.subTest(field=field):
                spec = pairing()
                change(spec["arms"]["B"])
                result = benchmark_plan.prepare_plan(spec)["validation"]
                self.assertEqual(result["plan_status"], "INVALID")
                self.assertTrue(any(path.startswith(field) for path in result["undeclared_differences"]),
                                result["undeclared_differences"])

    def test_unstaged_and_unobserved_identities_remain_distinct(self):
        spec = pairing()
        for arm in spec["arms"].values():
            arm["artifacts"]["requested"].pop("jni_jar_sha256")
        plan = benchmark_plan.prepare_plan(spec)
        self.assertEqual(plan["validation"]["plan_status"], "INCOMPLETE")
        self.assertEqual(plan["validation"]["identity_status"], "UNVERIFIED")
        self.assertIn("arms.A.artifacts.requested.jni_jar_sha256", plan["validation"]["missing"])
        self.assertIn("arms.A.artifacts.observed_loaded.jni_loaded_elf_sha256", plan["validation"]["unverified"])
        self.assertIsNone(plan["arms"]["A"]["artifacts"]["observed_loaded"])

    def test_loaded_mismatch_is_detected_even_when_planned_hashes_match(self):
        spec = pairing()
        spec["arms"]["B"]["artifacts"]["observed_loaded"] = {"jni_jar_sha256": "6" * 64}
        result = benchmark_plan.prepare_plan(spec)["validation"]
        self.assertEqual(result["identity_status"], "MISMATCH")
        self.assertEqual(result["identity_mismatches"], ["arms.B.artifacts.observed_loaded.jni_jar_sha256"])

    def test_equal_staged_jni_hashes_do_not_mask_different_loaded_elfs(self):
        spec = pairing()
        for label, digest in (("A", "6" * 64), ("B", "7" * 64)):
            spec["arms"][label]["artifacts"]["observed_loaded"] = {"jni_loaded_elf_sha256": digest}
        result = benchmark_plan.prepare_plan(spec)["validation"]
        self.assertEqual(result["identity_status"], "MISMATCH")
        self.assertIn("artifacts.observed_loaded.jni_loaded_elf_sha256", result["undeclared_differences"])

    def test_concurrency_requires_disjoint_workers_and_devices_plus_receipts(self):
        spec = pairing()
        spec["execution"] = "concurrent"
        shared = benchmark_plan.prepare_plan(spec)["validation"]
        self.assertEqual(shared["plan_status"], "INVALID")
        self.assertEqual(shared["isolation"], "unverified")
        spec["arms"]["B"] = arm("B", "c182")
        spec["arms"]["B"]["placement"].update(broker_worker="other-broker", worker_ids=["c182", "other-broker"],
                                             storage_device_ids=["other-worker-disk", "other-broker-disk"])
        self.assertEqual(benchmark_plan.prepare_plan(spec)["validation"]["plan_status"], "INVALID")
        for label in ("A", "B"):
            spec["arms"][label]["placement"]["isolation_evidence"] = {
                "reference": f"inventory-{label}.json", "sha256": "9" * 64}
        isolated = benchmark_plan.prepare_plan(spec)["validation"]
        self.assertEqual(isolated["plan_status"], "VALID", isolated)
        self.assertEqual(isolated["isolation"], "documented-disjoint")
        spec["arms"]["B"]["placement"]["storage_device_ids"].append("broker-disk")
        self.assertEqual(benchmark_plan.prepare_plan(spec)["validation"]["plan_status"], "INVALID")

    def test_mixed_and_relevant_rate_targets_are_different_workloads(self):
        workload = arm("A")["workload"]
        mixed = benchmark_plan.workload_targets(workload)
        relevant = benchmark_plan.workload_targets(
            workload | {"population": "q8-relevant-person-auction"})
        self.assertEqual(mixed["target_total_generated_events_per_second"], 80_000)
        self.assertEqual(mixed["expected_q8_relevant_events_per_second"], 6_400)
        self.assertEqual(relevant["target_total_generated_events_per_second"], 1_000_000)
        self.assertEqual(relevant["expected_q8_relevant_events_per_second"], 80_000)
        self.assertNotEqual(mixed["workload_id"], relevant["workload_id"])
        self.assertIsNone(mixed["realized"])

    def test_unqualified_360_second_horizon_is_reported_without_extending_it(self):
        spec = pairing()
        spec["claim"] = "learned-action"
        for arm in spec["arms"].values():
            arm["manifest"]["spec"]["flinkConfiguration"].update({
                "job.autoscaler.stabilization.interval": "300 s", "job.autoscaler.metrics.window": "150 s"})
            arm["horizon"].update(action_budget_seconds=30, post_action_seconds=30)
        result = benchmark_plan.prepare_plan(spec)
        self.assertEqual(result["validation"]["plan_status"], "INVALID")
        self.assertEqual(result["arms"]["A"]["horizon"]["duration_seconds"], 360)
        self.assertGreater(result["validation"]["arms"]["B"]["horizon"]["qualification_allowance_seconds"], 360)
        self.assertTrue(any("horizon is insufficient" in line for line in result["validation"]["issues"]))

    def test_short_retention_cannot_claim_a_fit_and_duration_is_required(self):
        spec = pairing()
        spec["claim"] = "learned-action"
        for arm in spec["arms"].values():
            arm["horizon"].update(duration_seconds=1000, action_budget_seconds=30, post_action_seconds=30)
        result = benchmark_plan.prepare_plan(spec)
        self.assertTrue(any("two compatible fit windows" in issue for issue in result["validation"]["issues"]))
        spec = pairing()
        for arm in spec["arms"].values():
            arm["horizon"].pop("duration_seconds")
        self.assertEqual(benchmark_plan.prepare_plan(spec)["validation"]["plan_status"], "INCOMPLETE")

    def test_legacy_receipt_is_still_readable_without_certifying_loaded_identity(self):
        raw = {"arm_names": ["hardcoded", "calibrated"], "scaling_path": "external", "worker": "c167"}
        plan = benchmark_plan.prepare_plan(raw)
        self.assertEqual(plan["validation"], {"plan_status": "HISTORICAL_BUNDLE", "identity_status": "UNVERIFIED"})
        self.assertEqual(plan["historical_record"], raw)

    def test_local_preflight_rechecks_exact_manifest_and_staged_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            hook = Path(tmp) / "hook"
            hook.write_text("pure input fixture")
            p = pairing()
            for side in p["arms"].values():
                side["workload"]["producer_hook_sha256"] = quick_pair.run.digest(hook)
            plan = benchmark_plan.prepare_plan(p)
            args = SimpleNamespace(arm="hardcoded", namespace="q8-trial-a", worker="c167", label=[],
                                   bootstrap_servers="broker-a:9092", revision="a" * 40,
                                   operator_jar_sha256="2" * 64, kafka_hook=hook, pair_index=1, pair_order="AB",
                                   observation_seconds=360)
            hashes = {filename: "1" * 64 for filename in quick_pair.LIBRARIES}
            workload = plan["arms"]["A"]["workload"]
            with patch.dict(os.environ, Q8_SQL_IMAGE=workload["producer_image"],
                            Q8_KAFKA_IMAGE=workload["broker_image"], Q8_KAFKA_HOST="broker-host"):
                self.assertEqual(quick_pair.bind_arm_plan(args, plan, hashes)["trial_id"], "q8-a")
                hashes["rocksdbjni.jar"] = "7" * 64
                with self.assertRaisesRegex(ValueError, "artifact hashes"):
                    quick_pair.bind_arm_plan(args, plan, hashes)
                hashes["rocksdbjni.jar"] = "1" * 64
                args.bootstrap_servers = "unexpected-broker:9092"
                with self.assertRaisesRegex(ValueError, "broker differs"):
                    quick_pair.bind_arm_plan(args, plan, hashes)

    def test_workload_receipt_uses_actual_topic_deltas_and_leaves_unknowns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            plan = {"release_ms": 100_000, "observation_seconds": 60}
            quick_pair.record_workload_measurements(directory, plan)
            receipt = directory / "workload-measurements.json"
            missing = json.loads(receipt.read_text())
            self.assertIsNone(missing["broker_generated_events"])
            self.assertIsNone(missing["broker_q8_relevant_events_per_second"])
            self.assertIsNone(missing["consumer_counters"])

            event = {"event": "start", "epoch": 100, "generation": 1,
                     "offered_records_per_second": 80_000, "first_event_id": 1}
            (directory / "kafka-generations.jsonl").write_text(json.dumps(event) + "\n")
            totals = {"person": 1600, "auction": 4800, "bid": 73600}
            offsets = [{"epoch": 100 + step, "generation": 1,
                        "topics": {quick_pair.TOPICS[event]: {"0": count * step}
                                   for event, count in totals.items()}} for step in (0, 1)]
            (directory / "kafka-offsets.jsonl").write_text(
                    "".join(json.dumps(row) + "\n" for row in offsets))
            (directory / "resource-series.jsonl").touch()
            quick_pair.record_workload_measurements(directory, plan)
            observed = json.loads(receipt.read_text())
            self.assertEqual(observed["broker_generated_events"], 80_000)
            self.assertEqual(observed["broker_generated_events_per_second"], 80_000)
            self.assertEqual(observed["broker_q8_relevant_events"], 6_400)
            self.assertEqual(observed["broker_q8_relevant_events_per_second"], 6_400)
            self.assertEqual(observed["consumer_counters"], "resource-series.jsonl")

    def test_dry_run_and_render_do_not_call_external_operations(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            marker = directory / "external-was-called"
            for command in ("kubectl", "ssh", "docker", "helm", "javap", "kafka-topics.sh", "producer-hook"):
                stub = directory / command
                stub.write_text(f"#!/bin/sh\ntouch '{marker}'\nexit 31\n")
                stub.chmod(0o700)
            for filename in quick_pair.LIBRARIES:
                (directory / filename).write_bytes(filename.encode())
            env = dict(os.environ, PATH=f"{directory}:{os.environ['PATH']}", MAIN_KAFKA_HOOK=str(directory / "producer-hook"))
            receipt = directory / "plan.json"
            receipt.write_text(json.dumps(pairing()))
            for command in (["bash", str(HERE / "quick_test.sh")],
                            [sys.executable, str(HERE / "run.py")]):
                result = subprocess.run([*command, "--dry-run", str(receipt)], env=env,
                                        capture_output=True, text=True, check=True)
                self.assertEqual(json.loads(result.stdout)["validation"]["plan_status"], "VALID")
            result = subprocess.run([sys.executable, str(HERE / "quick_pair.py"), "--render-arm", "calibrated",
                                     "--namespace", "q8-preview", "--artifacts", tmp,
                                     "--bootstrap-servers", "broker:9092"], env=env, capture_output=True,
                                    text=True, check=True)
            config = json.loads(result.stdout)["spec"]["flinkConfiguration"]
            self.assertEqual(config[benchmark_plan.ONLINE_COSTS], "true")
            self.assertFalse(marker.exists())

    def test_wrapper_starts_same_worker_arms_sequentially_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            marker = directory / "schedule"
            python = directory / "python3"
            python.write_text("""#!/bin/sh
case "$*" in
    *--validate-arm-arguments*) exit 0 ;;
    *'--arm hardcoded'*) arm=A ;;
    *'--arm calibrated'*) arm=B ;;
    *) exit 0 ;;
esac
printf '%s\n' "start-$arm" >> "$TEST_SCHEDULE"
sleep 0.1
printf '%s\n' "end-$arm" >> "$TEST_SCHEDULE"
""")
            python.chmod(0o700)
            env = dict(os.environ, PATH=f"{directory}:{os.environ['PATH']}", TEST_SCHEDULE=str(marker),
                       OBSERVATION_SECONDS="360", ARTIFACTS=tmp, KUBECONFIG="fixture",
                       Q8_LEGACY_IMPLEMENTATION_BUNDLE="1")
            for key in ("Q8_ORDER", "Q8_COMPARISON_PLAN"):
                env.pop(key, None)
            for arm in ("MAIN", "BRANCH"):
                for key in ("NAMESPACE", "OPERATOR_NAMESPACE", "OPERATOR_DEPLOYMENT", "OPERATOR_JAR",
                            "OPERATOR_JAR_SHA256", "REVISION", "BOOTSTRAP_SERVERS", "KAFKA_HOOK"):
                    env[arm + "_" + key] = "fixture"
            subprocess.run(["bash", str(HERE / "quick_test.sh"), str(directory / "out")], env=env,
                           capture_output=True, text=True, check=True)
            self.assertEqual(marker.read_text().splitlines(), ["start-A", "end-A", "start-B", "end-B"])


if __name__ == "__main__":
    unittest.main()
