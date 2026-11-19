"""Regression tests for the distinction between an accepted native request and applied placement."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from analyze import JOIN
from native_check import event_times, kafka_evidence, targets, verified, MIB
from q8_vertices import topology
from quick_series import capture
from native_observe import P_KEY, RP_KEY


class NativeTimingTest(unittest.TestCase):
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

    def test_sql_q8_identifies_join_without_datastream_uid(self):
        vertices = [{"id": "p", "name": "Source: person_kafka[1] -> Calc[2]"},
                    {"id": "a", "name": "Source: auction_kafka[7] -> Calc[8]"},
                    {"id": "j", "name": "WindowJoin[13] -> Sink: nexmark_q8[15]"}]
        nodes = [{"id": v["id"], "inputs": [] if v["id"] != "j" else [{"id": "p"}, {"id": "a"}]} for v in vertices]
        sources, join = topology({"vertices": vertices, "plan": {"nodes": nodes}})
        self.assertEqual([v["id"] for v in sources], ["p", "a"])
        self.assertEqual(join, "j")

    def test_observed_tasks_and_slots_distinguish_ack_from_placement(self):
        target = {"parallelism": {JOIN: 2}, "join_bytes_per_slot": 32 * MIB}
        sample = {"state": "RUNNING", "parallelism": {JOIN: 2}, "running_tasks": {JOIN: 1},
                  "native": {"0": 1024, "1": 1024}, "join_managed_slot_bytes": 32 * MIB,
                  "overview": {"slots-total": 4, "slots-available": 0, "taskmanagers": 2},
                  "join_locations": {"0": "tm1", "1": "tm2"},
                  "physical_slots": [{"id": "tm1", "job_slot_managed_mib": [32, 32]},
                                     {"id": "tm2", "job_slot_managed_mib": [32]}]}
        self.assertFalse(verified(sample, target))
        sample["running_tasks"][JOIN] = 2
        self.assertTrue(verified(sample, target))
        sample["physical_slots"][1]["job_slot_managed_mib"] = [158]
        self.assertFalse(verified(sample, target))

    def test_duplicate_kubernetes_event_polls_and_aggregate_repeats(self):
        event = {"uid": "evt", "reason": "Scaling", "message": "In-place scaling triggered",
                 "firstTimestamp": "2026-01-01T00:00:00Z", "lastTimestamp": "2026-01-01T00:00:10Z"}
        received = event_times([{"events": [event]}, {"events": [event]}], 0, float("inf"))
        self.assertEqual(len(received), 2)
        self.assertEqual(received[1][0] - received[0][0], 10)


if __name__ == "__main__":
    unittest.main()
