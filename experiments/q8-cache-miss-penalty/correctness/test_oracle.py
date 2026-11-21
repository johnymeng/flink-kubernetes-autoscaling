import copy
from pathlib import Path
import unittest

import oracle

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "fixture.tsv"


class OracleTest(unittest.TestCase):
    def setUp(self):
        self.rows, self.prefix, self.totals, self.crossing = oracle.expected(FIXTURE)

    def test_preserves_fifteen_static_results_and_needs_cross_cut_state(self):
        before = (HERE / "expected-fixed-p.tsv").read_text().splitlines()
        after = [f"{person}\t{name}\t{reserve}" for _, person, name, reserve in self.rows]
        self.assertCountEqual(after, before)
        self.assertEqual(len(after), 15)
        self.assertIn((1_700_000_020_000, 1, "AliceAtBoundary", 102), self.crossing)

    def test_multiplicity_and_result_values(self):
        oracle.compare(self.rows, list(reversed(self.rows)))
        for incorrect in (self.rows[:-1], self.rows + [self.rows[0]],
                          self.rows[1:] + [(self.rows[0][0], self.rows[0][1], "Wrong", 0)]):
            with self.assertRaisesRegex(ValueError, "oracle mismatch"):
                oracle.compare(self.rows, incorrect)

    def synthetic_receipt(self):
        before = {"job_id": "job", "jobmanager_uid": "existing-jm", "parallelism": 1,
                  "running": True, "join_attempts": ["before"]}
        after = {**before, "parallelism": 4, "join_attempts": ["p0", "p1", "p2", "p3"]}
        receipt = {
            "fixture_sha256": oracle.sha256(FIXTURE), "state": "FINISHED",
            "before": before, "after": after, "completed_checkpoint_id": 3,
            "restored_checkpoint_id": 3, "prefix_release_ms": 1_000,
            "checkpoint_completed_ms": 6_000, "requested_ms": 7_000, "acknowledged_ms": 8_000,
            "applied_ms": 10_000, "suffix_release_ms": 20_000, "finished_ms": 35_000,
            "deadline_ms": 60_000,
        }

        def line(action, checkpoint, source, count):
            kind, partition = source
            path = f"{kind}/2/{partition}/{receipt['fixture_sha256']}/{oracle.CUT_EVENT_MILLIS}/1000/20000/60000/"
            return (f"Q8_ORACLE_CURSOR action={action} source={path} "
                    f"checkpoint={checkpoint} next={count} end={self.totals[source]}")

        lines = []
        for source in sorted(oracle.SOURCES):
            lines += [
                line("snapshot", 3, source, self.prefix[source]),
                line("restore", 3, source, self.prefix[source]),
                line("complete", 9, source, self.totals[source]),
            ]
        return receipt, lines

    def test_transition_needs_all_checkpoint_cursors_and_stable_jm(self):
        receipt, lines = self.synthetic_receipt()
        oracle.verify_transition(FIXTURE, receipt, "\n".join(lines), self.prefix, self.totals)
        for bad_lines in (lines[:-3], [line for line in lines if "action=restore" not in line]):
            with self.assertRaisesRegex(ValueError, "cursor evidence"):
                oracle.verify_transition(FIXTURE, receipt, "\n".join(bad_lines), self.prefix, self.totals)
        replaced = copy.deepcopy(receipt)
        replaced["after"]["jobmanager_uid"] = "other-jm"
        with self.assertRaisesRegex(ValueError, "native P1-to-P4"):
            oracle.verify_transition(FIXTURE, replaced, "\n".join(lines), self.prefix, self.totals)

    def test_transition_needs_application_before_suffix_and_completed_job(self):
        receipt, lines = self.synthetic_receipt()
        receipt["applied_ms"] = receipt["suffix_release_ms"] + 1
        with self.assertRaisesRegex(ValueError, "out of order"):
            oracle.verify_transition(FIXTURE, receipt, "\n".join(lines), self.prefix, self.totals)
        receipt["state"] = "RUNNING"
        with self.assertRaisesRegex(ValueError, "must complete"):
            oracle.verify_transition(FIXTURE, receipt, "\n".join(lines), self.prefix, self.totals)


if __name__ == "__main__":
    unittest.main()
