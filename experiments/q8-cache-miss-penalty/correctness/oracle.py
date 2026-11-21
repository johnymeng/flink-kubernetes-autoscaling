#!/usr/bin/env python3
"""Independent SQLite Q8 oracle and checkpoint-transition evidence verifier."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sqlite3

WINDOW_MILLIS = 10_000
CUT_EVENT_MILLIS = 1_700_000_015_000
SOURCES = {(kind, subtask) for kind in ("person", "auction") for subtask in (0, 1)}
CURSOR = re.compile(
    r"Q8_ORACLE_CURSOR action=(\w+) source=(person|auction)/2/(\d+)/([0-9a-f]{64})/"
    r"(\d+)/(\d+)/(\d+)/(\d+)/ checkpoint=(-?\d+) next=(\d+) end=(\d+)"
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(rows):
    return "".join(f"{end}\t{person}\t{name}\t{reserve}\n"
                   for end, person, name, reserve in sorted(rows))


def expected(fixture):
    persons, auctions = [], []
    prefix, totals, last = Counter(), Counter(), {}
    for line in fixture.read_text().splitlines():
        kind, subtask, event, ident, name_or_seller, reserve = line.split("\t")
        subtask, event, ident = int(subtask), int(event), int(ident)
        source = kind, subtask
        if source not in SOURCES or event < last.get(source, 1):
            raise ValueError(f"Invalid source ordering or partition {source}")
        last[source] = event
        totals[source] += 1
        if event < CUT_EVENT_MILLIS:
            prefix[source] += 1
        if kind == "person":
            persons.append((ident, name_or_seller, event))
        else:
            auctions.append((ident, int(name_or_seller), int(reserve), event))
    if any(not 0 < prefix[source] < totals[source] for source in SOURCES):
        raise ValueError("Every source must have prefix and suffix records")
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE people (person INTEGER, name TEXT, event_ms INTEGER)")
        db.execute("CREATE TABLE auctions (id INTEGER, seller INTEGER, reserve INTEGER, event_ms INTEGER)")
        db.executemany("INSERT INTO people VALUES (?, ?, ?)", persons)
        db.executemany("INSERT INTO auctions VALUES (?, ?, ?, ?)", auctions)
        joined = db.execute("""
            SELECT (p.event_ms / :window + 1) * :window, p.person, p.name, a.reserve,
                   p.event_ms, a.event_ms
            FROM people p JOIN auctions a ON p.person = a.seller
                AND p.event_ms / :window = a.event_ms / :window
        """, {"window": WINDOW_MILLIS}).fetchall()
    crossing = [
        row[:4] for row in joined
        if (row[4] < CUT_EVENT_MILLIS) != (row[5] < CUT_EVENT_MILLIS)
    ]
    if not crossing:
        raise ValueError("Fixture does not require restored join state across the cut")
    return [row[:4] for row in joined], prefix, totals, crossing


def result_rows(path):
    for line in path.read_text().splitlines():
        end, person, name, reserve = line.split("\t")
        end = int(end)
        if end <= 0 or end % WINDOW_MILLIS != 0 or not name:
            raise ValueError(f"Invalid Q8 output window or key in {path.name}")
        yield end, int(person), name, int(reserve)


def compare(expected_rows, observed_rows):
    expected_counts, observed_counts = Counter(expected_rows), Counter(observed_rows)
    missing = expected_counts - observed_counts
    extra = observed_counts - expected_counts
    if missing or extra:
        raise ValueError(f"Q8 oracle mismatch. Missing {dict(missing)}, extra {dict(extra)}")


def verify_transition(fixture, receipt, cursor_log, prefix, totals):
    """Validate witnessed before/after native state and checkpoint source cursors."""
    if receipt["fixture_sha256"] != sha256(fixture) or receipt["state"] != "FINISHED":
        raise ValueError("The sealed fixture must complete on the observed job")
    before, after = receipt["before"], receipt["after"]
    if (not before["job_id"] or not before["jobmanager_uid"]
            or before["job_id"] != after["job_id"]
            or before["jobmanager_uid"] != after["jobmanager_uid"]
            or before["parallelism"] != 1 or after["parallelism"] != 4
            or not before["running"] or not after["running"]
            or len(before["join_attempts"]) != 1 or len(after["join_attempts"]) != 4
            or len(set(after["join_attempts"])) != 4
            or set(before["join_attempts"]) & set(after["join_attempts"])):
        raise ValueError("A running native P1-to-P4 transition with distinct attempts is required")
    checkpoint = receipt["restored_checkpoint_id"]
    if (not isinstance(checkpoint, int) or checkpoint < 0
            or receipt["completed_checkpoint_id"] != checkpoint
            or not (receipt["prefix_release_ms"] <= receipt["checkpoint_completed_ms"]
                    <= receipt["requested_ms"] <= receipt["acknowledged_ms"]
                    <= receipt["applied_ms"] < receipt["suffix_release_ms"]
                    < receipt["finished_ms"] <= receipt["deadline_ms"])):
        raise ValueError("Checkpoint, native action and suffix release are out of order")
    witnesses = {kind: {} for kind in ("snapshot", "restore", "complete")}
    for action, kind, subtask, digest, cut, prefix_ms, suffix_ms, deadline, check, pos, end in CURSOR.findall(cursor_log):
        source = kind, int(subtask)
        if (source not in SOURCES or digest != receipt["fixture_sha256"] or int(cut) != CUT_EVENT_MILLIS
                or int(prefix_ms) != receipt["prefix_release_ms"]
                or int(suffix_ms) != receipt["suffix_release_ms"]
                or int(deadline) != receipt["deadline_ms"]
                or int(end) != totals[source]):
            raise ValueError("Cursor receipt changed source assignment, schedule or input")
        if action == "complete" and int(check) > checkpoint:
            witnesses[action][source] = int(pos)
        if action in ("snapshot", "restore") and int(check) == checkpoint:
            if source in witnesses[action] and witnesses[action][source] != int(pos):
                raise ValueError("Conflicting cursor evidence for one checkpoint")
            witnesses[action][source] = int(pos)
    if (witnesses["snapshot"] != prefix or witnesses["restore"] != prefix
            or witnesses["complete"] != totals):
        raise ValueError("Missing or mismatched checkpoint, restore or terminal cursor evidence")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    commands = parser.add_subparsers(required=True, dest="mode")
    seal = commands.add_parser("seal")
    seal.add_argument("expected", type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("results", type=Path)
    verify.add_argument("--receipt", type=Path)
    verify.add_argument("--cursor-log", type=Path)
    args = parser.parse_args()
    rows, prefix, totals, crossing = expected(args.fixture)
    if args.mode == "seal":
        with args.expected.open("x") as stream:
            stream.write(canonical(rows))
        print(json.dumps({"fixture_sha256": sha256(args.fixture), "expected_sha256": sha256(args.expected),
                          "results": len(rows), "cross_cut_results": crossing}))
        return
    if bool(args.receipt) != bool(args.cursor_log):
        parser.error("Supply both the transition receipt and cursor log")
    parts = sorted(path for path in args.results.rglob("part-*") if path.is_file())
    compare(rows, (row for part in parts for row in result_rows(part)))
    if args.receipt:
        verify_transition(args.fixture, json.loads(args.receipt.read_text()),
                          args.cursor_log.read_text(), prefix, totals)
    print(json.dumps({"results": len(rows), "committed_parts": len(parts),
                      "transition_verified": bool(args.receipt)}))


if __name__ == "__main__":
    main()
