"""Sample Q8 throughput, Flink managed memory and occupied slots on a release-aligned clock.

Metric selection and aggregation come from the existing Q8 resource-figure collector.
Run once for each arm next to its manifest, plan, rest-url and finished.json receipts.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPException
import json
from math import ceil, isfinite
from pathlib import Path
import re
import time
from urllib.parse import quote, urlparse
from urllib.request import urlopen

from q8_vertices import topology

USED = "Status.Flink.Memory.Managed.Used"
TOTAL = "Status.Flink.Memory.Managed.Total"
COLS = ("source_records_per_second", "managed_memory_used_bytes",
        "managed_memory_allocated_bytes", "task_slots_used")
PERIOD = 5


SOURCE_POPULATION = "q8_source_task_emitted_records"
JOIN_POPULATION = "q8_join_input_records"


def attempt_roster(details, parallelism, *, running=True):
    attempts = {}
    for task in (details or {}).get("subtasks", []):
        index, number = task.get("subtask"), task.get("attempt")
        deployed, worker = task.get("start-time"), task.get("taskmanager-id")
        if (type(index) is not int or index in attempts or type(number) is not int or number < 0
                or type(deployed) is not int or deployed <= 0 or not isinstance(worker, str)
                or not worker or worker == "(unassigned)" or task.get("other-concurrent-attempts")
                or (running and task.get("status") != "RUNNING")):
            return None
        attempts[index] = [number, deployed, worker]
    return attempts if set(attempts) == set(range(parallelism)) and attempts else None


def metric_counts(values, names):
    if not isinstance(values, list) or not names:
        return None
    result = {}
    for name in names:
        candidates = [row.get("value") for row in values if row.get("id") == name]
        if len(candidates) != 1 or not re.fullmatch(r"[0-9]+", str(candidates[0])):
            return None
        result[str(int(name.split(".", 1)[0]))] = int(candidates[0])
    return result


def task_io_counts(details, field):
    # Vertex details bind MutableIOMetrics to the execution attempt's metric
    # store. The generic operator-metric endpoint can retain a prior attempt.
    counts = {}
    for task in (details or {}).get("subtasks", []):
        metrics = task.get("metrics") or {}
        count = metrics.get(field)
        if metrics.get(field + "-complete") is True and type(count) is int and count >= 0:
            counts[str(task["subtask"])] = count
    return counts


def bound_counter_endpoint(vertices, before, after, field):
    rosters, population = {}, {}
    counts_complete = True
    for vertex in vertices:
        start = attempt_roster(before.get(vertex["id"]), vertex["parallelism"])
        final = attempt_roster(after.get(vertex["id"]), vertex["parallelism"])
        if start is not None and final is not None and start != final:
            return {"status": "attempt_changed", "attempts": None, "counts": None}
        if start is None or final is None:
            return {"status": "attempt_unavailable", "attempts": None, "counts": None}
        current = task_io_counts(after.get(vertex["id"]), field)
        for index, identity in final.items():
            key = f"{vertex['id']}/{index}"
            rosters[key] = identity
            if str(index) not in current:
                counts_complete = False
            else:
                population[key] = current[str(index)]
    return {"status": "complete" if counts_complete and population else "counter_unavailable",
            "attempts": rosters or None, "counts": population if counts_complete and population else None}


def counter_endpoint(row, population):
    evidence = (row.get("counter_endpoints") or {}).get(population) or {}
    roster = evidence.get("attempts")
    counts = evidence.get("counts")
    job, epoch = row.get("job_id"), row.get("job_start_epoch_ms")
    identity = (job, epoch, roster) if (isinstance(job, str) and job and
                type(epoch) is int and epoch > 0 and isinstance(roster, dict) and roster) else None
    if (identity is None or row.get("job_state") != "RUNNING"
            or evidence.get("status") != "complete" or not isinstance(counts, dict)
            or counts.keys() != roster.keys()
            or any(type(value) is not int or value < 0 for value in counts.values())):
        return identity, None
    return identity, counts


def counter_intervals(rows, population):
    """Score endpoint deltas only. The trajectory inside every interval is unknown."""
    points = []
    for row in rows:
        stamp = row.get("capture_end_epoch", row.get("capture_epoch"))
        if type(stamp) in (int, float) and isfinite(stamp):
            points.append((stamp, row))
    if not points:
        return []
    # The first and last observations bound the available evidence. Do not
    # resolve duplicate times by taking the more favorable counter.
    if any(left[0] >= right[0] for left, right in zip(points, points[1:])):
        return []
    first, last = points[0][0], points[-1][0]
    span_start = first
    previous = None
    result = []

    def emit(start, end, reason=None, records=None, gap=False, identity=None):
        if end > start:
            result.append({
                "population": population, "start_epoch": start, "end_epoch": end, "records": records,
                "interval_average_records_per_second": records / (end - start) if records is not None else None,
                "unscored_reason": reason, "contains_observation_gap": gap,
                "execution_identity": identity, "within_interval_trajectory": None,
            })

    gap = False
    for stamp, row in points:
        identity, counts = counter_endpoint(row, population)
        evidence = (row.get("counter_endpoints") or {}).get(population) or {}
        boundary = (row.get("job_state") not in (None, "RUNNING")
                    or evidence.get("status") == "attempt_changed"
                    or (previous and identity is not None and identity != previous[1])
                    or (previous and row.get("job_start_epoch_ms") not in (None, previous[1][1])))
        if boundary:
            emit(span_start, stamp, "execution_changed")
            span_start, previous, gap = stamp, None, False
        if counts is None:
            gap = True
            continue
        if previous:
            prior = previous[2]
            capture_start = row.get("capture_epoch")
            if type(capture_start) not in (int, float) or capture_start < span_start:
                emit(span_start, stamp, "overlapping_or_unbound_capture_bounds")
            elif any(count < prior[key] for key, count in counts.items()):
                emit(span_start, stamp, "counter_decreased")
            else:
                emit(span_start, stamp, records=sum(counts[key] - prior[key] for key in counts),
                     gap=gap or row.get("values", {}).get(COLS[0]) is None, identity=identity)
        else:
            emit(span_start, stamp, "missing_counter_endpoint")
        previous = (stamp, identity, counts)
        span_start, gap = stamp, row.get("values", {}).get(COLS[0]) is None
    emit(span_start, last, "missing_counter_endpoint")
    return result


def kafka_source_counters(row):
    readings = {}
    for source in row.get("sources", []):
        metrics = {m["id"]: m["value"] for m in source.get("values") or []}
        counts = {key for key in metrics if key.endswith(".numRecordsOut")}
        rates = {key.removesuffix("PerSecond") for key in metrics if key.endswith(".numRecordsOutPerSecond")}
        if not counts or counts != rates:
            return {}
        for key in counts:
            try:
                count = int(metrics[key])
                rate = float(metrics[key + "PerSecond"])
            except (TypeError, ValueError, OverflowError):
                return {}
            if count < 0 or not isfinite(rate) or rate < 0:
                return {}
            readings[source["vertex_id"], key] = (count, rate)
    return readings


def confirmed_kafka_rates(rows, transactions):
    """After native scaling, Flink REST can retain the prior execution's meters."""
    previous = {}
    previous_execution = None
    for row in rows:
        elapsed = row["elapsed_sec"]
        rate = row["values"]["source_records_per_second"]
        change = next((step for step in reversed(transactions) if step["in_place_ack_from_release_s"] <= elapsed), None)
        ack = change["in_place_ack_from_release_s"] if change else None
        recovery = change.get("ack_to_running_requested_parallelism_s") if change else None
        typed = "counter_endpoints" in row
        identity, counts = counter_endpoint(row, SOURCE_POPULATION)
        execution = (ack, identity if typed else row.get("job_start_epoch_ms"))
        current = {}
        running = row.get("job_state") == "RUNNING"
        settled = change is None or (recovery is not None and elapsed >= ack + recovery)
        if running and settled and (not typed or counts is not None):
            current = kafka_source_counters(row)
        confirmed = (execution == previous_execution and current and current.keys() == previous.keys()
                     and all(count >= previous[key][0] and (meter == 0 or count > previous[key][0])
                             for key, (count, meter) in current.items())
                     and any(count > previous[key][0] for key, (count, _) in current.items()))
        first_legacy_gauge = not typed and change is None and previous_execution is None and running
        yield rate if confirmed or first_legacy_gauge else None
        previous, previous_execution = current, execution


def measure(metrics, names):
    if not isinstance(metrics, list):
        return None
    values = []
    for name in names:
        matches = [row.get("value") for row in metrics if row.get("id") == name]
        try:
            value = float(matches[0]) if len(matches) == 1 else float("nan")
        except (TypeError, ValueError, OverflowError):
            return None
        if not isfinite(value) or value < 0:
            return None
        values.append(value)
    return sum(values)


def capture(pool, get, job_path, *, kafka=False):
    job, overview, managers = pool.map(get, (job_path, "/overview", "/taskmanagers"))
    sources, join_id = topology(job)
    join = next((vertex for vertex in (job or {}).get("vertices", []) if vertex["id"] == join_id), None)
    vertices = [*sources, *([join] if join else [])]
    detail_paths = [f"{job_path}/vertices/{vertex['id']}" for vertex in vertices]
    before = dict(zip((vertex["id"] for vertex in vertices), pool.map(get, detail_paths)))
    tm = (managers or {}).get("taskmanagers", [])
    paths = [f"{job_path}/vertices/{v['id']}/metrics" for v in sources]
    tm_paths = [f"/taskmanagers/{quote(w['id'], safe='')}/metrics?get={USED},{TOTAL}" for w in tm]
    replies = list(pool.map(get, paths + tm_paths))
    catalogs, memory = replies[:len(sources)], replies[len(sources):]
    source_details = []
    for v, path, catalog in zip(sources, paths, catalogs):
        rates = [f"{n}.numRecordsOutPerSecond" for n in range(v["parallelism"])]
        counts = [f"{n}.numRecordsOut" for n in range(v["parallelism"])]
        actual = sorted(m["id"] for m in catalog or []
                        if re.fullmatch(r"\d+\.numRecordsOut(?:PerSecond)?", m.get("id", "")))
        if kafka:
            # Match 1730 Q8's Source operator meters on the person and auction vertices.
            # A chained Calc can emit a different count at the task boundary.
            operator_ids = [m["id"] for m in catalog or [] if re.fullmatch(
                r"\d+\.Source.+\.numRecordsOut(?:PerSecond)?", m.get("id", ""), re.IGNORECASE)]
            rates = sorted(name for name in operator_ids if name.endswith("PerSecond"))
            counts = sorted(name for name in operator_ids if name.endswith("numRecordsOut"))
            actual = sorted(operator_ids)
        expected = set(range(v["parallelism"]))
        def complete(names):
            return (len(names) == v["parallelism"]
                    and {int(name.split(".", 1)[0]) for name in names} == expected)
        rates = rates if complete(rates) and sorted(rates) == sorted(
            name for name in actual if name.endswith("PerSecond")) else []
        counts = counts if complete(counts) and sorted(counts) == sorted(
            name for name in actual if name.endswith(".numRecordsOut")) else []
        offsets = sorted(m["id"] for m in catalog or []
                         if m.get("id", "").endswith((".currentOffset", ".committedOffset")))
        # At 24 Kafka partitions the offset catalog alone exceeds Flink's HTTP
        # request-line limit. The chart only needs the source meters; kafka_control
        # samples partition offsets separately.
        requested = sorted(rates + counts + ([] if kafka else offsets))
        source_details.append({"vertex_id": v["id"], "name": v["name"],
                               "metric_ids": actual + offsets, "rates": rates, "counts": counts,
                               "query": path + "?get=" + quote(",".join(requested), safe=",")
                               if requested else None})
    queries = [entry["query"] for entry in source_details if entry["query"]]
    source_metrics = iter(list(pool.map(get, queries)))
    for detail in source_details:
        values = next(source_metrics) if detail["query"] else None
        detail["values"] = values
        rates = detail.pop("rates")
        detail["rate_total"] = measure(values, rates) if rates else None
        counts = metric_counts(values, detail.pop("counts"))
        detail["record_total"] = sum(counts.values()) if counts is not None else None

    after = dict(zip((vertex["id"] for vertex in vertices), pool.map(get, detail_paths)))
    endpoints = {}
    if len(sources) == 2:
        endpoints[SOURCE_POPULATION] = bound_counter_endpoint(sources, before, after, "write-records")
    if join:
        endpoints[JOIN_POPULATION] = bound_counter_endpoint([join], before, after, "read-records")

    result = dict.fromkeys(COLS)
    rates = [entry["rate_total"] for entry in source_details]
    if rates and all(rate is not None for rate in rates):
        result[COLS[0]] = sum(rates)
    if overview and tm and overview.get("taskmanagers") == len(tm):
        per_manager = [(measure(reply, [USED]), measure(reply, [TOTAL])) for reply in memory]
        if all(used is not None and total is not None and used <= total for used, total in per_manager):
            result[COLS[1]] = int(sum(used for used, _ in per_manager))
            result[COLS[2]] = int(sum(total for _, total in per_manager))
        allocated, free = overview.get("slots-total"), overview.get("slots-available")
        slot_counts = [(w.get("slotsNumber"), w.get("freeSlots")) for w in tm]
        if all(type(n) is int and type(f) is int and 0 <= f <= n for n, f in slot_counts):
            tm_allocated, tm_free = map(sum, zip(*slot_counts))
            if (allocated, free) == (tm_allocated, tm_free):
                result[COLS[3]] = allocated - free
    result = {"values": result, "job_state": (job or {}).get("state"),
              "job_start_epoch_ms": (job or {}).get("start-time"), "overview": overview,
              "sources": source_details, "taskmanagers": list(zip(tm, memory)),
              "counter_endpoints": endpoints,
              "vertex_attempts": {vertex["id"]: attempt_roster(
                  after.get(vertex["id"]), vertex["parallelism"], running=False) for vertex in vertices}}
    return result
def read_json(path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", type=Path)
    parser.add_argument("--wait-seconds", type=int, default=900)
    args = parser.parse_args()
    folder = args.arm
    deadline = time.monotonic() + args.wait_seconds
    while not (folder / "manifest.json").exists() or not (folder / "plan.json").exists():
        if time.monotonic() > deadline:
            raise TimeoutError("No benchmark manifest and plan")
        time.sleep(.25)
    manifest = read_json(folder / "manifest.json")
    plan = read_json(folder / "plan.json")
    release = plan["release_ms"] / 1000
    job_id = manifest["spec"]["flinkConfiguration"]["$internal.pipeline.job-id"]
    deploy = manifest["metadata"]["name"]
    arm = plan["arm"]
    def event(kind, **fields):
        return {"type": kind, "case": arm, "deployment": deploy, "job_id": job_id,
                "release_epoch": release, **fields}

    index = max(0, ceil((time.time() - release) / PERIOD))
    captured = 0
    with (folder / "resource-series.jsonl").open("x", buffering=1) as out, ThreadPoolExecutor(max_workers=8) as pool:
        def emit(row):
            out.write(json.dumps(row, allow_nan=False) + "\n")

        emit(event("started"))
        while time.monotonic() < deadline:
            if (folder / "finished.json").exists():
                finish = read_json(folder / "finished.json")
                emit(event("finished", snapshot_count=captured, finished_epoch=finish.get("epoch"),
                           flink_end_epoch_ms=(finish.get("job") or {}).get("end-time")))
                return
            if (folder / "window-ended.json").exists():
                window = read_json(folder / "window-ended.json")
                cutoff = window.get("deadline_epoch", window.get("epoch"))
                if cutoff is None or cutoff < release:
                    raise ValueError("Invalid observation deadline")
                emit(event("observed_until", snapshot_count=captured, deadline_epoch=cutoff,
                           observed_epoch=window.get("observed_epoch", window.get("epoch"))))
                return
            wait = release + index * PERIOD - time.time()
            if wait > 0:
                time.sleep(min(wait, .5))
                continue
            capture_epoch = time.time()
            row = event("sample", capture_epoch=capture_epoch, scheduled_elapsed_sec=index * PERIOD)
            errors = {}
            try:
                base = (folder / "rest-url").read_text().strip().rstrip("/")
                parsed = urlparse(base)
                if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
                    raise ValueError("Expected the private benchmark REST forward")
                def get(path):
                    try:
                        with urlopen(base + path, timeout=1.8) as response:
                            return json.load(response)
                    except (OSError, ValueError, HTTPException) as error:
                        errors[path] = str(error)
                        return None
                snapshot = capture(pool, get, "/jobs/" + job_id, kafka=plan.get("source_mode") == "kafka")
                if snapshot["job_state"] != "RUNNING":
                    snapshot["values"] = dict.fromkeys(COLS)
                row.update(snapshot)
            except (OSError, ValueError, KeyError, TypeError, HTTPException) as error:
                errors["sample"] = str(error)
                row.update(values=dict.fromkeys(COLS), job_state=None, job_start_epoch_ms=None)
            row["errors"] = errors
            row["capture_end_epoch"] = time.time()
            emit(row)
            captured += 1
            index = max(index + 1, ceil((time.time() - release) / PERIOD))
    raise TimeoutError("No FINISHED receipt during resource collection")


if __name__ == "__main__":
    main()
