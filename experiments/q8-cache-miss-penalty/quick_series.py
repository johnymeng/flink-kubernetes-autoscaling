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
    sources, _ = topology(job)
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
        complete_rates = (len(rates) == v["parallelism"] and
                          {int(name.split(".", 1)[0]) for name in rates} == set(range(v["parallelism"])))
        offsets = sorted(m["id"] for m in catalog or []
                         if m.get("id", "").endswith((".currentOffset", ".committedOffset")))
        # At 24 Kafka partitions the offset catalog alone exceeds Flink's HTTP
        # request-line limit. The chart only needs the source meters; kafka_control
        # samples partition offsets separately.
        requested = sorted(rates + counts + ([] if kafka else offsets))
        source_details.append({"vertex_id": v["id"], "name": v["name"],
                               "metric_ids": actual + offsets, "rates": rates, "counts": counts,
                               "query": path + "?get=" + quote(",".join(requested), safe=",")
                               if complete_rates and sorted(rates) == sorted(m for m in actual if m.endswith("PerSecond"))
                               else None})
    queries = [entry["query"] for entry in source_details if entry["query"]]
    source_metrics = iter(pool.map(get, queries))
    for detail in source_details:
        values = next(source_metrics) if detail["query"] else None
        detail["values"] = values
        detail["rate_total"] = measure(values, detail.pop("rates"))
        counts = detail.pop("counts")
        detail["record_total"] = measure(values, counts) if counts else None

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
              "sources": source_details, "taskmanagers": list(zip(tm, memory))}
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
            emit(row)
            captured += 1
            index = max(index + 1, ceil((time.time() - release) / PERIOD))
    raise TimeoutError("No FINISHED receipt during resource collection")


if __name__ == "__main__":
    main()
