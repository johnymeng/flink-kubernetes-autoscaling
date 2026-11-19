"""Archive source offsets, offered backlog and physical allocations for one Q8 job."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import subprocess
import signal
import time
from urllib.parse import quote
from urllib.request import urlopen

from q8_vertices import topology

JOIN = "11ffd1a2f5905feab358b0af9a75fe4e"
SOURCES = {
    "cbc357ccb763df2852fee8c4fc7d55f2": "Persons",
    "6cdc5bb954874d922eaee11a8e7b5dd5": "Auctions",
}


def get(base, route):
    with urlopen(base + route, timeout=3) as response:
        return json.load(response)


def capture(base, job_id, metric_ids, *, physical_slots=False, native_scaling=False, kafka=False):
    prefix = "/jobs/" + job_id
    job = get(base, prefix)
    data = {"state": job["state"], "job_start_ms": job["start-time"],
            "parallelism": {v["id"]: v["parallelism"] for v in job["vertices"]}}
    join_id = JOIN
    source_roles = SOURCES
    if kafka:
        sources, join_id = topology(job)
        source_roles = {vertex["id"]: "Kafka/" + vertex["id"] for vertex in sources}
        data.update(join_id=join_id, vertices={v["id"]: v["name"] for v in job["vertices"]},
                    source_ids=list(source_roles))
    if native_scaling:
        data["running_tasks"] = {v["id"]: v["tasks"].get("RUNNING", 0) for v in job["vertices"]}
    if job["state"] != "RUNNING":
        return data
    routes = {"overview": "/overview", "checkpoints": prefix + "/checkpoints"}
    if join_id:
        routes.update(join_managed_slot_bytes=prefix + "/justin",
                      native=prefix + "/vertices/" + join_id + "/a4s-metrics?get=cacheCostSamples,cacheCostCompaction")
    if physical_slots:
        routes["physical_slots"] = "/taskmanagers"
    if native_scaling and join_id:
        routes["join_locations"] = prefix + "/vertices/" + join_id
    for vertex, role in source_roles.items():
        for index in range(data["parallelism"][vertex]):
            routes[f"{role}/{index}"] = f"{prefix}/vertices/{vertex}/subtasks/{index}/metrics"

    def fetch(item):
        key, route = item
        if key == "join_locations":
            return key, {str(t["subtask"]): t["taskmanager-id"] for t in get(base, route)["subtasks"]
                         if t["status"] == "RUNNING"}
        if key == "physical_slots":
            summaries = get(base, route)["taskmanagers"]
            slots = []
            for summary in summaries:
                manager = get(base, "/taskmanagers/" + quote(summary["id"], safe=""))
                slots.append({"id": manager["id"], "total_slots": manager["slotsNumber"],
                              "free_slots": manager["freeSlots"],
                              "pool_bytes": manager["memoryConfiguration"]["managedMemory"],
                              "job_slot_managed_mib": [slot["resource"]["managedMemory"]
                                  for slot in manager["allocatedSlots"] if slot["jobId"] == job_id]})
            return key, slots
        if key in ("overview", "checkpoints", "native", "join_managed_slot_bytes"):
            value = get(base, route)
            if key == "join_managed_slot_bytes":
                if native_scaling:
                    return "managed_bytes_by_vertex", {vid: info["resourceProfile"]["managedMemory"]["bytes"]
                                                      for vid, info in value.items()}
                return key, value[join_id]["resourceProfile"]["managedMemory"]["bytes"]
            if key == "native":
                return key, {str(s["subtask"]): s["capacityBytes"] for s in value.get("cacheCostSamples", [])}
            if key == "checkpoints":
                return key, value.get("latest", {})
            return key, {k: value.get(k) for k in ("slots-total", "slots-available", "taskmanagers")}
        generation = (job["start-time"], tuple(sorted(data["parallelism"].items())), key)
        names = metric_ids.get(generation, [])
        if not names:
            names = [r["id"] for r in get(base, route)
                     if r["id"].split(".")[-1].startswith("q8") or r["id"].endswith("pendingRecords")
                     or (kafka and ("currentOffsets." in r["id"] or "committedOffsets." in r["id"]
                                    or r["id"].endswith((".currentOffset", ".committedOffset", ".records-lag-max"))))]
            if names:
                metric_ids[generation] = names
        if not names:
            return key, {}
        rows = get(base, route + "?get=" + quote(",".join(names), safe=".,"))
        if kafka:
            # Retain full names. Per-topic and per-partition counters may share suffixes.
            return key, {r["id"]: r["value"] for r in rows}
        return key, {r["id"].split(".")[-1]: int(r["value"]) for r in rows}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch, item): item[0] for item in routes.items()}
        for future, key in futures.items():
            try:
                name, value = future.result()
                data[name] = value
            except (OSError, ValueError, KeyError) as error:
                data.setdefault("unavailable", {})[key] = str(error)
    if native_scaling:
        data["join_managed_slot_bytes"] = data.get("managed_bytes_by_vertex", {}).get(join_id)
    return data


def follow_source_logs(kube, deployment, output, followers):
    pods = json.loads(subprocess.check_output([*kube, "get", "pods", "-l",
                      f"app={deployment},component=taskmanager", "-o", "json"], timeout=12))
    for pod in pods["items"]:
        name = pod["metadata"]["name"]
        containers = pod.get("status", {}).get("containerStatuses", [])
        active = next((c for c in containers if c["name"] == "flink-main-container"), {})
        if "running" not in active.get("state", {}):
            continue
        key = (name, pod["metadata"]["uid"], active["restartCount"])
        if key in followers:
            continue
        log = subprocess.Popen([*kube, "logs", "-f", name, "-c", "flink-main-container"],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        followers[key] = [log]
        try:
            with (output / f"{name}-{key[1]}-{key[2]}.offsets").open("ab") as outfile:
                match = subprocess.Popen(["grep", "--line-buffered", "Q8_INPUT_OFFSET"], stdin=log.stdout,
                                         stdout=outfile)
                followers[key].append(match)
        finally:
            log.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("deployment")
    parser.add_argument("job_id")
    parser.add_argument("namespace")
    parser.add_argument("--native-scaling", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    pair = args.output.parent / "pair.json"
    physical_slots = pair.is_file() and "expected_initial_occupied_slots" in json.loads(pair.read_text())
    kafka = json.loads((args.output / "plan.json").read_text()).get("source_mode") == "kafka"
    followers = {}
    metrics = {}
    kube = ["kubectl", "-n", args.namespace]
    try:
        with (args.output / "progress.jsonl").open("x", buffering=1) as series:
            while True:
                start = time.monotonic()
                row = {"epoch": time.time()}
                try:
                    if not kafka:
                        follow_source_logs(kube, args.deployment, args.output, followers)
                    base = (args.output / "rest-url").read_text().strip()
                    row.update(capture(base, args.job_id, metrics,
                                       physical_slots=physical_slots or args.native_scaling,
                                       native_scaling=args.native_scaling, kafka=kafka))
                except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
                    row["unavailable"] = str(error)
                row["capture_end_epoch"] = time.time()
                print(json.dumps(row), file=series)
                if (args.output / "finished.json").exists() or (args.output / "window-ended.json").exists():
                    break
                time.sleep(max(0, 5 - (time.monotonic() - start)))
    except KeyboardInterrupt:
        pass
    finally:
        children = [process for group in followers.values() for process in group]
        for process in children:
            process.terminate()
        deadline = time.monotonic() + 3
        for process in children:
            try:
                process.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
        for process in children:
            process.wait(timeout=2)


if __name__ == "__main__":
    main()
