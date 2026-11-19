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

JOIN = "11ffd1a2f5905feab358b0af9a75fe4e"
SOURCES = {
    "cbc357ccb763df2852fee8c4fc7d55f2": "Persons",
    "6cdc5bb954874d922eaee11a8e7b5dd5": "Auctions",
}


def get(base, route):
    with urlopen(base + route, timeout=3) as response:
        return json.load(response)


def capture(base, job_id, metric_ids, *, physical_slots=False):
    prefix = "/jobs/" + job_id
    job = get(base, prefix)
    data = {"state": job["state"], "job_start_ms": job["start-time"],
            "parallelism": {v["id"]: v["parallelism"] for v in job["vertices"]}}
    if job["state"] != "RUNNING":
        return data
    routes = {"join_managed_slot_bytes": prefix + "/justin", "overview": "/overview", "checkpoints": prefix + "/checkpoints",
              "native": prefix + "/vertices/" + JOIN + "/a4s-metrics?get=cacheCostSamples,cacheCostCompaction"}
    if physical_slots:
        routes["physical_slots"] = "/taskmanagers"
    for vertex, role in SOURCES.items():
        for index in range(data["parallelism"][vertex]):
            routes[f"{role}/{index}"] = f"{prefix}/vertices/{vertex}/subtasks/{index}/metrics"

    def fetch(item):
        key, route = item
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
                return key, value[JOIN]["resourceProfile"]["managedMemory"]["bytes"]
            if key == "native":
                return key, {str(s["subtask"]): s["capacityBytes"] for s in value.get("cacheCostSamples", [])}
            if key == "checkpoints":
                return key, value.get("latest", {})
            return key, {k: value.get(k) for k in ("slots-total", "slots-available", "taskmanagers")}
        generation = (job["start-time"], key)
        names = metric_ids.get(generation, [])
        if not names:
            names = [r["id"] for r in get(base, route)
                     if r["id"].split(".")[-1].startswith("q8") or r["id"].split(".")[-1] == "pendingRecords"]
            if names:
                metric_ids[generation] = names
        if not names:
            return key, {}
        rows = get(base, route + "?get=" + quote(",".join(names), safe=".,"))
        return key, {r["id"].split(".")[-1]: int(r["value"]) for r in rows}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch, item): item[0] for item in routes.items()}
        for future, key in futures.items():
            try:
                name, value = future.result()
                data[name] = value
            except (OSError, ValueError, KeyError) as error:
                data.setdefault("unavailable", {})[key] = str(error)
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
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    pair = args.output.parent / "pair.json"
    physical_slots = pair.is_file() and "expected_initial_occupied_slots" in json.loads(pair.read_text())
    followers = {}
    metrics = {}
    kube = ["kubectl", "-n", args.namespace]
    try:
        with (args.output / "progress.jsonl").open("x", buffering=1) as series:
            while True:
                start = time.monotonic()
                row = {"epoch": time.time()}
                try:
                    follow_source_logs(kube, args.deployment, args.output, followers)
                    base = (args.output / "rest-url").read_text().strip()
                    row.update(capture(base, args.job_id, metrics, physical_slots=physical_slots))
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
