#!/usr/bin/env python3
"""Follow the sampled Q8 latency messages from every owned TaskManager generation."""

import json
from pathlib import Path
import re
import signal
import subprocess
import threading
import time

INPUT = re.compile(r"Q8_LATENCY_INPUT auction=([0-9a-f]{64}) epoch_ms=(\d+)")
OUTPUT = re.compile(r"Q8_LATENCY_OUTPUT auction=([0-9a-f]{64}) person=([0-9a-f]{64}) epoch_ms=(\d+)")
CGROUP_LINE = re.compile(r"(\d+) (\d+) (\d+)\n?")
CGROUP_COMMAND = """
while :; do
    sample=$(date +%s%3N)
    test "$sample" -lt "$1" || exit 0
    usage=
    while read -r key value; do
        if test "$key" = usage_usec; then usage=$value; break; fi
    done </sys/fs/cgroup/cpu.stat
    test -n "$usage" && read -r memory </sys/fs/cgroup/memory.current || exit 1
    printf '%s %s %s\\n' "$sample" "$usage" "$memory"
    sleep 5
done
"""


def main():
    bundle = Path.cwd()
    spec = json.loads((bundle / "execution-spec.json").read_text())
    run = spec["run"]
    namespace = spec["virtual_namespace"]
    if not re.fullmatch(r"live-[0-9a-f]{8}", run) or namespace != f"q8-pr2-{run}-scout":
        raise ValueError("unexpected Q8 trial namespace")
    kube = ["kubectl", "--kubeconfig", str(bundle / "virtual.conf"), "-n", namespace]

    def read(*arguments):
        return json.loads(subprocess.check_output(
            [*kube, "--request-timeout=6s", *arguments, "-o", "json"], timeout=8, text=True))

    identity = read("get", "namespace", namespace)["metadata"]
    if (identity["uid"] != (bundle / "scout/namespace-uid.txt").read_text().strip()
            or identity["labels"]["a4s-pr2-run"] != run):
        raise ValueError("Q8 namespace ownership changed")

    followers = {}
    cgroup_readers = {}
    threads = []
    observed = {}
    errors = []
    lock = threading.Lock()
    cutoff = spec["run_deadline_ms"]
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    scout = bundle / "scout"
    with ((scout / "q8-record-latency.jsonl").open("x", buffering=1) as file,
          (scout / "q8-taskmanager-cgroups.jsonl").open("x", buffering=1) as cgroup_file):
        def follow(process, pod, uid):
            for line in process.stdout:
                for kind, pattern in (("input", INPUT), ("output", OUTPUT)):
                    match = pattern.search(line)
                    if match:
                        row = {"kind": kind, "auction": match[1], "epoch_ms": int(match[match.lastindex]),
                               "pod": pod, "pod_uid": uid}
                        if kind == "output":
                            row["person"] = match[2]
                        with lock:
                            file.write(json.dumps(row) + "\n")
                            observed[uid][kind] += 1
                        break

        def follow_cgroup(process, pod, uid):
            for line in process.stdout:
                match = CGROUP_LINE.fullmatch(line)
                if match:
                    epoch, cpu, memory = map(int, match.groups())
                    row = {"epoch_ms": epoch, "cpu_usage_usec": cpu, "memory_current_bytes": memory,
                           "pod": pod, "pod_uid": uid}
                    with lock:
                        cgroup_file.write(json.dumps(row) + "\n")
                        observed[uid]["cgroup_samples"] += 1

        try:
            while not stopping.is_set() and time.time_ns() // 1_000_000 < cutoff + 5000:
                try:
                    pods = read("get", "pods", "-l", "component=taskmanager")["items"]
                except (subprocess.SubprocessError, ValueError) as error:
                    errors.append(str(error))
                    time.sleep(2)
                    continue
                for pod in pods:
                    uid = pod["metadata"]["uid"]
                    name = pod["metadata"]["name"]
                    if pod["status"]["phase"] != "Running":
                        continue
                    if pod["spec"]["nodeName"] != spec["worker"]:
                        raise ValueError("Q8 latency witnesses require a shared worker clock")
                    if not any(container["name"] == "flink-main-container"
                               and container["image"] == spec["image"]
                               for container in pod["spec"]["containers"]):
                        raise ValueError("TaskManager image differs from the admitted Q8 release")
                    with lock:
                        observed.setdefault(uid, {"pod": name, "input": 0, "output": 0,
                                                  "connections": 0, "cgroup_samples": 0})
                    if uid not in cgroup_readers:
                        process = subprocess.Popen(
                            [*kube, "exec", name, "-c", "flink-main-container", "--",
                             "sh", "-c", CGROUP_COMMAND, "cgroup", str(cutoff)],
                            stdout=subprocess.PIPE, text=True,
                        )
                        cgroup_readers[uid] = process
                        thread = threading.Thread(target=follow_cgroup, args=(process, name, uid))
                        thread.start()
                        threads.append(thread)
                    previous = followers.get(uid)
                    if previous is not None and (previous.poll() is None or previous.returncode == 0):
                        continue
                    with lock:
                        observed[uid]["connections"] += 1
                    process = subprocess.Popen(
                        [*kube, "logs", "-f", name, "-c", "flink-main-container", "--tail=-1"],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                    )
                    followers[uid] = process
                    thread = threading.Thread(target=follow, args=(process, name, uid))
                    thread.start()
                    threads.append(thread)
                time.sleep(2)
        finally:
            processes = [*followers.values(), *cgroup_readers.values()]
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for thread in threads:
                thread.join(timeout=8)
            for process in processes:
                if process.poll() is None:
                    process.kill()
            for thread in threads:
                thread.join(timeout=2)
    (bundle / "scout/q8-record-latency-capture.json").write_text(
        json.dumps({"run": run, "pods": observed, "listing_errors": errors}, indent=2) + "\n")


if __name__ == "__main__":
    main()
