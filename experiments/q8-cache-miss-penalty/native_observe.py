"""Read-only observer for a Q8 job reconciled by the Flink Kubernetes Operator."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
P_KEY = "pipeline.jobvertex-parallelism-overrides"
RP_KEY = "pipeline.jobvertex-resourceprofile-overrides"


def compact_resource(resource):
    status = resource.get("status", {})
    reconciliation = status.get("reconciliationStatus", {})
    latest = reconciliation.get("lastReconciledSpec")
    reconciled = json.loads(latest).get("spec", {}) if latest else {}
    job = status.get("jobStatus", {})
    def overrides(spec):
        conf = spec.get("flinkConfiguration", {})
        return {key: conf.get(key) for key in (P_KEY, RP_KEY)}
    return {"uid": resource["metadata"]["uid"], "generation": resource["metadata"].get("generation"),
            "job_id": job.get("jobId"), "job_state": job.get("state"),
            "jobmanager_state": status.get("jobManagerDeploymentStatus"),
            "reconciliation_state": reconciliation.get("state"),
            "reconciliation_timestamp": reconciliation.get("reconciliationTimestamp"),
            "submitted_overrides": overrides(resource["spec"]), "reconciled_overrides": overrides(reconciled)}


def kube_poll(pool, namespace, name, uid):
    paths = {
        "resource": ["flinkdeployment", name], "jobmanagers": ["pods", "-l", f"app={name},component=jobmanager"],
        "events": ["events", "--field-selector", f"involvedObject.uid={uid}"],
    }
    def fetch(item):
        key, args = item
        try:
            command = ["kubectl", "-n", namespace, "get", *args, "-o", "json", "--request-timeout=4s"]
            run = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=6)
            data = json.loads(run.stdout)
            if key == "resource":
                return key, compact_resource(data)
            if key == "jobmanagers":
                return key, [{"uid": pod["metadata"]["uid"], "name": pod["metadata"]["name"],
                              "restarts": c["restartCount"]} for pod in data["items"]
                             for c in pod.get("status", {}).get("containerStatuses", [])
                             if c["name"] == "flink-main-container" and "running" in c.get("state", {})]
            return key, [{field: e.get(field) for field in ("reason", "message", "eventTime", "firstTimestamp",
                          "lastTimestamp", "count", "series")} | {"uid": e["metadata"]["uid"]}
                         for e in data["items"] if e["involvedObject"]["uid"] == uid]
        except (KeyError, ValueError, OSError, subprocess.SubprocessError) as exc:
            message = exc.stderr.decode(errors="replace") if isinstance(exc, subprocess.CalledProcessError) else str(exc)
            return key + "_error", message
    return dict(pool.map(fetch, paths.items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", type=Path)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    folder = args.arm
    created = json.loads((folder / "deployment-created.json").read_text())
    uid = created["metadata"]["uid"]
    name = created["metadata"]["name"]
    job = created["spec"]["flinkConfiguration"]["$internal.pipeline.job-id"]
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    commands = [[sys.executable, str(HERE / "observe.py"), str(folder), name, job, args.namespace, "--native-scaling"],
                [sys.executable, str(HERE / "quick_series.py"), str(folder)]]
    with ExitStack() as stack:
        children = []
        def stop(process):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for command in commands:
            child = subprocess.Popen(command)
            stack.callback(stop, child)
            children.append(child)
        with (folder / "native-operator-series.jsonl").open("x", buffering=1) as out, ThreadPoolExecutor(3) as pool:
            while True:
                begin = time.time()
                row = {"epoch": begin, **kube_poll(pool, args.namespace, name, uid), "capture_end_epoch": time.time()}
                print(json.dumps(row), file=out)
                if (folder / "window-ended.json").is_file() or (folder / "finished.json").is_file():
                    break
                for child in children:
                    if child.poll() is not None:
                        raise RuntimeError(f"Observer {child.args[1]} exited early with {child.returncode}")
                time.sleep(max(0, 5 - (time.time() - begin)))
        for child in children:
            if child.wait(timeout=30):
                raise RuntimeError(f"Observer {child.args[1]} exited with {child.returncode}")


if __name__ == "__main__":
    main()
