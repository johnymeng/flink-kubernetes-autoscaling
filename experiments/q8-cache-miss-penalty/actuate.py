"""Apply one private Ryan Q8 job's A4S profiles through its regular savepoint upgrade."""

import argparse
from datetime import datetime, timezone
from http.client import HTTPException
import json
from pathlib import Path
import re
import signal
import socket
import subprocess
import time
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen
from uuid import uuid4

from observe import capture


OWNER = "ece496.org/benchmark-owner"
JOIN = "11ffd1a2f5905feab358b0af9a75fe4e"
P_KEY = "pipeline.jobvertex-parallelism-overrides"
R_KEY = "pipeline.jobvertex-resourceprofile-overrides"
MIB = 2 ** 20
MANAGED = re.compile(r"managedMemory=(?:[\d.]+[a-z]+ \()?(\d+) bytes")
PLACEMENT = re.compile(
    r"Received task .*? \([a-f0-9]+_([a-f0-9]{32})_(\d+)_\d+\), "
    r"deploy into slot with allocation id ([a-f0-9]+)"
)
ALLOCATION = re.compile(r"Allocated slot for ([a-f0-9]+) with resources (ResourceProfile\{[^\n]+)")


def write_json(path, value):
    new = path.with_name(path.name + ".tmp")
    new.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    new.replace(path)


def savepoint_path(resource):
    job = resource.get("status", {}).get("jobStatus", {})
    return job.get("upgradeSavepointPath") or (
        job.get("savepointInfo", {}).get("lastSavepoint") or {}
    ).get("location")


class Guard:
    def __init__(self, args):
        self.args = args
        self.out = args.out
        self.out.mkdir(parents=True, exist_ok=True)
        self.forward = None
        self.port = None
        self.jvm = None
        self.active = None
        self.acknowledged = ""
        self.applied = 0
        self.cluster = ["kubectl", "--kubeconfig", str(args.kubeconfig),
                        "-n", args.namespace, "--request-timeout=12s"]
        self.pointer = self.out / "rest-url"
        self.gate = self.out / "actuation-state.json"
        self.events = self.out / "rescale-events.jsonl"
        self.proposals = self.out / "policy.jsonl"

    def kube(self, *command):
        result = subprocess.run([*self.cluster, *command], check=True, capture_output=True,
                                text=True, timeout=18)
        return result.stdout

    def resource(self):
        resource = json.loads(self.kube("get", "flinkdeployment", self.args.deployment, "-o", "json"))
        metadata = resource["metadata"]
        if (metadata["namespace"] != self.args.namespace or metadata["name"] != self.args.deployment
                or metadata.get("annotations", {}).get(OWNER) != self.args.owner):
            raise ValueError("The Flink resource is outside the authorized arm")
        if hasattr(self, "uid") and metadata["uid"] != self.uid:
            raise ValueError("The private Flink resource UID changed")
        spec = resource["spec"]
        config = spec["flinkConfiguration"]
        required = {
            "job.autoscaler.enabled": "false",
            "job.autoscaler.scaling.enabled": "false",
            "job.autoscaler.justin.enabled": "false",
            "job.autoscaler.a4s.enabled": "false",
            "kubernetes.operator.job.upgrade.inplace-scaling.enabled": "false",
            "pipeline.jobvertex-resourceprofile-overrides-on-submit.enabled": "true",
            "$internal.pipeline.job-id": self.args.job_id,
        }
        if any(str(config.get(key)).lower() != value for key, value in required.items()):
            raise ValueError("The private deployment is missing the exclusive savepoint path")
        if spec["job"].get("upgradeMode", "").lower() != "savepoint":
            raise ValueError("The owned deployment must explicitly upgrade with savepoints")
        return resource

    def phase(self, state):
        write_json(self.gate, {"state": state, "acknowledged_transaction": self.acknowledged})

    def publish(self, kind, **values):
        row = {"type": kind, "arm": self.args.arm, "job_id": self.args.job_id,
               "epoch": time.time(), **values}
        with self.events.open("a") as stream:
            stream.write(json.dumps(row, sort_keys=True) + "\n")

    def find_rest(self):
        if self.forward is None or self.forward.poll() is not None:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                self.port = sock.getsockname()[1]
            with (self.out / "port-forward.log").open("a") as log:
                self.forward = subprocess.Popen(
                    [*self.cluster[:-1], "port-forward", "--address", "127.0.0.1",
                     "svc/" + self.args.deployment + "-rest", f"{self.port}:8081"],
                    stdout=log, stderr=log,
                )
            endpoint = self.pointer.with_name("rest-url.tmp")
            endpoint.write_text(f"http://127.0.0.1:{self.port}\n")
            endpoint.replace(self.pointer)
        try:
            job = self.rest("/jobs/" + self.args.job_id)
            return job if job.get("jid") == self.args.job_id else None
        except (OSError, URLError, ValueError):
            return None

    def rest(self, route):
        with urlopen(f"http://127.0.0.1:{self.port}{route}", timeout=4) as response:
            return json.load(response)

    def submit(self, resource, job, proposal):
        if not job or job["state"] != "RUNNING" or proposal["job_id"] != job["jid"]:
            return
        if proposal["job_start_epoch_ms"] != job["start-time"]:
            return
        vertices = {item["id"]: item for item in job["vertices"]}
        parallelism = proposal["parallelism"]
        profiles = proposal["resource_profiles"]
        if (set(vertices) != set(parallelism) or set(vertices) != set(profiles)
                or JOIN not in profiles or not all(str(val).isdigit() and int(val) > 0
                                                  for val in parallelism.values())):
            raise ValueError("A4S proposal does not cover exactly the live Query8 vertices")
        byte_counts = {vertex: int(MANAGED.search(profile).group(1))
                       for vertex, profile in profiles.items()}
        config = resource["spec"]["flinkConfiguration"]
        next_config = {P_KEY: proposal["parallelism_config"], R_KEY: proposal["resource_profile_config"]}
        if all(config.get(key) == value for key, value in next_config.items()):
            return
        txn = uuid4().hex[:14]
        self.phase("pending")
        old_savepoint = savepoint_path(resource)
        write_json(self.out / f"{txn}-proposal.json", proposal)
        write_json(self.out / f"{txn}-before.json", resource)
        pointer = "/spec/flinkConfiguration/"
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": self.uid},
            {"op": "test", "path": "/metadata/resourceVersion",
             "value": resource["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/metadata/annotations/ece496.org~1benchmark-owner",
             "value": self.args.owner},
            {"op": "test", "path": "/status/jobStatus/jobId", "value": job["jid"]},
            *({"op": "add", "path": pointer + key, "value": value} for key, value in next_config.items()),
            {"op": "add", "path": "/spec/restartNonce", "value": int(time.time() * 1000)},
        ]
        self.publish("rescale_start", transaction=txn,
                     prior_job_start_epoch_ms=job["start-time"], proposed_parallelism=parallelism,
                     proposed_managed_bytes=byte_counts)
        try:
            applied = json.loads(self.kube("patch", "flinkdeployment", self.args.deployment,
                                           "--type=json", "-p", json.dumps(patch), "-o", "json"))
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            self.publish("rescale_failed", transaction=txn, reason=str(error))
            self.phase("stopped")
            raise
        write_json(self.out / f"{txn}-patch-receipt.json", applied)
        self.active = {"transaction": txn, "before_start": job["start-time"],
                       "old_savepoint": old_savepoint, "generation": applied["metadata"]["generation"],
                       "at": time.time(), "proposal": proposal, "managed_bytes": byte_counts}

    def restored_placement(self, job, parallelism, managed_bytes):
        detail = self.rest(f"/jobs/{self.args.job_id}/vertices/{JOIN}")
        subtasks = detail.get("subtasks") or []
        if (len(subtasks) != parallelism or any(task.get("status") != "RUNNING" for task in subtasks)):
            return None
        tm_ids = {task["taskmanager-id"] for task in subtasks}
        snapshots = {tm: self.rest("/taskmanagers/" + quote(tm, safe="")) for tm in tm_ids}
        allocations = [slot for detail in snapshots.values() for slot in detail.get("allocatedSlots", [])
                       if slot.get("jobId") == self.args.job_id
                       and slot.get("resource", {}).get("managedMemory") == managed_bytes / MIB]
        if len(allocations) < parallelism:
            return None
        pods = json.loads(self.kube("get", "pods", "-l",
                                    f"app={self.args.deployment},component=taskmanager", "-o", "json"))
        saved = {}
        from_time = datetime.fromtimestamp(self.active["at"], timezone.utc).isoformat(timespec="seconds")
        for pod in pods["items"]:
            name = pod["metadata"]["name"]
            lines = self.kube("logs", name, "-c", "flink-main-container", "--since-time", from_time)
            resources = {match[1]: int(managed[1]) for match in ALLOCATION.finditer(lines)
                         if (managed := MANAGED.search(match[2]))}
            saved[name] = {int(match[2]): match[3] for match in PLACEMENT.finditer(lines)
                           if match[1] == JOIN and resources.get(match[3]) == managed_bytes}
        if {subtask for per_pod in saved.values() for subtask in per_pod} != set(range(parallelism)):
            return None
        return {"job": job, "join": detail, "taskmanagers": snapshots,
                "join_placement_by_pod": saved, "allocated_slot_count_for_join_bytes": len(allocations)}

    def observe_restore(self, resource, job):
        active = self.active
        txn = active["transaction"]
        if time.time() - active["at"] > self.args.restore_timeout:
            self.publish("rescale_failed", transaction=txn, reason="Savepoint/restore qualification timeout")
            self.phase("stopped")
            raise RuntimeError("The owned Q8 profiles did not qualify after savepoint")
        status = resource.get("status", {})
        if status.get("jobManagerDeploymentStatus") == "MISSING" and not active.get("offline"):
            self.publish("source_offline_start", transaction=txn)
            active["offline"] = True
        if not job or job.get("state") != "RUNNING" or job.get("start-time", 0) <= active["before_start"]:
            return
        if active.get("offline") and not active.get("online"):
            self.publish("source_offline_end", transaction=txn)
            active["online"] = True
        if (status.get("observedGeneration", 0) < active["generation"]
                or status.get("reconciliationStatus", {}).get("state") != "DEPLOYED"):
            return
        savepoint = savepoint_path(resource)
        if not savepoint or savepoint == active["old_savepoint"]:
            return
        wanted = active["proposal"]["parallelism"]
        if any(item["parallelism"] != int(wanted[item["id"]]) for item in job["vertices"]):
            return
        evidence = self.restored_placement(job, int(wanted[JOIN]), active["managed_bytes"][JOIN])
        if evidence is None:
            return
        response = self.rest(f"/jobs/{self.args.job_id}/vertices/{JOIN}/a4s-metrics?get=cacheCostSamples,cacheCostCompaction")
        capacities = {s["subtask"]: s["capacityBytes"] for s in response.get("cacheCostSamples", [])}
        if set(capacities) != set(range(int(wanted[JOIN]))) or min(capacities.values()) <= 0:
            return
        evidence["native_cache_bytes"] = capacities
        evidence["checkpoint_status"] = self.rest(f"/jobs/{self.args.job_id}/checkpoints")
        evidence["savepoint"] = savepoint
        evidence["resource"] = resource
        write_json(self.out / f"{txn}-applied.json", evidence)
        self.publish("rescale_end", transaction=txn, savepoint=savepoint,
                     restored_job_start_epoch_ms=job["start-time"],
                     observed_parallelism=wanted, join_managed_bytes=active["managed_bytes"][JOIN])
        self.acknowledged = txn
        self.active = None
        self.applied += 1
        self.phase("ready")

    def run(self):
        initial_resource = self.resource()
        self.uid = initial_resource["metadata"]["uid"]
        self.events.touch(exist_ok=True)
        self.phase("ready")
        if self.args.policy_jar:
            with self.proposals.open("w") as out, (self.out / "policy.log").open("w") as err:
                jvm_options = ["-Dlog4j.configurationFile=" + str(self.args.log_config)]
                if self.args.main_empty_curves is not None:
                    jvm_options.append("-Da4s.quick.main-empty-curves=" + self.args.main_empty_curves)
                self.jvm = subprocess.Popen(
                    ["java", *jvm_options, "-cp", self.args.classpath + ":" + str(self.args.policy_jar),
                     "org.apache.flink.autoscaler.RyanQ8Policy", self.args.arm, self.args.job_id,
                     str(self.args.manifest), str(self.pointer), str(self.gate)], stdout=out, stderr=err,
                )
        window_end = (self.args.release_ms / 1000 + self.args.observation_seconds
                      if self.args.observation_seconds else None)
        seen_proposals = 0
        started = None
        deadline = time.monotonic() + 240
        while time.monotonic() <= deadline or self.active:
            resource = self.resource()
            job = self.find_rest()
            if self.jvm and self.jvm.poll() is not None:
                raise RuntimeError("The private A4S policy process exited early")
            if job and job["state"] == "RUNNING" and started is None:
                started = time.monotonic()
                deadline = started + self.args.duration
                write_json(self.out / "ready.json", job)
            state = (job or {}).get("state") or resource.get("status", {}).get("jobStatus", {}).get("state")
            if state == "FAILED":
                raise RuntimeError("Benchmark job failed")
            observed = time.time()
            ended = (job or {}).get("end-time", -1)
            on_time = window_end is None or observed <= window_end or 0 < ended <= window_end * 1000
            if state == "FINISHED" and not self.active and on_time:
                write_json(self.out / "finished.json", {"epoch": observed, "job": job,
                            "status": resource.get("status", {}).get("jobStatus")})
                return
            if window_end is not None and observed >= window_end:
                receipt = {"release_ms": self.args.release_ms, "observation_seconds": self.args.observation_seconds,
                           "deadline_epoch": window_end, "observed_epoch": observed, "state": state,
                           "job": job, "status": resource.get("status", {}).get("jobStatus"),
                           "active_transaction": self.active["transaction"] if self.active else None}
                if job and state == "RUNNING":
                    try:
                        receipt["snapshot"] = capture(self.pointer.read_text().strip(), self.args.job_id, {})
                    except (OSError, ValueError, KeyError, HTTPException) as error:
                        receipt["snapshot_error"] = str(error)
                receipt["snapshot_end_epoch"] = time.time()
                write_json(self.out / "window-ended.json", receipt)
                return
            if self.active:
                try:
                    self.observe_restore(resource, job)
                except (OSError, URLError, subprocess.CalledProcessError):
                    pass
            elif started is not None and time.monotonic() < deadline:
                if self.proposals.exists():
                    lines = self.proposals.read_text().splitlines(keepends=True)
                    for line in lines[seen_proposals:]:
                        if not line.endswith("\n"):
                            break
                        seen_proposals += 1
                        proposal = json.loads(line)
                        if proposal.get("type") == "proposal":
                            self.submit(resource, job, proposal)
                            if self.active:
                                break
            time.sleep(2)
        raise RuntimeError("Bounded Q8 job did not finish within the observation window")

    def close(self):
        self.phase("stopped")
        for process in (self.jvm, self.forward):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--arm", choices=("hardcoded", "calibrated"), required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path,
                        default=Path("/etc/flink-kubernetes-autoscaling/kubeconfig"))
    parser.add_argument("--classpath")
    parser.add_argument("--policy-jar", type=Path)
    parser.add_argument("--main-empty-curves", choices=("literal", "omit-empty"))
    parser.add_argument("--log-config", type=Path, default=Path(__file__).with_name("policy-log4j2.properties"))
    parser.add_argument("--release-ms", type=int)
    parser.add_argument("--observation-seconds", type=int, default=0)
    parser.add_argument("--duration", type=int, default=1800)
    parser.add_argument("--restore-timeout", type=int, default=420)
    args = parser.parse_args()
    if args.observation_seconds < 0 or (args.observation_seconds > 0 and not args.release_ms):
        parser.error("A positive observation horizon requires the source release timestamp")
    if args.main_empty_curves is not None and args.arm != "hardcoded":
        parser.error("Only the main controller accepts an empty-curve mode")
    if (args.namespace != "a4s-bench-ryan-q8"
            or not re.fullmatch(r"r8pen-[0-9]{6}", args.owner)
            or args.deployment != f"{args.owner}-{args.arm}"
            or not re.fullmatch("[a-f0-9]{32}", args.job_id)
            or not args.policy_jar or not args.classpath):
        parser.error("Expected an isolated Q8 arm, policy classpath and scoped job ID")
    guard = Guard(args)
    def terminate(_, __):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, terminate)
    try:
        guard.run()
    finally:
        guard.close()


if __name__ == "__main__":
    main()
