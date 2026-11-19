"""Run one side of the Q8 comparison using its deployed, built-in A4S operator."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from threading import Thread
from urllib.request import urlopen
from uuid import uuid4
from zipfile import ZipFile

import run


MODES = {"hardcoded": "main", "calibrated": "branch"}
SQL_JAR = "nexmark-flink-0.3-SNAPSHOT.jar"
KAFKA_JARS = ("flink-sql-connector-kafka-3.3.0.jar", "kafka-clients-3.4.0.jar")
LIBRARIES = {SQL_JAR: SQL_JAR, **{name: name for name in KAFKA_JARS}, "rocksdbjni.jar": "00-a4s-rocksdbjni.jar",
             "abi3-mrc-telemetry.jar": "00-a4s-abi3-telemetry.jar",
             "savepoint-profiles.jar": "000-q8-savepoint-profiles.jar"}


def configure_manifest(manifest, args, owner, hashes):
    spec = manifest["spec"]
    name = manifest["metadata"]["name"]
    spec["job"].update(jarURI="local:///opt/flink/lib/" + SQL_JAR,
                       entryClass="com.github.nexmark.flink.sql.SqlQueryJob",
                       parallelism=2, upgradeMode="stateless",
                       args=["--query", "q8_unique", "--job-name", name, "--source", "kafka",
                             "--bootstrap-servers", args.bootstrap_servers])
    config = spec["flinkConfiguration"]
    # Source vertex IDs in the shared historical template belong to the DataStream job.
    config.pop("job.autoscaler.vertex.exclude.ids", None)
    config.update({
        "job.autoscaler.enabled": "true",
        "job.autoscaler.scaling.enabled": "true",
        "job.autoscaler.justin.enabled": "true",
        "job.autoscaler.a4s.enabled": "true",
        "job.autoscaler.memory.tuning.enabled": "false",
        "job.autoscaler.a4s.memory-base.mb": "32",
        "kubernetes.operator.job.upgrade.inplace-scaling.enabled": "true",
        "benchmark.q8.initial-slot-sharing.enabled": "true",
        "jobmanager.adaptive-scheduler.resource-stabilization-timeout": "300 s",
        "execution.checkpointing.interval": "300 min",
        "parallelism.default": "2",
        "table.exec.resource.default-parallelism": "2",
        "taskmanager.numberOfTaskSlots": "2",
        "taskmanager.cpu.cores": "2",
        "taskmanager.memory.managed.size": "316m",
        "job.autoscaler.vertex.max-parallelism": config["pipeline.max-parallelism"],
    })
    spec["taskManager"]["resource"]["cpu"] = 2
    base = f"http://{owner}-artifacts.{args.namespace}.svc.cluster.local:8000"
    commands = []
    for filename, installed in LIBRARIES.items():
        target = "/stage/lib/" + installed
        commands.extend([f"wget -q -O {target} {base}/{filename}",
                         f"echo '{hashes[filename]}  {target}' | sha256sum -c -"])
    for component in ("jobManager", "taskManager"):
        pod = spec[component]["podTemplate"]["spec"]
        stage = next(c for c in pod["initContainers"] if c["name"] == "stage-ryan-q8-artifacts")
        stage["command"][-1] = "\n".join(commands)
        pod["volumes"] = [v for v in pod["volumes"] if v["name"] != "query-jar"]
        for container in pod["initContainers"] + pod["containers"]:
            container["volumeMounts"] = [v for v in container.get("volumeMounts", []) if v["name"] != "query-jar"]


def check_artifacts(artifacts):
    entry = "org/apache/flink/runtime/scheduler/adaptive/AdaptiveScheduler.class"
    with ZipFile(artifacts / "savepoint-profiles.jar") as archive:
        if b"benchmark.q8.initial-slot-sharing.enabled" not in archive.read(entry):
            raise ValueError("Two-slot startup requires the shared-start scheduler overlay")
    with ZipFile(artifacts / SQL_JAR) as archive:
        if "com/github/nexmark/flink/sql/SqlQueryJob.class" not in archive.namelist():
            raise ValueError("The Nexmark SQL JAR is missing its application entry point")
        if not any("q8_unique" in name.lower() and name.lower().endswith(".sql") for name in archive.namelist()):
            raise ValueError("The Nexmark SQL JAR must contain the q8_unique query")


def operator_identity(args, *, check_provenance=False):
    """Read the running controller container and verify its autoscaler artifact."""
    cmd = ["kubectl", "-n", args.operator_namespace, "--request-timeout=30s"]
    def read(*argv):
        return subprocess.check_output([*cmd, *argv], text=True)

    deploy = json.loads(read("get", "deployment", args.operator_deployment, "-o", "json"))
    selector = deploy["spec"]["selector"]
    labels = ",".join(f"{k}={v}" for k, v in selector["matchLabels"].items())
    pods = json.loads(read("get", "pods", "-l", labels, "-o", "json"))["items"]
    # Deployment selectors can also match terminating pods during an operator rollout.
    ready = [(p, c) for p in pods if not p["metadata"].get("deletionTimestamp")
             for c in p.get("status", {}).get("containerStatuses", [])
             if c["name"] == args.operator_container and c.get("ready")]
    if len(ready) != 1:
        raise RuntimeError(f"Expected one ready {args.operator_container} controller, found {len(ready)}")
    pod, container = ready[0]
    name = pod["metadata"]["name"]
    actual = read("exec", name, "-c", args.operator_container, "--", "sha256sum", args.operator_jar).split()[0]
    if actual != args.operator_jar_sha256:
        raise ValueError(f"Active {name}:{args.operator_jar} SHA-256 {actual} differs from the requested build")
    provenance = {}
    annotations = deploy["metadata"].get("annotations", {})
    recorded = annotations.get("q8.autoscaling/revision")
    if recorded is not None:
        if recorded != args.revision or annotations.get("q8.autoscaling/policy-sha256") != actual:
            raise ValueError("Deployed operator revision or policy digest differs from its build annotations")
        if check_provenance:
            code_path = read("exec", name, "-c", args.operator_container, "--", "readlink", "-f", args.operator_jar).strip()
            classes = ["org.apache.flink.autoscaler." + cls for cls in
                       ("ScalingMetricEvaluator", "RestApiMetricsCollector", "JobAutoScalerImpl", "a4s.A4S")]
            # The operator's DEBUG log can rotate during a benchmark. Re-run its own
            # class-origin check from the live process's classpath and working directory.
            live = read("exec", name, "-c", args.operator_container, "--", "cat", "/proc/1/cmdline").split("\0")
            if "-cp" not in live or "Q8NativeOperator" not in live:
                raise ValueError("The annotated operator is not running its verified Java entry point")
            classpath = live[live.index("-cp") + 1]
            logs = read("exec", name, "-c", args.operator_container, "--", "sh", "-c",
                        'cd /proc/1/cwd && exec "$@"', "verify", "java", "-Xmx96m", "-XX:ActiveProcessorCount=1",
                        "-cp", classpath, "Q8NativeOperator", "--verify-only")
            for cls in classes:
                marker = "Q8_NATIVE_OPERATOR " + cls + " "
                paths = [line.partition(marker)[2].strip() for line in logs.splitlines() if marker in line]
                if not paths or any(path != code_path for path in paths):
                    raise RuntimeError(f"Operator did not attest loading {cls} from {code_path}")
                provenance[cls] = code_path
    return {"namespace": args.operator_namespace, "deployment": args.operator_deployment,
            "pod": name, "pod_uid": pod["metadata"]["uid"], "container": args.operator_container,
            "container_id": container["containerID"], "image_id": container["imageID"], "class_origins": provenance,
            "revision": args.revision, "jar": args.operator_jar, "jar_sha256": actual}


def job_control(command, args, directory, *values):
    script = run.HERE.parents[1] / "scripts/autoscaling/job-management" / (command + ".sh")
    with (directory / (command + ".log")).open("a") as log:
        subprocess.run(["bash", str(script), "--namespace", args.namespace, *values],
                       stdout=log, stderr=subprocess.STDOUT, check=True)


def pair_receipt(pair, args):
    metadata = {"scenario": "two-slot-80k", "source_mode": "kafka", "scaling_path": "native_operator", "controller": "built-in A4S", "worker": args.worker,
                "observation_seconds": args.observation_seconds, "arm_names": list(MODES),
                "producer_target_events_per_second": 80000, "offered_records_per_second": 80000,
                "offered_rate_scope": "all generated Nexmark events", "expected_initial_total_slots": 2,
                "expected_initial_occupied_slots": 2}
    pair.mkdir(parents=True, exist_ok=True)
    # Both arms may launch concurrently. Publish a complete immutable shared receipt.
    draft = pair / (".pair-" + uuid4().hex)
    run.save(draft, metadata)
    path = pair / "pair.json"
    try:
        try:
            os.link(draft, path)
        except FileExistsError:
            if json.loads(path.read_text()) != metadata:
                raise ValueError("This result directory already contains a different Q8 configuration")
    finally:
        draft.unlink()


def watch(args, directory, name, job_id, plan):
    """Confirm the pre-release allocation, then observe the consumer and supervise its producer."""
    forward = None
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    (directory / "rest-url").write_text(url + "\n")
    with ExitStack() as stack:
        port_log = stack.enter_context((directory / "port-forward.log").open("w"))
        def ensure_forward():
            nonlocal forward
            if forward is None or forward.poll() is not None:
                forward = subprocess.Popen(["kubectl", "-n", args.namespace, "port-forward",
                    "--address", "127.0.0.1", "svc/" + name + "-rest", f"{port}:8081"],
                    stdout=port_log, stderr=subprocess.STDOUT, start_new_session=True)
        stack.callback(lambda: run.stop_observer(forward))
        deadline = time.monotonic() + args.startup_timeout_seconds
        while time.monotonic() < deadline and (args.release_ms is None or time.time() < args.release_ms / 1000):
            ensure_forward()
            try:
                with urlopen(url + "/jobs/" + job_id, timeout=2) as reply:
                    job = json.load(reply)
                with urlopen(url + "/overview", timeout=2) as reply:
                    overview = json.load(reply)
                vertices = job.get("vertices", [])
                total = overview["slots-total"]
                occupied = total - overview["slots-available"]
                if (job["jid"] == job_id and job["state"] == "RUNNING" and vertices
                        and all(v.get("tasks", {}).get("RUNNING", 0) == v["parallelism"] for v in vertices)
                        and total == occupied == 2):
                    break
            except (OSError, ValueError, KeyError, TypeError):
                pass
            time.sleep(2)
        else:
            raise RuntimeError("Could not confirm exactly two occupied slots before the producer release")
        now = time.time()
        release_ms = args.release_ms or max(plan["release_ms"], round((now + 8) * 1000))
        if release_ms <= now * 1000:
            raise RuntimeError("Initial slot verification finished after the requested input release")
        plan["release_ms"] = release_ms
        run.save(directory / "plan.json", plan)
        run.save(directory / "ready.json", job)
        run.save(directory / "initial-allocation.json", {"epoch": now, "total_slots": total,
                                                        "occupied_slots": occupied, "overview": overview})
        end = release_ms / 1000 + args.observation_seconds
        observer_log = stack.enter_context((directory / "observer.log").open("w"))
        kafka_log = stack.enter_context((directory / "kafka-control.log").open("w"))
        observer = subprocess.Popen([sys.executable, str(run.HERE / "native_observe.py"),
                                     str(directory), "--namespace", args.namespace],
                                    stdout=observer_log, stderr=subprocess.STDOUT, start_new_session=True)
        stack.callback(run.stop_observer, observer)
        kafka = subprocess.Popen([sys.executable, str(run.HERE / "kafka_control.py"), str(directory),
                                  "--hook", str(args.kafka_hook), "--tps", "80000",
                                  "--observation-seconds", str(args.observation_seconds)],
                                 stdout=kafka_log, stderr=subprocess.STDOUT, start_new_session=True)
        stack.callback(run.stop_observer, kafka)
        while time.time() < end:
            for child, logfile in ((observer, "observer.log"), (kafka, "kafka-control.log")):
                if child.poll() is not None:
                    raise RuntimeError(f"Monitor exited early ({child.returncode}); see {logfile}")
            ensure_forward()
            time.sleep(min(2, max(0, end - time.time())))
        run.save(directory / "window-ended.json", {"deadline_epoch": end, "observed_epoch": time.time()})
        if observer.wait(timeout=40):
            raise RuntimeError("Native observer failed; see observer.log")
        if kafka.wait(timeout=90):
            raise RuntimeError("Kafka release/reset coordinator failed; see kafka-control.log")


def capture_operator_logs(args, directory, identity, name, job_id):
    """Follow the original container while it runs so rotation cannot erase early decisions."""
    start = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cmd = ["kubectl", "-n", args.operator_namespace, "logs", identity["pod"],
           "-c", args.operator_container, "--timestamps", "--since-time", start]
    markers = (f"{args.namespace}/{name}", job_id)
    continuation = re.compile(r"^\s*(?:at |Caused by:|Suppressed:|\.\.\. |[\w.]+(?:Exception|Error):)")
    def scoped(lines):
        keep = False
        for line in lines:
            clean = re.sub(r"\x1b\[[0-9;]*m", "", line)
            if not continuation.match(clean.partition(" ")[2]):
                keep = any(marker in clean for marker in markers)
            if keep:
                yield clean

    live = directory / "operator-live.log"
    with (directory / "operator-log-stream.stderr").open("x") as errors:
        process = subprocess.Popen([*cmd, "-f"], stdout=subprocess.PIPE, stderr=errors,
                                   text=True, errors="replace", start_new_session=True)
    def drain():
        with live.open("x", buffering=1) as dst, process.stdout:
            dst.writelines(scoped(process.stdout))
    thread = Thread(target=drain, name="operator-log-stream")
    thread.start()

    def finish():
        disconnected = process.poll() is not None
        run.stop_observer(process)
        thread.join(10)
        if thread.is_alive():
            raise RuntimeError("Could not drain operator logs")
        final = directory / "operator.log"
        if not disconnected:
            os.link(live, final)
            return
        # Keep the live prefix and recover the available tail from Kubernetes.
        latest = subprocess.check_output(cmd, text=True, errors="replace")
        with live.open() as prefix, final.open("x") as dst:
            seen = set()
            for line in prefix:
                seen.add(line)
                dst.write(line)
            for line in scoped(latest.splitlines(keepends=True)):
                if line not in seen:
                    dst.write(line)
        run.save(directory / "operator-log-recovery.json", {"early_stream_exit": process.returncode,
                                                            "tail_queried_since": start})
    return finish


def run_arm(args):
    pair = args.output / "pair-1-AB"
    pair_receipt(pair, args)
    directory = pair / args.arm
    directory.mkdir()
    identity = operator_identity(args, check_provenance=True)
    run.save(directory / "operator.json", identity)
    owner = "r8native-" + uuid4().hex[:10]
    created = []
    resource = None
    name = f"{owner}-{args.arm}"
    job_id = uuid4().hex
    finish_log = capture_operator_logs(args, directory, identity, name, job_id)
    try:
        hashes = {n: run.digest(args.artifacts / n) for n in LIBRARIES}
        run.stage_objects(owner, args.worker, args.artifacts, hashes, directory, created)
        release = args.release_ms or round((time.time() + args.release_lead_seconds) * 1000)
        if release <= time.time() * 1000:
            raise ValueError("Requested input release elapsed during artifact staging")
        replacements = {"WORKER": args.worker, "OWNER": owner, "ARM": args.arm, "JOBID": job_id,
                        "MODE": str(args.arm == "calibrated").lower(), "PLAN": "", "RELEASE": release,
                        **{"HASH_" + k: v for k, v in hashes.items()}}
        manifest = run.render("deployment.json", replacements)
        configure_manifest(manifest, args, owner, hashes)
        for label in args.label:
            key, value = label.split("=", 1)
            if key in manifest["metadata"]["labels"]:
                raise ValueError(f"Reserved benchmark label {key}")
            manifest["metadata"]["labels"][key] = value
        run.save(directory / "manifest.json", manifest)
        plan = {"arm": args.arm, "release_ms": release, "source_mode": "kafka",
                                          "kafka_bootstrap_servers": args.bootstrap_servers,
                                          "topics": ["nexmark-person", "nexmark-auction", "nexmark-bid"],
                                          "producer_target_events_per_second": 80000, "artifacts": hashes,
                                          "observation_seconds": args.observation_seconds}
        receipt = directory / "deployment-created.json"
        try:
            job_control("submit-job", args, directory, "--receipt", str(receipt), str(directory / "manifest.json"))
        finally:
            if receipt.exists():
                resource = json.loads(receipt.read_text())
        job_control("job-status", args, directory, name)
        watch(args, directory, name, job_id, plan)
    finally:
        steps = [("Finalize live operator log", finish_log)]
        if resource:
            def archive():
                data = run.kube("get", "flinkdeployment", name, "-o", "json")
                (directory / "deployment-before-cleanup.json").write_text(data)
                data = run.kube("get", "events", "--field-selector",
                                "involvedObject.uid=" + resource["metadata"]["uid"], "-o", "json")
                (directory / "events.json").write_text(data)
            def confirm_controller():
                if any(v != identity[k] for k, v in operator_identity(args).items() if k != "class_origins"):
                    raise RuntimeError("The operator changed during the benchmark")
            steps += [("Capture Flink deployment and events", archive), ("Confirm running operator identity", confirm_controller),
                      ("Capture job status", lambda: job_control("job-status", args, directory, name)),
                      ("Delete owned workload", lambda: job_control("stop-job", args, directory,
                                                                  "--uid", resource["metadata"]["uid"], name)),
                      ("Wait for workload deletion", lambda: run.wait_for_deletion(name))]
        steps.append(("Remove owned infrastructure", lambda: run.cleanup_infra(owner, created, directory)))
        run.cleanup_steps(steps, sys.exc_info()[1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="Shared pair result root; both arms use the same path")
    parser.add_argument("--arm", required=True, choices=MODES)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--operator-namespace", required=True)
    parser.add_argument("--operator-deployment", required=True)
    parser.add_argument("--operator-container", default="flink-kubernetes-operator")
    parser.add_argument("--operator-jar", required=True, help="Path of the pinned autoscaler artifact in the operator")
    parser.add_argument("--operator-jar-sha256", required=True)
    parser.add_argument("--revision", required=True, help="Full source commit of the operator build")
    parser.add_argument("--label", action="append", default=[], help="Job label KEY=VALUE for operator selection")
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--worker", choices=("c167", "c182"), default="c167")
    parser.add_argument("--bootstrap-servers", required=True, help="Bootstrap address for this arm's isolated Kafka broker")
    parser.add_argument("--kafka-hook", required=True, type=Path, help="Per-arm executable producer and topic control hook")
    parser.add_argument("--observation-seconds", type=int, default=360)
    parser.add_argument("--startup-timeout-seconds", type=int, default=300)
    parser.add_argument("--release-lead-seconds", type=int, default=125)
    parser.add_argument("--release-ms", type=int, help="Optional epoch milliseconds for a synchronized input release")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or not re.fullmatch(r"[0-9a-f]{64}", args.operator_jar_sha256):
        parser.error("Expected a full source commit and lowercase SHA-256 of the deployed autoscaler JAR")
    if not Path(args.operator_jar).is_absolute() or any("=" not in label for label in args.label):
        parser.error("Use an absolute in-container operator JAR path and KEY=VALUE job labels")
    if min(args.observation_seconds, args.release_lead_seconds, args.startup_timeout_seconds) < 1:
        parser.error("Observation duration and release lead must be positive")
    args.kafka_hook = args.kafka_hook.resolve()
    if not args.kafka_hook.is_file() or not os.access(args.kafka_hook, os.X_OK):
        parser.error("Kafka hook must name an executable file")
    check_artifacts(args.artifacts)
    run.use_namespace(args.namespace)
    run_arm(args)


if __name__ == "__main__":
    main()
