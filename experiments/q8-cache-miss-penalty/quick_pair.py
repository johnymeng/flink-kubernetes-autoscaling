"""Run one side of the Q8 comparison using its deployed, built-in A4S operator."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
from threading import Thread
from urllib.request import urlopen
from uuid import uuid4
from zipfile import ZipFile

import benchmark_plan
import run


MODES = {"hardcoded": "main", "calibrated": "branch"}
SQL_JAR = "nexmark-flink-0.3-SNAPSHOT.jar"
KAFKA_JARS = benchmark_plan.WORKLOAD_DEPENDENCIES
PRODUCER_TARGET_EVENTS_PER_SECOND = 80_000
RECORDED_EVENT_MIX = {"person": 1, "auction": 3, "bid": 46}
TOPICS = {event: "nexmark-" + event for event in benchmark_plan.TOPIC_TYPES}
ARTIFACT_IDENTITIES = {
    "rocksdbjni.jar": "jni_jar_sha256",
    "abi3-mrc-telemetry.jar": "telemetry_jar_sha256",
    "savepoint-profiles.jar": "runtime_overlay_sha256",
    SQL_JAR: "benchmark_jar_sha256",
}
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
        pod.setdefault("nodeSelector", {})["kubernetes.io/hostname"] = args.worker
        stage = next(c for c in pod["initContainers"] if c["name"] == "stage-ryan-q8-artifacts")
        stage["command"][-1] = "\n".join(commands)
        pod["volumes"] = [v for v in pod["volumes"] if v["name"] != "query-jar"]
        for container in pod["initContainers"] + pod["containers"]:
            container["volumeMounts"] = [v for v in container.get("volumeMounts", []) if v["name"] != "query-jar"]


def render_trial_manifest(args, owner, job_id, hashes, release):
    if any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value)
           for value in (args.namespace, owner)):
        raise ValueError("Namespace and artifact owner must be Kubernetes DNS labels")
    replacements = {"WORKER": args.worker, "OWNER": owner, "ARM": args.arm, "JOBID": job_id,
                    "MODE": str(args.arm == "calibrated").lower(), "PLAN": "", "RELEASE": release,
                    **{"HASH_" + key: value for key, value in hashes.items()}}
    manifest = run.render("deployment.json", replacements, namespace=args.namespace)
    configure_manifest(manifest, args, owner, hashes)
    for label in args.label:
        key, value = label.split("=", 1)
        labels = manifest["metadata"]["labels"]
        if key in labels:
            raise ValueError(f"Reserved benchmark label {key}")
        labels[key] = value
    return manifest


def artifact_hashes(directory):
    return {name: run.digest(directory / name) for name in LIBRARIES}


def load_comparison(path):
    return benchmark_plan.prepare_plan(json.loads(path.read_text()))


def check_plan_order(plan, pair_index, order, seconds):
    if plan["validation"]["plan_status"] != "VALID":
        raise ValueError("Comparison plan is not valid; inspect its offline dry-run receipt")
    schedule = plan["orders"]
    if not 1 <= pair_index <= len(schedule):
        raise ValueError("Selected pair is outside the predeclared schedule")
    expected_order = "parallel" if plan["execution"] == "concurrent" else schedule[pair_index - 1]
    if order != expected_order:
        raise ValueError(f"Selected execution order differs from the plan ({expected_order})")
    for arm in plan["arms"].values():
        if seconds != arm["horizon"]["duration_seconds"]:
            raise ValueError("Requested observation duration differs from the finite plan")


def bind_arm_plan(args, plan, hashes):
    check_plan_order(plan, args.pair_index, args.pair_order, args.observation_seconds)
    label = "A" if args.arm == "hardcoded" else "B"
    arm = plan["arms"][label]
    requested = arm["artifacts"]["requested"]
    if arm["source"]["revision"] != args.revision or requested["operator_variant_sha256"] != args.operator_jar_sha256:
        raise ValueError("Selected operator commit or variant SHA-256 differs from the plan")
    if any(hashes[name] != requested[key] for name, key in ARTIFACT_IDENTITIES.items()):
        raise ValueError("Staged workload/runtime artifact hashes differ from the plan")
    dependencies = requested.get("benchmark_dependencies_sha256")
    if dependencies != {name: hashes[name] for name in KAFKA_JARS}:
        raise ValueError("Kafka connector and client hashes differ from the plan")
    workload = arm["workload"]
    target = plan["validation"]["arms"][label]["load"]["target_total_generated_events_per_second"]
    if (target != PRODUCER_TARGET_EVENTS_PER_SECOND or workload.get("topics") != TOPICS
            or workload.get("source_mode") != "kafka" or workload.get("event_mix") != RECORDED_EVENT_MIX
            or workload.get("topic_partitions") != 24 or workload.get("first_event_id") != 1
            or workload.get("total_event_budget") != 300_000_000
            or workload.get("generator_implementation") != "insert_kafka_unique"
            or workload.get("start_offsets") != {name: 0 for name in TOPICS.values()}):
        raise ValueError("The existing Kafka implementation requires its recorded 80k workload and initial state")
    if workload.get("producer_hook_sha256") != run.digest(args.kafka_hook):
        raise ValueError("The selected producer hook does not match the plan receipt")
    if (os.environ.get("Q8_SQL_IMAGE") != workload["producer_image"]
            or os.environ.get("Q8_KAFKA_IMAGE") != workload["broker_image"]
            or os.environ.get("Q8_KAFKA_HOST", "c153") != arm["placement"]["broker_worker"]):
        raise ValueError("The producer/broker images or broker worker differ from the plan")
    planned_manifest = arm["manifest"]
    if planned_manifest["metadata"]["namespace"] != args.namespace:
        raise ValueError("Selected namespace differs from the plan")
    if (benchmark_plan.job_arguments(planned_manifest).get("--bootstrap-servers")
            != args.bootstrap_servers):
        raise ValueError("Selected broker differs from the plan")
    manifest = render_trial_manifest(args, "plan-preview", "0" * 32, hashes, 0)
    if benchmark_plan.comparable_manifest(manifest) != benchmark_plan.comparable_manifest(planned_manifest):
        raise ValueError("The runner's effective manifest differs from the predeclared arm")
    return arm


def check_artifacts(artifacts):
    entry = "org/apache/flink/runtime/scheduler/adaptive/AdaptiveScheduler.class"
    overlay = artifacts / "savepoint-profiles.jar"
    requirements = "org.apache.flink.runtime.jobgraph.justin.JustinVertexResourceRequirements"
    with ZipFile(overlay) as archive:
        if b"benchmark.q8.initial-slot-sharing.enabled" not in archive.read(entry):
            raise ValueError("Two-slot startup requires the shared-start scheduler overlay")
        if requirements.replace(".", "/") + ".class" not in archive.namelist():
            raise ValueError("Rebuild the shared-start overlay with serializable in-place resource requirements")
    signature = subprocess.check_output(["javap", "-classpath", str(overlay), requirements], text=True)
    if not re.search(r"public class [^\n]+ implements [^\n]*java\.io\.Serializable\b", signature):
        raise ValueError("In-place resource persistence requires serializable JustinVertexResourceRequirements")
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
            "revision": args.revision, "jar": args.operator_jar, "jar_sha256": actual,
            "configured_artifact_annotations": {
                key: value for key, value in annotations.items() if key.startswith("q8.autoscaling/")},
            "configured_container_image": next(
                (entry["image"] for entry in pod["spec"]["containers"] if entry["name"] == args.operator_container),
                None),
            "class_origin_evidence_kind": ("same-classpath process probe" if provenance else None)}


def job_control(command, args, directory, *values):
    script = run.HERE.parents[1] / "scripts/autoscaling/job-management" / (command + ".sh")
    with (directory / (command + ".log")).open("a") as log:
        subprocess.run(["bash", str(script), "--namespace", args.namespace, *values],
                       stdout=log, stderr=subprocess.STDOUT, check=True)


def pair_receipt(pair, args):
    comparison = args.plan_document
    workers = ({arm["placement"]["flink_worker"] for arm in comparison["arms"].values()}
               if comparison else {args.worker})
    metadata = {"worker": next(iter(workers)) if len(workers) == 1 else None,
                "workers": sorted(workers), "scenario": "two-slot-80k-mixed", "source_mode": "kafka", "scaling_path": "native_operator",
                "controller": "built-in A4S", "observation_seconds": args.observation_seconds,
                "arm_names": list(MODES), "pair_order": args.pair_order, "pair_index": args.pair_index,
                "comparison": comparison["comparison"] if comparison else "implementation-bundle",
                "isolation": comparison["validation"]["isolation"] if comparison else "sequential",
                "comparison_plan_sha256": benchmark_plan.digest_json(comparison) if comparison else None,
                "producer_target_events_per_second": PRODUCER_TARGET_EVENTS_PER_SECOND,
                "offered_records_per_second": PRODUCER_TARGET_EVENTS_PER_SECOND,
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
                                  "--hook", str(args.kafka_hook), "--tps", str(PRODUCER_TARGET_EVENTS_PER_SECOND),
                                  "--observation-seconds", str(args.observation_seconds)],
                                 stdout=kafka_log, stderr=subprocess.STDOUT, start_new_session=True)
        def stop_kafka():
            # Signal the coordinator first so it can stop its owned external producer.
            # Killing its entire process group here would also kill the cleanup hook.
            if kafka.poll() is None:
                kafka.terminate()
                try:
                    kafka.wait(timeout=190)
                except subprocess.TimeoutExpired:
                    run.stop_observer(kafka)
        stack.callback(stop_kafka)
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


def record_workload_measurements(directory, plan):
    from native_check import kafka_evidence

    release = plan["release_ms"] / 1000
    generations, _, issues = kafka_evidence(directory, release, release + plan["observation_seconds"])
    measured = [row for row in generations if row["measured_s"] > 0]
    complete = [row for row in measured if set(row.get("per_topic_records", {})) == set(TOPICS.values())]
    if len(complete) != len(measured):
        issues.append("Broker samples include a topic population outside the declared plan")
    elapsed = sum(row["measured_s"] for row in complete)
    total = sum(row["records"] for row in complete)
    relevant = sum(row["per_topic_records"][TOPICS[event]]
                   for row in complete for event in ("person", "auction"))
    run.save(directory / "workload-measurements.json", {
        "broker_generated_events": total if elapsed else None,
        "broker_generated_events_per_second": total / elapsed if elapsed else None,
        "broker_q8_relevant_events": relevant if elapsed else None,
        "broker_q8_relevant_events_per_second": relevant / elapsed if elapsed else None,
        "broker_generations": generations,
        "issues": issues,
        "consumer_counters": "resource-series.jsonl" if (directory / "resource-series.jsonl").is_file() else None,
    })


def run_arm(args):
    pair = args.output / f"pair-{args.pair_index}-{args.pair_order}"
    pair_receipt(pair, args)
    directory = pair / args.arm
    directory.mkdir()
    identity = operator_identity(args, check_provenance=True)
    run.save(directory / "operator.json", identity)
    if args.plan_arm is not None:
        expected = args.plan_arm["artifacts"]["requested"]
        annotations = identity["configured_artifact_annotations"]
        for annotation, key in (("wiring-sha256", "operator_wiring_sha256"),
                                ("telemetry-sha256", "telemetry_jar_sha256"),
                                ("base-jar-sha256", "operator_jar_sha256")):
            if annotations.get("q8.autoscaling/" + annotation) != expected[key]:
                raise ValueError(f"Configured operator {annotation} differs from the plan")
        if identity["configured_container_image"] != expected["operator_image"]:
            raise ValueError("Configured operator image differs from the plan")
    if args.plan_document is not None:
        run.save(directory / "comparison-plan.json", args.plan_document)
    owner = "r8native-" + uuid4().hex[:10]
    created = []
    resource = None
    name = f"{owner}-{args.arm}"
    job_id = uuid4().hex
    finish_log = capture_operator_logs(args, directory, identity, name, job_id)
    try:
        hashes = args.artifact_hashes
        run.stage_objects(owner, args.worker, args.artifacts, hashes, directory, created)
        release = args.release_ms or round((time.time() + args.release_lead_seconds) * 1000)
        if release <= time.time() * 1000:
            raise ValueError("Requested input release elapsed during artifact staging")
        manifest = render_trial_manifest(args, owner, job_id, hashes, release)
        run.save(directory / "manifest.json", manifest)
        plan = {"arm": args.arm, "release_ms": release,
                "kafka_bootstrap_servers": args.bootstrap_servers,
                "topics": list(TOPICS.values()), "source_mode": "kafka", "worker": args.worker,
                "load_population": "all-generated-events",
                "producer_target_events_per_second": PRODUCER_TARGET_EVENTS_PER_SECOND,
                "event_mix": RECORDED_EVENT_MIX, "expected_q8_relevant_events_per_second": 6_400,
                "artifacts": hashes, "observation_seconds": args.observation_seconds,
                "planned_trial_id": args.plan_arm["trial_id"] if args.plan_arm else None,
                "workload_id": (args.plan_document["validation"]["arms"][
                    "A" if args.arm == "hardcoded" else "B"]["load"]["workload_id"]
                    if args.plan_document else None),
                "planned_artifact_identity": args.plan_arm.get("artifacts") if args.plan_arm else None,
                "artifact_evidence": {"workload_jars_staged": hashes, "worker_observed_loaded": None}}
        receipt = directory / "deployment-created.json"
        try:
            job_control("submit-job", args, directory, "--receipt", str(receipt), str(directory / "manifest.json"))
        finally:
            if receipt.exists():
                resource = json.loads(receipt.read_text())
        job_control("job-status", args, directory, name)
        watch(args, directory, name, job_id, plan)
        record_workload_measurements(directory, plan)
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
                probe_fields = {"class_origins", "class_origin_evidence_kind"}
                if any(value != identity[key] for key, value in operator_identity(args).items()
                       if key not in probe_fields):
                    raise RuntimeError("The operator changed during the benchmark")
            steps += [("Capture Flink deployment and events", archive), ("Confirm running operator identity", confirm_controller),
                      ("Capture job status", lambda: job_control("job-status", args, directory, name)),
                      ("Delete owned workload", lambda: job_control("stop-job", args, directory,
                                                                  "--uid", resource["metadata"]["uid"], name)),
                      ("Wait for workload deletion", lambda: run.wait_for_deletion(name))]
        steps.append(("Remove owned infrastructure", lambda: run.cleanup_infra(owner, created, directory)))
        run.cleanup_steps(steps, sys.exc_info()[1])


def offline(argv):
    parser = argparse.ArgumentParser(description="Render or validate Q8 inputs without external operations")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--dry-run", type=Path, metavar="PLAN")
    modes.add_argument("--preflight-plan", type=Path, metavar="PLAN")
    modes.add_argument("--render-arm", choices=MODES, dest="arm")
    parser.add_argument("--pair-index", type=int, default=1)
    parser.add_argument("--pair-order", choices=("AB", "BA", "parallel"), default="AB")
    parser.add_argument("--observation-seconds", type=int)
    parser.add_argument("--namespace")
    parser.add_argument("--worker", choices=("c167", "c182"), default="c167")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--owner", default="q8-preview")
    parser.add_argument("--label", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        if args.arm:
            if not args.namespace or not args.artifacts or not args.bootstrap_servers:
                parser.error("Arm rendering needs --namespace, --artifacts and --bootstrap-servers")
            hashes = artifact_hashes(args.artifacts)
            print(json.dumps(render_trial_manifest(args, args.owner, "0" * 32, hashes, 0), indent=2))
            return
        plan = load_comparison(args.dry_run or args.preflight_plan)
        if args.preflight_plan:
            check_plan_order(plan, args.pair_index, args.pair_order, args.observation_seconds)
        print(json.dumps(plan, indent=2, sort_keys=True, allow_nan=False))
        if plan["validation"]["plan_status"] in ("INVALID", "INCOMPLETE"):
            raise SystemExit(2)
    except (OSError, ValueError, TypeError, AttributeError, KeyError) as error:
        parser.error(str(error))


def main():
    if any(option.split("=", 1)[0] in ("--dry-run", "--preflight-plan", "--render-arm") for option in sys.argv[1:]):
        offline(sys.argv[1:])
        return
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
    parser.add_argument("--observation-seconds", type=int, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--comparison-plan", type=Path)
    mode.add_argument("--legacy-implementation-bundle", action="store_true")
    parser.add_argument("--pair-index", type=int, default=1)
    parser.add_argument("--pair-order", choices=("AB", "BA", "parallel"), default="AB")
    parser.add_argument("--validate-arm-arguments", action="store_true")
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
    try:
        args.artifact_hashes = artifact_hashes(args.artifacts)
        args.plan_document = load_comparison(args.comparison_plan) if args.comparison_plan else None
        if args.pair_index < 1:
            raise ValueError("Pair index must be positive")
        if not args.plan_document and args.pair_order == "parallel":
            raise ValueError("Concurrent arms require a plan with evidence of independent resources")
        args.plan_arm = bind_arm_plan(args, args.plan_document, args.artifact_hashes) if args.plan_document else None
    except (OSError, ValueError, TypeError, AttributeError, KeyError) as error:
        parser.error(str(error))
    if args.validate_arm_arguments:
        return
    check_artifacts(args.artifacts)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    run.use_namespace(args.namespace)
    run_arm(args)


if __name__ == "__main__":
    main()
