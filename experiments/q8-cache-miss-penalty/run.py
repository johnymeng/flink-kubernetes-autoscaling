"""Run a paired Q8 experiment on the lab cluster from explicitly supplied pinned JARs."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import uuid4

HERE = Path(__file__).resolve().parent
DEFAULT_NS = "a4s-bench-ryan-q8"
NS = DEFAULT_NS
NAMES = ("Query8.jar", "rocksdbjni.jar", "abi3-mrc-telemetry.jar", "savepoint-profiles.jar")
PLAN = "95:1800:6200,32:8000:32000,165:400:1400,12:8000:32000"
KUBE = ["kubectl", "-n", NS, "--request-timeout=30s"]


def use_namespace(namespace):
    global NS, KUBE
    NS = namespace
    KUBE = ["kubectl", "-n", NS, "--request-timeout=30s"]


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    # Independent observers use these files as readiness and completion signals.
    payload = json.dumps(value, sort_keys=True, indent=2) + "\n"
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp.write_text(payload)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def kube(*args, document=None):
    return subprocess.check_output([*KUBE, *args], input=json.dumps(document) if document is not None else None,
                                   text=True)


def create(manifest):
    return json.loads(kube("create", "-f", "-", "-o", "json", document=manifest))


def remove(created):
    meta = created["metadata"]
    kind = created["kind"].lower()
    plural = {"flinkdeployment": "flinkdeployments", "service": "services", "pod": "pods",
              "role": "roles", "rolebinding": "rolebindings", "serviceaccount": "serviceaccounts"}[kind]
    api = created["apiVersion"]
    base = "/api/v1" if api == "v1" else "/apis/" + api
    route = f'{base}/namespaces/{NS}/{plural}/{meta["name"]}'
    if meta["namespace"] != NS:
        raise ValueError("Namespace mismatch at cleanup")
    kube("delete", "--raw", route, "-f", "-", document={"apiVersion": "v1", "kind": "DeleteOptions",
         "preconditions": {"uid": meta["uid"]}})


def render(template, replacements):
    raw = (HERE / template).read_text().replace(DEFAULT_NS, NS)
    for key, value in replacements.items():
        raw = raw.replace(key, str(value))
    return json.loads(raw)


def stage_objects(owner, worker, artifacts, hashes, output, created):
    for obj in render("infra.json", {"OWNER": owner, "WORKER": worker})["items"]:
        receipt = create(obj)
        created.append(receipt)
        save(output / "infra-created.json", created)
    pod = owner + "-artifacts"
    for _ in range(90):
        status = json.loads(kube("get", "pod", pod, "-o", "json"))["status"]
        if any(x["name"] == "stage" and "running" in x.get("state", {})
               for x in status.get("initContainerStatuses", [])):
            break
        time.sleep(2)
    else:
        raise RuntimeError("Artifact init container did not become available")
    with subprocess.Popen(["tar", "-chf", "-", "-C", str(artifacts), *hashes], stdout=subprocess.PIPE) as pack:
        subprocess.run(["kubectl", "-n", NS, "--request-timeout=180s", "exec", "-i", pod, "-c", "stage",
                        "--", "tar", "-xf", "-", "-C", "/artifacts"], stdin=pack.stdout, check=True)
        pack.stdout.close()
        if pack.wait():
            raise RuntimeError("Artifact stream failed")
    checks = "".join(f"{hashes[name]}  {name}\n" for name in hashes)
    subprocess.run([*KUBE, "exec", "-i", pod, "-c", "stage", "--", "sh", "-ec",
                    "cd /artifacts && sha256sum -c - && chmod 0444 *.jar && touch .ready"],
                   input=checks, text=True, check=True)
    kube("wait", "--for=condition=Ready", "pod/" + pod, "--timeout=90s")
    kube("wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/" + owner + "-ssd-prep", "--timeout=60s")
    (output / "ssd.log").write_text(kube("logs", owner + "-ssd-prep", "-c", "prepare"))


def classpath_hashes(classpath):
    result = {}
    for item in classpath.split(os.pathsep):
        path = Path(item)
        paths = sorted(path.parent.glob("*.jar")) if path.name == "*" else [path]
        if not paths:
            raise ValueError(f"Empty classpath entry {path}")
        for jar in paths:
            result[str(jar.resolve())] = digest(jar)
    return result


def verify_classpath(directory, classpath, policy, *, legacy=False):
    effective = classpath + os.pathsep + str(policy.resolve())
    command = ["java", "-cp", effective, str(HERE / "ClasspathProof.java")]
    if legacy:
        command.append("--legacy")
    text = subprocess.check_output(command, text=True)
    origins = {}
    for row in text.splitlines():
        name, path = row.split("\t")
        origins[name] = {"path": path, "sha256": digest(Path(path))}
    receipt = {"ordered_argument": effective, "origins": origins}
    save(directory / "loaded-classes.json", receipt)
    return receipt


def stop_observer(process):
    if process is None or process.poll() is not None:
        return
    # Each observer runs in its own session, including any kubectl logs/grep children.
    for sig, grace in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(grace)
            return
        except subprocess.TimeoutExpired:
            if sig == signal.SIGKILL:
                raise


def wait_for_deletion(name):
    for _ in range(120):
        if (not kube("get", "flinkdeployment", name, "--ignore-not-found", "-o", "name").strip()
                and not kube("get", "pods", "-l", "app=" + name, "-o", "name").strip()):
            return
        time.sleep(3)
    raise RuntimeError(f"Timed out waiting for owned workload {name} to clear")


def cleanup_steps(steps, prior_error):
    errors = []
    for description, action in steps:
        try:
            action()
        except BaseException as error:
            print(f"{description}: {error}", file=sys.stderr, flush=True)
            errors.append(error)
    if errors and prior_error is None:
        raise errors[0]


def arm_run(args, owner, arm, hashes, cp_hash, template_hash, pair, *, release_ms=None,
            config=None, extra_observer=None, main_empty_curves=None, observation_seconds=0, configure=None):
    directory = pair / arm
    directory.mkdir()
    job_id = uuid4().hex
    name = f"{owner}-{arm}"
    release = release_ms
    if release is None:
        release = int(time.time() * 1000) + args.release_lead_seconds * 1000
    mode = str(arm == "calibrated").lower()
    replacements = {"WORKER": args.worker, "OWNER": owner, "ARM": arm, "JOBID": job_id, "MODE": mode,
                    "PLAN": args.plan, "RELEASE": release, **{"HASH_" + k: v for k, v in hashes.items()}}
    manifest = render("deployment.json", replacements)
    if config:
        manifest["spec"]["flinkConfiguration"].update(config)
    if configure:
        configure(manifest)
    save(directory / "manifest.json", manifest)
    save(directory / "plan.json", {"plan": args.plan, "release_ms": release, "arm": arm, "artifacts": hashes,
         "policy_sha256": digest(args.policy), "controller_classpath_sha256": cp_hash,
         "normalized_manifest_sha256": template_hash})
    resource = create(manifest)
    follower = None
    sampler = None
    try:
        save(directory / "deployment-created.json", resource)
        if extra_observer is not None:
            sampler = subprocess.Popen([sys.executable, str(extra_observer), str(directory)], start_new_session=True)
        follower = subprocess.Popen([sys.executable, str(HERE / "observe.py"), str(directory),
                                    name, job_id, NS], start_new_session=True)
        command = [sys.executable, str(HERE / "actuate.py"), "--namespace", NS, "--deployment", name,
                   "--owner", owner, "--job-id", job_id, "--arm", arm, "--manifest", str(directory / "manifest.json"),
                   "--policy-jar", str(args.policy), "--classpath", args.classpath, "--out", str(directory),
                   "--kubeconfig", os.environ["KUBECONFIG"], "--duration", str(args.timeout)]
        if observation_seconds > 0:
            command.extend(["--observation-seconds", str(observation_seconds), "--release-ms", str(release)])
        if main_empty_curves is not None:
            command.extend(["--main-empty-curves", main_empty_curves])
        with (directory / "controller.log").open("w", buffering=1) as log:
            subprocess.run(command, stdout=log, stderr=log, check=True)
        follower.wait(25)
        if sampler is not None and sampler.wait(15):
            raise RuntimeError(f"Resource sampler failed for {name}")
    finally:
        prior_error = sys.exc_info()[1]
        def archive_resource():
            live = json.loads(kube("get", "flinkdeployment", name, "-o", "json"))
            save(directory / "deployment-before-cleanup.json", live)

        def archive_events():
            events = kube("get", "events", "--field-selector", f"involvedObject.name={name}", "-o", "json")
            (directory / "events.json").write_text(events)

        cleanup_steps([
            ("Stop resource sampler", lambda: stop_observer(sampler)),
            ("Stop source log observer", lambda: stop_observer(follower)),
            ("Archive deployment", archive_resource),
            ("Archive events", archive_events),
            ("Delete owned deployment", lambda: remove(resource)),
            ("Confirm deployment deletion", lambda: wait_for_deletion(name)),
        ], prior_error)
    outcome = "observation ended" if (directory / "window-ended.json").exists() else "finished"
    print(f"{datetime.now(timezone.utc).isoformat()} {name} {outcome}", flush=True)


def cleanup_infra(owner, created, pair):
    def verify():
        kinds = "pods,services,serviceaccounts,roles,rolebindings"
        for _ in range(45):
            if not kube("get", kinds, "-l", "ece496-benchmark-owner=" + owner, "-o", "name").strip():
                records = [{"name": x["metadata"]["name"], "uid": x["metadata"]["uid"]} for x in created]
                save(pair / "cleanup.json", records)
                return
            time.sleep(2)
        raise RuntimeError(f"Owned infrastructure still present for {owner}")

    steps = [(f"Delete owned {r['metadata']['name']}", lambda obj=r: remove(obj)) for r in reversed(created)]
    cleanup_steps([*steps, ("Confirm infrastructure deletion", verify)], sys.exc_info()[1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--worker", choices=("c167", "c182"), default="c167")
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--plan", default=PLAN)
    parser.add_argument("--release-lead-seconds", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--order", choices=("AB", "BA", "ABBA"), default="ABBA")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir()
    hashes = {name: digest(args.artifacts / name) for name in NAMES}
    cp = classpath_hashes(args.classpath)
    save(args.output / "classpath.json", cp)
    verify_classpath(args.output, args.classpath, args.policy)
    cp_hash = hashlib.sha256(json.dumps(cp, sort_keys=True).encode()).hexdigest()
    template_hash = hashlib.sha256(json.dumps(render("deployment.json", {
        "WORKER": args.worker, "PLAN": args.plan, **{"HASH_" + k: v for k, v in hashes.items()}}), sort_keys=True).encode()).hexdigest()
    pairs = ("AB", "BA") if args.order == "ABBA" else (args.order,)
    for i, order in enumerate(pairs, 1):
        owner = "r8pen-" + datetime.now(timezone.utc).strftime("%H%M%S")
        pair = args.output / f"pair-{i}-{order}"
        pair.mkdir()
        created = []
        try:
            stage_objects(owner, args.worker, args.artifacts, hashes, pair, created)
            for letter in order:
                arm_run(args, owner, {"A": "hardcoded", "B": "calibrated"}[letter], hashes, cp_hash, template_hash, pair)
        finally:
            cleanup_infra(owner, created, pair)


if __name__ == "__main__":
    if "--legacy-external" in sys.argv:
        sys.argv.remove("--legacy-external")
        main()
    else:
        import quick_pair
        quick_pair.main()
