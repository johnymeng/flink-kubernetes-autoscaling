#!/usr/bin/env python3
"""Render the matched Q20 A4S arms locally using the checkpoint experiment template."""

import argparse
import copy
import json
import os
from pathlib import Path
import re
import subprocess

import yaml


ROOT = Path(__file__).resolve().parents[2]
MATRIX = (
    ("original", "embedded", "remote"),
    ("embedded", "remote", "original"),
    ("remote", "original", "embedded"),
)
RUNNER_URL = "http://a4s-runner:8080"
SNAPSHOT_FILE = "a4s-snapshots.jsonl"
MODE_SETTINGS = {
    "job.autoscaler.a4s.runner.url",
    "job.autoscaler.a4s.snapshot.record-file",
}
# These experiment-template settings belong to the separate Justin operator submodule.
OTHER_OPERATOR_SETTINGS = {
    "job.autoscaler.a4s.max-task-managed-memory.mb",
    "job.autoscaler.a4s.shards-adj.enabled",
}


def pinned_image(value):
    if not re.fullmatch(r"[^@\s]+@sha256:[a-f0-9]{64}", value):
        raise argparse.ArgumentTypeError("expected an image reference with a sha256 manifest digest")
    return value


def write_yaml(path, *documents):
    path.write_text(yaml.safe_dump_all(documents, sort_keys=False), encoding="utf-8")


def render_job(directory, run_id, args):
    original = directory / "rendered-template.yaml"
    renderer = ROOT / "experiments/1729-checkpoint-aware-rescaling/render-job.sh"
    env = os.environ | {
        "QUERY": "q20",
        "POLICY": "a4s",
        "EXPERIMENT_PROFILE": "paper",
        "RUN_ID": run_id,
        "CHECKPOINT_RESCALE": "false",
        "CHECKPOINT_INTERVAL": args.checkpoint_interval,
    }
    subprocess.run(["bash", str(renderer), str(original)], env=env, check=True, stdout=subprocess.DEVNULL)
    job = yaml.safe_load(original.read_text(encoding="utf-8"))
    original.unlink()

    job["metadata"]["namespace"] = run_id
    spec = job["spec"]
    spec["image"] = args.workload_image
    job_args = spec["job"]["args"]
    job_args[job_args.index("--bootstrap-servers") + 1] = args.bootstrap_servers
    selectors = (
        ("jobManager", args.jobmanager_node),
        ("taskManager", args.taskmanager_node),
    )
    for manager, node in selectors:
        node_selector = spec[manager]["podTemplate"]["spec"]["nodeSelector"]
        node_selector.pop("tier", None)
        node_selector["kubernetes.io/hostname"] = node
    config = spec["flinkConfiguration"]
    for key in OTHER_OPERATOR_SETTINGS | MODE_SETTINGS:
        config.pop(key, None)
    config["job.autoscaler.scaling.enabled"] = "true"
    config["job.autoscaler.memory.tuning.enabled"] = "false"
    config["pipeline.exclusive-vertex-slots"] = "true"
    return job


def render_arm(out, run_id, mode, args):
    directory = out / run_id
    directory.mkdir()
    job = render_job(directory, run_id, args)
    config = job["spec"]["flinkConfiguration"]
    operator_image = args.original_operator_image if mode == "original" else args.operator_image
    repository, digest = operator_image.split("@")
    values = {
        "watchNamespaces": [run_id],
        "image": {"repository": repository, "digest": digest},
        "webhook": {"create": False},
        "operatorPod": {"nodeSelector": {"kubernetes.io/hostname": args.jobmanager_node}},
    }
    if mode != "original":
        config["job.autoscaler.a4s.snapshot.record-file"] = SNAPSHOT_FILE
        values["operatorPod"]["env"] = [{"name": "A4S_SNAPSHOT_DIR", "value": "/tmp"}]
    if mode == "remote":
        config["job.autoscaler.a4s.runner.url"] = RUNNER_URL
        runner_path = ROOT / "cluster/a4s/runner.yaml"
        runner = list(yaml.safe_load_all(runner_path.read_text(encoding="utf-8")))
        for resource in runner:
            resource["metadata"]["namespace"] = run_id
            if resource["kind"] == "Deployment":
                pod = resource["spec"]["template"]["spec"]
                pod["nodeSelector"] = {"kubernetes.io/hostname": args.jobmanager_node}
                pod["containers"][0]["image"] = args.runner_image
        write_yaml(directory / "runner.yaml", *runner)
    write_yaml(directory / "job.yaml", job)
    write_yaml(directory / "operator-values.yaml", values)

    # Compare the full rendered shape, varying only the run identity and treatment.
    normalized = copy.deepcopy(job)
    normalized["metadata"]["namespace"] = "<run>"
    normalized_args = normalized["spec"]["job"]["args"]
    name = normalized_args.index("--job-name") + 1
    normalized_args[name] = normalized_args[name].removesuffix(run_id) + "<run>"
    normalized_config = normalized["spec"]["flinkConfiguration"]
    for key in ("state.checkpoints.dir", "state.savepoints.dir", "high-availability.storageDir"):
        normalized_config[key] = normalized_config[key].replace(f"/{run_id}/", "/<run>/")
    for key in MODE_SETTINGS:
        normalized_config.pop(key, None)
    return normalized


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--prefix", required=True, help="unique DNS label suffix for this series")
    parser.add_argument("--workload-image", required=True, type=pinned_image)
    parser.add_argument("--original-operator-image", required=True, type=pinned_image)
    parser.add_argument("--operator-image", required=True, type=pinned_image)
    parser.add_argument("--runner-image", required=True, type=pinned_image)
    parser.add_argument("--bootstrap-servers", required=True, help="isolated Kafka endpoint for all arms")
    parser.add_argument("--jobmanager-node", required=True)
    parser.add_argument("--taskmanager-node", required=True)
    parser.add_argument("--checkpoint-interval", default="1m")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?", args.prefix):
        parser.error("--prefix must be a DNS label of up to 40 characters")
    if args.original_operator_image.rsplit("@", 1)[1] == args.operator_image.rsplit("@", 1)[1]:
        parser.error("the original and refactored operator images must have distinct digests")

    output = args.output.resolve()
    output.mkdir(parents=True)
    common = None
    order = []
    for repetition, arms in enumerate(MATRIX, 1):
        for mode in arms:
            run_id = f"q20-{args.prefix}-{repetition}-{mode}"
            normalized = render_arm(output, run_id, mode, args)
            if common is None:
                common = normalized
            elif normalized != common:
                raise ValueError(f"Rendered Q20 settings drifted in {run_id}")
            order.append({"repetition": repetition, "mode": mode, "run_id": run_id})
    (output / "run-order.json").write_text(json.dumps(order, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
