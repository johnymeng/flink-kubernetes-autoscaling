"""Render a namespace-scoped native Flink operator installation as a Kubernetes List."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import yaml


MODULE = "sources/operators/flink-kubernetes-operator-a4s"
HERE = Path(__file__).resolve().parent
CLASSPATH = ("/opt/q8-operator/wiring.jar:/opt/q8-operator/variant.jar:/opt/q8-operator/telemetry.jar:"
             "./${KUBERNETES_STANDALONE_JAR}:./${OPERATOR_JAR}:${OPERATOR_LIB}/*")


def verify_source(repo, artifact, target, source_folder):
    with ZipFile(artifact) as jar:
        manifest = jar.read("META-INF/MANIFEST.MF").decode().replace("\r\n ", "")
    source = re.search(r"^Implementation-Revision: ([a-f0-9]{40})\r?$", manifest, re.MULTILINE)
    if not source:
        raise ValueError(f"Missing full Implementation-Revision in {artifact}. Rebuild with build.sh.")
    def tree(ref):
        return subprocess.check_output(["git", "-C", repo, "rev-parse", "--verify",
                                        f"{ref}:{MODULE}/{source_folder}/src/main/java"], text=True).strip()
    if tree(source.group(1)) != tree(target):
        raise ValueError(f"{artifact} has a different {source_folder} source tree from {target}")


def render(args):
    jars = {"variant.jar": args.autoscaler, "wiring.jar": args.wiring, "telemetry.jar": args.telemetry}
    verify_source(args.repo, args.autoscaler, args.revision, "flink-autoscaler")
    verify_source(args.repo, args.wiring, args.revision, "flink-kubernetes-operator")
    blobs = {name: path.read_bytes() for name, path in jars.items()}
    if sum(map(len, blobs.values())) > 1024 * 1024:
        raise ValueError("Operator overlays exceed the Kubernetes ConfigMap limit")
    digest = {name: hashlib.sha256(data).hexdigest() for name, data in blobs.items()}
    base_sha = hashlib.sha256(args.base_jar.read_bytes()).hexdigest()
    with ZipFile(args.base_jar) as archive:
        if "org/apache/flink/kubernetes/operator/FlinkOperator.class" not in archive.namelist():
            raise ValueError("Expected the operator image's shaded Flink operator JAR")
    repo, image_sha = args.image.rsplit("@", 1)
    values = {"watchNamespaces": [args.namespace], "webhook": {"create": False},
              "image": {"repository": repo, "digest": image_sha, "pullPolicy": "IfNotPresent"}}
    if args.node:
        values["operatorPod"] = {"nodeSelector": {"kubernetes.io/hostname": args.node}}
    with TemporaryDirectory() as tmp:
        values_file = Path(tmp) / "values.json"
        values_file.write_text(json.dumps(values))
        helm = subprocess.check_output(["helm", "template", "flink-kubernetes-operator", str(args.chart),
                                       "--namespace", args.namespace, "-f", str(values_file)])
    items = [{"apiVersion": "v1", "kind": "ConfigMap",
              "metadata": {"name": "q8-native-operator-jars", "namespace": args.namespace},
              "binaryData": {name: base64.b64encode(blob).decode() for name, blob in blobs.items()}}]
    for manifest in yaml.safe_load_all(helm):
        if not manifest:
            continue
        kind = manifest["kind"]
        if kind not in ("Role", "RoleBinding", "ServiceAccount", "ConfigMap", "Deployment", "Service"):
            raise ValueError(f"Unexpected resource {kind}; expected a namespace-scoped operator installation")
        manifest["metadata"]["namespace"] = args.namespace
        if kind == "ConfigMap" and "flink-conf.yaml" in manifest.get("data", {}):
            config = manifest["data"]
            for key in ("kubernetes.operator.reconcile.interval", "kubernetes.operator.observer.progress-check.interval"):
                previous = config["flink-conf.yaml"]
                config["flink-conf.yaml"] = "\n".join(l for l in previous.splitlines() if not l.startswith(key + ":"))
                config["flink-conf.yaml"] += "\n" + key + ": 5 s\n"
            config["log4j-operator.properties"] += (
                "\nlogger.q8metrics.name = org.apache.flink.autoscaler\nlogger.q8metrics.level = DEBUG\n"
                "logger.q8native.name = org.apache.flink.kubernetes.operator.service.NativeFlinkService\n"
                "logger.q8native.level = DEBUG\n")
        if kind == "Deployment":
            pod = manifest["spec"]["template"]
            annotations = {"q8.autoscaling/revision": args.revision, "q8.autoscaling/policy-sha256": digest["variant.jar"]}
            for metadata in (manifest["metadata"], pod["metadata"]):
                metadata.setdefault("annotations", {}).update(annotations)
            spec = pod["spec"]
            spec.setdefault("volumes", []).append({"name": "native-code", "configMap": {"name": "q8-native-operator-jars"}})
            c = next(c for c in spec["containers"] if c["name"] == "flink-kubernetes-operator")
            if c["image"] != args.image:
                raise ValueError("Helm rendered an unexpected operator image")
            c["resources"] = {"requests": {"cpu": "150m", "memory": "300Mi"}, "limits": {"cpu": "1", "memory": "768Mi"}}
            c.setdefault("volumeMounts", []).append({"name": "native-code", "mountPath": "/opt/q8-operator", "readOnly": True})
            checks = [f"echo '{sha}  /opt/q8-operator/{name}' | sha256sum -c -" for name, sha in digest.items()]
            checks += ["cd /flink-kubernetes-operator", f'echo "{base_sha}  $OPERATOR_JAR" | sha256sum -c -',
                       f'exec java -Xmx480m -XX:ActiveProcessorCount=2 $LOG_CONFIG -cp "{CLASSPATH}" Q8NativeOperator']
            c["command"], c["args"] = ["/bin/sh", "-ec"], ["\n".join(checks)]
        items.append(manifest)
    if sum(doc["kind"] == "Deployment" for doc in items) != 1:
        raise ValueError("Expected a single native operator deployment")
    return {"apiVersion": "v1", "kind": "List", "items": items}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=HERE.parents[2], type=Path)
    parser.add_argument("--chart", type=Path, help="Defaults to the repository's A4S operator Helm chart")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--revision", required=True, help="Full commit of the measured branch")
    parser.add_argument("--image", required=True, help="Repository and sha256 digest of the native operator base")
    parser.add_argument("--base-jar", required=True, type=Path, help="Shaded operator JAR extracted from --image")
    parser.add_argument("--autoscaler", required=True, type=Path, help="Pinned variant.jar")
    parser.add_argument("--wiring", required=True, type=Path, help="Pinned wiring.jar from build.sh")
    parser.add_argument("--telemetry", required=True, type=Path)
    parser.add_argument("--node", help="Optional Kubernetes hostname for the controller")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or not re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", args.image):
        parser.error("Pass a full Git commit and an immutable operator image digest")
    args.chart = args.chart or args.repo / MODULE / "helm/flink-kubernetes-operator"
    print(json.dumps(render(args)))


if __name__ == "__main__":
    main()
