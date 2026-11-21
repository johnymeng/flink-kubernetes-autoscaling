"""Offline validation of Q8 comparison plans and their recorded identities."""
from copy import deepcopy
import hashlib
import json
import math
import re


ONLINE_COSTS = "job.autoscaler.a4s.online-cache-costs.enabled"
NATIVE_ACTUATOR = "built-in-operator-native"
POPULATIONS = ("all-generated-events", "q8-relevant-person-auction")
ARTIFACT_KEYS = (
    "operator_image", "operator_jar_sha256", "operator_variant_sha256",
    "operator_wiring_sha256", "runtime_image", "runtime_jar_sha256",
    "runtime_overlay_sha256", "telemetry_jar_sha256", "jni_jar_sha256", "benchmark_jar_sha256",
)
LOADED_KEYS = (*ARTIFACT_KEYS, "jni_loaded_elf_sha256")
WORKLOAD_DEPENDENCIES = ("flink-sql-connector-kafka-3.3.0.jar", "kafka-clients-3.4.0.jar")
SHA256 = r"[a-f0-9]{64}"
GIT_OBJECT = r"[a-f0-9]{40}"
IMAGE_DIGEST = r"[^@\s]+@sha256:" + SHA256
PARTICIPANTS = ("A", "B")
TOPIC_TYPES = ("person", "auction", "bid")
DURATION_UNITS = {"ms": .001, "s": 1, "min": 60, "h": 3600,
                  "milliseconds": .001, "seconds": 1, "minutes": 60, "hours": 3600}


def digest_json(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def duration_seconds(raw):
    """Read the explicit Flink duration in an effective configuration."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([a-z]+)", str(raw).strip())
    if not match or match[2] not in DURATION_UNITS:
        return None
    seconds = float(match[1]) * DURATION_UNITS[match[2]]
    return seconds if math.isfinite(seconds) else None


def workload_targets(workload):
    population = workload.get("population")
    target = workload.get("target_events_per_second")
    mix = workload.get("event_mix")
    if population not in POPULATIONS or type(target) is not int or target < 1:
        raise ValueError("Provide a population and a positive integer target events/s")
    if (not isinstance(mix, dict) or set(mix) != set(TOPIC_TYPES)
            or any(type(weight) is not int or weight < 0 for weight in mix.values())
            or mix["person"] + mix["auction"] < 1):
        raise ValueError("Provide a nonnegative Person/Auction/Bid event mix with relevant input")
    relevant_share = (mix["person"] + mix["auction"]) / sum(mix.values())
    generated = target if population == POPULATIONS[0] else target / relevant_share
    relevant = generated * relevant_share if population == POPULATIONS[0] else target
    return {
        "workload_id": digest_json(workload),
        "input_population": population,
        "target_total_generated_events_per_second": generated,
        "expected_q8_relevant_events_per_second": relevant,
        "realized": None,
    }


def job_arguments(manifest):
    args = manifest.get("spec", {}).get("job", {}).get("args")
    if not isinstance(args, list) or len(args) % 2 or not all(isinstance(arg, str) for arg in args):
        return {}
    values = dict(zip(args[::2], args[1::2]))
    return values if len(values) == len(args) // 2 else {}


def _canonical_manifest(manifest, *, ignore_worker=False):
    """Normalize only per-trial identities in the harness's known fields."""
    clean = deepcopy(manifest)
    metadata = clean.get("metadata", {})
    namespace = metadata.pop("namespace", None)
    name = metadata.pop("name", None)
    owner = metadata.get("labels", {}).get("ece496-run")
    if owner:
        metadata["labels"]["ece496-run"] = "<owner>"
        annotations = metadata.get("annotations", {})
        if annotations.get("ece496.org/benchmark-owner") == owner:
            annotations["ece496.org/benchmark-owner"] = "<owner>"
    spec = clean.get("spec", {})
    config = spec.get("flinkConfiguration", {})
    config.pop(ONLINE_COSTS, None)
    config.pop("$internal.pipeline.job-id", None)

    # Keep directories, devices, resource settings and artifact digests comparable.
    def owned_path(value):
        if not name or not isinstance(value, str):
            return value
        return re.sub(r"(?<=/)" + re.escape(name) + r"(?=-ssd-v2(?:/|$)|/|$)", "<job>", value)

    for key in ("high-availability.storageDir", "state.checkpoints.dir", "state.savepoints.dir"):
        if key in config:
            config[key] = owned_path(config[key])
    if owner and spec.get("serviceAccount") == owner + "-flink":
        spec["serviceAccount"] = "<owner>-flink"
    arguments = job_arguments(clean)
    if arguments:
        args = spec["job"]["args"]
        for index in range(0, len(args), 2):
            if args[index] == "--job-name" and args[index + 1] == name:
                args[index + 1] = "<job>"
            if args[index] == "--bootstrap-servers":
                # The workload and placement records carry the owned broker identities.
                args[index + 1] = "<broker>"
    for component in ("jobManager", "taskManager"):
        pod = spec.get(component, {}).get("podTemplate", {})
        labels = pod.get("metadata", {}).get("labels", {})
        if owner and labels.get("ece496-run") == owner:
            labels["ece496-run"] = "<owner>"
        pod_spec = pod.get("spec", {})
        if ignore_worker:
            selectors = pod_spec.get("nodeSelector", {})
            if "kubernetes.io/hostname" in selectors:
                selectors["kubernetes.io/hostname"] = "<isolated-worker>"
        for volume in pod_spec.get("volumes", []):
            if "path" in volume.get("hostPath", {}):
                volume["hostPath"]["path"] = owned_path(volume["hostPath"]["path"])
        if owner and namespace:
            server = f"http://{owner}-artifacts.{namespace}.svc.cluster.local:8000/"
            for container in pod_spec.get("initContainers", []):
                if container.get("name") == "stage-ryan-q8-artifacts":
                    container["command"] = [arg.replace(server, "http://<artifact-server>/")
                                            for arg in container.get("command", [])]
    return clean


def comparable_manifest(manifest):
    return _canonical_manifest(manifest)


def _differences(first, second, path):
    if first == second:
        return []
    if isinstance(first, dict) and isinstance(second, dict):
        result = []
        for key in sorted(first.keys() | second.keys()):
            name = f"{path}.{key}"
            if key not in first or key not in second:
                result.append(name)
            else:
                result.extend(_differences(first[key], second[key], name))
        return result
    if isinstance(first, list) and isinstance(second, list) and len(first) == len(second):
        return [difference for i, (left, right) in enumerate(zip(first, second))
                for difference in _differences(left, right, f"{path}[{i}]")]
    return [path]


def _artifact_fields(arm, prefix, missing, unverified, mismatches):
    artifacts = arm.get("artifacts") or {}
    requested = artifacts.get("requested") or {}
    for stage, keys in (("requested", ARTIFACT_KEYS), ("deployed", ARTIFACT_KEYS),
                        ("observed_loaded", LOADED_KEYS)):
        record = artifacts.get(stage) or {}
        for key in keys:
            value = record.get(key)
            path = f"{prefix}.artifacts.{stage}.{key}"
            pattern = IMAGE_DIGEST if key.endswith("_image") else SHA256
            if not isinstance(value, str) or not re.fullmatch(pattern, value):
                (missing if stage == "requested" else unverified).append(path)
            elif stage != "requested" and requested.get(key) not in (None, value):
                mismatches.append(path)
        dependencies = record.get("benchmark_dependencies_sha256") or {}
        requested_dependencies = requested.get("benchmark_dependencies_sha256") or {}
        for jar in WORKLOAD_DEPENDENCIES:
            value = dependencies.get(jar)
            path = f"{prefix}.artifacts.{stage}.benchmark_dependencies_sha256.{jar}"
            if not isinstance(value, str) or not re.fullmatch(SHA256, value):
                (missing if stage == "requested" else unverified).append(path)
            elif stage != "requested" and requested_dependencies.get(jar) not in (None, value):
                mismatches.append(path)
        if stage != "requested" and not _evidence_reference(record.get("evidence")):
            unverified.append(f"{prefix}.artifacts.{stage}.evidence")


def _require_fields(value, keys, prefix, missing):
    for key in keys:
        if value.get(key) is None or value.get(key) == "":
            missing.append(f"{prefix}.{key}")


def _horizon(arm, claim, prefix, missing, issues):
    plan = arm.get("horizon") or {}
    duration = plan.get("duration_seconds")
    if type(duration) is not int or duration <= 0:
        missing.append(f"{prefix}.horizon.duration_seconds")
        return None
    if claim != "learned-action":
        return {"duration_seconds": duration}
    config = arm["manifest"].get("spec", {}).get("flinkConfiguration", {})
    stabilization = duration_seconds(config.get("job.autoscaler.stabilization.interval"))
    window = duration_seconds(config.get("job.autoscaler.metrics.window"))
    cadence = duration_seconds((arm.get("operator_configuration") or {})
                               .get("kubernetes.operator.reconcile.interval"))
    action = plan.get("action_budget_seconds")
    scoring = plan.get("post_action_seconds")
    values = (stabilization, window, cadence, action, scoring)
    names = ("stabilization", "metric_window", "collection_cadence", "action_budget", "post_action_window")
    unset = [name for name, seconds in zip(names, values)
             if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds < 0]
    if unset:
        missing.extend(f"{prefix}.horizon.{name}" for name in unset)
        return {"duration_seconds": duration}
    if min(window, cadence, action, scoring) <= 0:
        issues.append(f"{prefix}: learned-action windows and cadence must be positive")
        return {"duration_seconds": duration}
    # The selected ABI 3 fitter requires two adjacent 60–120 s intervals. The
    # collector needs a full post-stabilization metrics window before acting.
    aligned_interval = math.ceil(60 / cadence) * cadence
    two_intervals = 2 * aligned_interval
    if aligned_interval > 120 or window < two_intervals:
        issues.append(f"{prefix}: configured collection and retention cannot cover two compatible fit windows")
    qualification = stabilization + max(window, two_intervals) + cadence
    required = qualification + action + scoring
    if duration < required:
        issues.append(f"{prefix}: {duration} s horizon is insufficient for the {required:g} s learned-action budget")
    return {"duration_seconds": duration, "qualification_allowance_seconds": qualification,
            "minimum_planned_seconds": required}


def _evidence_reference(evidence):
    return (isinstance(evidence, dict) and isinstance(evidence.get("reference"), str)
            and bool(evidence["reference"].strip())
            and re.fullmatch(SHA256, str(evidence.get("sha256", ""))) is not None)


def prepare_plan(document):
    """Return a versioned comparison receipt without any network or subprocess calls."""
    if ("schema_version" not in document and "arms" not in document
            and ("arm_names" in document or "scaling_path" in document)):
        return {"schema_version": 1, "validation": {"plan_status": "HISTORICAL_BUNDLE",
                "identity_status": "UNVERIFIED"}, "historical_record": deepcopy(document)}
    plan = deepcopy(document)
    if not isinstance(plan.get("execution", "sequential"), str):
        raise ValueError("execution must be sequential or concurrent")
    execution = plan.setdefault("execution", "sequential")
    orders = plan.setdefault("orders", ["AB"])
    issues, missing, unverified, mismatches = [], [], [], []
    differences, armed = [], {}

    if plan.get("schema_version") != 1:
        issues.append("schema_version must be 1")
    if execution not in ("sequential", "concurrent"):
        issues.append("execution must be sequential or concurrent")
    if not isinstance(orders, list) or not orders or any(order not in ("AB", "BA") for order in orders):
        issues.append("orders must be a finite nonempty list of AB and BA")
    kind = plan.get("comparison")
    claim = plan.get("claim")
    if kind not in ("same-build", "implementation-bundle"):
        issues.append("comparison must be same-build or implementation-bundle")
    if claim not in ("measurement", "learned-action"):
        issues.append("claim must be measurement or learned-action")
    if kind == "implementation-bundle" and claim == "learned-action":
        issues.append("implementation bundles do not establish a same-build learned-action comparison")

    arms = plan.get("arms") or {}
    if set(arms) != set(PARTICIPANTS):
        missing.append("arms.A and arms.B")
    for label in PARTICIPANTS:
        arm = arms.get(label)
        if not isinstance(arm, dict):
            continue
        prefix = f"arms.{label}"
        _require_fields(arm, ("trial_id", "mode", "manifest", "operator_configuration", "actuation"),
                        prefix, missing)
        source = arm.get("source") or {}
        for key in ("revision", "tree", "native_gitlink"):
            if not re.fullmatch(GIT_OBJECT, str(source.get(key, ""))):
                missing.append(f"{prefix}.source.{key}")
        patches = source.get("native_patches")
        if (not isinstance(patches, list)
                or any(not isinstance(item, dict) or not item.get("path")
                       or not re.fullmatch(SHA256, str(item.get("sha256", ""))) for item in patches)):
            missing.append(f"{prefix}.source.native_patches")
        _artifact_fields(arm, prefix, missing, unverified, mismatches)

        runtime = arm.get("runtime_policy") or {}
        _require_fields(runtime, ("estimator", "timing_abi", "timing_instrumentation", "metadata_policy",
                                  "memory_policy", "resource_limits"), prefix + ".runtime_policy", missing)
        _require_fields(runtime.get("memory_policy") or {},
                        ("managed", "write_buffer_ratio", "floor_mib_by_vertex"),
                        prefix + ".runtime_policy.memory_policy", missing)
        placement = arm.get("placement") or {}
        _require_fields(placement, ("flink_worker", "broker_worker", "worker_ids",
                                    "storage_device_ids", "configuration"),
                        prefix + ".placement", missing)
        for key in ("worker_ids", "storage_device_ids"):
            values = placement.get(key)
            if not isinstance(values, list) or not values or not all(isinstance(v, str) and v.strip() for v in values):
                missing.append(f"{prefix}.placement.{key}")
        if placement.get("worker_ids") and not all(placement.get(key) in placement["worker_ids"]
                                                   for key in ("flink_worker", "broker_worker")):
            issues.append(f"{prefix}: the Flink and broker workers must be covered by the placement record")

        workload = arm.get("workload") or {}
        _require_fields(workload, ("query", "source_mode", "source_implementation", "generator_implementation",
                                   "topics", "start_offsets", "start_state", "reset_policy",
                                   "completion_criterion", "first_event_id", "total_event_budget",
                                   "topic_partitions"), prefix + ".workload", missing)
        for key, pattern in (("producer_hook_sha256", SHA256), ("producer_image", IMAGE_DIGEST),
                             ("broker_image", IMAGE_DIGEST)):
            if not re.fullmatch(pattern, str(workload.get(key, ""))):
                missing.append(f"{prefix}.workload.{key}")
        for key in ("first_event_id", "total_event_budget", "topic_partitions"):
            if key in workload and (type(workload[key]) is not int or workload[key] < 1):
                issues.append(f"{prefix}: {key} must be a positive integer")
        if type(workload.get("seed")) is not int and not re.fullmatch(SHA256, str(workload.get("dataset_sha256", ""))):
            missing.append(f"{prefix}.workload.seed or dataset_sha256")
        topics = workload.get("topics")
        offsets = workload.get("start_offsets")
        if (not isinstance(topics, dict) or set(topics) != set(TOPIC_TYPES)
                or any(not isinstance(topic, str) or not topic for topic in topics.values())
                or len(set(topics.values())) != len(TOPIC_TYPES)
                or not isinstance(offsets, dict) or set(offsets) != set(topics.values())
                or any(value is None for value in offsets.values())):
            missing.append(f"{prefix}.workload.topic and start-offset mapping")
        try:
            targets = workload_targets(workload)
        except ValueError as invalid:
            issues.append(f"{prefix}: {invalid}")
            targets = None

        manifest = arm.get("manifest") or {}
        if manifest:
            spec = manifest.get("spec", {})
            config = spec.get("flinkConfiguration", {})
            requested = (arm.get("artifacts") or {}).get("requested") or {}
            if requested.get("runtime_image") and spec.get("image") != requested["runtime_image"]:
                issues.append(f"{prefix}: manifest runtime image does not match requested identity")
            for component in ("jobManager", "taskManager"):
                actual_worker = (spec.get(component, {}).get("podTemplate", {}).get("spec", {})
                                 .get("nodeSelector", {}).get("kubernetes.io/hostname"))
                if actual_worker not in (placement.get("worker_ids") or []):
                    issues.append(f"{prefix}: manifest {component} placement is not in its placement record")
                elif component == "taskManager" and actual_worker != placement.get("flink_worker"):
                    issues.append(f"{prefix}: manifest TaskManager differs from the declared Flink worker")
            args = job_arguments(manifest)
            if (args.get("--query") != workload.get("query") or args.get("--source") != workload.get("source_mode")
                    or spec.get("job", {}).get("entryClass") != workload.get("source_implementation")):
                issues.append(f"{prefix}: manifest query/source differs from its workload record")
            if workload.get("source_mode") == "kafka" and not args.get("--bootstrap-servers"):
                missing.append(f"{prefix}.manifest.spec.job.args Kafka broker endpoint")
            mode = arm.get("mode")
            if mode not in ("fixed", "online") or config.get(ONLINE_COSTS) != str(mode == "online").lower():
                issues.append(f"{prefix}: requested mode differs from the effective cache-cost selector")
            if kind == "same-build" and (arm.get("actuation") != NATIVE_ACTUATOR
                    or config.get("jobmanager.scheduler") != "adaptive"
                    or config.get("job.autoscaler.justin.enabled") != "true"
                    or config.get("kubernetes.operator.job.upgrade.inplace-scaling.enabled") != "true"):
                issues.append(f"{prefix}: same-build plans require the built-in native operator")
            if claim == "learned-action" and config.get("job.autoscaler.scaling.enabled") != "true":
                issues.append(f"{prefix}: learned-action claim requires configured execution")
            if claim == "learned-action" and config.get("job.autoscaler.a4s.online-cache-costs.shadow") == "true":
                issues.append(f"{prefix}: shadow mode cannot make a learned-action claim")
        armed[label] = {"load": targets,
                        "horizon": _horizon(arm, claim, prefix, missing, issues) if manifest else None}

    isolated = False
    if set(armed) == set(PARTICIPANTS):
        left, right = (arms[key] for key in PARTICIPANTS)
        p, q = (arm.get("placement") or {} for arm in (left, right))
        if left.get("trial_id") == right.get("trial_id"):
            issues.append("A and B require distinct trial IDs")
        if left.get("mode") != "fixed" or right.get("mode") != "online":
            issues.append("A must be fixed and B must be online")
        if execution == "concurrent":
            isolated = (all(p.get(key) and q.get(key) and
                            set(p[key]).isdisjoint(q[key]) for key in ("worker_ids", "storage_device_ids"))
                        and _evidence_reference(p.get("isolation_evidence"))
                        and _evidence_reference(q.get("isolation_evidence")))
            if not isolated:
                issues.append("concurrent execution requires documented disjoint workers and physical storage")
        if kind == "same-build":
            for key in ("source", "operator_configuration", "runtime_policy", "actuation", "workload",
                        "horizon"):
                differences.extend(_differences(left.get(key), right.get(key), key))
            for stage in ("requested", "deployed", "observed_loaded"):
                a, b = ((arm.get("artifacts") or {}).get(stage) or {} for arm in (left, right))
                fields = ((a.keys() & b.keys()) if stage != "requested" else (a.keys() | b.keys()))
                fields = fields - {"evidence"}
                differences.extend(_differences({k: a.get(k) for k in fields},
                                                {k: b.get(k) for k in fields}, f"artifacts.{stage}"))
            ignored = {"isolation_evidence"}
            if isolated:
                ignored.update(("flink_worker", "broker_worker", "worker_ids", "storage_device_ids"))
            differences.extend(_differences({k: v for k, v in p.items() if k not in ignored},
                                            {k: v for k, v in q.items() if k not in ignored}, "placement"))
            diffs = (_canonical_manifest(arm.get("manifest") or {}, ignore_worker=isolated)
                     for arm in (left, right))
            differences.extend(_differences(*diffs, "manifest"))

    identity_mismatch = mismatches or any(path.startswith("artifacts.") for path in differences)
    plan["validation"] = {
        "plan_status": "INVALID" if issues or differences or mismatches else "INCOMPLETE" if missing else "VALID",
        "identity_status": "MISMATCH" if identity_mismatch else "UNVERIFIED" if unverified or missing else "VERIFIED",
        "issues": issues,
        "missing": sorted(set(missing)),
        "unverified": sorted(set(unverified)),
        "identity_mismatches": sorted(set(mismatches)),
        "undeclared_differences": sorted(set(differences)),
        "isolation": "documented-disjoint" if isolated else "sequential" if execution == "sequential" else "unverified",
        "arms": armed,
    }
    return plan
