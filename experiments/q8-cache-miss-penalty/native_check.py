"""Measure native operator scaling reports, acknowledgements, and observed physical placement."""
import argparse
from collections import Counter
from datetime import datetime
import json
from math import isfinite
from pathlib import Path
import re

from analyze import allocation, allocation_signature, input_spec, JOIN, read, rows, same_allocation_history
from benchmark_plan import digest_json
from native_observe import P_KEY, RP_KEY
from quick_series import JOIN_POPULATION, SOURCE_POPULATION, counter_intervals

MIB = 1024 ** 2
ID = r"[0-9a-f]{32}"
REQUIRED = ("job.autoscaler.enabled", "job.autoscaler.scaling.enabled", "job.autoscaler.justin.enabled",
            "job.autoscaler.a4s.enabled", "kubernetes.operator.job.upgrade.inplace-scaling.enabled")


def event_times(samples, start, end):
    # Core/v1 aggregates repeating events. Never invent timestamps for repeats it coalesced.
    found = {}
    for row in samples:
        for event in row.get("events", []):
            series = event.get("series") or {}
            timestamps = (event.get("eventTime"), event.get("firstTimestamp"),
                          event.get("lastTimestamp"), series.get("lastObservedTime"))
            for stamp in filter(None, timestamps):
                epoch = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
                if start <= epoch <= end:
                    found[event["uid"], epoch] = (epoch, event["reason"], event.get("message") or "")
    return sorted(found.values())


def targets(overrides, join_id=JOIN):
    counts = {vertex: int(value) for vertex, value in
              re.findall(f"({ID}):([0-9]+)", overrides.get(P_KEY) or "")}
    memory = {}
    for vertex, resource in re.findall(f"({ID}):ResourceProfile\\{{([^}}]+)\\}}", overrides.get(RP_KEY) or ""):
        amount = re.search(r"managedMemory=[^,]*?\((\d+) bytes\)", resource)
        if amount:
            memory[vertex] = int(amount[1])
    if join_id in counts and counts.keys() == memory.keys():
        return {"parallelism": counts, "join_bytes_per_slot": memory[join_id], "managed_bytes_by_vertex": memory}
    return None


def slot_profile(row):
    """Reconcile the job's occupied physical slots with the cluster overview."""
    overview = row.get("overview") or {}
    workers = row.get("physical_slots") or []
    join_p = row.get("parallelism", {}).get(row.get("join_id", JOIN))
    join_bytes = row.get("join_managed_slot_bytes")
    if not join_p or join_bytes is None or not workers:
        return None
    total = sum(w["total_slots"] for w in workers)
    occupied = sum(len(w["job_slot_managed_mib"]) for w in workers)
    if (len({w["id"] for w in workers}) != len(workers)
            or overview.get("taskmanagers") != len(workers) or overview.get("slots-total") != total
            or overview.get("slots-available") != total - occupied
            or any(w["total_slots"] - w["free_slots"] != len(w["job_slot_managed_mib"]) for w in workers)):
        return None
    return {"join_parallelism": join_p, "join_managed_slot_bytes": join_bytes,
            "occupied_slots": occupied, "total_slots": total, "workers": len(workers)}


def recorded_profile(row):
    if row is None:
        return None
    # Effective RocksDB capacity is a separate observation from task managed memory.
    cache = allocation(row) or {}
    return {**slot_profile(row), "native_bytes_per_join": cache.get("native_bytes_per_join")}


def verified(row, target):
    profile = slot_profile(row)
    wanted = target["parallelism"]
    if (not profile or row.get("state") != "RUNNING" or row.get("running_tasks") != row.get("parallelism")
            or any(row["parallelism"].get(key) != p for key, p in wanted.items())
            or any(row.get("managed_bytes_by_vertex", {}).get(key) != size
                   for key, size in target.get("managed_bytes_by_vertex", {}).items())
            or profile["join_managed_slot_bytes"] != target["join_bytes_per_slot"]):
        return False
    locations = row.get("join_locations", {})
    join_id = row.get("join_id", JOIN)
    if join_id not in wanted or set(locations) != {str(i) for i in range(wanted[join_id])}:
        return False
    slots = {tm["id"]: tm["job_slot_managed_mib"] for tm in row["physical_slots"]}
    # The job endpoint identifies a subtask's TaskManager, not its slot within the manager.
    return all(len(slots.get(tm, [])) >= count and all(size * MIB == target["join_bytes_per_slot"] for size in slots[tm])
               for tm, count in Counter(locations.values()).items())


def kafka_evidence(folder, release, end):
    """Measure broker arrivals within single topic generations; resets restart offsets at zero."""
    events = rows(folder / "kafka-generations.jsonl")
    offsets = rows(folder / "kafka-offsets.jsonl")
    starts = [e for e in events if e["event"] == "start" and e["epoch"] <= end]
    issues = []
    if not starts or any(e.get("offered_records_per_second") != 80000 or e.get("first_event_id") != 1 for e in starts):
        issues.append("Missing 80k mixed-event producer start receipt from event ID one")
    generations = []
    previous_start = None
    for start in starts:
        generation = start["generation"]
        if previous_start is not None and not any(e["event"] == "reset" and previous_start <= e["epoch"] <= start["epoch"] for e in events):
            issues.append("Producer generation restarted without a topic reset receipt")
        previous_start = start["epoch"]
        stop = min([end, *(e.get("started_epoch", e["epoch"]) for e in events
                            if e["generation"] == generation and e["event"] in ("pause", "stop")
                            and e["epoch"] >= start["epoch"])])
        points = []
        for row in offsets:
            if row.get("generation") != generation or not max(start["epoch"], release) <= row["epoch"] <= stop:
                continue
            topics = row.get("topics") or {}
            if len(topics) != 3 or not all(topic and partitions for topic, partitions in topics.items()):
                continue
            counters = {(topic, partition): offset for topic, partitions in topics.items()
                        for partition, offset in partitions.items()}
            if any(not isinstance(offset, int) or offset < 0 for offset in counters.values()):
                continue
            points.append((row["epoch"], counters))
        points.sort(key=lambda item: item[0])
        reset = any(t2 <= t1 or b.keys() != a.keys() or any(b[k] < a[k] for k in a)
                    for (t1, a), (t2, b) in zip(points, points[1:]))
        scope = {"generation": generation, "producer_start_from_release_s": start["epoch"] - release,
                 "samples": len(points), "measured_s": 0, "records": 0}
        if reset:
            issues.append(f"Broker offsets reset or partition membership changed within generation {generation}")
        elif len(points) >= 2:
            t0, first = points[0]
            t1, last = points[-1]
            counts = {topic: sum(last[k] - first[k] for k in first if k[0] == topic) for topic, _ in first}
            scope.update(measured_s=t1 - t0, records=sum(counts.values()), per_topic_records=counts,
                         records_per_second=sum(counts.values()) / (t1 - t0))
        generations.append(scope)
    seconds = sum(g["measured_s"] for g in generations)
    actual = sum(g["records"] for g in generations) / seconds if seconds > 0 else None
    if actual is None:
        issues.append("Broker samples did not establish a mixed-event delivery rate within a producer generation")
    return generations, actual, issues


REQUEST_ACTIONS = {"POLICY_REQUEST", "THROUGHPUT_SCALE_UP_RETAINED_PROFILES"}


def log_epoch(line):
    try:
        stamp = datetime.fromisoformat(line.split(" ", 1)[0].replace("Z", "+00:00"))
        return stamp.timestamp() if stamp.tzinfo else None
    except ValueError:
        return None


def log_field(text, field):
    match = re.search(r"(?<![.\w])" + re.escape(field) + r"=([^\s,})]+)", text)
    return match[1] if match and match[1] not in ("null", "unavailable") else None


def log_number(text, field, parse=float):
    try:
        value = parse(log_field(text, field))
        return value if isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def normalized_request(value):
    if not isinstance(value, dict):
        return None
    parallelism, profiles = value.get("parallelism"), value.get("profiles")
    if not isinstance(parallelism, dict) or not isinstance(profiles, dict):
        return None
    if not parallelism or parallelism.keys() != profiles.keys():
        return None
    if any(not isinstance(vertex, str) or not re.fullmatch(ID, vertex)
           or not re.fullmatch(r"[1-9][0-9]*", str(count)) for vertex, count in parallelism.items()):
        return None
    if any(not isinstance(profile, str) or not re.fullmatch(r"ResourceProfile\{[^{}]+\}", profile)
           for profile in profiles.values()):
        return None
    return {"parallelism": {vertex: int(count) for vertex, count in parallelism.items()},
            "profiles": dict(profiles)}


def logged_request(text):
    fields = re.search(r"requestedParallelismOverrides=(.+?) requestedResourceProfileOverrides=(.+?) "
                       r"(?:requestedDecisionId|appliedOverrides)=", text)
    if not fields:
        return None
    parallel_entry = r"(" + ID + r")=([0-9]+)"
    profile_entry = r"(" + ID + r")=(ResourceProfile\{[^{}]+\})"
    for pattern, field in ((parallel_entry, fields[1]), (profile_entry, fields[2])):
        if not re.fullmatch(r"\{" + pattern + r"(?:, " + pattern + r")*\}", field):
            return None
    parallelism = re.findall(parallel_entry, fields[1])
    profiles = re.findall(profile_entry, fields[2])
    if len(dict(parallelism)) != len(parallelism) or len(dict(profiles)) != len(profiles):
        return None
    return normalized_request({"parallelism": dict(parallelism), "profiles": dict(profiles)})


def logged_memory(text, vertex):
    match = re.search(re.escape(vertex or "") + r"=EvaluatedMetrics\.ManagedMemoryDecision\(([^)]*)\)", text)
    if not match:
        return {}
    return {key: log_number(match[1], key, int) for key in (
            "trustedCurrentManagedBytes", "configuredFloorBytes", "unconstrainedManagedBytes",
            "retainedManagedBytes", "floorLimitedManagedBytes", "requestedManagedBytes",
            "finalManagedBytes", "nominalNativeCacheBytes")}


def logged_model(text):
    observation = re.search(r" observation=EvaluatedMetrics\.ObservationEvidence\(status=([A-Z_]+)", text)
    recommendation = re.search(r" recommendation=QuickMrcCostProjection\.Recommendation\(([^)]*)\)", text)
    scope = log_field(recommendation[1], "predictionScope") if recommendation else None
    return {
        "mode": log_field(text, "mode"),
        "reason": log_field(text, "reason"),
        "fit_reason": log_field(text, "fitReason"),
        "observation_status": observation[1] if observation else None,
        "selected_cost_policy": log_field(text, "selectedCostPolicy"),
        "fallback_reason": log_field(text, "fallbackReason"),
        "fit_end_ms": log_number(text, "windowEndMillis", int),
        "prediction_scope": scope,
        "recommended_total_native_bytes": log_number(recommendation[1], "totalNativeCacheBytes", int)
            if recommendation else None,
        "fit_parallelism": log_number(text, "expectedSubtasks", int),
        "residual_busy_seconds_per_input": log_number(text, "residualBusySecondsPerInput"),
        "slope_seconds_per_input_per_unit_miss_fraction": log_number(
            text, "slopeSecondsPerInputPerUnitMissFraction"),
        "reported_evidence": text,
    }


def request_target(request, join_id):
    memory = {}
    for vertex, profile in request["profiles"].items():
        match = re.search(r"\bmanagedMemory=(?:[^,}]+?\(([0-9]+) bytes\)|([0-9]+) bytes)", profile)
        if not match:
            return None
        memory[vertex] = int(match[1] or match[2])
    if join_id not in memory:
        return None
    return {"parallelism": request["parallelism"], "managed_bytes_by_vertex": memory,
            "join_bytes_per_slot": memory[join_id]}


def recommendation_allocation(candidate):
    if not isinstance(candidate, dict):
        return None
    p = candidate.get("parallelism")
    fields = ("managed_bytes_per_slot", "native_cache_bytes")
    memory = {key: candidate[key] for key in fields if type(candidate.get(key)) is int and candidate[key] > 0}
    return (p, memory) if type(p) is int and p > 0 and memory else None


def alternate_comparison(receipts, evaluation_id, chosen, request):
    comparisons = [row for row in receipts if row.get("kind") == "comparison"
                   and row.get("evaluation_id") == evaluation_id
                   and row.get("source") in ("same_serialized_decision", "active_policy_replay")]
    if len(comparisons) != 1:
        return None, None, None
    comparison = comparisons[0]
    fixed, online = comparison.get("fixed"), comparison.get("online")
    if not isinstance(fixed, dict) or not isinstance(online, dict):
        return None, None, None
    input_hash = comparison.get("decision_input_sha256")
    if (not isinstance(input_hash, str) or not re.fullmatch(r"[a-f0-9]{64}", input_hash)
            or fixed.get("input_sha256") != input_hash or online.get("input_sha256") != input_hash):
        return None, None, None
    pre_a, pre_b = (recommendation_allocation(candidate.get("recommendation"))
                    for candidate in (fixed, online))
    pre_differs = (pre_a != pre_b if pre_a is not None and pre_b is not None
                   and pre_a[1].keys() == pre_b[1].keys() else None)
    final_a, final_b = (normalized_request(candidate.get("final_request")) for candidate in (fixed, online))
    selected = final_b if chosen == "online" else final_a if chosen in ("fixed", "fixed_fallback") else None
    differs = (final_a != final_b if final_a is not None and final_b is not None
               and (request is None or request == selected) else None)
    return pre_differs, differs, comparison


def observed_request(receipts, request, join_id, accepted, nominal_native_bytes):
    """Validate placement receipts linked to a specific accepted request."""
    target = request_target(request, join_id) if request is not None else None
    if target is None or accepted is None:
        return None, None, None
    p = target["parallelism"][join_id]
    subtasks = {str(index) for index in range(p)}
    managed_verified = None
    managed_epoch = None
    for receipt in receipts:
        if receipt.get("kind") != "placement" or receipt["epoch"] < accepted["epoch"]:
            continue
        observation = receipt.get("observation")
        if not isinstance(observation, dict):
            continue
        started, finished = observation.get("epoch"), observation.get("capture_end_epoch")
        if (observation.get("join_id") != join_id
                or type(started) not in (int, float) or type(finished) not in (int, float)
                or not accepted["epoch"] <= started <= finished <= receipt["epoch"]):
            continue
        try:
            matches_target = verified(observation, target)
        except (TypeError, KeyError, AttributeError):
            continue
        if not matches_target:
            if managed_verified is None:
                managed_verified = False
            continue
        managed_verified = True
        if managed_epoch is None:
            managed_epoch = finished
        native = observation.get("native") or {}
        identities = receipt.get("cache_identities") or {}
        if (not isinstance(native, dict) or not isinstance(identities, dict)
                or set(native) != subtasks or set(identities) != subtasks
                or any(type(value) is not int or value <= 0 for value in native.values())
                or any(not isinstance(identity, dict)
                       or not all(isinstance(identity.get(field), str) and identity[field]
                                  for field in ("attempt", "process"))
                       or type(identity.get("cache_id")) is not int or identity["cache_id"] <= 0
                       or type(identity.get("capacity_generation")) is not int
                       or identity["capacity_generation"] < 0 for identity in identities.values())):
            continue
        physical_caches = {(identity["process"], identity["cache_id"]) for identity in identities.values()}
        if len(physical_caches) != p:
            continue
        matches = (all(value == nominal_native_bytes for value in native.values())
                   if nominal_native_bytes is not None else None)
        return managed_verified, matches, {
            "observed_epoch": finished, "managed_placement_epoch": managed_epoch,
            "native_bytes_by_subtask": native,
            "join_allocation": allocation_signature(p, target["join_bytes_per_slot"], native),
        }
    return managed_verified, None, ({"observed_epoch": managed_epoch, "managed_placement_epoch": managed_epoch,
                                     "native_bytes_by_subtask": None, "join_allocation": None}
                                    if managed_epoch is not None else None)


def decision_evidence(lines, receipts, *, job_id, join_id, online_configured, start, end,
                      legacy_ack_count=0):
    """Join controller logs and optional v1 receipts by stable IDs.

    decision-evidence.jsonl records carry schema_version, kind, job_id,
    decision_id and epoch. Native acknowledgement records include accepted
    and the exact request. Placement records include the physical observation
    and cache identities. Comparisons tie both candidates to the same
    evaluation and input hash. An outcome links its evidence to the action.
    """
    decisions, models, handoffs, holds = {}, {}, {}, {}
    for line in lines:
        epoch = log_epoch(line)
        if epoch is None or not start <= epoch <= end or not job_id:
            continue
        if "A4S cache decision " in line:
            text = line.split("A4S cache decision ", 1)[1]
            eid = log_field(text, "evaluationId")
            if eid and log_field(text, "job") == job_id:
                decisions.setdefault(eid, set()).add((epoch, text))
        elif "A4S cache cost vertex=" in line:
            text = line.split("A4S cache cost ", 1)[1]
            eid = log_field(text, "evaluationId")
            if eid and join_id and log_field(text, "vertex") == join_id:
                models.setdefault(eid, set()).add(text)
        elif "A4S hold job=" in line:
            text = line.split("A4S hold ", 1)[1]
            eid, vertex = log_field(text, "evaluationId"), log_field(text, "vertex")
            if eid and log_field(text, "job") == job_id and vertex in (None, join_id):
                holds.setdefault((eid, vertex), set()).add(log_field(text, "reason"))
        elif "A4S realizer returned " in line:
            text = line.split("A4S realizer returned ", 1)[1]
            request_id = log_field(text, "decisionId")
            if request_id and log_field(text, "job") == job_id:
                handoffs.setdefault(request_id, []).append(epoch)

    by_request = {}
    for receipt in receipts:
        if not isinstance(receipt, dict):
            continue
        epoch = receipt.get("epoch")
        if (receipt.get("schema_version") == 1 and job_id and receipt.get("job_id") == job_id
                and isinstance(receipt.get("decision_id"), str) and type(epoch) in (int, float)
                and isfinite(epoch) and start <= epoch
                and (epoch <= end or receipt.get("kind") == "comparison")):
            by_request.setdefault(receipt["decision_id"], []).append(receipt)

    evaluations, requests = [], {}
    for eid, entries in sorted(decisions.items(), key=lambda item: min(item[1])):
        epoch, text = min(entries)
        unambiguous = len({line for _, line in entries}) == 1
        action = log_field(text, "action")
        is_request = action in REQUEST_ACTIONS
        request_id = log_field(text, "requestedDecisionId") if is_request and unambiguous else None
        request = logged_request(text) if request_id else None
        candidates = models.get(eid, set())
        model = logged_model(next(iter(candidates))) if unambiguous and len(candidates) == 1 else {}
        memory = logged_memory(text, join_id) if unambiguous and join_id else {}
        fit_reason = model.get("fit_reason")
        fit_end = model.get("fit_end_ms")
        qualified = (fit_reason == "QUALIFIED" and fit_end <= epoch * 1000
                     and model.get("observation_status") == "VALID"
                     if fit_reason is not None and fit_end is not None else
                     False if fit_reason not in (None, "QUALIFIED") else None)
        nominal = memory.get("nominalNativeCacheBytes")
        p = request["parallelism"].get(join_id) if request else None
        supported = (p == model.get("fit_parallelism")
                     and model.get("prediction_scope") == "OBSERVED_ALLOCATION" and nominal > 0
                     and model.get("recommended_total_native_bytes") == nominal * p
                     if nominal is not None and p is not None and model else None)
        selected = model.get("selected_cost_policy")
        if action == "THROUGHPUT_SCALE_UP_RETAINED_PROFILES":
            used = "throughput_recovery"
        elif is_request and memory.get("retainedManagedBytes") is not None:
            used = "retained_profile"
        elif (is_request and selected == "ONLINE_QUALIFIED" and model.get("mode") == "ONLINE_QUALIFIED"
              and qualified is True and supported is True):
            used = "online"
        elif is_request and request and selected in ("LEGACY_FIXED", "OFFLINE_CALIBRATED"):
            used = "fixed_fallback" if model.get("fallback_reason") else "fixed"
        else:
            used = None

        linked = sorted((row for row in by_request.get(request_id, []) if row["epoch"] >= epoch),
                        key=lambda row: row["epoch"])
        matching_ack = [row for row in linked if row.get("kind") == "acknowledgement"
                        and request is not None and normalized_request(row.get("request")) == request]
        accepted = next((row for row in matching_ack if row.get("accepted") is True), None)
        sent = next((row["epoch"] for row in linked if row.get("kind") == "request"
                     and request is not None and normalized_request(row.get("request")) == request
                     and (accepted is None or row["epoch"] <= accepted["epoch"])), None)
        returned = any(t >= epoch for t in handoffs.get(request_id, []))
        if accepted is not None:
            request_status = "accepted"
        elif any(row.get("accepted") is False for row in matching_ack):
            request_status = "rejected"
        elif returned:
            request_status = "realizer_returned"
        else:
            request_status = "requested" if is_request else "none"
        recommendation_differs, final_differs, comparison = alternate_comparison(
            linked, eid, used, request)
        managed, native, placement = observed_request(linked, request, join_id, accepted, nominal)
        applied = (False if managed is False or native is False else
                   True if managed is True and native is True else None)
        outcomes = {row["valid"] for row in linked
                    if placement and row.get("kind") == "outcome" and type(row.get("valid")) is bool
                    and isinstance(row.get("evidence"), str) and row["evidence"]
                    and row["epoch"] >= placement["observed_epoch"]}
        observed = next(iter(outcomes)) if len(outcomes) == 1 else None
        controlled = (online_configured is True and qualified is True and used == "online"
                      and supported is True and accepted is not None and managed is True and native is True)
        held_by_budget = (action in ("POLICY_NO_CHANGE", "POLICY_HOLD")
                          and memory.get("trustedCurrentManagedBytes") is not None
                          and memory["trustedCurrentManagedBytes"] == memory.get("finalManagedBytes")
                          and memory.get("floorLimitedManagedBytes") is not None
                          and (memory.get("configuredFloorBytes") == memory.get("floorLimitedManagedBytes")
                               or memory.get("retainedManagedBytes") == memory.get("floorLimitedManagedBytes"))
                          and memory.get("unconstrainedManagedBytes") not in (None, memory.get("finalManagedBytes")))
        typed_hold = holds.get((eid, join_id)) or holds.get((eid, None)) or set()
        hold = log_field(text, "holdReason") or (next(iter(typed_hold)) if len(typed_hold) == 1 else None)
        reason = model.get("reason") or hold or fit_reason
        if held_by_budget:
            classification = "floor_or_retention_bound_no_op"
        elif used in ("throughput_recovery", "retained_profile"):
            classification = used
        elif controlled and final_differs is False:
            classification = "same_action_control"
        elif controlled:
            classification = "verified_learned_control"
        elif request_status == "accepted":
            classification = "acknowledged_unverified" if managed is not True or native is not True else "placed"
        elif request_status == "rejected":
            classification = "request_rejected"
        elif model.get("mode") == "ONLINE_SHADOW":
            classification = "shadow_fixed_fallback" if used == "fixed_fallback" else "shadow"
        elif action == "ONLINE_CACHE_ONLY_ACTUATION_UNAVAILABLE":
            classification = "actuator_unavailable"
        elif model.get("prediction_scope") == "ADVISORY_CAPACITY_CHANGE":
            classification = "advisory_candidate"
        else:
            classifications = {
                "MISSING_OBSERVATIONS": "missing_curve_or_observations",
                "ABSENT": "missing_curve_or_observations",
                "MISSING_TIMING": "missing_timing_samples",
                "TOO_FEW_SAMPLES": "insufficient_timing_samples",
                "INSUFFICIENT_HISTORY": "warm_up",
                "CAPACITY_UNSUPPORTED": "unsupported_capacity",
                "INSUFFICIENT_CURRENT_PARALLELISM": "valid_current_parallelism_insufficiency",
            }
            policy_status = ("insufficient_evidence" if is_request else
                             "policy_no_change" if action == "POLICY_NO_CHANGE" else "policy_hold")
            classification = classifications.get(reason, policy_status)
        evaluations.append({
            "evaluation_id": eid, "classification": classification, "decision_id": request_id, "decision_epoch": epoch,
            "action": action, "hold_reason": hold,
            "online_configured": online_configured,
            "model_qualified": qualified, "model_used": used, "model": model,
            "candidate_supported_for_final_request": supported,
            "recommendation_differs": recommendation_differs, "final_action_differs": final_differs,
            "memory_resolution": memory, "same_snapshot_comparison": comparison,
            "floor_or_retention_bound_no_op": held_by_budget,
            "final_request": request, "request_status": request_status,
            "request_sent_epoch": sent, "native_acknowledgement_epoch": accepted["epoch"] if accepted else None,
            "applied_resources_verified": applied, "managed_placement_verified": managed,
            "native_capacity_verified": native, "physical_placement": placement,
            "valid_outcome_observed": observed, "proven_learned_control": controlled,
            "proven_distinct_learned_action": controlled and final_differs is True,
        })
        if is_request:
            key = request_id or eid
            requests.setdefault(key, []).append(evaluations[-1])

    # A stable request may be reissued after a failed attempt. Count the original decision once.
    original = []
    for attempts in requests.values():
        first = attempts[0]
        if any(row["final_request"] != first["final_request"] for row in attempts[1:]):
            for row in attempts:
                row.update(request_status="conflicting_request_identity", proven_learned_control=False,
                           proven_distinct_learned_action=False,
                           classification="insufficient_evidence", model_used=None,
                           applied_resources_verified=None, managed_placement_verified=None,
                           native_capacity_verified=None,
                           valid_outcome_observed=None, request_sent_epoch=None,
                           native_acknowledgement_epoch=None, physical_placement=None)
        original.append(first)
    proven = sum(row["proven_learned_control"] for row in original)
    unclassified = sum(row["model_used"] is None or
                       (row["model_used"] == "online" and not row["proven_learned_control"])
                       for row in original)
    # Legacy operator events have no request ID. Count only the guaranteed
    # excess beyond the available request records and keep any linkage unknown.
    unattributed = max(0, legacy_ack_count - len(original))
    count = len(original) + unattributed
    composition = ("mixed" if proven and any(row["model_used"] in (
        "fixed", "fixed_fallback", "throughput_recovery", "retained_profile") for row in original)
                   else "learned" if count and proven == count
                   else "non_learned" if count and not proven and not unclassified and not unattributed
                   else "insufficient_evidence" if count else "no_request")
    return {"confidence": "decision_id" if decisions else "historical",
            "evaluations": evaluations, "request_count": count,
            "proven_learned_requests": proven,
            "proven_distinct_learned_actions": sum(row["proven_distinct_learned_action"] for row in original),
            "unclassified_requests": unclassified + unattributed,
            "learned_decision_fraction": proven / count if count and not (unclassified + unattributed) else None,
            "verified_learned_fraction_lower_bound": proven / count if count else None,
            "request_composition": composition, "native_ack_events_without_decision_ids": legacy_ack_count}


def bounded_number(value):
    return type(value) in (int, float) and isfinite(value)


def score_window(intervals, start, end):
    """Report observed inner progress separately from a complete declared window."""
    if not bounded_number(start) or not bounded_number(end) or end <= start:
        return {"planned_start_epoch": start, "planned_end_epoch": end,
                "unscored_reason": "window_bounds_unavailable", "records": None,
                "rate_records_per_second": None}
    included = [span for span in intervals if start <= span["start_epoch"]
                and span["end_epoch"] <= end and span["records"] is not None]
    observed_seconds = sum(span["end_epoch"] - span["start_epoch"] for span in included)
    observed_records = sum(span["records"] for span in included) if included else None
    contiguous = included and all(left["end_epoch"] == right["start_epoch"]
                                  for left, right in zip(included, included[1:]))
    covered = (contiguous and included[0]["start_epoch"] == start and included[-1]["end_epoch"] == end)
    return {
        "planned_start_epoch": start, "planned_end_epoch": end,
        "included_intervals": [[span["start_epoch"], span["end_epoch"]] for span in included],
        "observed_coverage_seconds": observed_seconds, "observed_progress_records": observed_records,
        "observed_inner_interval_average_records_per_second":
            observed_records / observed_seconds if contiguous else None,
        "records": observed_records if covered else None,
        "rate_records_per_second": observed_records / (end - start) if covered else None,
        "unscored_reason": None if covered else "incomplete_observed_window",
        "inclusion_rule": "complete observed counter intervals inside bounds; no interpolation",
    }


def matched_progress_window(intervals, anchor, lower, upper):
    if (not bounded_number(anchor) or type(lower) is not int or type(upper) is not int
            or lower < 0 or upper <= lower):
        return {"records": None, "rate_records_per_second": None,
                "unscored_reason": "matched_progress_bounds_unavailable"}
    stable = []
    for span in intervals:
        if span["start_epoch"] < anchor:
            continue
        if span["records"] is None or (stable and (
                span["start_epoch"] != stable[-1]["end_epoch"]
                or span["execution_identity"] != stable[0]["execution_identity"])):
            break
        stable.append(span)
    if not stable:
        return {"records": None, "rate_records_per_second": None,
                "unscored_reason": "stationary_counter_chain_unavailable"}
    points = [(0, stable[0]["start_epoch"])]
    for span in stable:
        points.append((points[-1][0] + span["records"], span["end_epoch"]))

    def measured_boundary(records):
        return next((when for count, when in points if count == records), None)

    def bracket(records):
        return next(([a[1], b[1]] for a, b in zip(points, points[1:]) if a[0] < records < b[0]), None)

    start, end = measured_boundary(lower), measured_boundary(upper)
    result = score_window(stable, start, end)
    if start is None or end is None:
        result["unscored_reason"] = "record_progress_boundary_not_observed"
    result.update(progress_anchor_epoch=points[0][1], progress_start_records=lower, progress_end_records=upper,
                  start_boundary_bracket_epoch=bracket(lower), end_boundary_bracket_epoch=bracket(upper))
    return result


def scoring_windows(intervals, declarations, release, end, healthy_epoch, transitions):
    def after_anchor_delay():
        delay = declarations.get("post_placement_delay_seconds")
        length = declarations.get("post_placement_seconds")
        if (bounded_number(healthy_epoch) and bounded_number(delay) and delay >= 0
                and bounded_number(length) and length > 0):
            return healthy_epoch + delay, healthy_epoch + delay + length
        return None, None

    # Exclusion applies to service windows. The whole policy window includes the
    # time lost to transitions even when some progress is unscored.
    stationary = [span for span in intervals if not any(
        left < span["end_epoch"] and span["start_epoch"] < right for left, right in transitions)]
    tail = declarations.get("tail_seconds")
    tail_start = end - tail if bounded_number(tail) and 0 < tail <= end - release else None
    steady_start, steady_end = after_anchor_delay()
    return {
        "population": declarations.get("population", SOURCE_POPULATION),
        "whole_horizon": score_window(intervals, release, end),
        "tail": score_window(intervals, tail_start, end),
        "post_placement": score_window(stationary, steady_start, steady_end),
        "matched_progress": matched_progress_window(
            stationary, healthy_epoch, declarations.get("matched_progress_start_records"),
            declarations.get("matched_progress_end_records")),
        "transition_intervals_excluded_from_service": [list(span) for span in transitions],
    }


def compare_windows(fixed, online):
    ranks = {}
    for label in ("whole_horizon", "tail", "post_placement", "matched_progress"):
        a, b = (arm.get(label, {}) for arm in (fixed, online))
        ra, rb = (score.get("rate_records_per_second") for score in (a, b))
        if label == "matched_progress":
            comparable = (a.get("progress_start_records"), a.get("progress_end_records")) == (
                b.get("progress_start_records"), b.get("progress_end_records"))
        else:
            comparable = all(bounded_number(score.get(key)) for score in (a, b)
                             for key in ("planned_start_epoch", "planned_end_epoch")) and (
                a["planned_end_epoch"] - a["planned_start_epoch"] ==
                b["planned_end_epoch"] - b["planned_start_epoch"])
        comparable = comparable and fixed.get("population") == online.get("population")
        ranks[label] = {"fixed_records_per_second": ra, "online_records_per_second": rb,
                        "ranking": ("equal" if ra == rb else "fixed" if ra > rb else "online")
                        if comparable and bounded_number(ra) and bounded_number(rb) else None}
    return ranks


def declared_windows(folder, plan):
    expected = read(folder.parent / "pair.json").get("comparison_plan_sha256")
    comparison = read(folder / "comparison-plan.json")
    # Both declarations are captured by the plan hash before the arms run.
    if expected or comparison:
        if not expected or not comparison or digest_json(comparison) != expected:
            return {}, "comparison_plan_identity_unverified"
        arm = {"hardcoded": "A", "calibrated": "B"}.get(plan.get("arm"))
        windows = comparison.get("arms", {}).get(arm, {}).get("horizon", {}).get("scoring_windows")
        return (windows or {}), "paired_plan" if windows else "windows_not_declared"
    windows = plan.get("scoring_windows")
    return (windows or {}), "legacy_plan" if windows else "windows_not_declared"


def transition_timeline(policy, spans, resources, progress, expected, intervals, release, end):
    def absolute(value):
        return release + value if bounded_number(value) else None

    def milestones(item):
        ack, placed = item.get("acknowledged_epoch"), item.get("managed_placement_epoch")
        running = item.get("all_running_epoch")
        if bounded_number(ack) and bounded_number(running):
            times = [identity[1] / 1000 for sample in resources
                     if ack <= sample.get("capture_epoch", -1) <= running
                     for roster in (sample.get("vertex_attempts") or {}).values() if roster
                     for identity in roster.values() if ack <= identity[1] / 1000 <= running]
            item["replacement_deploy_first_observed_task_epoch"] = min(times, default=None)
            item["replacement_deploy_last_observed_task_epoch"] = max(times, default=None)
        else:
            item["replacement_deploy_first_observed_task_epoch"] = None
            item["replacement_deploy_last_observed_task_epoch"] = None
        stable = next((span for span in intervals if bounded_number(running)
                       and span["start_epoch"] >= running and span["end_epoch"] < item["observation_end_epoch"]
                       and span["records"] is not None), None)
        recovered = stable["end_epoch"] if stable else None
        item["counter_rate_recovery_observed_epoch"] = recovered
        positive_backlog = False
        caught_up = None
        for row in progress:
            stamp = row.get("capture_end_epoch")
            if (not expected or not bounded_number(ack) or not bounded_number(stamp)
                    or not ack <= stamp < item["observation_end_epoch"] or row.get("state") != "RUNNING"):
                continue
            values = [(row.get(key) or {}).get("q8Backlog") for key in expected]
            if any(type(value) is not int or value < 0 for value in values):
                continue
            if positive_backlog and recovered is not None and stamp >= recovered and sum(values) == 0:
                caught_up = stamp
                break
            positive_backlog |= sum(values) > 0
        item["backlog_caught_up_observed_epoch"] = caught_up
        item["ack_to_managed_placement_seconds"] = placed - ack if bounded_number(placed) and bounded_number(ack) else None
        item["ack_to_all_running_seconds"] = running - ack if bounded_number(running) and bounded_number(ack) else None
        sent = item.get("request_sent_epoch")
        item["request_to_ack_seconds"] = ack - sent if bounded_number(sent) and bounded_number(ack) else None
        return item

    linked = {}
    for decision in policy.get("evaluations", []):
        request_id = decision.get("decision_id")
        if not request_id or request_id in linked or decision.get("final_request") is None:
            continue
        placement = decision.get("physical_placement") or {}
        snapshot = placement.get("observed_epoch")
        managed = placement.get("managed_placement_epoch", snapshot) if decision.get("managed_placement_verified") else None
        linked[request_id] = {
            "decision_id": request_id, "confidence": "decision_id",
            "request_status": decision["request_status"], "decision_epoch": decision["decision_epoch"], "request_sent_epoch": decision.get("request_sent_epoch"),
            "acknowledged_epoch": decision.get("native_acknowledgement_epoch"),
            "all_running_epoch": managed, "managed_placement_epoch": managed,
            "native_placement_epoch": snapshot if decision.get("native_capacity_verified") else None,
        }
    ordered = sorted(linked.values(), key=lambda item: item["decision_epoch"])
    for index, item in enumerate(ordered):
        item["observation_end_epoch"] = ordered[index + 1]["decision_epoch"] if index + 1 < len(ordered) else end
    legacy = []
    for index, span in enumerate(spans):
        ack = absolute(span.get("in_place_ack_from_release_s"))
        if not bounded_number(ack):
            continue
        sample = span.get("ack_to_running_requested_parallelism_s")
        next_ack = (absolute(spans[index + 1].get("in_place_ack_from_release_s"))
                    if index + 1 < len(spans) else end)
        legacy.append({
            "decision_id": None, "confidence": "legacy_event_span",
            "decision_epoch": absolute(span.get("decision_from_release_s")), "request_sent_epoch": None,
            "acknowledged_epoch": ack, "all_running_epoch": ack + sample if bounded_number(sample) else None,
            "managed_placement_epoch": absolute(span.get("observed_from_release_s")),
            "native_placement_epoch": None, "observation_end_epoch": next_ack or end,
        })
    return {"linked": [milestones(item) for item in ordered],
            "legacy_event_spans": [milestones(item) for item in legacy]}


def selected_healthy_epoch(declarations, timeline, initial):
    linked = [item for item in timeline["linked"] if item.get("request_status") != "rejected"]
    if not linked and not timeline["legacy_event_spans"] and initial:
        profile = recorded_profile(initial)
        if allocation_signature(profile.get("join_parallelism"), profile.get("join_managed_slot_bytes"),
                                profile.get("native_bytes_per_join")) is not None:
            return initial["capture_end_epoch"]
        return None
    chosen = declarations.get("placement_index", 0 if len(linked) == 1 else None)
    if type(chosen) is int and 0 <= chosen < len(linked):
        return linked[chosen]["native_placement_epoch"]
    return None


def analyze_arm(folder):
    plan = read(folder / "plan.json")
    release = plan["release_ms"] / 1000
    window = read(folder / "window-ended.json")
    finish = read(folder / "finished.json")
    end = window.get("deadline_epoch") or finish.get("epoch")
    if end is None:
        return {"issues": ["No completed measurement window"], "transactions": []}
    created = read(folder / "deployment-created.json")
    conf = created["spec"]["flinkConfiguration"]
    kafka = plan.get("source_mode") == "kafka"
    issues = []
    if any(str(conf.get(key)).lower() != "true" for key in REQUIRED):
        issues.append("The deployed job did not enable the built-in Justin/A4S autoscaler with in-place scaling")
    enabled = str(conf.get("job.autoscaler.a4s.online-cache-costs.enabled")).lower()
    online_configured = enabled == "true" if enabled in ("true", "false") else None
    if online_configured is not None and (plan.get("arm") == "calibrated") != online_configured:
        issues.append("Deployed online cost option differs from arm assignment")
    expected = {}
    declared_input_stages = []
    if kafka:
        args = created["spec"]["job"]
        if not args.get("entryClass", "").endswith("SqlQueryJob") or "q8_unique" not in args.get("args", []):
            issues.append("The deployed job is not the Kafka Q8 SQL job")
    else:
        expected, flags = input_spec(created)
        stages = [tuple(map(int, stage.split(":"))) for stage in flags["--q8-input-plan"].split(",")]
        elapsed = 0
        for duration, person, auction in stages:
            declared_input_stages.append({"scheduled_epoch": release + elapsed,
                                          "person_records_per_source_second": person,
                                          "auction_records_per_source_second": auction})
            elapsed += duration
        if any(int(flags["--p-person-source"]) * p + int(flags["--p-auction-source"]) * a != 80000
               for _, p, a in stages) or sum(s[0] for s in stages) < end - release - .01:
            issues.append("Scheduled input is not 80,000 records/s for the observed window")
        if int(flags["--q8-release-epoch-ms"]) != plan["release_ms"]:
            issues.append("Manifest and plan release clocks differ")

    progress = [row for row in rows(folder / "progress.jsonl") if row["capture_end_epoch"] <= end]
    resources = [row for row in rows(folder / "resource-series.jsonl") if row.get("type") == "sample"
                 and row.get("capture_end_epoch", row["capture_epoch"]) <= end]
    snapshots = rows(folder / "native-operator-series.jsonl")
    events = event_times(snapshots, created_time(created), end)
    decisions = [t for t, reason, message in events if reason == "ScalingReport" and "scaling execution enabled" in message.lower()]
    actions = [t for t, reason, message in events if reason == "Scaling" and "in-place scaling triggered" in message.lower()]
    suspended = [t for t, reason, _ in events if reason == "Suspended"]
    snapshots = [s for s in snapshots if s["epoch"] <= end]
    pods = {(p["uid"], p["restarts"]) for s in snapshots for p in s.get("jobmanagers", [])}
    starts = sorted({r["job_start_ms"] for r in progress if r.get("job_start_ms", -1) > 0})
    same_jm = len(pods) == 1
    # An adaptive execution-graph restart can change Flink timestamps in the same
    # JobManager. A replacement pod/container or operator suspension is stronger evidence.
    reissued = len(pods) > 1 or bool(suspended)
    join_ids = {s["join_id"] for s in progress if s.get("join_id")}
    join_id = (next(iter(join_ids)) if len(join_ids) == 1 else None) if kafka else JOIN
    if not join_id:
        issues.append("A unique Q8 join could not be identified in the running job graph")
    changes = []
    for snap in snapshots:
        resource = snap.get("resource", {})
        # A reconciled spec may contain native overrides even when the CR's persisted spec does not.
        current = targets(resource.get("reconciled_overrides", {}), join_id) or targets(resource.get("submitted_overrides", {}), join_id)
        if current is not None and (not changes or current != changes[-1][1]):
            changes.append((snap["capture_end_epoch"], current))
    first_action = min(actions, default=end)
    initial_rows = [r for r in progress if r["epoch"] < first_action and r.get("state") == "RUNNING"
                    and r.get("running_tasks") == r.get("parallelism") and r.get("physical_slots")]
    initial = initial_rows[0] if initial_rows else {}
    workers = initial.get("physical_slots", [])
    initial_slots = {"total": sum(tm["total_slots"] for tm in workers),
                     "occupied_by_job": sum(len(tm["job_slot_managed_mib"]) for tm in workers)}
    initial_row = next((row for row in initial_rows if row.get("parallelism") == initial.get("parallelism")
                        and (p := slot_profile(row)) and p["occupied_slots"] == initial_slots["occupied_by_job"]), None)
    baseline = slot_profile(initial_row) if initial_row else None
    if initial_slots != {"total": 2, "occupied_by_job": 2}:
        issues.append("Initial physical allocation of two occupied slots was not confirmed")
    filled = [s for s in resources if s["values"]["task_slots_used"] is not None]
    if not filled or filled[0]["values"]["task_slots_used"] != 2:
        issues.append("The aligned Q8 samples did not confirm the two-slot start")
    if not resources or resources[0]["capture_epoch"] - release > 10 or end - resources[-1]["capture_epoch"] > 10:
        issues.append("Resource sampling did not cover the measurement window")
    if not snapshots or not any("events" in snap and "resource" in snap for snap in snapshots):
        issues.append("Missing Kubernetes operator event or resource observations")
    if not same_jm:
        issues.append("Continuous JobManager pod identity was not established")
    if reissued:
        issues.append("JobManager replacement, container restart, or operator suspension observed")
    if len(starts) > 1 and not actions:
        issues.append("Flink job start changed without a native scale acknowledgement")

    spans = []
    for i, ack in enumerate(actions):
        next_ack = actions[i + 1] if i + 1 < len(actions) else end + .000001
        previous_ack = actions[i - 1] if i else created_time(created)
        reports = [t for t in decisions if previous_ack < t <= ack]
        report = max(reports) if reports else None
        # The operator may commit the target just before the RPC or in the following poll.
        candidates = [(t, target) for t, target in changes if (report or previous_ack) <= t < next_ack]
        target = candidates[0][1] if len(candidates) == 1 else None
        running = next((s for s in progress if target and ack <= s["epoch"] and s["capture_end_epoch"] < next_ack
                        and s.get("state") == "RUNNING" and s.get("running_tasks") == s.get("parallelism")
                        and all(s["parallelism"].get(k) == v for k, v in target["parallelism"].items())), None)
        snapshot = next((s for s in progress if ack <= s["epoch"] and s["capture_end_epoch"] < next_ack
                         and target and verified(s, target)), None)
        complete = snapshot["capture_end_epoch"] if snapshot else None
        before_slots = [s["values"]["task_slots_used"] for s in filled if s["capture_epoch"] < ack]
        previous_slots = before_slots[-1] if before_slots else None
        changed_slots = next((s for s in filled if ack <= s["capture_epoch"] < next_ack
                              and s["values"]["task_slots_used"] > 0
                              and previous_slots is not None and s["values"]["task_slots_used"] != previous_slots), None)
        spans.append({"decision_evidence_link": None,
                      "decision_from_release_s": report - release if report is not None else None,
                      "in_place_ack_from_release_s": ack - release, "requested": target,
                      "first_changed_occupied_slots": changed_slots["values"]["task_slots_used"] if changed_slots else None,
                      "ack_to_first_changed_slots_s": changed_slots["capture_epoch"] - ack if changed_slots else None,
                      "ack_to_running_requested_parallelism_s": running["capture_end_epoch"] - ack if running else None,
                      "observed_from_release_s": complete - release if complete else None,
                      "decision_to_observed_s": complete - report if complete and report is not None else None,
                      "ack_to_observed_s": complete - ack if complete else None,
                      "observed_profile": recorded_profile(snapshot)})
    if reissued:
        outcome = "restart_observed"
    elif spans:
        outcome = "in_place_verified" if same_jm and all(s["observed_profile"] for s in spans) else "in_place_acknowledged_only"
    elif changes or (baseline and any((p := slot_profile(r)) and p != baseline for r in progress)):
        outcome = "target_or_resource_change_without_in_place_ack"
        issues.append("Observed target or physical allocation changed without a native acknowledgement")
    else:
        outcome = "no_in_place_resize_observed"
    observations = [(r["epoch"], sum(r[k]["q8Offered"] for k in expected)) for r in progress if not kafka and r["epoch"] >= release
                    and all(r.get(k, {}).get("q8Offered", -1) >= 0 for k in expected)]
    live_rate = None
    if len(observations) >= 2:
        t0, n0 = observations[0]
        t1, n1 = observations[-1]
        if t1 - t0 >= 30:
            live_rate = (n1 - n0) / (t1 - t0)
            if abs(n1 - n0 - 80000 * (t1 - t0)) > 2 * 80000:
                issues.append("Source offer counters differ from the configured rate by over two seconds of input")
    generations = []
    if kafka:
        generations, live_rate, broker_issues = kafka_evidence(folder, release, end)
        issues.extend(broker_issues)
        producer = read(folder / "kafka-result.json")
        if producer.get("status") != "complete":
            issues.append("The Kafka producer coordinator did not complete the measured window")
    if not kafka and live_rate is None:
        issues.append("Insufficient live source offer counters to verify sustained 80k input")
    scoped_log = folder / "operator.log"
    lines = scoped_log.read_text(errors="replace").splitlines() if scoped_log.exists() else []
    online = [line for line in lines if f"A4S online cache costs for vertex {join_id}" in line]
    empty_curve = [line for line in lines if "MissRateCurve has no points" in line]
    errors = sorted({message for _, reason, message in events if reason.lower() == "autoscalererror"})
    if outcome == "no_in_place_resize_observed" and errors:
        outcome = "no_resize_with_autoscaler_errors"
    fallbacks = fallback_failures(lines, release, end, decisions)
    if fallbacks:
        issues.append("The native request failed and the operator entered its regular upgrade path")
        outcome = "in_place_failed_then_upgrade"
    if not lines:
        issues.append("Scoped operator logs are missing")
    policy_evidence = decision_evidence(
        lines, rows(folder / "decision-evidence.jsonl"), job_id=conf.get("$internal.pipeline.job-id"),
        join_id=join_id, online_configured=online_configured,
        start=created_time(created), end=end, legacy_ack_count=len(actions))
    windows, scoring_source = declared_windows(folder, plan)
    population = windows.get("population", SOURCE_POPULATION)
    intervals = counter_intervals(resources, population) if population in (SOURCE_POPULATION, JOIN_POPULATION) else []
    timeline = transition_timeline(policy_evidence, spans, resources, progress, expected, intervals, release, end)
    healthy = selected_healthy_epoch(windows, timeline, initial_row)
    transitions = []
    for item in [*timeline["linked"], *timeline["legacy_event_spans"]]:
        if item.get("request_status") == "rejected":
            continue
        left = item["request_sent_epoch"] or item["acknowledged_epoch"] or item["decision_epoch"]
        right = item["native_placement_epoch"] or item["managed_placement_epoch"] or item["observation_end_epoch"]
        if bounded_number(left) and bounded_number(right) and right > left:
            transitions.append((left, right))
    timeline["scheduled_input_release_epoch"] = release
    timeline["declared_input_stages"] = declared_input_stages
    timeline["recorded_producer_events"] = [row for row in rows(folder / "kafka-generations.jsonl")
                                            if row.get("event") in ("start", "pause", "stop", "reset")
                                            and bounded_number(row.get("epoch")) and row["epoch"] <= end] if kafka else []
    return {"outcome": outcome, "decision_evidence": policy_evidence,
            "timeline": timeline,
            "counter_progress_intervals": intervals,
            "comparison_windows": scoring_windows(intervals, windows, release, end, healthy, transitions),
            "scoring_declaration_status": scoring_source,
            "offered_population": "mixed_nexmark_broker_arrivals" if kafka else "q8_replay_offered_records",
            "valid_measurement_window_observed": live_rate is not None and not issues, "window_s": end - release,
            "source_mode": plan.get("source_mode", "replay"), "join_id": join_id,
            "sampling_period_s": 5, "initial_profile": recorded_profile(initial_row), "initial_slots": initial_slots,
            "last_observed_occupied_slots": filled[-1]["values"]["task_slots_used"] if filled else None,
            "observed_occupied_slots": sorted({s["values"]["task_slots_used"] for s in filled}),
            "failed_in_place_requests": fallbacks,
            "join_cost_at_scaling_reports": join_cost_at_reports(lines, join_id, plan["arm"] == "calibrated"),
            "kafka_producer_generations": generations, "observed_offered_records_per_second": live_rate, "job_start_epochs_ms": starts,
            "jobmanager_pods": sorted(pods), "scaling_reports_from_release_s": [t-release for t in decisions],
            "suspended_from_release_s": [t-release for t in suspended], "transactions": spans,
            "observed_targets": [{"observed_from_release_s": t-release, **target} for t, target in changes],
            "qualified_online_log_lines": len(online), "empty_curve_error_lines": len(empty_curve),
            "autoscaler_errors": errors,
            "issues": issues}


def created_time(created):
    return datetime.fromisoformat(created["metadata"]["creationTimestamp"].replace("Z", "+00:00")).timestamp()


def join_cost_at_reports(lines, _join_id, _online_enabled):
    """Retain legacy report times without using adjacent lines as model-selection evidence."""
    return [{"operator_utc": line.split(" ", 1)[0], "join_cost": "unknown"}
            for line in lines if "SCALINGREPORT" in line and "Scaling execution enabled" in line]


def fallback_failures(lines, release, end, reports):
    failures = []
    active = None
    for line in lines:
        stamp, _, message = line.partition(" ")
        if "Error while rescaling, falling back to regular upgrade" in message:
            try:
                epoch = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
            except ValueError:
                active = None
                continue
            if epoch > end:
                active = None
                continue
            decision = max((t for t in reports if t <= epoch), default=None)
            active = {"native_failure_from_release_s": epoch - release,
                      "report_to_native_failure_s": epoch - decision if decision is not None else None,
                      "cause": None}
            failures.append(active)
        elif active and "Caused by:" in message:
            cause = message.split("Caused by:", 1)[1].strip()
            if not active["cause"] or "RestClientException:" not in cause:
                active["cause"] = cause[:600]
        elif re.match(r"\d{4}-\d\d-\d\d[ T]", message):
            active = None
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pair", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = {name: analyze_arm(args.pair / name) for name in ("hardcoded", "calibrated")}
    operators = [read(args.pair / arm / "operator.json") for arm in result]
    plans = [read(args.pair / arm / "plan.json") for arm in result]
    pair_issues = []
    pair = read(args.pair / "pair.json")
    comparison = pair.get("comparison", "implementation-bundle")
    for field in ("revision", "jar_sha256"):
        values = [op.get(field) for op in operators]
        if not all(values):
            pair_issues.append(f"Operator {field} identity is unavailable")
        elif comparison == "same-build" and values[0] != values[1]:
            pair_issues.append(f"Same-build operator {field} identities differ")
    if not plans[0].get("artifacts") or plans[0]["artifacts"] != plans[1].get("artifacts"):
        pair_issues.append("The two arms do not share identical worker and workload artifacts")
    if all(plan.get("source_mode") == "kafka" for plan in plans):
        brokers = [plan.get("kafka_bootstrap_servers") for plan in plans]
        if not all(brokers) or len(set(brokers)) != 2:
            pair_issues.append("Isolated Kafka broker endpoints were not established")
    histories = [[allocation_signature(profile.get("join_parallelism"), profile.get("join_managed_slot_bytes"),
                                        profile.get("native_bytes_per_join"))
                  if (profile := tx.get("observed_profile")) else None
                  for tx in run.get("transactions", [])] for run in result.values()]
    same_actions = same_allocation_history(histories)
    scores = compare_windows(*(run.get("comparison_windows", {}) for run in result.values()))
    for arm, operator in zip(result, operators):
        result[arm]["paired_window_rankings"] = scores
        result[arm]["comparison"] = comparison
        result[arm]["same_observed_actions"] = same_actions
        result[arm]["same_action_control"] = same_actions is True and not pair_issues and not result[arm]["issues"]
        result[arm]["revision"] = operator.get("revision")
        result[arm]["operator_jar_sha256"] = operator.get("jar_sha256")
        result[arm]["issues"].extend(pair_issues)
    output = args.output or args.pair / "native-results.json"
    try:
        with output.open("x") as report:
            report.write(json.dumps(result, indent=2) + "\n")
    except FileExistsError:
        parser.error(f"Report already exists: {output}. Select a new --output path.")
    print(json.dumps({arm: {"outcome": run.get("outcome"), "issues": run["issues"]} for arm, run in result.items()}))
    return int(any(run["issues"] for run in result.values()))


if __name__ == "__main__":
    raise SystemExit(main())
