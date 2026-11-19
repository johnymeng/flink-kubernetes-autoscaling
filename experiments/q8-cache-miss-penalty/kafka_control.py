"""Restart the external Nexmark input from event one on Q8 consumer rescaling."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from urllib.request import urlopen


def receipt(log, **fields):
    print(json.dumps(fields, allow_nan=False), file=log, flush=True)


def read_job(base, job_id, pool, expected_name=None):
    if expected_name:
        # A FlinkDeployment may recreate the same named application with a different immutable ID.
        # A REST interruption or missing job by itself is insufficient evidence of a replacement.
        with urlopen(f"{base}/jobs/overview", timeout=3) as reply:
            jobs = json.load(reply)["jobs"]
        live = [j for j in jobs if j.get("name") == expected_name
                and j["state"] not in {"FAILED", "FINISHED", "CANCELED", "SUSPENDED"}]
        if len(live) != 1:
            raise ValueError(f"Expected one active Q8 job named {expected_name!r}; observed {len(live)}")
        job_id = live[0]["jid"]

    def fetch(path):
        with urlopen(f"{base}/jobs/{job_id}{path}", timeout=3) as reply:
            return json.load(reply)

    job = fetch("")
    if job["jid"] != job_id:
        raise ValueError(f"Unexpected REST job {job['jid']}")
    if job["state"] in {"FAILED", "FINISHED", "CANCELED", "SUSPENDED"}:
        raise RuntimeError(f"Q8 consumer reached {job['state']}")
    vertices = job.get("vertices", [])
    tasks = {}
    sizes = {v["id"]: v["parallelism"] for v in vertices if v.get("parallelism", 0) > 0}
    active = bool(sizes) and job["state"] == "RUNNING"
    # Subtasks need only be compared once Flink publishes the execution graph.
    if active:
        requests = ("/vertices/" + v["id"] for v in vertices)
        for vertex in pool.map(fetch, requests):
            entries = vertex.get("subtasks", [])
            active &= len(entries) == sizes.get(vertex["id"]) and all(t["status"] == "RUNNING" for t in entries)
            for task in entries:
                if task.get("start-time", -1) > 0:
                    key = f"{vertex['id']}/{task['subtask']}"
                    tasks[key] = (task.get("attempt", 0), task["start-time"])
    return {"job_id": job["jid"], "state": job["state"], "created": job.get("timestamps", {}).get("CREATED", 0),
            "job_start": job.get("start-time", 0), "parallelism": sizes, "tasks": tasks, "active": active}


def change_reason(before, after):
    if before.get("job_id") != after.get("job_id"):
        return "job_id"
    for field in ("created", "job_start"):
        if before[field] > 0 and after[field] > before[field]:
            return field
    if after["state"] in {"RESTARTING", "RECONCILING", "CREATED"} and before["state"] == "RUNNING":
        return "job_state_" + after["state"]
    for vertex, count in after["parallelism"].items():
        if vertex in before["parallelism"] and count != before["parallelism"][vertex]:
            return "vertex_parallelism"
    for task, attempt in after["tasks"].items():
        if task in before["tasks"] and attempt != before["tasks"][task]:
            return "task_attempt"
    return None


def topics_from(value):
    if isinstance(value, dict):
        for key in ("end_offsets", "topics"):
            if key in value:
                value = value[key]
                break
    if not isinstance(value, dict) or not value:
        raise ValueError("offsets hook must return an object with the owned topics")
    out = {}
    for topic, partitions in value.items():
        if isinstance(partitions, int):
            partitions = {"all": partitions}
        if not isinstance(partitions, dict) or not partitions or any(
                type(offset) is not int or offset < 0 for offset in partitions.values()):
            raise ValueError(f"Invalid end offsets for {topic}")
        out[topic] = partitions
    return out


class Producer:
    def __init__(self, args, arm, events, samples):
        self.args, self.arm, self.events, self.samples = args, arm, events, samples
        self.generation = 0
        self.started = False
        self.pending_sample = None

    def call(self, verb):
        env = dict(os.environ, Q8_ARM=self.arm, Q8_TPS=str(self.args.tps),
                   Q8_GENERATION=str(self.generation), Q8_FIRST_EVENT_ID="1")
        result = subprocess.run([str(self.args.hook), verb], env=env, capture_output=True,
                                text=True, timeout=self.args.hook_timeout)
        if result.returncode:
            raise RuntimeError(f"{verb} hook exited {result.returncode}: {result.stderr[-1000:]}")
        return result.stdout.strip()

    def event(self, name, **values):
        receipt(self.events, event=name, epoch=time.time(), generation=self.generation,
                offered_records_per_second=self.args.tps, first_event_id=1, **values)

    def action(self, name):
        start = time.time()
        if name == "start":
            # A timed-out start may have partially started the producer. Stop it in finally.
            self.started = True
        result = self.call(name)
        self.event(name, started_epoch=start, output=result[-900:])
        if name == "stop":
            self.started = False

    def capture(self):
        start = time.time()
        topics = topics_from(json.loads(self.call("offsets")))
        return dict(epoch=time.time(), started_epoch=start, generation=self.generation, topics=topics)

    def sample(self):
        row = self.capture()
        receipt(self.samples, **row)
        return row

    def flush(self):
        if self.pending_sample is not None:
            receipt(self.samples, **self.pending_sample.result())
            self.pending_sample = None

    def stop(self):
        try:
            self.flush()
        finally:
            if self.started:
                self.action("stop")


def control(args, producer, rest_pool, offsets_pool):
    directory = args.arm
    config = json.loads((directory / "plan.json").read_text())
    deployment = json.loads((directory / "deployment-created.json").read_text())
    job_id = deployment["spec"]["flinkConfiguration"]["$internal.pipeline.job-id"]
    job_name = deployment.get("metadata", {}).get("name")
    release = config["release_ms"] / 1000
    end = release + args.observation_seconds
    clock = time.monotonic
    ready = directory / "initial-allocation.json"
    base = (directory / "rest-url").read_text().strip()
    initial = producer.sample()
    if any(offset for partitions in initial["topics"].values() for offset in partitions.values()):
        raise ValueError("Initial Kafka topics contain data before the first release")
    baseline = None
    while time.time() < release and baseline is None:
        if ready.exists():
            try:
                sample = read_job(base, job_id, rest_pool, job_name)
                if sample["active"] and sample["job_id"] == job_id:
                    baseline = sample
            except (OSError, ValueError) as error:
                producer.event("unavailable", error=str(error))
        if baseline is None:
            time.sleep(0.25)
    if baseline is None:
        raise RuntimeError("Could not establish the initial Q8 tasks before Kafka release")
    time.sleep(max(0, release-time.time()))
    at_release = read_job(base, job_id, rest_pool, job_name)
    if not at_release["active"] or change_reason(baseline, at_release):
        raise RuntimeError("Q8 changed its initial allocation before the producer started")
    producer.event("initial_allocation", consumer=at_release)
    producer.action("start")
    last_success = clock()
    next_offsets = clock()
    waiting = False
    changes = 0
    candidate = None
    while time.time() < end and not (directory / "window-ended.json").exists():
        if producer.pending_sample is not None and producer.pending_sample.done():
            producer.flush()
        if producer.started and producer.pending_sample is None and clock() >= next_offsets:
            producer.pending_sample = offsets_pool.submit(producer.capture)
            next_offsets = clock() + args.offset_interval
        try:
            job = read_job(base, job_id, rest_pool, job_name)
        except (OSError, ValueError) as error:
            if clock() - last_success > args.rest_timeout:
                raise RuntimeError("Lost the Q8 consumer REST view") from error
            producer.event("unavailable", error=str(error))
            time.sleep(args.poll_interval)
            continue
        last_success = clock()
        reason = change_reason(baseline, job)
        if not waiting and reason:
            changes += 1
            producer.event("consumer_transition", trigger=reason, consumer=job)
            producer.action("pause")
            producer.stop()
            producer.sample()
            producer.generation += 1
            producer.action("reset")
            cleared = producer.sample()
            if set(cleared["topics"]) != set(initial["topics"]) or any(
                    offset for v in cleared["topics"].values() for offset in v.values()):
                raise RuntimeError("Owned topics were not recreated empty by reset")
            waiting = True
            candidate = None
            continue
        if waiting:
            # A job-level RUNNING can precede full task recovery. Require two
            # consistent complete snapshots to coalesce this execution graph's updates.
            if job["active"] and reason and candidate == job:
                producer.event("consumer_ready", consumer=job)
                producer.action("start")
                baseline = job
                waiting = False
                next_offsets = clock()
            candidate = job if job["active"] and reason else None
        time.sleep(max(0, min(args.poll_interval, end-time.time())))
    producer.stop()
    producer.sample()
    if waiting:
        raise RuntimeError("Observation ended before the reset producer could be restarted on the running consumer")
    return changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", type=Path)
    parser.add_argument("--hook", required=True, type=Path, help="Owned broker/producer adapter accepting start, pause, stop, reset, offsets")
    parser.add_argument("--tps", required=True, type=int, help="Total offered Nexmark events per second")
    parser.add_argument("--observation-seconds", required=True, type=int)
    parser.add_argument("--poll-interval", default=1.0, type=float)
    parser.add_argument("--offset-interval", default=8.0, type=float)
    parser.add_argument("--rest-timeout", default=60, type=int)
    parser.add_argument("--hook-timeout", default=180, type=int)
    args = parser.parse_args()
    args.hook = args.hook.resolve(strict=True)
    if not os.access(args.hook, os.X_OK) or min(args.tps, args.observation_seconds, args.poll_interval,
            args.offset_interval, args.rest_timeout, args.hook_timeout) <= 0:
        parser.error("Expected an executable hook and positive rates and durations")
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    result = {"status": "failed", "offered_records_per_second": args.tps, "first_event_id": 1}
    with ExitStack() as stack:
        events = stack.enter_context((args.arm / "kafka-generations.jsonl").open("x"))
        offsets = stack.enter_context((args.arm / "kafka-offsets.jsonl").open("x"))
        rest_pool = stack.enter_context(ThreadPoolExecutor(6))
        offsets_pool = stack.enter_context(ThreadPoolExecutor(1))
        p = Producer(args, args.arm.name, events, offsets)
        try:
            count = control(args, p, rest_pool, offsets_pool)
            result.update(status="complete", consumer_transitions=count)
        except BaseException as error:
            result["error"] = repr(error)
            raise
        finally:
            try:
                p.stop()
            finally:
                (args.arm / "kafka-result.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
