#!/usr/bin/env python3
"""Manage one Q20-style external Kafka broker and Nexmark producer per Q8 policy."""
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

TOPICS = ("nexmark-person", "nexmark-auction", "nexmark-bid")
OWNER = "ece496.q8.kafka.run"
SQL = "/opt/flink/lib/nexmark-flink-0.3-SNAPSHOT.jar"
BIN = "/opt/kafka/bin/"


def docker(*args, check=True):
    return subprocess.run(["sudo", "-n", "docker", *args], check=check,
                          text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def container(name, owner):
    result = docker("container", "ls", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}")
    if name not in result.stdout.splitlines():
        return None
    obj = json.loads(docker("inspect", name).stdout)[0]
    if (obj["Config"].get("Labels") or {}).get(OWNER) != owner:
        raise RuntimeError(f"Container {name} belongs to another run")
    return obj


def remove(name, owner):
    if container(name, owner):
        docker("rm", "-fv", name)


def broker_ready(name, host, port):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), 1):
                pass
            result = docker("exec", name, BIN+"kafka-topics.sh", "--bootstrap-server",
                            f"127.0.0.1:{port}", "--list", check=False)
            if result.returncode == 0:
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError(docker("logs", "--tail", "20", name, check=False).stderr)


def run(config, verb):
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,37}", config["run"]) or config["arm"] not in ("calibrated","hardcoded"):
        raise ValueError("Expected a unique lowercase Q8_RUN_ID and a known Q8_ARM")
    owner = config["run"] + "-" + config["arm"]
    broker, producer = owner + "-kafka", owner + "-producer"
    host = str(ipaddress.ip_address(config["ip"]))
    port, rest = int(config["port"]), int(config["rest"])
    if any(not 1024 <= value <= 65535 for value in (port, port+1, rest)):
        raise ValueError("Invalid Kafka or producer port")
    address = f"{host}:{port}"
    admin = ["exec", broker, BIN+"kafka-topics.sh", "--bootstrap-server", f"127.0.0.1:{port}"]
    if verb in ("provision","reset"):
        if container(producer, owner):
            raise RuntimeError("Stop this run's producer before resetting its broker")
        if verb == "reset":
            remove(broker, owner)
        elif container(broker, owner):
            raise RuntimeError(f"Broker {broker} already exists")
        # Each run gets a disposable store. Replacing its broker also clears consumer groups.
        env = {"KAFKA_NODE_ID":"1", "KAFKA_PROCESS_ROLES":"broker,controller",
               "KAFKA_LISTENERS":f"PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:{port+1}",
               "KAFKA_ADVERTISED_LISTENERS":f"PLAINTEXT://{address}",
               "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":"CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
               "KAFKA_CONTROLLER_LISTENER_NAMES":"CONTROLLER",
               "KAFKA_CONTROLLER_QUORUM_VOTERS":f"1@{host}:{port+1}",
               "KAFKA_AUTO_CREATE_TOPICS_ENABLE":"false","KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":"1",
               "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":"1","KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":"1",
               "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":"0","KAFKA_LOG_DIRS":"/var/lib/kafka/data"}
        docker("run", "-d", "--name", broker, "--label", OWNER+"="+owner, "--network", "host",
               "--tmpfs","/var/lib/kafka/data:rw,size=16g,uid=1000,gid=1000",
               *[x for k,v in env.items() for x in ("-e",k+"="+v)], config["kafka_image"])
        broker_ready(broker, host, port)
        for topic in TOPICS:
            docker(*admin, "--create", "--topic", topic, "--partitions", "24", "--replication-factor", "1")
        return {"broker":broker,"address":address,"topic_partitions":24}
    if verb == "start":
        if not container(broker, owner) or container(producer, owner):
            raise RuntimeError("A producer start requires an owned broker and a stopped producer")
        tps = int(config["tps"])
        if tps <= 0 or int(config["first"]) != 1:
            raise ValueError("Expected a positive target rate and replay from event ID 1")
        docker("run", "-d", "--name", producer, "--label", OWNER+"="+owner, "--network", "host",
               "--cpus", "16", config["sql_image"], "/opt/flink/bin/flink", "run",
               "-Dexecution.target=local", "-Dparallelism.default=4", "-Dtaskmanager.numberOfTaskSlots=4",
               "-Dblob.server.port=0", "-Djobmanager.rpc.port=0", "-Dtaskmanager.rpc.port=0",
               "-Dtaskmanager.cpu.cores=16", "-Djobmanager.memory.process.size=2048m",
               "-Dtaskmanager.memory.process.size=8192m","-Drest.bind-address=0.0.0.0",
               f"-Drest.address={host}",f"-Drest.bind-port={rest}",f"-Drest.port={rest}",
               "-Dweb.submit.enable=true","-Dweb.cancel.enable=true",
               "-c","com.github.nexmark.flink.sql.SqlQueryJob",SQL,"--query","insert_kafka_unique",
               "--tps",str(tps),"--events","300000000","--max-emit-speed","false","--first-event-id","1",
               "--job-name",producer,"--bootstrap-servers",address,"--wait-for-finish")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            obj = container(producer, owner)
            if not obj or not obj["State"]["Running"]:
                logs = docker("logs", "--tail", "50", producer, check=False)
                raise RuntimeError(f"Producer exited during startup: {logs.stdout} {logs.stderr}")
            try:
                with urlopen(f"http://127.0.0.1:{rest}/jobs/overview", timeout=2) as response:
                    jobs = json.load(response)["jobs"]
                active = next((j for j in jobs if j["name"] == producer and j["state"] == "RUNNING"), None)
                if active:
                    return {"producer":producer,"rest_port":rest,"job_id":active["jid"],
                            "producer_job_start_ms":active["start-time"],"tps":tps,"first_event_id":1}
            except (URLError, TimeoutError):
                pass
            time.sleep(1)
        raise RuntimeError(f"Producer {producer} never reached RUNNING on its assigned REST port {rest}")
    if verb == "pause":
        obj = container(producer, owner)
        if obj and obj["State"]["Running"] and not obj["State"]["Paused"]:
            docker("pause",producer)
    elif verb == "stop":
        remove(producer,owner)
    elif verb == "cleanup":
        remove(producer,owner)
        remove(broker,owner)
    elif verb == "offsets":
        if not container(broker,owner):
            raise RuntimeError("Owned broker missing")
        rows = docker("exec",broker,BIN+"kafka-get-offsets.sh","--bootstrap-server",f"127.0.0.1:{port}",
                      "--topic","nexmark-(person|auction|bid)","--time","-1").stdout.splitlines()
        offsets = {t:{} for t in TOPICS}
        for row in rows:
            topic,partition,end = row.split(":")
            offsets[topic][partition] = int(end)
        if any(len(parts) != 24 for parts in offsets.values()):
            raise RuntimeError("Incomplete broker end offsets")
        return offsets
    else:
        raise ValueError(f"Unknown hook action {verb}")
    return {"action": verb, "owner": owner}


def main():
    verb = sys.argv[1]
    if len(sys.argv) > 2:
        print(json.dumps(run(json.loads(sys.argv[2]),verb)))
        return
    arm = os.environ["Q8_ARM"]
    is_main = arm == "hardcoded"
    config = {"run":os.environ["Q8_RUN_ID"],"arm":arm,"ip":os.environ["Q8_KAFKA_HOST_IP"],
         "port":os.environ.get("Q8_KAFKA_PORT",29082 if is_main else 29182),
         "rest":os.environ.get("Q8_PRODUCER_REST_PORT",28081 if is_main else 28181),
         "first":os.environ.get("Q8_FIRST_EVENT_ID","1"),"tps":os.environ.get("Q8_TPS","80000"),
         "sql_image":os.environ.get("Q8_SQL_IMAGE","142.150.234.165:5000/flink-benchmark-runtime:sql-three-tables-unique"),
         "kafka_image":os.environ.get("Q8_KAFKA_IMAGE","apache/kafka:3.9.2")}
    cmd = shlex.join(["python3","-",verb,json.dumps(config)])
    subprocess.run(["ssh","-o","BatchMode=yes",os.environ.get("Q8_KAFKA_HOST","c153"),cmd],
                   input=Path(__file__).read_text(),text=True,check=True)

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        if error.stderr:
            print(error.stderr,file=sys.stderr)
        raise
