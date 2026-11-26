#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 <patched Flink 1.18 classpath> <output directory> [Kafka 3.7 distribution directory]" >&2
    exit 2
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$script_dir/../.." && pwd)
runner="${A4S_RUNNER_JAR:-$repo/sources/a4s/a4s-runner/target/a4s-runner.jar}"
operator="${A4S_OPERATOR_CLASSES:-$repo/sources/operators/flink-kubernetes-operator-a4s/flink-autoscaler/target/classes}"
java=java
javac=javac
if [[ -n "${JAVA_HOME:-}" ]]; then
    java="$JAVA_HOME/bin/java"
    javac="$JAVA_HOME/bin/javac"
fi

if [[ ! -r "$runner" || ! -e "$operator" ]]; then
    echo "Build the A4S runner and Flink operator before running the smoke test." >&2
    exit 2
fi

mkdir -p "$2"
out_dir=$(cd "$2" && pwd)
trial=$(mktemp -d "$out_dir/local-flink.XXXXXX")
echo "Writing local smoke evidence to $trial"
classpath="$1:$operator:$runner"
sources=("$script_dir/LocalAdaptiveA4sSmoke.java")
driver=LocalAdaptiveA4sSmoke
broker_args=()
owned_pids=()
cleanup() {
    if (( ${#owned_pids[@]} )); then
        kill "${owned_pids[@]}" 2>/dev/null || true
        wait "${owned_pids[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ $# -eq 3 ]]; then
    kafka=$(cd "$3" && pwd)
    sources+=("$script_dir/LocalAdaptiveA4sKafkaSmoke.java")
    driver=LocalAdaptiveA4sKafkaSmoke
    ports=$(python3 - <<'PY'
import socket

with socket.socket() as broker, socket.socket() as controller:
    broker.bind(('127.0.0.1', 0))
    controller.bind(('127.0.0.1', 0))
    print(broker.getsockname()[1], controller.getsockname()[1])
PY
    )
    read -r broker_port controller_port <<< "$ports"
    broker_args=("127.0.0.1:$broker_port")
    config="$trial/kafka.properties"
    cat > "$config" <<EOF
node.id=1
process.roles=broker,controller
controller.quorum.voters=1@127.0.0.1:$controller_port
listeners=PLAINTEXT://127.0.0.1:$broker_port,CONTROLLER://127.0.0.1:$controller_port
advertised.listeners=PLAINTEXT://127.0.0.1:$broker_port
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$trial/kafka-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
EOF
    export LOG_DIR="$trial/kafka-logs"
    export KAFKA_HEAP_OPTS='-Xms128m -Xmx256m'
    cluster_id=$("$kafka/bin/kafka-storage.sh" random-uuid)
    "$kafka/bin/kafka-storage.sh" format -t "$cluster_id" -c "$config" > "$trial/kafka.log"
    "$kafka/bin/kafka-server-start.sh" "$config" >> "$trial/kafka.log" 2>&1 &
    owned_pids+=("$!")
fi

"$javac" --release 11 -cp "$classpath" -d "$trial" "${sources[@]}"

A4S_BIND=127.0.0.1 A4S_PORT=0 "$java" -jar "$runner" > "$trial/runner.log" 2>&1 &
runner_pid=$!
owned_pids+=("$runner_pid")

for _ in $(seq 1 100); do
    port=$(sed -n 's/^{"event":"runner_started","port":\([0-9]*\)}$/\1/p' "$trial/runner.log")
    if [[ -n "$port" ]]; then
        break
    fi
    kill -0 "$runner_pid"
    sleep 0.1
done
if [[ -z "$port" ]]; then
    echo "The local A4S runner did not report a port. See $trial/runner.log." >&2
    exit 1
fi

A4S_SNAPSHOT_DIR="$trial" timeout -k 5s 120s "$java" \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.io=ALL-UNNAMED \
    --add-opens java.base/java.net=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    -cp "$trial:$classpath" "$driver" "http://127.0.0.1:$port" "$trial" "${broker_args[@]}" \
    2>&1 | tee "$trial/rescale.log"

"$java" -jar "$runner" replay --verify \
    --input "$trial/local-adaptive.jsonl" --output "$trial/replay.jsonl"
echo "Replayed and verified the recorded remote recommendation."
