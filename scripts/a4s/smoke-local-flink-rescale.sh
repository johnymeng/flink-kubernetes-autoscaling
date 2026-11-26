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

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <patched Flink 1.18 classpath> <output directory>" >&2
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
"$javac" --release 11 -cp "$classpath" -d "$trial" "$script_dir/LocalAdaptiveA4sSmoke.java"

A4S_BIND=127.0.0.1 A4S_PORT=0 "$java" -jar "$runner" > "$trial/runner.log" 2>&1 &
runner_pid=$!
cleanup() {
    kill "$runner_pid" 2>/dev/null || true
    wait "$runner_pid" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
    -cp "$trial:$classpath" LocalAdaptiveA4sSmoke "http://127.0.0.1:$port" "$trial" \
    2>&1 | tee "$trial/rescale.log"

"$java" -jar "$runner" replay --verify \
    --input "$trial/local-adaptive.jsonl" --output "$trial/replay.jsonl"
echo "Replayed and verified the recorded remote recommendation."
