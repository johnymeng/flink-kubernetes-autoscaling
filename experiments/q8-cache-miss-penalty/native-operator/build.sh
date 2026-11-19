#!/usr/bin/env bash
# Compile the operator's built-in scaling code from a specific committed tree.
set -euo pipefail
if [[ $# != 9 ]]; then
    echo "Usage: $0 REPO REVISION BASE_REVISION BASE_OPERATOR_JAR TELEMETRY_JAR FLINK_LIB COMPILE_LIB LOMBOK_JAR OUTPUT_DIR" >&2
    exit 2
fi
repo=$(realpath "$1")
revision=$(git -C "$repo" rev-parse --verify "$2^{commit}")
base_revision=$(git -C "$repo" rev-parse --verify "$3^{commit}")
base_jar=$(realpath "$4")
telemetry=$(realpath "$5")
flink_lib=$(realpath "$6")
compile_lib=$(realpath "$7")
lombok=$(realpath "$8")
mkdir -p "$9"
out=$(realpath "$9")
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
module=sources/operators/flink-kubernetes-operator-a4s
autoscaler="$module/flink-autoscaler/src/main/java"
operator="$module/flink-kubernetes-operator/src/main/java"
wiring="$operator/org/apache/flink/kubernetes/operator"
# This recipe overlays the scaling code on a compatible packaged operator. Decline
# a base with other operator changes that would be missing from this classpath.
for path in "$module/flink-kubernetes-operator" "$module/flink-kubernetes-operator-api"; do
    git -C "$repo" diff --quiet "$base_revision" "$revision" -- "$path" || {
        echo "Operator modules differ at $path. Supply a base built from $revision." >&2
        exit 1
    }
done
git -C "$repo" archive "$revision" "$autoscaler" "$wiring/autoscaler" \
    "$wiring/reconciler/deployment/ApplicationReconciler.java" \
    "$wiring/service/AbstractFlinkService.java" "$wiring/service/NativeFlinkService.java" | tar -xf - -C "$tmp"

mkdir -p "$tmp/policy-classes" "$tmp/wiring-classes"
cp="$telemetry:$flink_lib/*:$compile_lib/*:$base_jar:$lombok"
mapfile -d '' policy < <(find "$tmp/$autoscaler" -name '*.java' -print0)
javac -encoding UTF-8 --release 11 -cp "$cp" -processorpath "$lombok" -d "$tmp/policy-classes" "${policy[@]}"
printf 'Manifest-Version: 1.0\nImplementation-Revision: %s\n\n' "$revision" > "$tmp/revision.mf"
jar cfm "$out/variant.jar" "$tmp/revision.mf" -C "$tmp/policy-classes" .
mapfile -d '' classes < <(find "$tmp/$wiring" -name '*.java' -print0)
javac -encoding UTF-8 --release 11 -cp "$out/variant.jar:$telemetry:$base_jar:$flink_lib/*:$compile_lib/*:$lombok" \
    -processorpath "$lombok" -d "$tmp/wiring-classes" "${classes[@]}" "$here/Q8NativeOperator.java"
jar cfm "$out/wiring.jar" "$tmp/revision.mf" -C "$tmp/wiring-classes" .
