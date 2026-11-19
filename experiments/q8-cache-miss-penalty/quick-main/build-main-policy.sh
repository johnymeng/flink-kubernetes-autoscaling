#!/usr/bin/env bash
# Build the fixed controller from the pinned main commit, plus its tracing adapter.
set -euo pipefail
if [[ $# != 5 ]]; then
    echo "Usage: $0 MAIN_REPOSITORY FLINK_DIST_LIB COMPILE_DEPENDENCIES SHARED_TELEMETRY_JAR OUTPUT_DIRECTORY" >&2
    exit 2
fi
repo=$(realpath "$1")
lib=$(realpath "$2")
deps=$(realpath "$3")
telemetry=$(realpath "$4")
mkdir -p "$5"
out=$(realpath "$5")
experiment=$(cd "$(dirname "$0")" && pwd)
revision=f8ce7fbbfae48869d8b1869748ad517801a9f726
source_dir=sources/operators/flink-kubernetes-operator-a4s/flink-autoscaler/src/main/java
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT

# Archive the actual commit. The caller's current branch and uncommitted files have no effect.
git -C "$repo" archive "$revision" "$source_dir" | tar -xf - -C "$scratch"
mkdir "$scratch/controller" "$scratch/driver"
find "$scratch/$source_dir" -name '*.java' -print0 | sort -z >"$scratch/java-sources.lst"
mapfile -d '' java_sources <"$scratch/java-sources.lst"
javac --release 11 -cp "$telemetry:$lib/*:$deps/*" -d "$scratch/controller" "${java_sources[@]}"
printf 'Manifest-Version: 1.0\nImplementation-Revision: %s\n\n' "$revision" >"$scratch/manifest"
jar cfm "$out/main-autoscaler.jar" "$scratch/manifest" -C "$scratch/controller" .
javac -proc:none --release 11 -cp "$out/main-autoscaler.jar:$telemetry:$lib/*" \
    -d "$scratch/driver" "$experiment/RyanQ8Policy.java"
jar cf "$out/main-driver.jar" -C "$scratch/driver" .
# The literal driver refuses the omit-empty mode. Both use identical class files.
touch "$scratch/driver/a4s-quick-literal-main"
jar cf "$out/main-literal-driver.jar" -C "$scratch/driver" .
(cd "$out" && sha256sum main-autoscaler.jar main-driver.jar main-literal-driver.jar)
