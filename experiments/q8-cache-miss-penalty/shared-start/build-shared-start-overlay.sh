#!/usr/bin/env bash
# Add the common benchmark scheduler to a private copy of the normal profile overlay.
set -euo pipefail
if [[ $# != 5 ]]; then
    echo "Usage: $0 REPOSITORY FLINK_LIB BUILD_DEPENDENCIES NORMAL_PROFILE_JAR OUTPUT_JAR" >&2
    exit 2
fi
source_path=sources/flink/flink-runtime-1-18-a4s/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/adaptive/AdaptiveScheduler.java
here=$(cd "$(dirname "$0")" && pwd)
build=$(mktemp -d)
trap 'rm -rf -- "$build"' EXIT
test ! -e "$5"
mkdir -p "$build/classes"
cp "$1/$source_path" "$build/AdaptiveScheduler.java"
patch --fuzz=0 --silent --directory="$build" -p1 <"$here/q8-initial-slot-sharing.patch"
javac --release 11 -proc:none -cp "$4:$2/*:$3/*" -d "$build/classes" "$build/AdaptiveScheduler.java"
cp "$4" "$build/overlay.jar"
jar uf "$build/overlay.jar" -C "$build/classes" .
mkdir -p "$(dirname "$5")"
cp "$build/overlay.jar" "$5"
