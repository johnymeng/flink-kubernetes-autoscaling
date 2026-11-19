#!/usr/bin/env bash
# Compile the branch's four job JARs and private autoscaler on the pinned lab runtime.
set -euo pipefail

if [[ $# != 5 ]]; then
    echo "Usage: $0 REPOSITORY FLINK_LIB BUILD_DEPENDENCIES NATIVE_JAR OUTPUT_DIRECTORY" >&2
    exit 2
fi
repo=$(realpath "$1")
lib=$(realpath "$2")
deps=$(realpath "$3")
native=$(realpath "$4")
mkdir -p "$5"
out=$(realpath "$5")
build=$(mktemp -d)
trap 'rm -rf -- "$build"' EXIT
mkdir -p "$build"/{replay,profiles,telemetry,controller,tests}

rt="$repo/sources/flink/flink-runtime-1-18-a4s"
runtime="$rt/flink-runtime/src/main/java/org/apache/flink/runtime"
state="$rt/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/contrib/streaming/state"
core="$rt/flink-a4s/src/main/java/org/apache/flink/a4s/core"
controller="$repo/sources/operators/flink-kubernetes-operator-a4s/flink-autoscaler/src"
query="$repo/sources/benchmarks/nexmark-datastream-benchmarks-from-justin"
cp="$lib/*:$deps/*"

cp "$native" "$out/rocksdbjni.jar"
cp -R "$query/src" "$query/pom.xml" "$build/replay"
(cd "$build/replay" && mvn -o compile jar:jar org.apache.maven.plugins:maven-shade-plugin:3.6.2:shade@Query8)
cp "$build/replay/target/Query8.jar" "$out/Query8.jar"
javac --release 11 -cp "$out/Query8.jar" -d "$build/tests" \
    "$repo/scripts/autoscaling/smoke/Q8ReplaySmoke.java"
java -ea -cp "$build/tests:$out/Query8.jar" be.uclouvain.gepiciad.sources.Q8ReplaySmoke

javac --release 11 -cp "$cp" -d "$build/profiles" \
    "$runtime/dispatcher/Dispatcher.java" "$runtime/jobgraph/JobVertexResourceProfileOverrides.java"
jar cf "$out/savepoint-profiles.jar" -C "$build/profiles" .

javac --release 11 -cp "$native:$out/savepoint-profiles.jar:$cp" -d "$build/telemetry" \
    "$core"/{MissRateCurve,StackDistanceHistogram}.java \
    "$state"/{A4SBlockLatencyWindow,RocksDBNativeMetricMonitor,RocksDBNativeMetricOptions,RocksDBResourceContainer}.java \
    "$runtime/metrics/A4SHistogramMetricCodec.java" \
    "$runtime/rest/handler/job/metrics/A4SAggregatingVertexMetricsHandler.java" \
    "$runtime/rest/handler/legacy/metrics/MetricStore.java" \
    "$runtime/rest/messages/job/metrics"/{A4SAggregatedMetricsResponseBody,A4SCacheCostSample}.java
jar cf "$out/abi3-mrc-telemetry.jar" -C "$build/telemetry" .

cp="$out/abi3-mrc-telemetry.jar:$out/savepoint-profiles.jar:$cp"
# All controller implementation classes come from this source tree.
mapfile -d '' sources < <(find "$controller/main/java" -name '*.java' -print0)
javac --release 11 -cp "$cp" -d "$build/controller" "${sources[@]}"
jar cf "$out/branch-autoscaler.jar" -C "$build/controller" .

testdir="$controller/test/java/org/apache/flink/autoscaler"
profiletest="$rt/flink-runtime/src/test/java/org/apache/flink/runtime/jobgraph/JobVertexResourceProfileOverridesTest.java"
cp="$out/branch-autoscaler.jar:$cp"
javac --release 11 -cp "$cp" -d "$build/tests" \
    "$testdir/ScalingMetricEvaluatorTest.java" \
    "$testdir/a4s"/{A4STest,MemoryParallelismCurveTest,OnlineCacheCostTest}.java "$profiletest"
java -cp "$build/tests:$cp" org.junit.platform.console.ConsoleLauncher --disable-ansi-colors --details summary \
    --select-class org.apache.flink.autoscaler.ScalingMetricEvaluatorTest \
    --select-class org.apache.flink.autoscaler.a4s.A4STest \
    --select-class org.apache.flink.autoscaler.a4s.MemoryParallelismCurveTest \
    --select-class org.apache.flink.autoscaler.a4s.OnlineCacheCostTest \
    --select-class org.apache.flink.runtime.jobgraph.JobVertexResourceProfileOverridesTest
(cd "$out" && sha256sum Query8.jar rocksdbjni.jar abi3-mrc-telemetry.jar \
    savepoint-profiles.jar branch-autoscaler.jar > sha256sums.txt)
