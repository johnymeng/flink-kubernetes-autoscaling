#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 || ! "$4" =~ ^[0-9]+$ ]]; then
    printf 'Usage: %s ORIGINAL_JNI_JAR EXTRACTED_JNI_JAR OUTPUT_DIR CPU\n' "$0" >&2
    exit 2
fi

export LC_ALL=C
original=$(realpath -e -- "$1")
extracted=$(realpath -e -- "$2")
cpu=$4
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
java=java
javac=javac
if [[ -n "${JAVA_HOME:-}" ]]; then
    java="$JAVA_HOME/bin/java"
    javac="$JAVA_HOME/bin/javac"
fi
mkdir -p -- "$3"
out=$(mktemp -d -- "${3%/}/native-shards.XXXXXXXX")
scratch=$(mktemp -d -- "${TMPDIR:-/tmp}/a4s-native-shards.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
mkdir "$scratch/classes" "$scratch/jni"

sha256sum -- "$original" "$extracted" > "$out/native-inputs.sha256"
"$java" -version > "$out/java-version.txt" 2>&1
"$javac" --release 11 -cp "$extracted" -d "$scratch/classes" "$script_dir/NativeShardsOverhead.java"
printf 'Results directory %s\n' "$out"

measure() {
    local binary=$1
    local mode=$2
    timeout --kill-after=3s 30s taskset -c "$cpu" "$java" \
        -Xms96m -Xmx96m -Djava.io.tmpdir="$scratch/jni" -cp "$scratch/classes:$binary" \
        NativeShardsOverhead "$scratch/db" "$mode"
}

measure "$extracted" prepare
sha256sum "$scratch/db"/*.sst > "$out/database-sst.sha256"
printf 'CPU %s\n' "$cpu" > "$out/affinity.txt"

# Each setting occupies each sequential position once across the six blocks.
modes=(original extracted original extracted original extracted)
rates=(0.00 0.00 0.02 0.02 1.00 1.00)
partial="$out/samples.csv.partial"
printf '%s\n' 'block,position,implementation,sampling_rate,reads,checksum,wall_ns,cpu_ns,anon_before_kib,anon_after_kib,anon_delta_kib,capacity_bytes,cache_used_bytes,data_hits,sampled_refs,tracked_entries,tracked_bytes' > "$partial"
for ((block = 0; block < 6; block++)); do
    for ((position = 0; position < 6; position++)); do
        index=$(((position + block) % 6))
        mode=${modes[index]}
        binary=$original
        if [[ "$mode" == extracted ]]; then binary=$extracted; fi
        row=$(measure "$binary" "${rates[index]}")
        printf '%d,%d,%s,%s\n' "$((block + 1))" "$((position + 1))" "$mode" "$row" >> "$partial"
    done
    printf 'Completed block %d of 6\n' "$((block + 1))"
done

sha256sum -c "$out/database-sst.sha256"
mv "$partial" "$out/samples.csv"
