#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/env.sh"

# The runtime consumes a specific native ABI. Build from the Git link and the
# matching branch patches, including when the user's submodule has local edits.
pin=$(git -C "$REPO_ROOT" rev-parse HEAD:sources/libs/frocksdb)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/a4s-frocksdb.XXXXXXXX")
trap 'rm -rf -- "$build_dir"' EXIT
mkdir -p "$build_dir/source" "$build_dir/artifacts"
git -C "$FROCKSDB_SOURCE" archive "$pin" | tar -xf - -C "$build_dir/source"
for patch in frocksdb-a4s-block-latency frocksdb-a4s-cache-lookup-v1; do
    git -C "$build_dir/source" apply --no-index "$REPO_ROOT/sources/libs/patches/$patch.patch"
done

image=rocksdb-builder-jammy
docker build --platform linux/amd64 -t "$image" "$SCRIPT_DIR"
docker run --rm --platform linux/amd64 \
    -v "$build_dir/source:/rocksdb-host:ro" \
    -v "$build_dir/artifacts:/rocksdb-java-target" \
    -e DEBUG_LEVEL=0 \
    --entrypoint /rocksdb-host/java/crossbuild/docker-build-linux-ubuntu.sh \
    "$image"

mkdir -p "$FROCKSDB_SOURCE/java/target"
cp "$build_dir/artifacts/"* "$FROCKSDB_SOURCE/java/target/"
cp "$build_dir/artifacts/rocksdbjni-6.20.3-linux64.jar" "$FLINK_RUNTIME_A4S_SOURCE/"
