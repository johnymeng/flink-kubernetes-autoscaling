#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT

# Exercise the actual Docker argv, without depending on a daemon or invoking a build.
cat > "$scratch/docker" <<'DOCKER'
#!/usr/bin/env bash
printf '%s\n' "$@"
DOCKER
chmod +x "$scratch/docker"
export PATH="$scratch:$PATH"

expect_line() {
  if ! grep -Fx -- "$1" "$2" >/dev/null; then
    printf 'Expected %s in %s\n' "$1" "$2" >&2
    cat "$2" >&2
    exit 1
  fi
}

runtime=images/flink-runtime-a4s/build.sh
args=(--source-dir 'fixture/runtime source' --tag fixture-tag --image-name fixture/a4s)
FLINK_RUNTIME_A4S_SOURCE_DIR=other/source FLINK_RUNTIME_A4S_IMAGE_TAG=other-tag \
  bash "$runtime" "${args[@]}" --dry-run > "$scratch/runtime-dry"
! grep -Fx -- 'build' "$scratch/runtime-dry" || exit 1
grep -F 'FLINK_RUNTIME_SOURCE_DIR=fixture/runtime\ source' "$scratch/runtime-dry" >/dev/null
grep -F ' -t fixture/a4s:fixture-tag ' "$scratch/runtime-dry" >/dev/null

bash "$runtime" "${args[@]}" > "$scratch/runtime"
expect_line 'FLINK_RUNTIME_SOURCE_DIR=fixture/runtime source' "$scratch/runtime"
expect_line 'fixture/a4s:fixture-tag' "$scratch/runtime"
expect_line "$repo/images/flink-runtime-a4s/Dockerfile" "$scratch/runtime"
bash "$runtime" "${args[@]}" --local-image fixture/local:override > "$scratch/runtime"
expect_line 'fixture/local:override' "$scratch/runtime"

FLINK_RUNTIME_A4S_SOURCE_DIR='fixture/runtime source' \
  bash images/flink-kubernetes-operator-a4s/build.sh > "$scratch/operator"
expect_line '--build-context' "$scratch/operator"
expect_line "flink-src=$repo/fixture/runtime source" "$scratch/operator"
expect_line "$repo/sources/operators/flink-kubernetes-operator-a4s" "$scratch/operator"

echo 'A4S runtime overrides and operator source context verified'
