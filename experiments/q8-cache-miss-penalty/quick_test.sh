#!/usr/bin/env bash
# Run the short matched Q8 pair, validate its decisions, then render the resource charts.
set -euo pipefail
if [[ $# -lt 1 || $# -gt 3 ]]; then
    echo "Usage: $0 NEW_RESULTS_DIRECTORY [literal|omit-empty] [burst|two-slot-80k]" >&2
    exit 2
fi
results=$(realpath -m "$1")
mode=${2:-literal}
scenario=${3:-burst}
case "$mode" in
    literal) driver=main-literal-driver.jar; label='main · unmodified evaluator' ;;
    omit-empty) driver=main-driver.jar; label='main · empty curves omitted' ;;
    *) echo "Expected literal or omit-empty" >&2; exit 2 ;;
esac
: "${ARTIFACTS:?Set ARTIFACTS to the common Q8 workload artifacts}"
: "${DIST_LIB:?Set DIST_LIB to the pinned Flink distribution lib directory}"
: "${BUILD:?Set BUILD to the directory containing policy.jar and main/}"
: "${KUBECONFIG:?Set KUBECONFIG to the lab kubeconfig}"
here=$(cd "$(dirname "$0")" && pwd)
shared="$ARTIFACTS/abi3-mrc-telemetry.jar:$DIST_LIB/*"
main_revision=f8ce7fbbfae48869d8b1869748ad517801a9f726
branch_revision=${BRANCH_REVISION:-$(git -C "$here" rev-parse HEAD)}
plot_args=()
if [[ -n "${RYAN_PLOTTER:-}" ]]; then
    plot_args+=(--ryan-plotter "$RYAN_PLOTTER")
fi
python3 "$here/quick_pair.py" --output "$results" --scenario "$scenario" --worker "${Q8_WORKER:-c167}" --artifacts "$ARTIFACTS" \
    --main-classpath "$BUILD/main/main-autoscaler.jar:$shared" --main-policy "$BUILD/main/$driver" \
    --main-revision "$main_revision" --main-empty-curves "$mode" \
    --branch-classpath "$ARTIFACTS/branch-autoscaler.jar:$shared" --branch-policy "$BUILD/policy.jar" \
    --branch-revision "$branch_revision"
status=0
python3 "$here/quick_check.py" "$results" || status=$?
MPLBACKEND=Agg python3 "$here/plot_quick.py" "$results/pair-1-AB" --output "$results/plots" \
    --main-label "$label" "${plot_args[@]}"
exit "$status"
