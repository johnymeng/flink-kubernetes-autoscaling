#!/usr/bin/env bash
# Run both native operators, check their observed actions and draw the Q8 charts.
set -euo pipefail
if [[ $# != 1 ]]; then echo "Usage: $0 NEW_RESULTS_DIRECTORY" >&2; exit 2; fi
results=$(realpath -m "$1")
mkdir -- "$results"
here=$(cd "$(dirname "$0")" && pwd)
: "${ARTIFACTS:?Set ARTIFACTS to the common Q8 runtime JAR directory}"
run_arm() {
    local arm=$1 env_prefix=$2 key optname
    local -a options=()
    for key in NAMESPACE OPERATOR_NAMESPACE OPERATOR_DEPLOYMENT OPERATOR_JAR OPERATOR_JAR_SHA256 REVISION BOOTSTRAP_SERVERS KAFKA_HOOK; do
        optname="${env_prefix}_$key"
        : "${!optname:?Missing required per-arm variable}"
        key="${key,,}"; options+=("--${key//_/-}" "${!optname}")
    done
    for key in OPERATOR_CONTAINER LABEL; do
        optname="${env_prefix}_$key"
        if [[ -n ${!optname:-} ]]; then key="${key,,}"; options+=("--${key//_/-}" "${!optname}"); fi
    done
    optname="${env_prefix}_KUBECONFIG"
    KUBECONFIG="${!optname:-${KUBECONFIG:?Set the kubeconfig for each arm}}" \
        python3 "$here/quick_pair.py" --arm "$arm" --output "$results" --worker "${Q8_WORKER:-c167}" \
        --artifacts "$ARTIFACTS" --observation-seconds "${OBSERVATION_SECONDS:-360}" "${options[@]}"
}
case "${Q8_ORDER:-parallel}" in
    parallel)
        run_arm hardcoded MAIN & a=$!
        run_arm calibrated BRANCH & b=$!
        status=0; wait "$a" || status=$?; wait "$b" || status=$?
        if ((status)); then exit "$status"; fi ;;
    AB) run_arm hardcoded MAIN; run_arm calibrated BRANCH ;;
    BA) run_arm calibrated BRANCH; run_arm hardcoded MAIN ;;
    *) echo 'Q8_ORDER must be parallel, AB or BA' >&2; exit 2 ;;
esac
pair="$results/pair-1-AB"
status=0
python3 "$here/native_check.py" "$pair" --output "$results/quick-results.json" || status=$?
plot_args=()
if [[ -n "${RYAN_PLOTTER:-}" ]]; then plot_args+=(--ryan-plotter "$RYAN_PLOTTER"); fi
MPLBACKEND=Agg python3 "$here/plot_quick.py" "$pair" --output "$results/plots" "${plot_args[@]}"
exit "$status"
