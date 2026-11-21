#!/usr/bin/env bash
# Run a predeclared Q8 pair through the built-in native operator.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)

if [[ ${1:-} == --dry-run ]]; then
    if [[ $# != 2 ]]; then echo "Usage: $0 --dry-run COMPARISON_PLAN.json" >&2; exit 2; fi
    exec python3 "$here/quick_pair.py" --dry-run "$2"
fi
if [[ $# != 1 ]]; then echo "Usage: $0 NEW_RESULTS_DIRECTORY" >&2; exit 2; fi

order=${Q8_ORDER:-AB}
index=${Q8_PAIR_INDEX:-1}
: "${OBSERVATION_SECONDS:?Set an explicit finite observation duration}"
: "${ARTIFACTS:?Set ARTIFACTS to the common Q8 runtime JAR directory}"
case "$order" in AB|BA|parallel) ;; *) echo 'Q8_ORDER must be AB, BA or parallel' >&2; exit 2 ;; esac
mode=()
if [[ -n ${Q8_COMPARISON_PLAN:-} ]]; then
    mode=(--comparison-plan "$Q8_COMPARISON_PLAN")
    python3 "$here/quick_pair.py" --preflight-plan "$Q8_COMPARISON_PLAN" \
        --pair-index "$index" --pair-order "$order" \
        --observation-seconds "$OBSERVATION_SECONDS" >/dev/null
elif [[ ${Q8_LEGACY_IMPLEMENTATION_BUNDLE:-0} == 1 && "$order" != parallel ]]; then
    mode=(--legacy-implementation-bundle)
else
    echo 'Set Q8_COMPARISON_PLAN, or explicitly select a sequential historical implementation bundle' >&2
    exit 2
fi
results=$(realpath -m "$1")

run_arm() {
    local arm=$1 env_prefix=$2 key optname worker_var broker_var broker_ip_var
    shift 2
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
    worker_var="${env_prefix}_WORKER"
    broker_var="${env_prefix}_KAFKA_HOST"
    broker_ip_var="${env_prefix}_KAFKA_HOST_IP"
    Q8_KAFKA_HOST="${!broker_var:-${Q8_KAFKA_HOST:-c153}}" \
    Q8_KAFKA_HOST_IP="${!broker_ip_var:-${Q8_KAFKA_HOST_IP:-}}" \
    KUBECONFIG="${!optname:-${KUBECONFIG:?Set the kubeconfig for each arm}}" \
        python3 "$here/quick_pair.py" --arm "$arm" --output "$results" \
        --worker "${!worker_var:-${Q8_WORKER:-c167}}" --artifacts "$ARTIFACTS" \
        --observation-seconds "$OBSERVATION_SECONDS" --pair-index "$index" \
        --pair-order "$order" "${mode[@]}" "${options[@]}" "$@"
}

# Verify both bindings before making any trial directories or contacting the cluster.
run_arm hardcoded MAIN --validate-arm-arguments
run_arm calibrated BRANCH --validate-arm-arguments
mkdir -- "$results"
case "$order" in
    parallel)
        run_arm hardcoded MAIN & a=$!
        run_arm calibrated BRANCH & b=$!
        status=0; wait "$a" || status=$?; wait "$b" || status=$?
        if ((status)); then exit "$status"; fi ;;
    AB) run_arm hardcoded MAIN; run_arm calibrated BRANCH ;;
    BA) run_arm calibrated BRANCH; run_arm hardcoded MAIN ;;
esac
pair="$results/pair-$index-$order"
status=0
python3 "$here/native_check.py" "$pair" --output "$results/quick-results.json" || status=$?
plot_args=()
if [[ -n "${RYAN_PLOTTER:-}" ]]; then plot_args+=(--ryan-plotter "$RYAN_PLOTTER"); fi
MPLBACKEND=Agg python3 "$here/plot_quick.py" "$pair" --output "$results/plots" "${plot_args[@]}"
exit "$status"
