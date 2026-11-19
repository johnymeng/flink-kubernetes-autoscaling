#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

namespace_args=()
namespace=
uid=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) namespace=${2:?missing namespace}; namespace_args=(-n "$namespace"); shift 2 ;;
    --uid) uid=${2:?missing UID}; shift 2 ;;
    *) break ;;
  esac
done
deployment="${1:-flink}"
timeout="${WAIT_SECONDS:-180}s"

if [[ -n "$uid" ]]; then
  # A retry must never delete a replacement CR that reused the original name.
  [[ -n "$namespace" && "$uid" =~ ^[[:xdigit:]-]+$ ]] || { echo "UID deletion requires namespace and UID" >&2; exit 2; }
  route="/apis/flink.apache.org/v1beta1/namespaces/$namespace/flinkdeployments/$deployment"
  current=$(kubectl "${namespace_args[@]}" get flinkdeployment "$deployment" --ignore-not-found -o name)
  if [[ -n "$current" ]]; then
    printf '{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"%s"}}' "$uid" |
      kubectl delete --raw="$route" -f - >/dev/null
    current=$(kubectl "${namespace_args[@]}" get flinkdeployment "$deployment" --ignore-not-found -o name)
    if [[ -n "$current" ]]; then
      kubectl "${namespace_args[@]}" wait --for=delete "flinkdeployment/$deployment" --timeout="$timeout"
    fi
  fi
  exit 0
fi

if [[ -z "$namespace" && "$deployment" == flink ]]; then
  pkill -f "kubectl port-forward.*flink-rest.*8081:8081" 2>/dev/null || true
fi

if kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}" >/dev/null 2>&1; then
  kubectl "${namespace_args[@]}" delete flinkdeployment "${deployment}" --wait=false
  if ! kubectl "${namespace_args[@]}" wait --for=delete "flinkdeployment/${deployment}" --timeout="${timeout}" >/dev/null 2>&1; then
    kubectl "${namespace_args[@]}" patch flinkdeployment "${deployment}" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
    kubectl "${namespace_args[@]}" delete flinkdeployment "${deployment}" --ignore-not-found=true --wait=false
  fi
fi

kubectl "${namespace_args[@]}" get flinkdeployment 2>/dev/null || true
kubectl "${namespace_args[@]}" get pods -l "app=${deployment}" 2>/dev/null || true
