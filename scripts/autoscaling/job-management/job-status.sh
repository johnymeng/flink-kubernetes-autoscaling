#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

namespace_args=()
if [[ ${1:-} == --namespace ]]; then namespace_args=(-n "${2:?missing namespace}"); shift 2; fi
deployment="${1:-flink}"

echo "Flink deployments"
if ((${#namespace_args[@]})); then
  kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}" 2>/dev/null || true
else
  kubectl get flinkdeployment 2>/dev/null || true
fi

echo
echo "Deployment detail: ${deployment}"
kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}" -o wide 2>/dev/null || true

echo
echo "JobManager pods"
kubectl "${namespace_args[@]}" get pods -l "app=${deployment},component=jobmanager" -o wide 2>/dev/null || true

echo
echo "TaskManager pods"
kubectl "${namespace_args[@]}" get pods -l "app=${deployment},component=taskmanager" -o wide 2>/dev/null || true

echo
echo "Flink services"
kubectl "${namespace_args[@]}" get svc -l "app=${deployment}" 2>/dev/null || true
kubectl "${namespace_args[@]}" get svc "${deployment}-rest" 2>/dev/null || true

echo
echo "Recent Flink events"
if ((${#namespace_args[@]})); then
  kubectl "${namespace_args[@]}" get events --field-selector "involvedObject.name=${deployment}" \
    --sort-by=.lastTimestamp 2>/dev/null | tail -20 || true
else
  kubectl get events --sort-by=.lastTimestamp 2>/dev/null \
    | grep -E "flink|${deployment}|jobmanager|taskmanager" | tail -20 || true
fi
