#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

namespace_args=()
receipt=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) namespace_args=(-n "${2:?missing namespace}"); shift 2 ;;
    --receipt) receipt=${2:?missing receipt path}; shift 2 ;;
    *) break ;;
  esac
done
if [[ $# -ne 1 ]]; then
  echo "Usage: $(basename "$0") [--namespace NS] [--receipt NEW_FILE] <flinkdeployment-yaml>" >&2
  exit 2
fi

job_yaml="$1"
if [[ ! -f "${job_yaml}" ]]; then
  job_yaml="${SCRIPT_DIR}/${job_yaml}"
fi
if [[ ! -f "${job_yaml}" ]]; then
  echo "Job YAML not found: $1" >&2
  exit 1
fi

deployment="$(kubectl "${namespace_args[@]}" create --dry-run=client -f "${job_yaml}" -o jsonpath='{.metadata.name}')"

if kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}" >/dev/null 2>&1; then
  echo "FlinkDeployment ${deployment} already exists. Stop it first with scripts/autoscaling/job-management/stop-job.sh ${deployment}." >&2
  exit 1
fi

if [[ -n "$receipt" ]]; then
  # Exclusive create and the API server UID let automation safely clean up a partially started run.
  [[ ! -e "$receipt" ]] || { echo "Receipt already exists: $receipt" >&2; exit 1; }
  kubectl "${namespace_args[@]}" create -f "$job_yaml" -o json >"$receipt.tmp"
  mv "$receipt.tmp" "$receipt"
else
  kubectl "${namespace_args[@]}" apply -f "$job_yaml"
fi

for _ in $(seq 1 60); do
  lifecycle="$(kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}" -o jsonpath='{.status.lifecycleState}' 2>/dev/null || true)"
  job_state="$(kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
  if [[ "${lifecycle}" == "FAILED" || "${job_state}" == "FAILED" ]]; then
    kubectl "${namespace_args[@]}" describe flinkdeployment "${deployment}" || true
    exit 1
  fi
  if kubectl "${namespace_args[@]}" get pods -l "app=${deployment},component=jobmanager" --no-headers 2>/dev/null | awk '{print $3}' | grep -q '^Running$'; then
    break
  fi
  sleep 5
done

kubectl "${namespace_args[@]}" get flinkdeployment "${deployment}"
kubectl "${namespace_args[@]}" get pods -l "app=${deployment}"
