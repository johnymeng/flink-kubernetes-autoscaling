#!/usr/bin/env bash
# Run from an admitted Q20 bundle. Standard output is sampled broker metadata.
set -Eeuo pipefail
spec=execution-spec.json
run=$(jq -er .run "$spec")
namespace=$(jq -er .virtual_namespace "$spec")
start=$(jq -er .prefix_or_timed_release_ms "$spec")
cutoff=$(jq -er .run_deadline_ms "$spec")
[[ "$run" =~ ^live-[0-9a-f]{8}$ && "$namespace" == "q8-pr2-$run-scout" ]]
[[ "$(jq -er .query "$spec")" == q20_unique ]]
[[ "$start" =~ ^[0-9]+$ && "$cutoff" =~ ^[0-9]+$ && "$cutoff" -gt "$start" ]]
export KUBECONFIG="$PWD/virtual.conf"
k() { kubectl -n "$namespace" "$@"; }
test "$(k get namespace "$namespace" -o jsonpath='{.metadata.uid}')" = "$(cat scout/namespace-uid.txt)"
test "$(k get namespace "$namespace" -o jsonpath='{.metadata.labels.a4s-pr2-run}')" = "$run"
test "$(k get pod q20-kafka -o jsonpath='{.metadata.labels.a4s-pr2-run}')" = "$run"
test "$(k get pod q20-kafka -o jsonpath='{.spec.containers[0].image}')" = "$(jq -er .images.broker.ref release-receipt.json)"
before=$(date +%s%3N)
broker_clock=$(k exec q20-kafka -- date +%s%3N)
after=$(date +%s%3N)
jq -cn --argjson before "$before" --argjson broker "$broker_clock" --argjson after "$after" \
  '{controller_before_ms:$before,broker_ms:$broker,controller_after_ms:$after}' >scout/broker-clock.json
k exec -i q20-kafka -- sh -c 'umask 077; cat > /tmp/NativeRescaleKafkaOffsets.class' < NativeRescaleKafkaOffsets.class
k exec q20-kafka -- java -cp '/tmp:/opt/kafka/libs/*' NativeRescaleKafkaOffsets "$start" "$cutoff"
