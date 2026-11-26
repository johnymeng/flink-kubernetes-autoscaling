#!/usr/bin/env bash
set -euo pipefail

api=${1:-sources/a4s/a4s-api/target/a4s-api-0.1.0.jar}
core=${2:-sources/a4s/a4s-core/target/a4s-core-0.1.0.jar}
for jar in "$api" "$core"; do
  if [[ ! -f "$jar" ]]; then
    printf 'A4S archive missing: %s\n' "$jar" >&2
    exit 1
  fi
done

dependencies=$(jdeps --class-path "$api" -verbose:class -filter:none "$api" "$core")
status=0
rg \
    -e '-> (java\.net\.|javax\.net\.|java\.rmi\.|javax\.naming\.|java\.nio\.channels\.)' \
    -e '-> (java\.nio\.file\.|java\.io\.(File|RandomAccessFile)|java\.lang\.(Process|Runtime\s))' \
    -e '-> (org\.apache\.flink\.|org\.rocksdb\.|io\.kubernetes\.|io\.fabric8\.)' \
    -e '\snot found\s*$' <<< "$dependencies" || status=$?
case "$status" in
  0) printf 'A4S API or core has forbidden or unresolved bytecode dependencies.\n' >&2; exit 1 ;;
  1) exit 0 ;; # No forbidden dependencies matched.
  *) exit "$status" ;;
esac
