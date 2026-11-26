#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <Q8 execution spec> <Justin response with include-applied-profile=true>" >&2
    exit 2
fi

if ! jq -e --slurpfile spec "$1" '
    ($spec[0].vertices | [.person, .auction, .join] | sort) as $vertices
    | (($vertices | length) == 3 and ($vertices | unique | length) == 3)
      and (keys == $vertices)
      and all(.[]; .appliedResourceProfile as $profile
          | ($profile.cpuCores.value | type == "number" and . > 0)
            and ($profile.managedMemory.bytes | type == "number" and . >= 0)
            and ($profile.extendedResources == {}))
' "$2" >/dev/null; then
    echo "Q8 startup requires three confirmed applied slot profiles before input release." >&2
    exit 1
fi
