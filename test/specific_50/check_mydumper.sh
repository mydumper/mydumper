#!/usr/bin/env bash
# JSON aliases must match native MySQL JSON without reclassifying ordinary LONGTEXT.
[ "${1:-0}" -eq 0 ] || exit "$1"
set -eu
check() {
  actual=$(awk -v section="[\`specific_50\`.\`$1\`]" '
    $0 == section { found=1; next }
    /^\[/ { found=0 }
    found && $1 == "schema_checksum" { print $3 }
  ' /tmp/data/metadata)
  [ "$actual" = "$2" ] || { echo "$1: expected $2, got $actual" >&2; exit 1; }
}
check native_json 6d211d98
check quoted_json 981b1993
check plain_text 48401840
check checked_other 109450c2
check permissive_check 48401840
