#!/usr/bin/env bash
[ "${1:-0}" -eq 0 ] || exit "$1"
set -eu
checksum() {
  awk -v section="[\`$1\`.\`v\`]" '
    $0 == section { found=1; next }
    /^\[/ { found=0 }
    found && $1 == "schema_checksum" { print $3 }
  ' /tmp/data/metadata
}
source_checksum=$(checksum specific_51_source)
target_checksum=$(checksum specific_51_target)
[ -n "$source_checksum" ] && [ "$source_checksum" = "$target_checksum" ] || {
  echo 'Equivalent views in different databases must have equal checksums without changing literals' >&2
  exit 1
}
