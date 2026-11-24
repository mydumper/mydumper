#!/usr/bin/env bash
# Index names and columns are identical; uniqueness and prefix length must matter.
[ "${1:-0}" -eq 0 ] || exit "$1"
set -eu
checksum() {
  local value
  value=$(awk -v section="[\`specific_52\`.\`$1\`]" '
    $0 == section { found=1; next }
    /^\[/ { found=0 }
    found && $1 == "indexes_checksum" { print $3 }
  ' /tmp/data/metadata)
  [ -n "$value" ] || { echo "Missing index checksum: $1" >&2; exit 1; }
  printf '%s' "$value"
}
plain=$(checksum plain_index)
[ "$plain" = "$(checksum plain_copy)" ] || { echo 'Identical indexes differ' >&2; exit 1; }
seen=''
failed=0
for table in plain_index unique_index prefix_10 prefix_20 unique_prefix; do
  value=$(checksum "$table")
  case " $seen " in
    *" $value "*) echo "Index definitions collide: $table" >&2; failed=1 ;;
  esac
  seen="$seen $value"
done
[ "$(checksum composite_full)" != "$(checksum composite_prefix)" ] || {
  echo 'Composite index prefix was ignored' >&2; failed=1;
}
exit "$failed"
