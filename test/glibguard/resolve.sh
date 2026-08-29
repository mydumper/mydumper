#!/bin/bash
# Turn glibguard's "module+0xoffset" locations into file:line.
# Usage: resolve.sh <glibguard-log> [binary ...]
set -u
log=${1:?log}; shift
declare -A bin
for b in "$@"; do bin[$(basename "$b")]=$b; done
while IFS= read -r line; do
  out=$line
  for tok in $(grep -oE '[A-Za-z0-9_.+-]+\+0x[0-9a-f]+' <<<"$line" | sort -u); do
    mod=${tok%%+*}; off=${tok##*+}
    [ -n "${bin[$mod]:-}" ] || continue
    loc=$(addr2line -f -e "${bin[$mod]}" "$off" 2>/dev/null | paste -sd' ')
    [ -n "$loc" ] && out=${out//$tok/$tok [$loc]}
  done
  printf '%s\n' "$out"
done < "$log"
