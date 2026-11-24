#!/usr/bin/env bash
# --hex-blob must hex encode only binary columns (BLOB), never TEXT.
# Dump should have exactly 1 hex literal per row (a_blob) and text kept readable.
[ "${1:-0}" -eq 0 ] || exit "$1"
f=$(ls /tmp/data/specific_54.t.*.sql 2>/dev/null | head -n1)
[ -n "$f" ] || { echo "dump file not found" >&2; exit 1; }
hex=$(grep -o ",0x[0-9A-Fa-f]*" "$f" | wc -l)
[ "$hex" -eq 2 ] || { echo "expected 2 hex literals (BLOB only), got $hex" >&2; exit 1; }
grep -q "plain latin1 text" "$f" || { echo "latin1 TEXT was hex encoded" >&2; exit 1; }
grep -q "second utf8 text" "$f" || { echo "utf8mb4 TEXT was hex encoded" >&2; exit 1; }
exit 0
