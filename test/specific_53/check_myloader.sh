#!/usr/bin/env bash
# --skip-indexes must not make myloader fail the index checksum verification (issue #2292).
[ "${1:-0}" -eq 0 ] || exit "$1"
set -eu
if grep -q 'Schema index checksum mismatch' /tmp/test_myloader.log.tmp 2>/dev/null; then
  echo 'specific_53: index checksum was verified although --skip-indexes was used' >&2
  exit 1
fi
