#!/bin/bash
# Drives test_glibguard under glibguard and checks that each deliberate
# lock-discipline error is reported, and that the clean run reports none.
# Usage: run_selftest.sh <path-to-libglibguard.so> <path-to-test_glibguard>
set -u
lib=${1:?libglibguard.so}; prog=${2:?test_glibguard}
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
rc=0

check() { # <case> <expected-kind|none>
  local case=$1 want=$2 log=$tmp/$1.log
  # Some cases abort in GLib or deadlock on purpose; only the log matters.
  GLIBGUARD_LOG=$log LD_PRELOAD=$lib timeout 3 "$prog" "$case" >/dev/null 2>&1
  local n; n=$(sed -n 's/^glibguard: \([0-9]*\) finding.*/\1/p' "$log")
  if [ "$want" = none ]; then
    if [ "${n:-x}" = 0 ]; then echo "ok   $case: no findings"
    else echo "FAIL $case: expected none, got ${n:-?}"; sed 's/^/       /' "$log"; rc=1; fi
  else
    if grep -q "glibguard: $want " "$log"; then echo "ok   $case: reported $want"
    else echo "FAIL $case: $want not reported"; sed 's/^/       /' "$log"; rc=1; fi
  fi
}

check clean                none
check unlock-not-held      unlock-not-held
check unlock-wrong-thread  unlock-wrong-thread
check double-lock          double-lock
check lock-order-inversion lock-order-inversion
check destroy-while-held   destroy-while-held
exit $rc
