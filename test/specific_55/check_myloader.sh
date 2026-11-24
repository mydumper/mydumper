#!/usr/bin/env bash
# Issue #1925: a deadlock (ERROR 1213) retried by myloader must either restore
# every row and exit 0, or never be reported as a success.
# pre_myloader.sh forces the deadlock on an INSERT in the middle of a
# transaction; InnoDB rolls back the whole transaction, not only that INSERT.
error=${1:-0}
dir=/tmp/specific_55
log=/tmp/test_myloader.log.tmp
expected=100

type mysql_replica >/dev/null 2>&1 || mysql_replica() { mysql "$@"; }

for _ in $(seq 1 100); do
  grep -q 'helper finished' "$dir/status" 2>/dev/null && break
  sleep 0.1
done

if ! grep -q 'myloader waiting' "$dir/status" 2>/dev/null; then
  echo "specific_55: the deadlock was not triggered" >&2
  cat "$dir/status" "$dir/helper.out" >&2 2>/dev/null
  exit 1
fi
if [ -f "$log" ] && ! grep -q 'ERROR 1213' "$log"; then
  echo "specific_55: myloader did not report the deadlock" >&2
  exit 1
fi

rows=$(mysql_replica --batch --skip-column-names -e "SELECT COUNT(*) FROM specific_55.t")
if [ "$rows" != "$expected" ]; then
  if [ "$error" -eq 0 ]; then
    echo "specific_55: myloader exited 0 but restored $rows of $expected rows (rows rolled back by the deadlock were not replayed)" >&2
    exit 1
  fi
  echo "specific_55: restored $rows of $expected rows, myloader exited $error" >&2
  exit "$error"
fi
if [ "$error" -ne 0 ]; then
  echo "specific_55: all $expected rows restored after the deadlock retry but myloader exited $error" >&2
  exit "$error"
fi
exit 0
