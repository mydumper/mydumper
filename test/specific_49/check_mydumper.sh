#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

LOG=/tmp/test_mydumper.log.tmp

fail() { echo "specific_49: $*" >&2; exit 1; }
sent() { grep -qF "$1: OK" "$LOG"; }

ls /tmp/data/specific_49.t_innodb.*sql >/dev/null 2>&1 || fail "t_innodb was not dumped"
ls /tmp/data/specific_49.t_myisam.*sql >/dev/null 2>&1 || fail "t_myisam was not dumped"

grep -qF "setting --skip-ddl-locks" "$LOG" || fail "mydumper did not report that it is setting --skip-ddl-locks"

for q in "FLUSH TABLES WITH READ LOCK" "LOCK TABLES FOR BACKUP" "LOCK BINLOG FOR BACKUP" "LOCK INSTANCE FOR BACKUP" "BACKUP STAGE"
do
  ! grep -qF "$q" "$LOG" || fail "$q was sent"
done

exit 0
