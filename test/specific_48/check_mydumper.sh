#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

LOG=/tmp/test_mydumper.log.tmp

fail() { echo "specific_48: $*" >&2; exit 1; }
sent() { grep -qF "$1: OK" "$LOG"; }

ls /tmp/data/specific_48.t_innodb.*sql >/dev/null 2>&1 || fail "t_innodb was not dumped"
ls /tmp/data/specific_48.t_myisam.*sql >/dev/null 2>&1 || fail "t_myisam was not dumped"

percona57=$(mysql -N -e "SELECT @@version_comment LIKE '%Percona%' AND @@version LIKE '5.7.%'")

if [ "$percona57" -eq 1 ]
then
  sent "FLUSH TABLES WITH READ LOCK" || fail "FLUSH TABLES WITH READ LOCK was not sent"
  ! grep -qF "LOCK TABLES FOR BACKUP" "$LOG" || fail "LOCK TABLES FOR BACKUP was sent"
  ! grep -qF "LOCK BINLOG FOR BACKUP" "$LOG" || fail "LOCK BINLOG FOR BACKUP was sent"
fi

! grep -qF "LOCK INSTANCE FOR BACKUP" "$LOG" || fail "LOCK INSTANCE FOR BACKUP was sent"
! grep -qF "BACKUP STAGE" "$LOG"             || fail "BACKUP STAGE was sent"

exit 0
