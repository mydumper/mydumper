#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

LOG=/tmp/test_mydumper.log.tmp

fail() { echo "specific_47: $*" >&2; exit 1; }
sent() { grep -qF "$1: OK" "$LOG"; }

ls /tmp/data/specific_47.t_innodb.*sql >/dev/null 2>&1 || fail "t_innodb was not dumped"
ls /tmp/data/specific_47.t_myisam.*sql >/dev/null 2>&1 || fail "t_myisam was not dumped"

percona57=$(mysql -N -e "SELECT @@version_comment LIKE '%Percona%' AND @@version LIKE '5.7.%'")

log_bin=$(mysql -N -e "SELECT @@log_bin")
gtid_on=$(mysql -N -e "SELECT @@gtid_mode = 'ON'")

if [ "$percona57" -eq 1 ]
then
  # SNAP_CLONE is used by AUTO when log_bin=OFF or gtid_mode=ON
  if [ "$log_bin" -eq 1 ] && [ "$gtid_on" -eq 0 ]
  then
    sent "FLUSH TABLES WITH READ LOCK" || fail "FLUSH TABLES WITH READ LOCK was not sent"
  else
    grep -qF "STWCS" "$LOG" || fail "SNAP_CLONE was not used"
  fi
  ! grep -qF "LOCK TABLES FOR BACKUP" "$LOG" || fail "LOCK TABLES FOR BACKUP was sent"
  ! grep -qF "LOCK BINLOG FOR BACKUP" "$LOG" || fail "LOCK BINLOG FOR BACKUP was sent"
fi

exit 0
