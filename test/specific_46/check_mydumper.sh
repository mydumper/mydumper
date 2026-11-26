#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

LOG=/tmp/test_mydumper.log.tmp

fail() { echo "specific_46: $*" >&2; exit 1; }
sent() { grep -qF "$1: OK" "$LOG"; }

ls /tmp/data/specific_46.t_innodb.*sql >/dev/null 2>&1 || fail "t_innodb was not dumped"
ls /tmp/data/specific_46.t_myisam.*sql >/dev/null 2>&1 || fail "t_myisam was not dumped"

percona57=$(mysql -N -e "SELECT @@version_comment LIKE '%Percona%' AND @@version LIKE '5.7.%'")
log_bin=$(mysql -N -e "SELECT @@log_bin")
gtid_on=$(mysql -N -e "SELECT @@gtid_mode = 'ON'")

# AUTO uses SNAP_CLONE on Percona Server when the binlog position can be
# trusted (log_bin=OFF or gtid_mode=ON). Otherwise, on Percona Server 5.7 it
# uses backup locks instead of FLUSH TABLES WITH READ LOCK.
if [ "$percona57" -eq 1 ] && [ "$log_bin" -eq 1 ] && [ "$gtid_on" -eq 0 ]
then
  sent "LOCK TABLES FOR BACKUP" || fail "LOCK TABLES FOR BACKUP was not sent"
  sent "LOCK BINLOG FOR BACKUP" || fail "LOCK BINLOG FOR BACKUP was not sent"
  sent "UNLOCK BINLOG"          || fail "UNLOCK BINLOG was not sent"
  ! grep -qF "FLUSH TABLES WITH READ LOCK" "$LOG" || fail "FLUSH TABLES WITH READ LOCK was sent"
  ! grep -qF "STWCS" "$LOG" || fail "SNAP_CLONE was used with log_bin=ON and gtid_mode=OFF"
elif [ "$percona57" -eq 1 ]
then
  grep -qF "STWCS" "$LOG" || fail "SNAP_CLONE was not used"
  sent "LOCK TABLES FOR BACKUP" || fail "LOCK TABLES FOR BACKUP was not sent"
  ! grep -qF "FLUSH TABLES WITH READ LOCK" "$LOG" || fail "FLUSH TABLES WITH READ LOCK was sent"
  if [ "$log_bin" -eq 0 ]
  then
    ! grep -qF "LOCK BINLOG FOR BACKUP" "$LOG" || fail "LOCK BINLOG FOR BACKUP was sent with log_bin=OFF"
  fi
fi

exit 0
