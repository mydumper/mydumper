#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

LOG=/tmp/test_mydumper.log.tmp

fail() { echo "specific_46: $*" >&2; exit 1; }
sent() { grep -qF "$1: OK" "$LOG"; }

ls /tmp/data/specific_46.t_innodb.*sql >/dev/null 2>&1 || fail "t_innodb was not dumped"
ls /tmp/data/specific_46.t_myisam.*sql >/dev/null 2>&1 || fail "t_myisam was not dumped"

percona57=$(mysql -N -e "SELECT @@version_comment LIKE '%Percona%' AND @@version LIKE '5.7.%'")
log_bin=$(mysql -N -e "SELECT @@log_bin")

# Backup locks only exist on Percona Server 5.7, and LOCK BINLOG FOR BACKUP
# only blocks commits that are written to the binary log.
if [ "$percona57" -eq 1 ] && [ "$log_bin" -eq 1 ]
then
  sent "LOCK TABLES FOR BACKUP" || fail "LOCK TABLES FOR BACKUP was not sent"
  sent "LOCK BINLOG FOR BACKUP" || fail "LOCK BINLOG FOR BACKUP was not sent"
  sent "UNLOCK BINLOG"          || fail "UNLOCK BINLOG was not sent"
  ! grep -qF "FLUSH TABLES WITH READ LOCK" "$LOG" || fail "FLUSH TABLES WITH READ LOCK was sent"
elif [ "$percona57" -eq 1 ]
then
  sent "FLUSH TABLES WITH READ LOCK" || fail "FLUSH TABLES WITH READ LOCK was not sent with log_bin=OFF"
  ! grep -qF "LOCK BINLOG FOR BACKUP" "$LOG" || fail "LOCK BINLOG FOR BACKUP was sent with log_bin=OFF"
fi

exit 0
