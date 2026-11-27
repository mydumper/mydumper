#!/bin/bash
#
# Replication test: takes a backup of the primary with mydumper while data is
# being written, restores it on the replica with myloader, lets myloader
# configure and start the replication from the metadata ([source] section,
# mydumper --source-data) and checks that the replica ends up with the same
# data as the primary.
#
# Traffic (transactions that insert, update and delete in every table) is
# sent to the primary in these phases:
#   1. before the backup, for --traffic-before seconds
#   2. during the backup
#   3. during the import
#   4. after the import, for --traffic-after seconds, while the replica is
#      already applying the changes
# Then the traffic is stopped and the replica must catch up with the primary
# in --catchup-timeout seconds, or the test fails. At the end, every table must
# be the same on the primary and on the replica.
#
# It needs two different servers: the primary with the binary log enabled and
# the replica, both with different server_id. The replica must be able to
# connect to the primary with --source-host/--source-port (by default, the
# same address used by this script to connect to the primary).
#
# Usage:
#   test/test_replication.sh [--primary-host H] [--primary-port P]
#                            [--replica-host H] [--replica-port P]
#                            [--source-host H] [--source-port P]
#                            [--user U] [--password P] [--database DB]
#                            [--workdir DIR] [--threads N] [--rows N]
#                            [--seed-rows N] [--traffic-before SECONDS]
#                            [--traffic-after SECONDS]
#                            [--catchup-timeout SECONDS] [--keep]
#
# Defaults: primary on $MYSQL_HOST:$MYSQL_TCP_PORT (127.0.0.1:3306), replica
# on the same host and port 3307, user root without password, like
# test_mydumper.sh in the CI test executors. The password is only taken from
# --password: $MYSQL_PASSWORD is not used, as the CI executors define it
# without a value.

set -u

die()
{
  echo "ERROR: $*" >&2
  exit 1
}

log()
{
  echo "[$(date '+%H:%M:%S')] $*"
}

primary_host=${MYSQL_HOST:-127.0.0.1}
primary_port=${MYSQL_TCP_PORT:-3306}
unset replica_host replica_port source_host source_port
mysql_user=root
mysql_password=
database=mydumper_replication_test
workdir=/tmp/test_replication
threads=4
rows=500
seed_rows=32768
keep=0
# Seconds of traffic before the backup and after the import
traffic_before=5
traffic_after=5
# Seconds that the replica has to catch up with the primary once the traffic
# is stopped
catchup_timeout=120

opts=$(getopt -o "" --long "primary-host:,primary-port:,replica-host:,replica-port:,source-host:,source-port:,user:,password:,database:,workdir:,threads:,rows:,seed-rows:,traffic-before:,traffic-after:,catchup-timeout:,timeout:,keep" --name "$0" -- "$@") ||
  exit 1
eval set -- "$opts"
while true
do
  case "$1" in
    --primary-host) primary_host=$2; shift 2;;
    --primary-port) primary_port=$2; shift 2;;
    --replica-host) replica_host=$2; shift 2;;
    --replica-port) replica_port=$2; shift 2;;
    --source-host) source_host=$2; shift 2;;
    --source-port) source_port=$2; shift 2;;
    --user) mysql_user=$2; shift 2;;
    --password) mysql_password=$2; shift 2;;
    --database) database=$2; shift 2;;
    --workdir) workdir=$2; shift 2;;
    --threads) threads=$2; shift 2;;
    --rows) rows=$2; shift 2;;
    --seed-rows) seed_rows=$2; shift 2;;
    --traffic-before) traffic_before=$2; shift 2;;
    --traffic-after) traffic_after=$2; shift 2;;
    # --timeout: kept as an alias of --catchup-timeout
    --catchup-timeout|--timeout) catchup_timeout=$2; shift 2;;
    --keep) keep=1; shift;;
    --) shift; break;;
  esac
done
replica_host=${replica_host:-$primary_host}
replica_port=${replica_port:-3307}
source_host=${source_host:-$primary_host}
source_port=${source_port:-$primary_port}

if [ -x ./mydumper ] && [ -x ./myloader ]
then
  mydumper=./mydumper
  myloader=./myloader
else
  mydumper=$(command -v mydumper) || die "mydumper not found"
  myloader=$(command -v myloader) || die "myloader not found"
fi
mysql_exe=$(command -v mariadb) || mysql_exe=$(command -v mysql) ||
  die "mysql client not found"

# The clients take the password from the environment, so it is not shown in
# the process list nor in the logs. Without --password, the environment is
# not changed, like in test_mydumper.sh.
if [ -n "$mysql_password" ]
then
  export MYSQL_PWD=${mysql_password}
fi

mysql_primary()
{
  "$mysql_exe" --no-defaults --protocol=TCP -h "$primary_host" -P "$primary_port" -u "$mysql_user" "$@"
}

mysql_replica()
{
  "$mysql_exe" --no-defaults --protocol=TCP -h "$replica_host" -P "$replica_port" -u "$mysql_user" "$@"
}

dump_dir=${workdir}/data
mydumper_log=${workdir}/mydumper.log
myloader_log=${workdir}/myloader.log
writer_log=${workdir}/writer.log
writer_stop_file=${workdir}/writer.stop
unset writer_pid
success=0

################################################################################
# Server checks
################################################################################

mysql_primary -N -e "SELECT 1" >/dev/null ||
  die "Cannot connect to the primary on ${primary_host}:${primary_port}"
mysql_replica -N -e "SELECT 1" >/dev/null ||
  die "Cannot connect to the replica on ${replica_host}:${replica_port}"

primary_version=$(mysql_primary -N -e "SELECT VERSION()")
replica_version=$(mysql_replica -N -e "SELECT VERSION()")
log "Primary: ${primary_host}:${primary_port} ${primary_version}"
log "Replica: ${replica_host}:${replica_port} ${replica_version}"

if [[ "$primary_version" == *TiDB* ]] || [[ "$replica_version" == *TiDB* ]]
then
  log "SKIPPED: TiDB does not support MySQL replication"
  exit 0
fi

primary_server_id=$(mysql_primary -N -e "SELECT @@server_id")
replica_server_id=$(mysql_replica -N -e "SELECT @@server_id")
[ "$primary_server_id" != "$replica_server_id" ] ||
  die "Primary and replica have the same server_id (${primary_server_id}): use two different servers (--replica-host/--replica-port)"
[ "$(mysql_primary -N -e "SELECT @@log_bin")" = "1" ] ||
  die "The binary log is disabled on the primary"

# Replication statements of the replica: REPLICA since MySQL 8.0.22, SLAVE
# before it and on MariaDB, which keeps accepting it.
if [[ "$replica_version" == *MariaDB* ]]
then
  replica_keyword=SLAVE
else
  IFS=. read -r v_major v_minor v_patch <<< "${replica_version%%-*}"
  v_patch=${v_patch%%[^0-9]*}
  if (( v_major > 8 || (v_major == 8 && (v_minor > 0 || v_patch >= 22)) ))
  then
    replica_keyword=REPLICA
  else
    replica_keyword=SLAVE
  fi
fi

replica_status()
{
  mysql_replica -e "SHOW ${replica_keyword} STATUS\G"
}

# Value of a column of SHOW REPLICA STATUS, which has the old (Slave_,
# Master_) and the new (Replica_, Source_) names depending on the version
status_field()
{
  local status=$1
  shift
  local name
  for name in "$@"
  do
    echo "$status" | sed -n "s/^ *${name}: //p" | head -1 | grep . && return
  done
}

reset_replica()
{
  mysql_replica -e "STOP ${replica_keyword}; RESET ${replica_keyword} ALL;"
}

################################################################################
# Writer: keeps writing in the primary, in transactions that change every
# table, until the stop file is created. Only deterministic statements are
# used, so it is safe with any binlog_format.
################################################################################

writer()
{
  local i=0 r
  while [ ! -f "$writer_stop_file" ]
  do
    i=$(( i + 1 ))
    r=$RANDOM
    cat <<EOF
BEGIN;
INSERT INTO t_int (k, c) VALUES (${r}, 'writer-${i}');
UPDATE t_int SET k = k + 1, c = CONCAT('updated-', ${i}) WHERE id = $(( RANDOM % seed_rows + 1 ));
DELETE FROM t_int WHERE id = $(( RANDOM % seed_rows + 1 ));
INSERT INTO t_str (id, val, payload) VALUES ('w-${i}-${r}', ${r} % 1000, REPEAT('p', ${r} % 200));
UPDATE t_str SET val = val + 1 WHERE val = ${r} % 1000;
DELETE FROM t_str WHERE id = 's-$(( RANDOM % seed_rows ))';
INSERT INTO t_nopk (a, b) VALUES (${r}, 'writer-${i}');
DELETE FROM t_nopk WHERE a = $(( RANDOM % 5000 ));
UPDATE t_counter SET n = n + 1 WHERE id = 1;
COMMIT;
EOF
  done | mysql_primary --force "$database" >"$writer_log" 2>&1
}

start_writer()
{
  rm -f "$writer_stop_file"
  writer &
  writer_pid=$!
  log "Writer started (pid ${writer_pid})"
}

writer_alive()
{
  [ -n "${writer_pid:-}" ] && kill -0 "$writer_pid" 2>/dev/null
}

stop_writer()
{
  [ -n "${writer_pid:-}" ] || return 0
  touch "$writer_stop_file"
  local i
  for i in $(seq 60)
  do
    kill -0 "$writer_pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$writer_pid" 2>/dev/null
  then
    pkill -P "$writer_pid" 2>/dev/null
    kill "$writer_pid" 2>/dev/null
  fi
  wait "$writer_pid" 2>/dev/null
  unset writer_pid
  log "Writer stopped"
}

counter()
{
  "$@" -N -e "SELECT n FROM \`${database}\`.t_counter WHERE id = 1"
}

cleanup()
{
  stop_writer
  if (( success == 0 ))
  then
    echo "=== Replica status ==="
    replica_status
    for f in "$mydumper_log" "$myloader_log" "$writer_log"
    do
      if [ -s "$f" ]
      then
        echo "=== Last lines of $f ==="
        tail -30 "$f"
      fi
    done
    echo "=== [source] section of the metadata ==="
    [ -f "${dump_dir}/metadata" ] && sed -n '/^\[source\]/,/^\[/p' "${dump_dir}/metadata"
    echo "Test FAILED, files kept in ${workdir}"
  fi
  # Leave both servers as they were, so the next tests are not affected:
  # replication stopped and the test database dropped.
  reset_replica >/dev/null 2>&1
  mysql_replica -e "DROP DATABASE IF EXISTS \`${database}\`" 2>/dev/null
  mysql_primary -e "DROP DATABASE IF EXISTS \`${database}\`" 2>/dev/null
  if (( success == 1 )) && (( keep == 0 ))
  then
    rm -rf "$workdir"
  fi
}

################################################################################
# Test
################################################################################

rm -rf "$workdir"
mkdir -p "$workdir"
trap cleanup EXIT

reset_replica || die "Not able to reset the replication on the replica"
mysql_replica -e "DROP DATABASE IF EXISTS \`${database}\`" ||
  die "Not able to clean the replica"

log "Preparing ${database} on the primary"
mysql_primary <<EOF || die "Not able to prepare the primary"
DROP DATABASE IF EXISTS \`${database}\`;
CREATE DATABASE \`${database}\`;
USE \`${database}\`;
CREATE TABLE t_int (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  k INT NOT NULL,
  c VARCHAR(64) NOT NULL,
  KEY (k)
) ENGINE=InnoDB;
CREATE TABLE t_str (
  id VARCHAR(40) NOT NULL PRIMARY KEY,
  val INT NOT NULL,
  payload TEXT,
  KEY (val)
) ENGINE=InnoDB;
CREATE TABLE t_nopk (
  a INT NOT NULL,
  b VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE t_counter (
  id INT NOT NULL PRIMARY KEY,
  n BIGINT NOT NULL
) ENGINE=InnoDB;
INSERT INTO t_counter VALUES (1, 0);
EOF

# Seeding: one multi-row INSERT per table and batch of rows
seed()
{
  local i batch=1000
  for (( i = 0; i < seed_rows; i += batch ))
  do
    local j end=$(( i + batch < seed_rows ? i + batch : seed_rows ))
    local int_values="" str_values="" nopk_values=""
    for (( j = i; j < end; j++ ))
    do
      int_values+="${int_values:+,}(${j} % 977, 'seed-${j}')"
      str_values+="${str_values:+,}('s-${j}', ${j} % 1000, REPEAT('s', ${j} % 200))"
      (( j < 5000 )) && nopk_values+="${nopk_values:+,}(${j}, 'seed-${j}')"
    done
    echo "INSERT INTO t_int (k, c) VALUES ${int_values};"
    echo "INSERT INTO t_str (id, val, payload) VALUES ${str_values};"
    [ -n "$nopk_values" ] && echo "INSERT INTO t_nopk (a, b) VALUES ${nopk_values};"
  done
}
seed | mysql_primary "$database" || die "Not able to seed the primary"
log "Seeded ${seed_rows} rows per table"

# Transactions committed by the writer, read from the primary
traffic()
{
  counter mysql_primary
}

# Fails when the writer stopped by itself or when it did not commit any
# transaction in a phase
check_traffic()
{
  local phase=$1 from=$2 to=$3
  writer_alive || die "The writer stopped ${phase}, see ${writer_log}"
  (( to > from )) || die "No transaction was committed on the primary ${phase}"
  log "  $(( to - from )) transactions ${phase}"
}

log "Phase 1: traffic before the backup (${traffic_before} seconds)"
start_writer
sleep "$traffic_before"
traffic_before_backup=$(traffic)
check_traffic "before the backup" 0 "$traffic_before_backup"

log "Phase 2: backup with traffic"
password_arg=()
[ -n "$mysql_password" ] && password_arg=(--password "$mysql_password")
"$mydumper" --host "$primary_host" --port "$primary_port" --user "$mysql_user" "${password_arg[@]}" \
  --database "$database" --outputdir "$dump_dir" --threads "$threads" --rows "$rows" \
  --source-data=7 --verbose 3 --logfile "$mydumper_log" ||
  die "mydumper failed, see ${mydumper_log}"
traffic_after_backup=$(traffic)
# The backup must overlap with the traffic: this is what checks that the
# binlog coordinates in the metadata match the snapshot of the backup
check_traffic "during the backup" "$traffic_before_backup" "$traffic_after_backup"

# The connection to the primary is written commented out in the metadata
# file, as it must be provided by the user
metadata=${dump_dir}/metadata
grep -q '^\[source\]' "$metadata" ||
  die "The metadata has no [source] section"
grep -Eq '^SOURCE_LOG_FILE = ".+"' "$metadata" && grep -Eq '^SOURCE_LOG_POS = [0-9]+' "$metadata" ||
  die "The [source] section has no binlog coordinates"
sed -i -e '/^\[source\]/,/^\[/{
  s|^#SOURCE_HOST = .*|SOURCE_HOST = "'"${source_host}"'"|
  s|^#SOURCE_PORT = .*|SOURCE_PORT = '"${source_port}"'|
  s|^#SOURCE_USER = .*|SOURCE_USER = "'"${mysql_user}"'"|
  s|^#SOURCE_PASSWORD = .*|SOURCE_PASSWORD = "'"${mysql_password}"'"|
}' "$metadata"
grep -q "^SOURCE_HOST = \"${source_host}\"" "$metadata" ||
  die "Not able to set the source connection in the metadata"
log "Replication will start from $(sed -n 's/^SOURCE_LOG_FILE = //p' "$metadata"):$(sed -n 's/^SOURCE_LOG_POS = //p' "$metadata") on ${source_host}:${source_port}"

log "Phase 3: import with traffic"
"$myloader" --host "$replica_host" --port "$replica_port" --user "$mysql_user" "${password_arg[@]}" \
  --directory "$dump_dir" --threads "$threads" --verbose 3 --logfile "$myloader_log" ||
  die "myloader failed, see ${myloader_log}"
grep -q "Sending start replica" "$myloader_log" ||
  die "myloader did not start the replication, see ${myloader_log}"
traffic_after_import=$(traffic)
# Every change after the backup reaches the replica through the replication,
# no matter if it was made during or after the import, so a fast import
# without traffic is not an error
writer_alive || die "The writer stopped during the import, see ${writer_log}"
if (( traffic_after_import > traffic_after_backup ))
then
  log "  $(( traffic_after_import - traffic_after_backup )) transactions during the import"
else
  log "  WARNING: no transaction was committed during the import, it was too fast (try a larger --seed-rows)"
fi

log "Phase 4: traffic after the import, while replicating (${traffic_after} seconds)"
sleep "$traffic_after"
traffic_final=$(traffic)
check_traffic "after the import" "$traffic_after_import" "$traffic_final"

stop_writer
traffic_final=$(traffic)
log "Traffic stopped: ${traffic_final} transactions in total"

log "Waiting up to ${catchup_timeout} seconds for the replica to catch up"
caught_up=0
catchup_start=$SECONDS
catchup_deadline=$(( SECONDS + catchup_timeout ))
while (( SECONDS <= catchup_deadline ))
do
  status=$(replica_status)
  io_running=$(status_field "$status" Replica_IO_Running Slave_IO_Running)
  sql_running=$(status_field "$status" Replica_SQL_Running Slave_SQL_Running)
  if [ "$io_running" != "Yes" ] || [ "$sql_running" != "Yes" ]
  then
    # The IO thread stays Connecting for a while when the source is not
    # reachable yet: it is only an error when it gives up or when the SQL
    # thread stopped
    last_error=$(status_field "$status" Last_SQL_Error Last_IO_Error)
    if [ "$sql_running" != "Yes" ] || [ "$io_running" = "No" ]
    then
      die "Replication is not running (IO: ${io_running:-none}, SQL: ${sql_running:-none}): ${last_error:-no error reported}"
    fi
  else
    traffic_on_replica=$(counter mysql_replica 2>/dev/null)
    if [ "$traffic_on_replica" = "$traffic_final" ]
    then
      caught_up=1
      break
    fi
  fi
  sleep 1
done
if (( caught_up == 0 ))
then
  last_error=$(status_field "$status" Last_IO_Error Last_SQL_Error)
  die "The replica did not catch up in ${catchup_timeout} seconds (IO: ${io_running:-none}, SQL: ${sql_running:-none}, transactions applied on the replica: $(counter mysql_replica 2>/dev/null) of ${traffic_final}, $(status_field "$status" Seconds_Behind_Source Seconds_Behind_Master) seconds behind)${last_error:+: ${last_error}}"
fi
log "Replica caught up in $(( SECONDS - catchup_start )) seconds: Replica_IO_Running=${io_running} Replica_SQL_Running=${sql_running}"

log "Comparing primary and replica"
differences=0
for table in t_int t_str t_nopk t_counter
do
  query="SELECT COUNT(*) FROM \`${database}\`.\`${table}\`; CHECKSUM TABLE \`${database}\`.\`${table}\`"
  on_primary=$(mysql_primary -N -e "$query" | awk '{print $NF}' | paste -sd'/')
  on_replica=$(mysql_replica -N -e "$query" | awk '{print $NF}' | paste -sd'/')
  if [ "$on_primary" = "$on_replica" ]
  then
    log "  ${table}: rows/checksum ${on_primary} OK"
  else
    log "  ${table}: primary ${on_primary} != replica ${on_replica}"
    differences=$(( differences + 1 ))
  fi
done
(( differences == 0 )) || die "${differences} table(s) differ between primary and replica"

success=1
log "Replication test finished successfully!"
