#!/usr/bin/env bash
# Issue #1925: force an InnoDB deadlock (ERROR 1213) on a myloader INSERT that
# is not the first statement of its transaction (queries-per-transaction > 1).
#
# A helper session holds the lock on row id=50, which myloader inserts in a
# later INSERT of the same transaction. Once myloader is waiting for that lock
# the helper asks for row id=1, which myloader inserted earlier in the
# transaction. InnoDB detects the deadlock and rolls back the lighter
# transaction (myloader's, the helper updated 999 rows first). The helper then
# rolls back so myloader's retry can go through.
#
# check_myloader.sh verifies the outcome.

type mysql_replica >/dev/null 2>&1 || mysql_replica() { mysql "$@"; }

dir=/tmp/specific_55
rm -rf "$dir"
mkdir -p "$dir"
mkfifo "$dir/fifo"

(
  # Keep the helper connection open: commands are fed through the fifo.
  mysql_replica --batch --skip-column-names --unbuffered < "$dir/fifo" > "$dir/helper.out" 2>&1 &
  client=$!
  exec 3> "$dir/fifo"
  echo "SELECT CONNECTION_ID();
BEGIN;
UPDATE specific_55_helper.weight SET v = v + 1;
INSERT INTO specific_55.t VALUES (50, 'helper');
SELECT 'locked';" >&3

  for _ in $(seq 1 300); do
    grep -q locked "$dir/helper.out" 2>/dev/null && break
    sleep 0.1
  done
  helper_id=$(head -n1 "$dir/helper.out")

  # INNODB_TRX is a cache refreshed every ~0.1s and may still list a lock wait
  # from a previous run: only a connection opened after the helper's is myloader.
  wait_query="SELECT trx_mysql_thread_id, trx_rows_modified, LEFT(trx_query, 60)
    FROM information_schema.INNODB_TRX
    WHERE trx_state = 'LOCK WAIT' AND trx_mysql_thread_id > ${helper_id:-0}"
  waiting=0
  for _ in $(seq 1 600); do
    w=$(mysql_replica --batch --skip-column-names -e "$wait_query" 2>/dev/null)
    if [ -n "$w" ]; then waiting=1; break; fi
    sleep 0.1
  done

  if [ "$waiting" -eq 1 ]; then
    echo "myloader waiting" >> "$dir/status"
    echo "$w" >> "$dir/status"
    # Closes the deadlock cycle: myloader holds id=1 and waits for id=50.
    echo "SELECT id FROM specific_55.t WHERE id = 1 FOR UPDATE;" >&3
  else
    echo "myloader never waited for the helper lock" >> "$dir/status"
  fi
  echo "ROLLBACK;
SELECT 'released';" >&3
  exec 3>&-
  wait "$client"
  echo "helper finished" >> "$dir/status"
) > /dev/null 2>&1 &

# Do not start myloader before the helper holds the lock on id=50.
for _ in $(seq 1 300); do
  grep -q locked "$dir/helper.out" 2>/dev/null && exit 0
  sleep 0.1
done
echo "specific_55: helper could not lock row id=50" >&2
cat "$dir/helper.out" >&2
exit 1
