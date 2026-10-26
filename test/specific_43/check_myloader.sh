#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

fail() { echo "specific_43: $*" >&2; exit 1; }

for n in 1 2 3
do
  rows=$(mysql -N -e "SELECT COUNT(*), MIN(val), MAX(val) FROM specific_43_r${n}.table1" | tr '\t' ' ')
  [ "$rows" == "${n} db${n} db${n}" ] || fail "specific_43_r${n}.table1 has: ${rows}"
done

# The source databases are renamed, so they must not be created ...
sources=$(mysql -N -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'specific\_43\_db%'")
[ "$sources" -eq 0 ] || fail "source databases were created"

# ... and drop-database must have been executed before restoring the tables.
leftover=$(mysql -N -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='specific_43_r1' AND TABLE_NAME='must_be_dropped'")
[ "$leftover" -eq 0 ] || fail "specific_43_r1 was not dropped"

exit 0
