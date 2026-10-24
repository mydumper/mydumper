#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

fail() { echo "specific_44: $*" >&2; exit 1; }

rows=$(mysql -N -e "SELECT COUNT(*), MIN(val) FROM specific_44.t1" | tr '\t' ' ')
[ "$rows" == "1 db1" ] || fail "specific_44.t1 has: ${rows}"

rows=$(mysql -N -e "SELECT COUNT(*), MIN(val) FROM specific_44.t2" | tr '\t' ' ')
[ "$rows" == "2 db2" ] || fail "specific_44.t2 has: ${rows}"

sources=$(mysql -N -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'specific\_44\_db%'")
[ "$sources" -eq 0 ] || fail "source databases were created"

exit 0
