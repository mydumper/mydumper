#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

fail() { echo "specific_43: $*" >&2; exit 1; }

rows=$(mysql -N -e "SELECT COUNT(*), MIN(val) FROM specific_43_r1.table1" | tr '\t' ' ')
[ "$rows" == "1 db1" ] || fail "specific_43_r1.table1 has: ${rows}"

rows=$(mysql -N -e "SELECT COUNT(*), MIN(val) FROM specific_43_db2.table1" | tr '\t' ' ')
[ "$rows" == "2 db2" ] || fail "specific_43_db2.table1 has: ${rows}"

# specific_43_db2 is not renamed, so it must be created from its schema-create file
charset=$(mysql -N -e "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='specific_43_db2'")
[ "$charset" == "latin1" ] || fail "specific_43_db2 was created with charset: ${charset}"

exists=$(mysql -N -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='specific_43_db1'")
[ "$exists" -eq 0 ] || fail "specific_43_db1 was created"

exit 0
