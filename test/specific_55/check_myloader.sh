#!/usr/bin/env bash
# myloader -s src -B dst must create dst with the collation of src, not the server default (issue #2341).
[ "${1:-0}" -eq 0 ] || exit "$1"

fail() { echo "specific_55: $*" >&2; exit 1; }

collation=$(mysql_replica -N -e "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='specific_55_dst'")
[ "$collation" == "latin1_german1_ci" ] || fail "specific_55_dst was created with collation: ${collation}"

rows=$(mysql_replica -N -e "SELECT COUNT(*) FROM specific_55_dst.t")
[ "$rows" -eq 1 ] || fail "specific_55_dst.t has ${rows} rows"

exit 0
