#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

fail() { echo "specific_39: $*" >&2; exit 1; }

data_type=$(mysql -N -e "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='specific_39_restore' AND TABLE_NAME='uuid_test' AND COLUMN_NAME='id'")
if [[ -z "$data_type" ]]; then
  fail "restored UUID test column is missing"
fi
source_type=$(cat /tmp/data/specific_39.source-column-type)
if [[ "$source_type" == uuid ]]; then
  [[ "${data_type,,}" == uuid ]] || fail "restored column type is ${data_type}, expected UUID"
else
  [[ "${data_type,,}" == char ]] || fail "restored fallback column type is ${data_type}, expected CHAR"
fi

rows=$(mysql -N -e "SELECT CONCAT(id, ':', val) FROM specific_39_restore.uuid_test ORDER BY val" | paste -sd ',' -)
expected='00000000-0000-0000-0000-000000000001:row-1,11111111-1111-4111-8111-111111111111:row-2,abcdefab-cdef-4abc-8def-abcdefabcdef:row-3'
[[ "$rows" == "$expected" ]] || fail "restored rows differ: $rows"
if [[ "$source_type" != uuid ]]; then
  echo "specific_39: native UUID unsupported; CHAR fallback round trip passed (${data_type})"
fi

exit 0
