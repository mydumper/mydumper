#!/usr/bin/env bash

schema_file=/tmp/data/specific_39.uuid_test-schema.sql
expected='00000000-0000-0000-0000-000000000001:row-1,11111111-1111-4111-8111-111111111111:row-2,abcdefab-cdef-4abc-8def-abcdefabcdef:row-3'

data_type=$(mysql -N -e "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='specific_39' AND TABLE_NAME='uuid_test' AND COLUMN_NAME='id'")
printf '%s\n' "${data_type,,}" > /tmp/data/specific_39.source-column-type
rows=$(mysql -N -e "SELECT CONCAT(id, ':', val) FROM specific_39.uuid_test ORDER BY val" | paste -sd ',' -)
if [[ "$rows" != "$expected" ]]; then
  echo "specific_39: source UUID data differs: $rows" >&2
  exit 1
fi

if [ ! -f "$schema_file" ]
then
  echo "specific_39: dump schema file missing: $schema_file" >&2
  exit 1
fi

if [[ "${data_type,,}" == uuid ]]; then
  if ! grep -Eqi '(^|[[:space:]`])id[[:space:]`]+UUID([[:space:]]|$)' "$schema_file"; then
    echo "specific_39: dump did not preserve native UUID column type" >&2
    exit 1
  fi
  if grep -qi '_binary' /tmp/data/specific_39.uuid_test.*.sql; then
    echo "specific_39: UUID INSERT still has _binary introducer" >&2
    exit 1
  fi
else
  echo "specific_39: native UUID unsupported; verifying CHAR fallback (${data_type:-missing})"
fi

dump_rows=$(grep -hE "row-[123]" /tmp/data/specific_39.uuid_test.*.sql | tr '\n' ' ')
for uuid in 00000000-0000-0000-0000-000000000001 11111111-1111-4111-8111-111111111111 abcdefab-cdef-4abc-8def-abcdefabcdef; do
  if [[ "$dump_rows" != *"$uuid"* ]]; then
    echo "specific_39: dump is missing UUID $uuid" >&2
    exit 1
  fi
done

exit 0
