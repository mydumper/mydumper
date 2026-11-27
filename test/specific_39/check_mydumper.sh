#!/usr/bin/env bash
set -eu

if [ "$1" -ne 0 ]; then
  exit "$1"
fi

schema=/tmp/data/specific_39.uuid_test-schema.sql
data=(/tmp/data/specific_39.uuid_test.*.sql)
test -f "$schema"
test -f "${data[0]}"
if grep -Eiq '`id` uuid([ ,]|$)' "$schema"; then
  grep -q '_utf8mb4"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"' "${data[@]}"
  grep -q '_utf8mb4"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"' "${data[@]}"
  grep -q '_utf8mb4"cccccccc-cccc-4ccc-8ccc-cccccccccccc"' "${data[@]}"
fi
if grep -Eiq '`ip4` inet4([ ,]|$)' "$schema"; then
  grep -q '_utf8mb4"192.0.2.1"' "${data[@]}"
fi
if grep -Eiq '`ip6` inet6([ ,]|$)' "$schema"; then
  grep -q '_utf8mb4"2001:db8::1"' "${data[@]}"
fi
grep -qi '0x62626262626262622d626262622d346262622d386262622d626262626262626262626262,0x0041ff' "${data[@]}"
