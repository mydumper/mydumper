#!/usr/bin/env bash

schema_file=/tmp/data/specific_39.uuid_test-schema.sql

if [ ! -f "$schema_file" ]
then
  exit 1
fi

if ! grep -qi 'UUID' "$schema_file"
then
  exit 0
fi

if grep -qi '_binary' /tmp/data/specific_39.uuid_test.*.sql
then
  exit 1
fi

exit 0
