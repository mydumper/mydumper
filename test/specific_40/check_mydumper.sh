#!/usr/bin/env bash

LOG=/tmp/test_mydumper.log.tmp

# The tables must be dumped ...
if [ ! -f /tmp/data/specific_40.t1-schema.sql ]
then
  exit 1
fi

# ... while the database object is not matched by '^specific_40\.' ...
if [ -f /tmp/data/specific_40-schema-create.sql ]
then
  exit 1
fi

# ... and that must be reported instead of passing silently.
if ! grep -q 'specific_40-schema-create.sql will not be dumped' "$LOG"
then
  exit 1
fi

exit 0
