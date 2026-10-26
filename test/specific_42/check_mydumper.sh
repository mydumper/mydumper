#!/usr/bin/env bash

[ "${1:-0}" -eq 0 ] || exit "$1"

if [ ! -f /tmp/data/specific_42.t1-schema.sql ]
then
  exit 1
fi

exit 0
