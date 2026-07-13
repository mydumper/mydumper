#!/bin/bash
set -euo pipefail

myloader_bin="${1:-./myloader}"

help_output="$("$myloader_bin" --help 2>&1)"
grep -q -- '--aws-session-command' <<<"$help_output"

aws_output="$("$myloader_bin" --help --source-control-command=AWS --enable-binlog 2>&1)"
if grep -qE '^enable-binlog' <<<"$aws_output"; then
  echo "enable-binlog should be suppressed in AWS mode" >&2
  exit 1
fi

echo "myloader AWS session regression passed"
