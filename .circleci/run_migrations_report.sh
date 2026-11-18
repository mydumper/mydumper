#!/bin/bash
#
# Runs a CircleCI "all databases" job locally (through run_job_locally.rb) and
# reports how many "Backup from X and restore into Y" migrations succeeded,
# and why each failed one failed.
#
#   .circleci/run_migrations_report.sh [options] [job_name] [-- run_job_locally.rb args]
#
#   job_name:   default compile_and_test_mydumper_in_resolute_all_databases
#   -l FILE:    do not run the job, only parse an existing log
#   -o FILE:    where to keep the job log (default /tmp/<job_name>.<date>.log)
#   -h:         help
#
# Arguments after -- go to run_job_locally.rb, e.g. -- -b master
# The job always runs with -N (no interactive shell at the end).
# Exit code: 0 when every migration succeeded, 1 otherwise.

set -o pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
job_name=compile_and_test_mydumper_in_resolute_all_databases
log_file=
parse_only=0

usage()
{
  sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

while (( $# > 0 ))
do
  case $1 in
    -h|--help) usage 0 ;;
    -l|--log) log_file=$2; parse_only=1; shift 2 ;;
    -o|--output) log_file=$2; shift 2 ;;
    --) shift; break ;;
    -*) echo "Unknown option $1" >&2; usage ;;
    *) job_name=$1; shift ;;
  esac
done
[ -n "$log_file" ] || [ $parse_only = 1 ] || log_file="/tmp/${job_name}.$(date +%Y%m%d-%H%M%S).log"

if (( parse_only ))
then
  [ -f "$log_file" ] || { echo "Log file '$log_file' not found" >&2; exit 2; }
else
  echo "Running $job_name, log: $log_file"
  "$script_dir/run_job_locally.rb" "$job_name" -N "$@" 2>&1 | tee "$log_file"
  echo
fi

# Strip colours and parse every migration step:
#   ==> [19/41] Backup from percona57 and restore into percona80
#   ... "Test finished successfully!"  -> OK
#   ... "==> [19/41] step failed"      -> FAIL, reason = first meaningful error
sed 's/\x1b\[[0-9;]*m//g' "$log_file" | awk '
  function clean(s) {
    sub(/^.*CRITICAL \*\*: [0-9:.]+: /, "", s)
    sub(/^[0-9-]+ [0-9:]+ \[[A-Z]+\] - /, "", s)
    sub(/^Thread [0-9]+ using connection [0-9]+ - /, "", s)
    sub(/Error occurs between lines: [0-9]+ and [0-9]+: /, "", s)
    gsub(/[[:space:]]+$/, "", s)
    return substr(s, 1, 200)
  }
  function finish(status) {
    if (step == "") return
    total++
    if (status == "OK") ok++
    else {
      if (reason == "") reason = (fallback != "" ? fallback : "unknown, see the log")
      fail_reason[total] = reason
    }
    st[total] = status; name[total] = step
    step = ""
  }
  /^==> \[[0-9]+\/[0-9]+\] Backup from / {
    finish("UNKNOWN")
    step = $0
    sub(/^==> \[[0-9]+\/[0-9]+\] Backup from /, "", step)
    sub(/ and restore into /, " -> ", step)
    reason = ""; fallback = ""
    next
  }
  step == "" { next }
  /Test finished successfully!/ { finish("OK"); next }
  /^==> \[[0-9]+\/[0-9]+\] step failed/ { finish("FAIL"); next }
  /^==> / { finish("UNKNOWN"); next }
  reason == "" && (/CRITICAL \*\*/ || /\[CRITICAL\]/) { reason = clean($0); next }
  reason == "" && /^Error adapting the backup/ { reason = clean($0); next }
  reason == "" && /ERROR [0-9]+:/ && fallback == "" { fallback = clean($0); next }
  /^Error running: .*mydumper/ && fallback == "" { fallback = "mydumper export failed"; next }
  /^Error running: .*myloader/ && fallback == "" { fallback = "myloader import failed"; next }
  END {
    finish("UNKNOWN")
    if (total == 0) {
      print "No migration steps found in the log (did the build fail?)"
      exit 2
    }
    print "Migrations:"
    for (i = 1; i <= total; i++) {
      line = sprintf("  %-7s %s", st[i], name[i])
      if (st[i] != "OK") line = line "\n            " fail_reason[i]
      print line
    }
    printf "\n%d of %d migrations succeeded, %d failed\n", ok, total, total - ok
    if (ok == total) exit 0

    # Group failures by reason, ignoring checksum values
    for (i = 1; i <= total; i++) {
      if (st[i] == "OK") continue
      key = fail_reason[i]
      gsub(/got [0-9A-Fa-f]+, expecting [0-9A-Fa-f]+/, "got X, expecting Y", key)
      if (!(key in count)) order[++groups] = key
      count[key]++
      who[key] = who[key] "\n      " name[i]
    }
    print "\nFailures by reason:"
    for (g = 1; g <= groups; g++)
      printf "  [%d] %s%s\n", count[order[g]], order[g], who[order[g]]
    exit 1
  }'
status=$?
[ -n "$log_file" ] && echo "Log: $log_file"
exit $status
