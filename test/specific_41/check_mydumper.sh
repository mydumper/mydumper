#!/bin/bash
#
# Regression test for the myloader --stream carry-over (issue #1687).
#
# When a buffer ends in the middle of what could be a file header, myloader
# moves the tail of the buffer back to the front and reads the rest on the
# next fread(). That move used to be a g_strlcpy(), which stops at the first
# NUL byte. Compressed file content is binary and full of NUL bytes, so the
# tail was silently truncated and the bytes past the NUL were replaced with
# whatever the previous buffer had left there. The byte count was preserved,
# so no "Different file size" message was ever printed: the received file had
# the right size and the wrong content.
#
# The trigger is a chance byte pattern at a 1 MB boundary, which is far too
# rare to hit with a test-sized dump, so this case builds it deterministically:
# it appends one synthetic file to the stream mydumper just produced and pads it
# so that LF, NUL, X land on the last three bytes of the first buffer. The first
# fread() asks for STREAM_BUFFER_SIZE - 1 bytes, so for a buffer of N those are
# stream offsets N-4, N-3 and N-2 -- 999996/999997/999998 at the stock 1000000.
#
# myloader is run here rather than by the harness because the assertion is a
# byte-for-byte comparison of the received files, which needs the files to
# survive; the harness fails a case that leaves files behind. --stream=UNPACK
# keeps them and skips the restore, so the case does not depend on server state.
#
set -u

[ "${1:-0}" -eq 0 ] || exit "$1"

STREAM_BUFFER_SIZE=1000000              # src/common.h
FIRST_BUFFER_LEN=$(( STREAM_BUFFER_SIZE - 1 ))

DUMP_DIR=/tmp/data
RECV_DIR=/tmp/specific_41_recv
STREAM=/tmp/stream.sql
PROBE_STREAM=/tmp/specific_41_stream
PROBE_NAME=specific_41.probe.bin
PROBE_SIZE=1000000                      # only has to span the boundary
EXPECTED=/tmp/specific_41_expected
MYLOADER_ERR=/tmp/specific_41_myloader.err
PROBE_CNF="$(dirname "$0")/probe.cnf"

myloader=$(command -v myloader) || myloader=./myloader
[ -x ./myloader ] && myloader=./myloader

cleanup() { rm -rf "$RECV_DIR" "$PROBE_STREAM" "$EXPECTED" "$MYLOADER_ERR"; }
trap cleanup EXIT

fail() { echo "specific_41: $*" >&2; exit 1; }

[ -s "$STREAM" ] || fail "no stream at $STREAM"

# Where the probe file's content starts inside the finished stream.
base=$(wc -c < "$STREAM" | tr -d ' ')
header=$(printf '\n-- %s %d\n' "$PROBE_NAME" "$PROBE_SIZE" | wc -c | tr -d ' ')
content_start=$(( base + header ))
i0=$(( FIRST_BUFFER_LEN - 3 - content_start ))

[ "$i0" -gt 0 ] && [ $(( i0 + 3 )) -lt "$PROBE_SIZE" ] ||
  fail "dump is too large to place the probe (stream=$base, i0=$i0)"

# Filler is a single repeated byte so the only LF and the only NUL in the probe
# are the two we place on purpose.
head -c "$i0" /dev/zero | tr '\0' 'A'                      >  "$EXPECTED"
printf '\n\000X'                                           >> "$EXPECTED"
head -c $(( PROBE_SIZE - i0 - 3 )) /dev/zero | tr '\0' 'A' >> "$EXPECTED"

cat "$STREAM"                                     >  "$PROBE_STREAM"
printf '\n-- %s %d\n' "$PROBE_NAME" "$PROBE_SIZE" >> "$PROBE_STREAM"
cat "$EXPECTED"                                   >> "$PROBE_STREAM"

# The whole point of the case: LF, NUL, X as the last three bytes of buffer 1.
trigger=$(dd if="$PROBE_STREAM" bs=1 skip=$(( FIRST_BUFFER_LEN - 3 )) count=3 \
            status=none | od -An -tx1 | tr -d ' \n')
[ "$trigger" = "0a0058" ] ||
  fail "probe is misaligned: bytes at $(( FIRST_BUFFER_LEN - 3 )) are $trigger, want 0a0058"

rm -rf "$RECV_DIR"
"$myloader" --user "${mysql_user:-root}" --directory "$RECV_DIR" \
            --defaults-extra-file="$PROBE_CNF" \
            --stream=UNPACK --checksum-all --checksum=fail \
            --logfile /tmp/test_myloader.log.tmp < "$PROBE_STREAM" > "$MYLOADER_ERR" 2>&1
rc=$?
[ "$rc" -eq 0 ] || { cat "$MYLOADER_ERR" >&2; fail "myloader exited $rc"; }

# 1. the synthetic file must arrive byte for byte
cmp -s "$EXPECTED" "$RECV_DIR/$PROBE_NAME" ||
  fail "$PROBE_NAME differs from what was sent: $(cmp -l "$EXPECTED" "$RECV_DIR/$PROBE_NAME" | wc -l) byte(s)"

# 2. and so must every file of the real dump
for f in "$DUMP_DIR"/*
do
  [ -f "$f" ] || fail "no files left in $DUMP_DIR to compare"
  b=$(basename "$f")
  [ -f "$RECV_DIR/$b" ] || fail "$b was not received"
  cmp -s "$f" "$RECV_DIR/$b" || fail "$b differs from what was sent"
done

exit 0
