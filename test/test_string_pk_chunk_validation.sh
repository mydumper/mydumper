#!/bin/bash
set -euo pipefail

mydumper_bin="${1:-./mydumper}"
myloader_bin="${2:-./myloader}"

mysql_host="${MYSQL_HOST:-127.0.0.1}"
mysql_port="${MYSQL_TCP_PORT:-13307}"
mysql_user="${MYSQL_USER:-root}"
mysql_password="${MYSQL_PASSWORD:-percona}"
password_args=()
if [[ -n "${mysql_password}" ]]; then
  password_args=(-p "${mysql_password}")
fi

source_db="${STRING_PK_TEST_SOURCE_DB:-mydumper_string_pk_src}"
target_db="${STRING_PK_TEST_TARGET_DB:-mydumper_string_pk_dst}"
table_name="LedgerEntry"
rows_per_prefix="${STRING_PK_TEST_ROWS_PER_PREFIX:-2000}"
prefix_group_count="${STRING_PK_TEST_PREFIX_GROUP_COUNT:-64}"
rows_per_group="${STRING_PK_TEST_ROWS_PER_GROUP:-512}"
loader_stream_mode="${STRING_PK_TEST_LOADER_STREAM:-NO_STREAM}"

workdir="$(mktemp -d "${TMPDIR:-/tmp}/mydumper-string-pk.XXXXXX")"
dumpdir="${workdir}/dump"
sqlfile="${workdir}/seed.sql"

cleanup() {
  local mysql_cmd=(mysql --protocol=tcp --host "${mysql_host}" --port "${mysql_port}" --user "${mysql_user}" --batch --skip-column-names)
  if [[ "${STRING_PK_TEST_KEEP_WORKDIR:-0}" != "1" ]]; then
    "${mysql_cmd[@]}" -e "DROP DATABASE IF EXISTS \`${target_db}\`; DROP DATABASE IF EXISTS \`${source_db}\`;" >/dev/null 2>&1 || true
    rm -rf "${workdir}"
  else
    echo "Keeping regression workdir: ${workdir}" >&2
  fi
}
trap cleanup EXIT

mysql_cmd=(mysql --protocol=tcp --host "${mysql_host}" --port "${mysql_port}" --user "${mysql_user}" --batch --skip-column-names)
if [[ -n "${mysql_password}" ]]; then
  export MYSQL_PWD="${mysql_password}"
fi

if ! "${mysql_cmd[@]}" -e "SELECT 1" >/dev/null 2>&1; then
  echo "Skipping string PK chunk validation: cannot connect to ${mysql_host}:${mysql_port}" >&2
  if [[ "${STRING_PK_TEST_REQUIRE_DB:-0}" == "1" ]]; then
    exit 1
  fi
  exit 0
fi

if [[ ! -x "${mydumper_bin}" ]]; then
  echo "mydumper binary not found: ${mydumper_bin}" >&2
  exit 1
fi

if [[ ! -x "${myloader_bin}" ]]; then
  echo "myloader binary not found: ${myloader_bin}" >&2
  exit 1
fi

mkdir -p "${dumpdir}"

"${mysql_cmd[@]}" -e "
DROP DATABASE IF EXISTS \`${target_db}\`;
DROP DATABASE IF EXISTS \`${source_db}\`;
CREATE DATABASE \`${source_db}\`;
CREATE DATABASE \`${target_db}\`;
CREATE TABLE \`${source_db}\`.\`${table_name}\` (
  \`product_ari\` varchar(16) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  \`uuid\` varchar(36) NOT NULL,
  \`payload\` int NOT NULL,
  PRIMARY KEY (\`product_ari\`,\`uuid\`)
) ENGINE=InnoDB;
" >/dev/null

{
  printf 'USE `%s`;\n' "${source_db}"
  printf 'INSERT INTO `%s` (`product_ari`,`uuid`,`payload`) VALUES\n' "${table_name}"
  total_rows=$(( prefix_group_count * rows_per_group ))
  current=0
  for group in $(seq 0 $((prefix_group_count - 1))); do
    prefix=$(printf '%02X%02X' $((10 + group / 16)) $((group % 16)))
    for i in $(seq 1 "${rows_per_group}"); do
      current=$((current + 1))
      suffix=$(printf '%010d' "${i}")
      product_ari="${prefix}${suffix}"
      uuid=$(printf '00000000-0000-0000-0000-%012d' "${current}")
      payload="${current}"
      printf "('%s','%s',%s)%s\n" "${product_ari}" "${uuid}" "${payload}" "$([[ "${current}" -lt "${total_rows}" ]] && printf ',' || printf ';')"
    done
  done
} > "${sqlfile}"

"${mysql_cmd[@]}" < "${sqlfile}"

source_count=$("${mysql_cmd[@]}" -D "${source_db}" -e "SELECT COUNT(*) FROM \`${table_name}\`;")

set +e
pipeline_log="${workdir}/pipeline.log"
"${mydumper_bin}" \
  --host "${mysql_host}" \
  --port "${mysql_port}" \
  -u "${mysql_user}" \
  "${password_args[@]}" \
  --regex "^(${source_db//./\\.}\\.${table_name})$" \
  -o "${dumpdir}" \
  --stream NO_STREAM_AND_NO_DELETE \
  --clear \
  -t 4 \
  --max-threads-per-table=4 \
  --bulk-metadata-prefetch \
  --split-partitions \
  --split-string-pk \
  --string-pk-planner=metadata \
  --string-pk-planner-timeout=30 \
  --string-pk-planner-max-probes=256 \
  --string-pk-planner-max-prefixes=8 \
  --string-pk-planner-min-rows=1 \
  --max-items-per-string-chunk=1 \
  --max-char-size=2 \
  --compress=zstd \
  --sync-thread-lock-mode NO_LOCK \
  --trx-tables \
  -F 10 \
  --verbose 3 \
| "${myloader_bin}" \
  --host "${mysql_host}" \
  --port "${mysql_port}" \
  -u "${mysql_user}" \
  "${password_args[@]}" \
  --logfile "${workdir}/myloader.log" \
  -d "${dumpdir}" \
  -B "${target_db}" \
  --stream "${loader_stream_mode}" \
  --drop-table=DROP \
  -t 4 \
  --max-threads-per-table=4 \
  --checksum skip \
  --optimize-keys=AFTER_IMPORT_PER_TABLE \
  --verbose 3 \
  > "${pipeline_log}" 2>&1
pipeline_status=$?
set -e

if [[ ${pipeline_status} -ne 0 ]]; then
  echo "Validation run failed. Pipeline log:" >&2
  cat "${pipeline_log}" >&2
  exit "${pipeline_status}"
fi

target_count=$("${mysql_cmd[@]}" -D "${target_db}" -e "SELECT COUNT(*) FROM \`${table_name}\`;")

if [[ "${source_count}" != "${target_count}" ]]; then
  echo "Row-count mismatch: source=${source_count} target=${target_count}" >&2
  exit 1
fi

missing_rows=$("${mysql_cmd[@]}" -D "${source_db}" -e \
  "SELECT COUNT(*) FROM \`${source_db}\`.\`${table_name}\` s LEFT JOIN \`${target_db}\`.\`${table_name}\` d ON d.product_ari=s.product_ari AND d.uuid=s.uuid WHERE d.uuid IS NULL;")
if [[ "${missing_rows}" != "0" ]]; then
  echo "Primary-key mismatch: ${missing_rows} source rows are missing from the target" >&2
  exit 1
fi

extra_rows=$("${mysql_cmd[@]}" -D "${source_db}" -e \
  "SELECT COUNT(*) FROM \`${target_db}\`.\`${table_name}\` d LEFT JOIN \`${source_db}\`.\`${table_name}\` s ON s.product_ari=d.product_ari AND s.uuid=d.uuid WHERE s.uuid IS NULL;")
if [[ "${extra_rows}" != "0" ]]; then
  echo "Primary-key mismatch: ${extra_rows} target rows are not in the source" >&2
  exit 1
fi

if grep -q "Duplicate entry" "${workdir}/myloader.log" 2>/dev/null; then
  echo "Validation run hit duplicate-entry errors" >&2
  cat "${workdir}/myloader.log" >&2
  exit 1
fi

echo "String PK chunk validation passed: ${source_count} rows restored without duplicate-key errors"
