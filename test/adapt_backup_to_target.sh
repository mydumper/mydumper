#!/bin/bash
#
# Adapts a mydumper backup taken on one server so myloader can import it into
# a server of a different vendor and/or version.
#
#   test/adapt_backup_to_target.sh <source> <target> <backup_dir>
#
#   source/target: vendor:version, vendor is mysql, percona or mariadb
#                  (e.g. percona:5.7, mysql:9.7, mariadb:10.6)
#
# Only the backup is changed: SQL files (plain, .gz or .zst) and the metadata
# file. Data rows are never touched, only the SQL_MODE header of data files.
#
# Fixes applied:
#   - target MySQL/Percona >= 8.0: drop SQL modes removed in 8.0
#     (NO_AUTO_CREATE_USER, ...)
#   - MariaDB -> MySQL/Percona: drop MariaDB only SQL modes
#   - MySQL/Percona -> MariaDB: drop ENCRYPTION table/database options, map
#     the MySQL 8 utf8mb4_0900_* collations to MariaDB ones and drop the
#     ONLY_FULL_GROUP_BY SQL mode
#   - between vendors: view definitions are rewritten by each server, so
#     the view schema checksums can not match and are removed; CHECKSUM TABLE
#     differs on rows with NULLs, so data checksums of tables with nullable
#     columns are removed
#   - when database collations are changed, their schema checksum is removed

set -o pipefail

usage()
{
  echo "Usage: $0 <source vendor:version> <target vendor:version> <backup_dir>" >&2
  exit 2
}

(( $# == 3 )) || usage
source_db=$1
target_db=$2
backup_dir=$3
[ -d "$backup_dir" ] || { echo "$0: backup directory '$backup_dir' not found" >&2; exit 2; }

# percona:5.7 -> vendor=percona family=mysql version=5007
parse_db()
{
  local db=$1 prefix=$2 vendor version major minor
  vendor=${db%%:*}
  version=${db#*:}
  [[ "$db" == *:* && "$version" =~ ^[0-9]+(\.[0-9]+)?$ ]] ||
    { echo "$0: '$db' must be vendor:version, e.g. mysql:8.0" >&2; exit 2; }
  major=${version%%.*}
  minor=0
  [[ "$version" == *.* ]] && minor=${version#*.}
  case $vendor in
    mysql|percona) printf -v "${prefix}_family" mysql ;;
    mariadb)       printf -v "${prefix}_family" mariadb ;;
    *) echo "$0: unknown vendor '$vendor' (mysql, percona or mariadb)" >&2; exit 2 ;;
  esac
  printf -v "${prefix}_vendor" %s "$vendor"
  printf -v "${prefix}_version" %s "$(( 10#$major * 1000 + 10#$minor ))"
}
parse_db "$source_db" source
parse_db "$target_db" target

echo "Adapting backup in $backup_dir from $source_db to $target_db"

sql_mode_remove=()       # SQL modes to remove from SET SQL_MODE / metadata
schema_sed=()            # sed expressions for schema files (*-schema*.sql)
drop_view_checksums=0
drop_database_schema_checksums=0
drop_nullable_data_checksums=0

if [ "$target_family" = mysql ] && (( target_version >= 8000 ))
then
  sql_mode_remove+=(NO_AUTO_CREATE_USER DB2 MAXDB MSSQL MYSQL323 MYSQL40 ORACLE
                    POSTGRESQL NO_FIELD_OPTIONS NO_KEY_OPTIONS NO_TABLE_OPTIONS)
fi

if [ "$source_family" = mariadb ] && [ "$target_family" = mysql ]
then
  sql_mode_remove+=(SIMULTANEOUS_ASSIGNMENT EMPTY_STRING_IS_NULL TIME_ROUND_FRACTIONAL)
fi

if [ "$source_family" = mysql ] && [ "$target_family" = mariadb ]
then
  schema_sed+=(
    # CREATE DATABASE ... /*!80016 DEFAULT ENCRYPTION='N' */
    -e "s#[[:space:]]*/\*![0-9]* DEFAULT ENCRYPTION='[NnYy]' \*/##g"
    # CREATE TABLE ... ENCRYPTION='N' (MariaDB uses ENCRYPTED=YES/NO)
    -e "s#[[:space:]]ENCRYPTION='[Nn]'##g"
    -e "s#[[:space:]]ENCRYPTION='[Yy]'# ENCRYPTED=YES#g"
    # MySQL 8 collations, unknown before MariaDB 11.4
    -e "s#utf8mb4_0900_bin#utf8mb4_bin#g"
    -e "s#utf8mb4_[a-z0-9_]*0900_[a-z_]*#utf8mb4_unicode_520_ci#g"
  )
  drop_database_schema_checksums=1
  # MariaDB does not detect functional dependencies, so views valid in MySQL
  # fail with ERROR 1055 "isn't in GROUP BY" under ONLY_FULL_GROUP_BY
  sql_mode_remove+=(ONLY_FULL_GROUP_BY)
fi

if [ "$source_family" != "$target_family" ]
then
  drop_view_checksums=1
  # CHECKSUM TABLE gives different results on MySQL and MariaDB for rows that
  # contain NULL values, so tables with nullable columns can not be compared
  drop_nullable_data_checksums=1
fi

# sed expressions that drop the modes, applied only on SQL_MODE lines
sql_mode_sed=()
for mode in "${sql_mode_remove[@]}"
do
  sql_mode_sed+=(
    -e "/SQL_MODE[[:space:]]*=/I s/\([',]\)${mode},/\1/g"
    -e "/SQL_MODE[[:space:]]*=/I s/,${mode}'/'/g"
    -e "/SQL_MODE[[:space:]]*=/I s/'${mode}'/''/g"
  )
done

if (( ${#sql_mode_sed[@]} == 0 && ${#schema_sed[@]} == 0 && drop_view_checksums == 0 ))
then
  echo "Nothing to adapt"
  exit 0
fi

cat_file()
{
  case $1 in
    *.gz)  gzip -dc "$1" ;;
    *.zst) zstd -qdc "$1" ;;
    *)     cat "$1" ;;
  esac
}

# Runs sed with the given expressions on a plain, .gz or .zst file and
# rewrites it (keeping its compression) only when something changed
sed_file()
{
  local file=$1; shift
  local tmp="${file}.adapt.tmp"
  cat_file "$file" | sed "$@" > "$tmp" ||
    { echo "$0: failed to adapt $file" >&2; rm -f "$tmp"; return 1; }
  if cmp -s <(cat_file "$file") "$tmp"
  then
    rm -f "$tmp"
    return 0
  fi
  case $file in
    *.gz)  gzip -c "$tmp" > "$file" ;;
    *.zst) zstd -qc "$tmp" > "$file" ;;
    *)     cat "$tmp" > "$file" ;;
  esac || { echo "$0: failed to write $file" >&2; rm -f "$tmp"; return 1; }
  rm -f "$tmp"
  echo "  adapted $(basename "$file")"
}

errors=0
shopt -s nullglob
for file in "$backup_dir"/*.sql "$backup_dir"/*.sql.gz "$backup_dir"/*.sql.zst
do
  expressions=("${sql_mode_sed[@]}")
  [[ "$(basename "$file")" == *-schema* ]] && expressions+=("${schema_sed[@]}")
  (( ${#expressions[@]} > 0 )) || continue
  sed_file "$file" "${expressions[@]}" || errors=1
done

# Tables (db.table, as in the file names) with at least one nullable column:
# a column definition line of the CREATE TABLE without NOT NULL
nullable_tables=""
if (( drop_nullable_data_checksums ))
then
  for file in "$backup_dir"/*-schema.sql "$backup_dir"/*-schema.sql.gz "$backup_dir"/*-schema.sql.zst
  do
    if cat_file "$file" | grep -E '^  [`"]' | grep -qvi 'NOT NULL'
    then
      name=$(basename "$file")
      nullable_tables+=" ${name%-schema.sql*}"
    fi
  done
fi

metadata="$backup_dir/metadata"
if [ -f "$metadata" ]
then
  tmp="${metadata}.adapt.tmp"
  # Database sections are [`db`], table and view ones [`db`.`table`]
  sed -e '' "${sql_mode_sed[@]}" "$metadata" |
    awk -v drop_views=$drop_view_checksums -v drop_dbs=$drop_database_schema_checksums \
        -v nullable="$nullable_tables" '
      BEGIN { n = split(nullable, list, " "); for (i = 1; i <= n; i++) is_nullable[list[i]] = 1 }
      /^\[/ {
        section = $0
        quotes = gsub(/[`"]/, "&", section)
        is_database = (quotes == 2)
        is_view = 0
        key = section
        gsub(/^\[|\]$|[`"]/, "", key)
      }
      /^is_view[[:space:]]*=[[:space:]]*1/ { is_view = 1 }
      /^schema_checksum[[:space:]]*=/ {
        if ((drop_views && is_view) || (drop_dbs && is_database)) next
      }
      /^data_checksum[[:space:]]*=/ && (key in is_nullable) { next }
      { print }' > "$tmp" || { echo "$0: failed to adapt $metadata" >&2; errors=1; }
  if [ -f "$tmp" ] && ! cmp -s "$metadata" "$tmp"
  then
    mv "$tmp" "$metadata"
    echo "  adapted metadata"
  else
    rm -f "$tmp"
  fi
fi

exit $errors
