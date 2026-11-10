# View checksum regression tests

`test_checksum_view` tests qualifier handling, expression grouping, literals and
conservative fallback without a database. It is part of CTest.

`test_checksum_view_server` calls the production `checksum_view_structure()`
function, including its SQL CRC32 query. `test_checksum_view_matrix.py` transfers
`SHOW CREATE VIEW` output between disposable MySQL and MariaDB servers, changes
the database qualifier in these controlled fixtures, and compares their checksums.
It does not run a full mydumper/myloader dump or restore.

## Reproduce the four-direction matrix

Use disposable containers only. The script creates and drops databases named
`checksum_view_source` and `checksum_view_target`. It connects as root with an
empty password; do not publish these containers' ports or use application servers.

1. Build a test image with the project's normal C build dependencies:

   ```sh
   docker build -t mydumper-view-test - <<'DOCKERFILE'
   FROM debian:bookworm
   RUN apt-get update && apt-get install -y --no-install-recommends \
       build-essential cmake pkg-config libglib2.0-dev libmariadb-dev \
       libmariadb-dev-compat libpcre2-dev zlib1g-dev libssl-dev \
       && rm -rf /var/lib/apt/lists/*
   DOCKERFILE
   ```

2. Start isolated database and build containers from the repository root:

   ```sh
   docker network create --internal view-checksum-test-net
   docker run -d --name view-checksum-mysql-test --network view-checksum-test-net \
       -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.0.46 --skip-log-bin \
       --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci
   docker run -d --name view-checksum-mariadb-test --network view-checksum-test-net \
       -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=yes mariadb:11.8.9 --skip-log-bin \
       --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci
   docker run -d --name view-checksum-build-runner --network view-checksum-test-net \
       -v "$PWD:/src" -w /src mydumper-view-test sleep infinity
   ```

   Wait until both databases accept connections before running the next step.

3. Compile and run:

   ```sh
   docker exec view-checksum-build-runner sh -c '
       cmake -S /src -B /tmp/view-checksum-build &&
       cmake --build /tmp/view-checksum-build -j4 &&
       ctest --test-dir /tmp/view-checksum-build --output-on-failure'
   python3 test/test_checksum_view_matrix.py \
       view-checksum-mysql-test view-checksum-mariadb-test view-checksum-build-runner
   ```

4. Remove only these disposable resources:

   ```sh
   docker rm -fv view-checksum-mysql-test view-checksum-mariadb-test view-checksum-build-runner
   docker network rm view-checksum-test-net
   ```

## Coverage and limits

The matrix checks MySQL-to-MySQL, MySQL-to-MariaDB, MariaDB-to-MySQL and
MariaDB-to-MariaDB using actual server-generated definitions. Each direction
checks 23 unchanged definitions and 23 intentional changes: arithmetic and
Boolean grouping, BETWEEN nesting and comparison binding, literals, CASE,
CAST precision, functions, EXISTS predicates, JOIN type/grouping, IN lists,
NOT, aliases, collations, ordering, limits, UNION and table references.

MySQL 8.0.46 discards LIMIT 0 and LIMIT/OFFSET inside the tested EXISTS view
queries before exposing VIEW_DEFINITION. The script verifies this separately;
it must not be counted as successful detection of a definition change.
MariaDB 11.8.9 retains these clauses, and their checksums must differ. The
normalizer removes only a terminal LIMIT 1 in EXISTS, without OFFSET. It
preserves LIMIT 0, offset syntax and limits inside nested queries.

This is bounded syntactic normalization, not a proof that two SQL programs
behave identically. Special grammars such as INTERVAL, window functions and
MATCH/AGAINST, comments, mode-sensitive operators, and parser limits retain
verbatim definition text after qualifier handling. False mismatches remain
possible. Unsupported syntax must not be declared semantically equivalent.

The checksum still covers the SELECT definition, not DEFINER, SQL SECURITY,
CHECK OPTION, ALGORITHM, account privileges or the definitions of dependencies.
Those require separate policy and verification. It cannot recover information
already discarded by a server or prove equal results under different collations,
SQL modes or function implementations. CRC32 remains non-cryptographic.

This changes view checksum values. Dump and verification must use the matching
algorithm; checksums from older backups are not compatible. Versioned checksum
metadata and a wider engine/version/SQL-mode matrix remain follow-up work.

Relevant references:

- [MySQL VIEWS and canonical definitions](https://dev.mysql.com/doc/refman/8.0/en/information-schema-views-table.html)
- [MariaDB SHOW CREATE VIEW and identifier quoting](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/show/show-create-view)
- [MariaDB operator precedence](https://mariadb.com/docs/server/reference/sql-structure/operators/operator-precedence)
- [MySQL grammar: bool_pri and predicate](https://github.com/mysql/mysql-server/blob/8.0/sql/sql_yacc.yy)
