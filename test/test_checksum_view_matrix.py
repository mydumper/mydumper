#!/usr/bin/env python3
"""Four-direction view DDL checks on two disposable Docker database containers.

Usage: python3 test/test_checksum_view_matrix.py MYSQL_CONTAINER MARIA_CONTAINER BUILD_CONTAINER
The build container must mount this checkout at /src and contain the compiled
/tmp/view-checksum-build/test_checksum_view_server helper. No production data is used.
Only the fixed checksum_view_source and checksum_view_target databases are touched.
"""
import subprocess
import sys

mysql, maria, build = sys.argv[1:]
servers = [(mysql, "mysql"), (maria, "mariadb")]
source, target = "checksum_view_source", "checksum_view_target"

def sql(server, query):
    container, client = server
    return subprocess.check_output(
        ["docker", "exec", "-i", container, client, "-uroot", "--batch", "--raw", "--skip-column-names"],
        input=("SET SESSION sql_mode='', sql_quote_show_create=1;\n" + query).encode(),
    ).decode().strip()

def checksums(server, database):
    output = subprocess.check_output([
        "docker", "exec", build, "/tmp/view-checksum-build/test_checksum_view_server", server[0], database,
    ]).decode()
    return dict(line.split("\t") for line in output.splitlines())

# Each changed definition must have a different checksum; all expressions are
# valid in both dialects. The target parses the source server's SHOW CREATE output.
cases = {
    "nested_between": ("SELECT a BETWEEN b AND (c BETWEEN 0 AND 1) AS value FROM t", "SELECT (a BETWEEN b AND c) BETWEEN 0 AND 1 AS value FROM t"),
    "between_comparison": ("SELECT (a=b) BETWEEN 0 AND 1 AS value FROM t", "SELECT a=(b BETWEEN 0 AND 1) AS value FROM t"),
    "arithmetic": ("SELECT (a+b)*c AS value FROM t", "SELECT a+b*c AS value FROM t"),
    "subtraction": ("SELECT a-(b-c) AS value FROM t", "SELECT (a-b)-c AS value FROM t"),
    "boolean": ("SELECT a FROM t WHERE (a=1 OR b=2) AND c=3", "SELECT a FROM t WHERE a=1 OR (b=2 AND c=3)"),
    "comparison": ("SELECT a FROM t WHERE a>2", "SELECT a FROM t WHERE a>=2"),
    "literal": ("SELECT 'a b' AS value", "SELECT 'ab' AS value"),
    "literal_schema": ("SELECT '`old`.`t`' AS value", "SELECT '`new`.`t`' AS value"),
    "case_between": ("SELECT CASE WHEN a BETWEEN 1 AND 5 THEN b+c ELSE a END AS value FROM t", "SELECT CASE WHEN a BETWEEN 1 AND 6 THEN b+c ELSE a END AS value FROM t"),
    "cast": ("SELECT CAST((a+b)*c AS DECIMAL(20,6)) AS value FROM t", "SELECT CAST((a+b)*c AS DECIMAL(20,5)) AS value FROM t"),
    "functions": ("SELECT IF(a=1,GREATEST(b,c),a+b) AS value FROM t", "SELECT IF(a=1,LEAST(b,c),a+b) AS value FROM t"),
    "exists": ("SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.a=t.a)", "SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.a=t.b)"),
    "exists_zero": ("SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u LIMIT 0)", "SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u)"),
    "exists_offset": ("SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u LIMIT 1 OFFSET 1)", "SELECT a FROM t WHERE EXISTS(SELECT 1 FROM u LIMIT 1)"),
    "join_type": ("SELECT t.a FROM t LEFT JOIN u ON t.a=u.a", "SELECT t.a FROM t JOIN u ON t.a=u.a"),
    "join_group": ("SELECT t.a FROM t LEFT JOIN (u JOIN w ON u.a=w.a) ON t.a=u.a", "SELECT t.a FROM (t LEFT JOIN u ON t.a=u.a) JOIN w ON u.a=w.a"),
    "in_list": ("SELECT a FROM t WHERE a IN(1,2,3)", "SELECT a FROM t WHERE a IN(1,2,4)"),
    "not": ("SELECT a FROM t WHERE NOT(a=1 AND b=2)", "SELECT a FROM t WHERE NOT(a=1) AND b=2"),
    "alias": ("SELECT a AS first_name FROM t", "SELECT a AS other_name FROM t"),
    "collation": ("SELECT s COLLATE utf8mb4_bin AS value FROM t", "SELECT s COLLATE utf8mb4_unicode_ci AS value FROM t"),
    "order": ("SELECT a FROM t ORDER BY a ASC", "SELECT a FROM t ORDER BY a DESC"),
    "limit": ("SELECT a FROM t LIMIT 1", "SELECT a FROM t LIMIT 2"),
    "union": ("SELECT a FROM t UNION ALL SELECT a FROM u", "SELECT a FROM t UNION SELECT a FROM u"),
    "decimal": ("SELECT 1.25 AS value", "SELECT 1.26 AS value"),
    "qualified": ("SELECT t.a FROM t", "SELECT u.a FROM u"),
}

# MySQL 8.0 drops these clauses from VIEW_DEFINITION itself. Keep the
# server-level loss visible rather than calling it a normalizer success.
server_limits = {name: cases.pop(name) for name in ("exists_zero", "exists_offset")}

def setup(server, database):
    sql(server, f"DROP DATABASE IF EXISTS {database}; CREATE DATABASE {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; USE {database};" +
        "".join(f"CREATE TABLE {t}(a INT,b INT,c INT,s VARCHAR(20));" for t in ("t", "u", "w")))

try:
    for server in servers:
        print(server[0], sql(server, "SELECT VERSION()"), flush=True)
        setup(server, source)
        sql(server, f"USE {source};" + "".join(f"CREATE VIEW `{name}` AS {pair[0]};" for name, pair in cases.items()))
    for src in servers:
        expected = checksums(src, source)
        ddls = {}
        for name in cases:
            # Fixture identifiers are fixed; no literal includes either schema name.
            row = sql(src, f"SHOW CREATE VIEW {source}.`{name}`").split("\t")
            ddls[name] = row[1].replace(f"`{source}`.", f"`{target}`.")
        for dst in servers:
            setup(dst, target)
            sql(dst, f"USE {target};" + ";".join(ddls.values()) + ";")
            actual = checksums(dst, target)
            mismatches = [n for n in cases if expected.get(n) != actual.get(n)]
            for name in mismatches:
                for server, database in ((src, source), (dst, target)):
                    print(server[0], name, sql(server, f"SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='{name}'"), flush=True)
            assert not mismatches, (src[0], dst[0], "unchanged", mismatches)
            sql(dst, f"USE {target};" + "".join(f"CREATE OR REPLACE VIEW `{name}` AS {pair[1]};" for name, pair in cases.items()))
            changed = checksums(dst, target)
            missed = [n for n in cases if expected.get(n) == changed.get(n)]
            assert not missed, (src[0], dst[0], "undetected change", missed)
            print(f"PASS {src[0]} -> {dst[0]}: {len(cases)} unchanged, {len(cases)} changed", flush=True)
    for server in servers:
        setup(server, target)
        sql(server, f"USE {target};" + "".join(
            f"CREATE VIEW `{name}_{variant}` AS {definition};"
            for name, pair in server_limits.items() for variant, definition in enumerate(pair)))
        values = checksums(server, target)
        for name in server_limits:
            definition = sql(server, f"SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA='{target}' AND TABLE_NAME='{name}_0'")
            if server == servers[0]:
                assert "limit" not in definition.lower(), definition
                assert values[name + "_0"] == values[name + "_1"]
                print(f"SERVER LIMITATION MySQL {name}: server discarded the clause before checksum", flush=True)
            else:
                assert "limit" in definition.lower(), definition
                assert values[name + "_0"] != values[name + "_1"]
                print(f"PASS MariaDB {name}: retained clause changes checksum", flush=True)
finally:
    for server in servers:
        sql(server, f"DROP DATABASE IF EXISTS {target}; DROP DATABASE IF EXISTS {source};")
