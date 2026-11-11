#!/usr/bin/env python3
"""Restore small view fixtures between disposable MySQL and MariaDB containers.

Usage: python3 test/test_view_database_mapping.py MYSQL_CONTAINER MARIA_CONTAINER BUILD_CONTAINER BUILD_DIR
The build container must share the servers' Docker network and mount this checkout
at /src. Set MYSQL_PWD for the disposable servers' root password. Only databases
with the view_map_2323_ prefix and the view_map_2323 user are modified. The test
temporarily changes sql_mode on these disposable servers and restores it afterward.
"""
import os
import subprocess
import sys

mysql, maria, build, binaries = sys.argv[1:]
servers = [(maria, "mariadb"), (mysql, "mysql")]
source, target = "view_map_2323_source", "view_map_2323_target"
external = "view_map_2323_external"
external_target = "view_map_2323_external_target"
password = os.environ["MYSQL_PWD"]


def sql(server, query):
    container, client = server
    return subprocess.check_output(
        ["docker", "exec", "-i", "-e", "MYSQL_PWD", container, client,
         "-uroot", "--batch", "--raw", "--skip-column-names"],
        input=("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci;\n" + query).encode(),
    ).decode().strip()


def run_tool(server, tool, directory, *args, user="root"):
    result = subprocess.run(
        ["docker", "exec", "-e", "MYSQL_PWD", build, binaries + "/" + tool,
         "--host=" + server[0], "--user=" + user, "--password=" + password,
         "--threads=2", ("--outputdir=" if tool == "mydumper" else "--directory=") + directory, *args],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, (server[0], tool, result.returncode, result.stderr[-6000:])
    # Upstream view checksums are not invariant under schema renames (#2386).
    # Keep verification enabled and verify view results and target bindings below.
    for line in result.stderr.splitlines():
        if "checksum mismatch" in line:
            assert "View checksum mismatch" in line, line


views = {
    "simple": "SELECT u.id, u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id=u.id) order_count FROM users u",
    "user_order_summary": f"SELECT u.id, u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id=u.id) order_count FROM users u JOIN {external}.marker m ON m.id=u.id",
    "literal_alias": f"SELECT `{source}`.id, '`{source}`.`orders`' note FROM users `{source}` JOIN {external}.marker m ON m.id=`{source}`.id",
    "comma_join": f"SELECT u.id, o.id order_id FROM users u, orders o, {external}.marker m WHERE u.id=o.user_id AND m.id=u.id",
    "nested_join": f"SELECT u.id, o.id order_id FROM users u LEFT JOIN (orders o JOIN {external}.marker m ON m.id=o.user_id) ON u.id=o.user_id",
    "qualified_column": f"SELECT {source}.users.id FROM {source}.users JOIN {external}.marker m ON m.id={source}.users.id",
    "expression": f"SELECT u.id, CONCAT('a b', '.{source}.', 'it''s') note FROM users u JOIN {external}.marker m ON m.id=u.id",
    "dependent": f"SELECT s.id, s.order_count FROM user_order_summary s JOIN {external}.marker m ON m.id=s.id",
}
original_modes = {server[0]: sql(server, "SELECT @@GLOBAL.sql_mode") for server in servers}
restores = 0
try:
    for server in servers:
        sql(server, f"""
            CREATE DATABASE IF NOT EXISTS {external} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
            CREATE TABLE IF NOT EXISTS {external}.marker (id INT PRIMARY KEY);
            REPLACE INTO {external}.marker VALUES (1), (2);
            CREATE DATABASE IF NOT EXISTS {external_target} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
            CREATE TABLE IF NOT EXISTS {external_target}.marker (id INT PRIMARY KEY);
            REPLACE INTO {external_target}.marker VALUES (1), (2);
        """)
    for ansi in (False, True):
        mode = "NO_ENGINE_SUBSTITUTION" + (",ANSI_QUOTES" if ansi else "")
        for server in servers:
            sql(server, f"SET GLOBAL sql_mode='{mode}'")
        for src in servers:
            sql(src, f"""
                DROP DATABASE IF EXISTS {source};
                CREATE DATABASE {source} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
                USE {source};
                CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
                CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
                INSERT INTO users VALUES (1, 'Ada'), (2, 'Bob');
                INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2);
            """ + "".join(f"CREATE SQL SECURITY INVOKER VIEW {name} AS {definition};" for name, definition in views.items()))
            expected = {name: sorted(sql(src, f"SELECT * FROM {source}.{name}").splitlines()) for name in views}
            for compressed in (False, True):
                directory = "/work/view-map-" + src[0]
                dump_options = ["--compress=GZIP"] if compressed else []
                run_tool(src, "mydumper", directory, "--database=" + source + "," + external, "--clear", "--skip-definer",
                         "--data-checksums", *dump_options)
                for dst in servers:
                    for mapping_kind in ("single", "pairs", "merge"):
                        sql(dst, f"""
                            DROP DATABASE IF EXISTS {target};
                            CREATE DATABASE {target} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
                            CREATE USER IF NOT EXISTS 'view_map_2323'@'%' IDENTIFIED BY '{password}';
                            GRANT ALL ON {target}.* TO 'view_map_2323'@'%';
                            GRANT SELECT ON {external}.* TO 'view_map_2323'@'%';
                            GRANT SELECT, CREATE ON {external_target}.* TO 'view_map_2323'@'%';
                        """)
                        mapping = f"{source}:{target},{external}:{external_target}" if mapping_kind == "pairs" else target
                        selection = [] if mapping_kind == "merge" else ["--source-db=" + source]
                        run_tool(dst, "myloader", directory, *selection,
                                 "--database=" + mapping, "--skip-definer", "--ignore-set=SQL_LOG_BIN",
                                 "--checksum=warn", user="view_map_2323")
                        for name in views:
                            actual = sorted(sql(dst, f"SELECT * FROM {target}.{name}").splitlines())
                            assert expected[name] == actual, (src[0], dst[0], name, expected[name], actual)
                        definition = sql(dst, f"SHOW CREATE VIEW {target}.user_order_summary")
                        wanted_external = {"pairs": external_target, "single": external, "merge": target}[mapping_kind]
                        assert wanted_external in definition, definition
                        sql(dst, f"INSERT INTO {target}.orders VALUES (4, 1)")
                        changed = sql(dst, f"SELECT order_count FROM {target}.user_order_summary WHERE id=1")
                        assert changed == "3", ("view still reads source data", changed)
                        restores += 1
                        print(f"PASS {src[0]} -> {dst[0]}: {len(views)} views, ANSI={ansi}, GZIP={compressed}, mapping={mapping_kind}", flush=True)
    print(f"PASS {restores} restores; {restores * len(views)} view result comparisons", flush=True)
finally:
    for server in servers:
        sql(server, f"DROP DATABASE IF EXISTS {source}; DROP DATABASE IF EXISTS {target};"
                    f"DROP DATABASE IF EXISTS {external}; DROP DATABASE IF EXISTS {external_target};"
                    "DROP USER IF EXISTS 'view_map_2323'@'%';"
                    f"SET GLOBAL sql_mode='{original_modes[server[0]]}';")
