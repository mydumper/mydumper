#!/usr/bin/env python3
"""Test global rollback on disposable MySQL/MariaDB containers only.

Usage: MYSQL_PWD=... python3 test/test_global_variables_rollback.py MYSQL_CONTAINER MARIA_CONTAINER BUILD_CONTAINER BUILD_DIR
The build container must share the servers' network and provide writable /work.
Only the global_rollback_fixture/global_rollback_target databases are used.
Global timeout/connection settings are restored after the test, including failure.
"""
import os
import shlex
import subprocess
import sys
import time

mysql, maria, builder, binaries = sys.argv[1:]
password = os.environ["MYSQL_PWD"]
database, target = "global_rollback_fixture", "global_rollback_target"
variables = ("net_write_timeout", "net_read_timeout", "max_connections")


def run(args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          timeout=30, **kwargs).stdout.strip()


def client(server):
    return ["docker", "exec", "-i", "-e", "MYSQL_PWD", server[0], server[1],
            "-uroot", "--batch", "--skip-column-names", "--unbuffered"]


def sql(server, query):
    return run(client(server), input=query + ";\n")


def globals_now(server):
    return sql(server, "SELECT " + ",".join("@@GLOBAL." + v for v in variables))


def wait_for_blocked_flush(server, process):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        assert process.poll() is None, "dump exited before reaching the lock wait"
        count = sql(server, "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
                    "WHERE ID<>CONNECTION_ID() AND (INFO LIKE 'FLUSH%' "
                    "OR INFO LIKE 'BACKUP STAGE%')")
        if int(count):
            return
        time.sleep(0.1)
    raise AssertionError("dump did not reach the lock wait")


for server in ((mysql, "mysql"), (maria, "mariadb")):
    original = globals_now(server).split("\t")
    try:
        sql(server, f"DROP DATABASE IF EXISTS {database}; CREATE DATABASE {database}; "
                    f"CREATE TABLE {database}.t(id INT PRIMARY KEY); "
                    f"INSERT INTO {database}.t VALUES (1),(2),(3)")
        print(server[0], sql(server, "SELECT VERSION()"), flush=True)
        for mode in ("normal", "TERM", "INT"):
            sql(server, "SET GLOBAL net_write_timeout=60; SET GLOBAL net_read_timeout=30")
            expected = globals_now(server)
            root = "/work/global-rollback-" + server[0] + "-" + mode
            config = ("[mydumper_global_variables]\nnet_write_timeout=77\n"
                      "net_read_timeout=88\nmax_connections=211\n"
                      "[mydumper_session_variables]\nnet_read_timeout=321\n"
                      "[myloader_global_variables]\nnet_write_timeout=77\n"
                      "net_read_timeout=88\nmax_connections=211\n"
                      "[myloader_session_variables]\nnet_read_timeout=321\n")
            run(["docker", "exec", "-i", builder, "sh", "-c",
                 'mkdir -p "$1"; cat > "$1/config.cnf"', "sh", root], input=config)
            blocker = process = None
            try:
                if mode != "normal":
                    blocker = subprocess.Popen(client(server), stdin=subprocess.PIPE,
                                               stdout=subprocess.PIPE, text=True)
                    blocker.stdin.write(f"LOCK TABLES {database}.t WRITE; SELECT 'locked';\n")
                    blocker.stdin.flush()
                    assert blocker.stdout.readline().strip() == "locked"
                command = [binaries + "/mydumper", "--host=" + server[0], "--user=root",
                           "--password=" + password, "--database=" + database, "--threads=2",
                           "--defaults-extra-file=" + root + "/config.cnf", "--data-checksums",
                           "--outputdir=" + root + "/dump", "--clear", "--long-query-guard=0",
                           "--logfile=" + root + "/tool.log", "--verbose=3"]
                shell = 'echo $$ > "$1/pid"; exec ' + shlex.join(command)
                process = subprocess.Popen(["docker", "exec", builder, "sh", "-c", shell, "sh", root],
                                           stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True)
                if mode != "normal":
                    wait_for_blocked_flush(server, process)
                    assert globals_now(server) == "77\t88\t211"
                    pid = run(["docker", "exec", builder, "cat", root + "/pid"])
                    run(["docker", "exec", builder, "kill", "-" + mode, pid])
                    time.sleep(0.2)
                    blocker.stdin.close()
                    blocker.wait(timeout=5)
                output, error = process.communicate(timeout=20)
                assert process.returncode == (0 if mode == "normal" else 1), (output, error)
                actual = globals_now(server)
                assert actual == expected, (server[0], mode, "restored globals", actual, "expected", expected)
                run(["docker", "exec", builder, "test", "-s", root + "/tool.log"])
                metadata = "metadata" if mode == "normal" else "metadata.partial"
                run(["docker", "exec", builder, "test", "-f", root + "/dump/" + metadata])
                sql(server, f"SET SESSION lock_wait_timeout=2; CREATE TABLE {database}.probe(id INT); "
                            f"DROP TABLE {database}.probe")
                print("PASS", server[0], "mydumper", mode, "globals", actual, flush=True)
                if mode == "normal":
                    sql(server, f"DROP DATABASE IF EXISTS {target}; CREATE DATABASE {target}")
                    run(["docker", "exec", builder, binaries + "/myloader",
                         "--host=" + server[0], "--user=root", "--password=" + password,
                         "--directory=" + root + "/dump", "--database=" + target,
                         "--source-db=" + database, "--threads=2", "--checksum=fail",
                         "--defaults-extra-file=" + root + "/config.cnf"])
                    assert globals_now(server) == expected, (server[0], "myloader", globals_now(server), expected)
                    assert sql(server, f"SELECT COUNT(*) FROM {target}.t") == "3"
                    print("PASS", server[0], "myloader normal", flush=True)
            finally:
                if blocker and blocker.poll() is None:
                    blocker.stdin.close()
                    blocker.wait(timeout=5)
                if process and process.poll() is None:
                    pid = run(["docker", "exec", builder, "cat", root + "/pid"])
                    # Releasing the blocker can let the dump exit before kill reaches it.
                    subprocess.run(["docker", "exec", builder, "kill", "-KILL", pid],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                    process.communicate(timeout=5)
    finally:
        sql(server, ";".join(f"SET GLOBAL {name}={value}" for name, value in zip(variables, original)) +
            f";DROP DATABASE IF EXISTS {database}; DROP DATABASE IF EXISTS {target}")
