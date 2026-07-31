[![CircleCI](https://circleci.com/gh/mydumper/mydumper/tree/master.svg?style=svg)](https://circleci.com/gh/mydumper/mydumper/tree/master)

 # What is MyDumper?
MyDumper is a MySQL Logical Backup Tool. It has 2 tools:
* `mydumper` which is responsible to export a consistent backup of MySQL databases
* `myloader` reads the backup from mydumper, connects to the destination database and imports the backup.

Both tools use multithreading capabilities.
<br>MyDumper is Open Source and maintained by the community, it is not a Percona, MariaDB or MySQL product.

# Why do we need MyDumper?
* Parallelism (hence, speed) and performance (avoids expensive character set conversion routines, efficient code overall)
* Easier to manage output (separate files for tables, dump metadata, etc, easy to view/parse data)
* Consistency - maintains snapshot across all threads, provides accurate master and slave log positions, etc
* Manageability - supports PCRE for specifying database and tables inclusions and exclusions

# Documentation
[Official Documentation](https://mydumper.github.io/mydumper/) (work in progress)
[Machine JSON Logging](./README-machine-log.md)

## How to install mydumper/myloader?

[This sections has been migrated](https://mydumper.github.io/mydumper/docs/html/installing.html) 

## Dependencies for building MyDumper

[This sections has been migrated](https://mydumper.github.io/mydumper/docs/html/compiling.html) 

### Build Docker image
You can download the [official docker image](https://hub.docker.com/r/mydumper/mydumper) or you can build the Docker image either from local sources or directly from Github sources with [the provided Dockerfile](./docker/Dockerfile).
```shell
docker build --build-arg CMAKE_ARGS='-DWITH_ZSTD=ON' -t mydumper \
    https://github.com/mydumper/mydumper.git#master:docker
```
Keep in mind that the main purpose the Dockerfile addresses is development and build from source locally. It might not be optimal for distribution purposes, but can also work as a quick build and run solution with the above one-liner, though.

# How to use MyDumper

See [Usage](https://mydumper.github.io/mydumper/docs/html/mydumper_usage.html)

## How does consistent snapshot work?

This is all done following best MySQL practices and traditions:

* As a precaution, slow running queries on the server either abort the dump, or get killed
* Global read lock is acquired ("FLUSH TABLES WITH READ LOCK")
* Various metadata is read ("SHOW SLAVE STATUS","SHOW MASTER STATUS")
* Other threads connect and establish snapshots ("START TRANSACTION WITH CONSISTENT SNAPSHOT")
** On pre-4.1.8 it creates a dummy InnoDB table, and reads from it.
* Once all worker threads announce the snapshot establishment, master executes "UNLOCK TABLES" and starts queueing jobs.

This for now does not provide consistent snapshots for non-transactional engines - support for that is expected in 0.2 :)

## How to exclude (or include) databases?

[This sections has been migrated](https://mydumper.github.io/mydumper/docs/html/examples.html#regex) 

## How to use --exec?

You can execute external commands with --exec like this:

```bash
 mydumper --exec "/usr/bin/gzip FILENAME"
```

--exec is single threaded, similar implementation than Stream. The exec program must be an absolute path. FILENAME will be replaced by the filename that you want to be processed. You can set FILENAME in any place as an argument.

## Defaults file

The default file (aka: --defaults-file parameter) is starting to be more important in MyDumper
- mydumper and myloader sections:

```bash
[mydumper]
host = 127.0.0.1
user = root
password = p455w0rd
database = db
rows = 10000
string-pk-planner = auto
string-pk-planner-timeout = 30
string-pk-planner-max-probes = 64
string-pk-planner-max-prefixes = 256
string-pk-planner-min-rows = 1000000
string-pk-planner-target-rows-per-prefix = 0

[myloader]
host = 127.0.0.1
user = root
password = p455w0rd
database = new_db
optimize-keys = AFTER_IMPORT_PER_TABLE
```

- Variables for mydumper and myloader executions:

Prior to v0.14.0-1:
```bash
[mydumper_variables]
wait_timeout = 300
sql_mode = ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION

[myloader_variables]
long_query_time = 300
innodb_flush_log_at_trx_commit = 0
```
From to v0.14.0-1:
```bash
[mydumper_session_variables]
wait_timeout = 300
sql_mode = ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION

[mydumper_global_variables]
sync_binlog = 0
slow_query_log = OFF

[myloader_session_variables]
long_query_time = 300

[myloader_global_variables]
sync_binlog = 0
innodb_flush_log_at_trx_commit = 0
```

For Aurora/MySQL 5.7 restores where `SET SESSION SQL_LOG_BIN = 0` is rejected, use:

```bash
[myloader]
source-control-command = AWS
aws-session-command = CALL mysql.rds_disable_session_binlog()
```

`aws-session-command` is executed on every myloader connection after the normal session setup. You can repeat the option on multiple lines in the config file, or pass multiple statements separated by `;\n`.
When `--source-control-command=AWS` is set, `--enable-binlog` is ignored and myloader uses the AWS session binlog call instead.
See [`myloader.cnf`](/Users/daniel/gitRepos/mydumper/myloader.cnf) for a complete sample.

Examples:

```ini
[myloader]
source-control-command = AWS
aws-session-command = CALL mysql.rds_disable_session_binlog()
aws-session-command = SET SESSION some_aws_setting = 1
aws-session-command = CALL mysql.some_other_aws_proc()
```

```bash
myloader \
  --source-control-command=AWS \
  --aws-session-command='CALL mysql.rds_disable_session_binlog()' \
  --aws-session-command='SET SESSION some_aws_setting = 1' \
  --aws-session-command='CALL mysql.some_other_aws_proc()'
```

For very large tables with string primary keys, `mydumper` has a bounded
metadata-assisted planner that seeds prefix-based root chunks before falling
back to the existing recursive splitter. The defaults keep the current
behavior as a safe fallback, but you can tune the planner with:

* `--string-pk-planner=auto|metadata|recursive`
* `--string-pk-planner-timeout=<seconds>`
* `--string-pk-planner-max-probes=<n>`
* `--string-pk-planner-max-prefixes=<n>`
* `--string-pk-planner-min-rows=<n>`
* `--string-pk-planner-target-rows-per-prefix=<n>`

The planner works with three orthogonal bounds:

* `--string-pk-planner-target-rows-per-prefix` sets the desired chunk size, in
  rows. When `0` (the default) the target is derived as
  `table_rows / --string-pk-planner-max-prefixes`; a positive value is used
  directly. The planner deepens prefixes (uses more leading characters) until
  each prefix's estimated row count is at or under this target.
* `--max-char-size` (default 2) caps how many leading characters a prefix may
  use, i.e. the maximum planning depth. A larger value allows finer chunks on
  skewed key distributions at the cost of more `EXPLAIN` probes before export.
* `--string-pk-planner-max-prefixes` caps the total number of prefix chunks.

The planner starts from single-character prefixes and deepens one character at
a time, expanding only the prefixes that are still over target. If deepening
would exceed `--string-pk-planner-max-prefixes`, it keeps the shallowest level
that still fits (graceful degradation): coverage stays complete and the table
is never collapsed to a single chunk. The row targeting is best-effort because
depth is chosen from `EXPLAIN` estimates; skewed data or a single dominant
primary-key value may leave some chunks above the target.

The `String PK planner selected metadata-assisted prefix chunks ...` log line
reports the achieved root count, prefix length, and effective target so you can
confirm the planner produced the parallelism you expect.

Tuning for a very large (e.g. 30TB) table: set
`--string-pk-planner=metadata`, pick a `--string-pk-planner-target-rows-per-prefix`
that matches your desired per-chunk size (or leave it `0` and size via
`--string-pk-planner-max-prefixes`), and raise `--max-char-size` to `2` or `3`
if a single leading character does not spread the key space enough. Keep
`--string-pk-planner-timeout` and `--string-pk-planner-max-probes` at sane
non-zero values: setting both to `0` removes all planning-cost bounds, and on a
30TB table with a skewed key that can lead to a very large number of `EXPLAIN`
probes before export begins.

- Per table sections:
```bash
[`db`.`table`]
where = column > 20
limit = 10000

[`myd_test`.`t`]
columns_on_select=qty,price+20
columns_on_insert=qty,price
```

IMPORTANT: when using options that don't require an argument like: --no-data or --events, you need to set any value to those variables which will always indicate: TRUE/ON/ENABLE. It is a MISCONCEPTION if you think that adding `--no-data=0` will export data:
```
[mydumper]
no-data=0
```
Will NOT export the data as no-data is being specified.


# Misc

## Versioning

mydumper is versioned MAJOR.MINOR.PATCH-revision.

Even patch versions indicate a pre-release. [More info](https://github.com/mydumper/mydumper/wiki/Versioning).
