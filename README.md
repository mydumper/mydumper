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

