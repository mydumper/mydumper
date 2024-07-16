mydumper Usage
==============

Synopsis
--------

:program:`mydumper` [:ref:`OPTIONS <mydumper-options-label>`]

Description
-----------

:program:`mydumper` is a tool used for backing up MySQL database servers much
faster than the mysqldump tool distributed with MySQL.  It also has the
capability to retrieve the binary logs from the remote server at the same time
as the dump itself.  The advantages of mydumper are:

  * Parallelism (hence, speed) and performance (avoids expensive character set conversion routines, efficient code overall)
  * Easier to manage output (separate files for tables, dump metadata, etc, easy to view/parse data)
  * Consistency - maintains snapshot across all threads, provides accurate master and slave log positions, etc
  * Manageability - supports PCRE for specifying database and tables inclusions and exclusions

.. _mydumper-options-label:

Options
-------

The :program:`mydumper` tool has several available options:

.. program:: mydumper

Connection Options
------------------
.. option:: -h, --host

  The host to connect to

.. option:: -u, --user

  Username with the necessary privileges

.. option:: -p, --password

  User password

.. option:: -a, --ask-password

  Prompt For User password

.. option:: -P, --port

  TCP/IP port to connect to

.. option:: -S, --socket

  UNIX domain socket file to use for connection

.. option:: -C, --compress-protocol

  Use compression on the MySQL connection

.. option:: --ssl

  Connect using SSL

.. option:: --ssl-mode

  Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY

.. option:: --key

  The path name to the key file

.. option:: --cert

  The path name to the certificate file

.. option:: --ca

  The path name to the certificate authority file

.. option:: --capath

  The path name to a directory that contains trusted SSL CA certificates in PEM format

.. option:: --cipher

  A list of permissible ciphers to use for SSL encryption

.. option:: --tls-version

  Which protocols the server permits for encrypted connections

Filter Options
--------------
.. option:: -x, --regex

  Regular expression for 'db.table' matching

.. option:: -B, --database

  Database to dump

.. option:: -i, --ignore-engines

  Comma delimited list of storage engines to ignore

.. option:: --where

  Dump only selected records.

.. option:: -U, --updated-since

  Use Update_time to dump only tables updated in the last U days

.. option:: --partition-regex

  Regex to filter by partition name.

.. option:: -O, --omit-from-file

  File containing a list of database[.table] entries to skip, one per line (skips before applying regex option)

.. option:: -T, --tables-list

  Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2

Lock Options
------------
.. option:: -z, --tidb-snapshot

  Snapshot to use for TiDB

.. option:: -k, --no-locks

  Do not execute the temporary shared read lock.

  WARNING: This will cause inconsistent backups

.. option:: --use-savepoints

  Use savepoints to reduce metadata locking issues, needs SUPER privilege

.. option:: --no-backup-locks

  Do not use Percona backup locks

.. option:: --lock-all-tables

  Use LOCK TABLE for all, instead of FTWRL

.. option:: --less-locking

  Minimize locking time on InnoDB tables.

.. option:: --trx-consistency-only

  Transactional consistency only

PMM Options
-----------
.. option:: --pmm-path

  which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution

.. option:: --pmm-resolution

  which default will be high

Exec Options
------------
.. option:: --exec-threads

  Amount of threads to use with --exec

.. option:: --exec

  Command to execute using the file as parameter

.. option:: --exec-per-thread

  Set the command that will receive by STDIN and write in the STDOUT into the output file

.. option:: --exec-per-thread-extension

  Set the extension for the STDOUT file when --exec-per-thread is used

If long query running found
---------------------------
.. option:: --long-query-retries

  Retry checking for long queries, default 0 (do not retry)

.. option:: --long-query-retry-interval

  Time to wait before retrying the long query check in seconds, default 60

.. option:: -l, --long-query-guard

  Set long query timer in seconds, default 60

.. option:: -K, --kill-long-queries

  Kill long running queries (instead of aborting)

Job Options
-----------
.. option:: --max-rows

  Limit the number of rows per block after the table is estimated, default 1000000. It has been deprecated, use --rows instead. Removed in future releases

.. option:: --char-deep



.. option:: --char-chunk



.. option:: -r, --rows

  Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds

.. option:: --split-partitions

  Dump partitions into separate files. This options overrides the --rows option for partitioned tables.

Checksum Options
----------------
.. option:: -M, --checksum-all

  Dump checksums for all elements

.. option:: --data-checksums

  Dump table checksums with the data

.. option:: --schema-checksums

  Dump schema table and view creation checksums

.. option:: --routine-checksums

  Dump triggers, functions and routines checksums

Objects Options
---------------
.. option:: -m, --no-schemas

  Do not dump table schemas with the data and triggers

.. option:: -Y, --all-tablespaces

  Dump all the tablespaces.

.. option:: -d, --no-data

  Do not dump table data

.. option:: -G, --triggers

  Dump triggers. By default, it do not dump triggers

.. option:: -E, --events

  Dump events. By default, it do not dump events

.. option:: -R, --routines

  Dump stored procedures and functions. By default, it do not dump stored procedures nor functions

.. option:: --views-as-tables

  Export VIEWs as they were tables

.. option:: -W, --no-views

  Do not dump VIEWs

Statement Options
-----------------
.. option:: --load-data



.. option:: --csv

  Automatically enables --load-data and set variables to export in CSV format.

.. option:: --fields-terminated-by



.. option:: --fields-enclosed-by



.. option:: --fields-escaped-by

  Single character that is going to be used to escape characters in theLOAD DATA stament, default: '\'

.. option:: --lines-starting-by

  Adds the string at the begining of each row. When --load-data is usedit is added to the LOAD DATA statement. Its affects INSERT INTO statementsalso when it is used.

.. option:: --lines-terminated-by

  Adds the string at the end of each row. When --load-data is used it isadded to the LOAD DATA statement. Its affects INSERT INTO statementsalso when it is used.

.. option:: --statement-terminated-by

  This might never be used, unless you know what are you doing

.. option:: -N, --insert-ignore

  Dump rows with INSERT IGNORE

.. option:: --replace

  Dump rows with REPLACE

.. option:: --complete-insert

  Use complete INSERT statements that include column names

.. option:: --hex-blob

  Dump binary columns using hexadecimal notation

.. option:: --skip-definer

  Removes DEFINER from the CREATE statement. By default, statements are not modified

.. option:: -s, --statement-size

  Attempted size of INSERT statement in bytes, default 1000000

.. option:: --tz-utc

  SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable.

.. option:: --skip-tz-utc



.. option:: --set-names

  Sets the names, use it at your own risk, default binary

Extra Options
-------------
.. option:: -F, --chunk-filesize

  Split tables into chunks of this output file size. This value is in MB

.. option:: --exit-if-broken-table-found

  Exits if a broken table has been found

.. option:: --success-on-1146

  Not increment error count and Warning instead of Critical in case of table doesn't exist

.. option:: -e, --build-empty-files

  Build dump files even if no data available from table

.. option:: --no-check-generated-fields

  Queries related to generated fields are not going to be executed.It will lead to restoration issues if you have generated columns

.. option:: --order-by-primary

  Sort the data by Primary Key or Unique key if no primary key exists

.. option:: -c, --compress

  Compress output files using: /usr/bin/gzip and /usr/bin/zstd. Options: GZIP and ZSTD. Default: GZIP

Daemon Options
--------------
.. option:: -D, --daemon

  Enable daemon mode

.. option:: -I, --snapshot-interval

  Interval between each dump snapshot (in minutes), requires --daemon, default 60

.. option:: -X, --snapshot-count

  number of snapshots, default 2

Application Options
-------------------
.. option:: -?, --help

  Show help options

.. option:: -o, --outputdir

  Directory to output files to

.. option:: --stream

  It will stream over STDOUT once the files has been written. Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given

.. option:: -L, --logfile

  Log file name to use, by default stdout is used

.. option:: --disk-limits

  Set the limit to pause and resume if determines there is no enough disk space.Accepts values like: '<resume>:<pause>' in MB.For instance: 100:500 will pause when there is only 100MB free and willresume if 500MB are available

.. option:: -t, --threads

  Number of threads to use, default 4

.. option:: -V, --version

  Show the program version and exit

.. option:: --identifier-quote-character

  This set the identifier quote character that is used to INSERT statements onlyon mydumper and to split statement on myloader. Use SQL_MODE to change theCREATE TABLE statementsPosible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK

.. option:: -v, --verbose

  Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, default 2

.. option:: --defaults-file

  Use a specific defaults file. Default: /etc/mydumper.cnf

.. option:: --defaults-extra-file

  Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
