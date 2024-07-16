myloader Usage
==============

Synopsis
--------

:program:`myloader` :option:`--directory <myloader --directory>` = /path/to/mydumper/backup [:ref:`OPTIONS <myloader-options-label>`]

Description
-----------

:program:`myloader` is a tool used for multi-threaded restoration of mydumper
backups.

.. _myloader-options-label:

Options
-------

The :program:`myloader` tool has several available options:

.. program:: myloader

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

.. option:: -s, --source-db

  Database to restore

.. option:: --skip-triggers

  Do not import triggers. By default, it imports triggers

.. option:: --skip-post

  Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions

.. option:: --no-data

  Do not dump or import table data

.. option:: -O, --omit-from-file

  File containing a list of database[.table] entries to skip, one per line (skips before applying regex option)

.. option:: -T, --tables-list

  Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2

PMM Options
-----------
.. option:: --pmm-path

  which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution

.. option:: --pmm-resolution

  which default will be high

Execution Options
-----------------
.. option:: -e, --enable-binlog

  Enable binary logging of the restore data

.. option:: --innodb-optimize-keys

  Creates the table without the indexes and it adds them at the end. Options: AFTER_IMPORT_PER_TABLE and AFTER_IMPORT_ALL_TABLES. Default: AFTER_IMPORT_PER_TABLE

.. option:: --purge-mode

  This specify the truncate mode which can be: NONE, DROP, TRUNCATE and DELETE

.. option:: --disable-redo-log

  Disables the REDO_LOG and enables it after, doesn't check initial status

.. option:: -o, --overwrite-tables

  Drop tables if they already exist

.. option:: --serialized-table-creation

  Table recreation will be executed in series, one thread at a time. This means --max-threads-for-schema-creation=1. This option will be removed in future releases

.. option:: --stream

  It will receive the stream from STDIN and creates the file in the disk before start processing. Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given

Threads Options
---------------
.. option:: --max-threads-per-table

  Maximum number of threads per table to use, default 4

.. option:: --max-threads-per-table-hard

  Maximum hard number of threads per table to use, we are not going to use more than this amount of threads per table, default 4

.. option:: --max-threads-for-index-creation

  Maximum number of threads for index creation, default 4

.. option:: --max-threads-for-schema-creation

  Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation, default 4

.. option:: --exec-per-thread

  Set the command that will receive by STDIN from the input file and write in the STDOUT

.. option:: --exec-per-thread-extension

  Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored

Statement Options
-----------------
.. option:: -r, --rows

  Split the INSERT statement into this many rows.

.. option:: -q, --queries-per-transaction

  Number of queries per transaction, default 1000

.. option:: --append-if-not-exist

  Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented

.. option:: --set-names

  Sets the names, use it at your own risk, default binary

.. option:: --skip-definer

  Removes DEFINER from the CREATE statement. By default, statements are not modified

Application Options
-------------------
.. option:: -?, --help

  Show help options

.. option:: -d, --directory

  Directory of the dump to import

.. option:: -L, --logfile

  Log file name to use, by default stdout is used

.. option:: -B, --database

  An alternative database to restore into

.. option:: --resume

  Expect to find resume file in backup dir and will only process those files

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
