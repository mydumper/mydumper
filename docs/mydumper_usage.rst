Mydumper Usage
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

.. option:: --help, -?

   Show help text

.. option:: --defaults-file
   
   Use the given option file. If the file does not exist or is otherwise inaccessible, no failure occurs

.. option:: --host, -h

   Hostname of MySQL server to connect to (default localhost)

.. option:: --user, -u

   MySQL username with the correct privileges to execute the dump

.. option:: --password, -p

   The corresponding password for the MySQL user

.. option:: --port, -P

   The port for the MySQL connection.

   .. note::

      For localhost TCP connections use 127.0.0.1 for :option:`--host`.

.. option:: --socket, -S

   The UNIX domain socket file to use for the connection

.. option:: --database, -B

   Database to dump

.. option:: --tables-list, -T

   A comma separated list of tables to dump

.. option:: --threads, -t

   The number of threads to use for dumping data, default is 4

   .. note::

      Other threads are used in mydumper, this option does not control these

.. option:: --outputdir, -o

   Output directory name, default is export-YYYYMMDD-HHMMSS

.. option:: --statement-size, -s

   The maximum size for an insert statement before breaking into a new
   statement, default 1,000,000 bytes

.. option:: --rows, -r

   Split table into chunks of this many rows, default unlimited

.. option:: --compress, -c

   Compress the output files

.. option:: --compress-input, -C

   Use client protocol compression for connections to the MySQL server

.. option:: --build-empty-files, -e

   Create empty dump files if there is no data to dump

.. option:: --regex, -x

   A regular expression to match against database and table

.. option:: --omit-from-file, -O

   File containing a list of database.table entries to skip, one per line; the
   skipped entries have precedence over patterns specified by the regex option

.. option:: --ignore-engines, -i

   Comma separated list of storage engines to ignore

.. option:: --no-schemas, -m

   Do not dump schemas with the data
   
.. option:: --no-data, -d

   Do not dump table data
   
.. option:: --triggers, -G

   Dump triggers

.. option:: --events, -E

   Dump events

.. option:: --routines, -R

   Dump stored procedures and functions

.. option:: --no-views, -W

   Do not dump views

.. option:: --long-query-guard, -l

   Timeout for long query execution in seconds, default 60

.. option:: --kill-long-queries, -K

   Kill long running queries instead of aborting the dump

.. option:: --version, -V

   Show the program version and exit

.. option:: --verbose, -v

   The verbosity of messages.  0 = silent, 1 = errors, 2 = warnings, 3 = info.
   Default is 2.

.. option:: --binlogs, -b

   Get the binlogs from the server as well as the dump files (You need to compile with -DWITH_BINLOG=ON)

.. option::  --daemon, -D

   Enable daemon mode

.. option:: --snapshot-interval, -I

   Interval between each dump snapshot (in minutes), requires
   :option:`--daemon`, default 60 (minutes)

.. option:: --logfile, -L

   A file to log mydumper output to instead of console output.  Useful for
   daemon mode.

.. option:: --no-locks, -k

   Do not execute the temporary shared read lock.

   .. warning::
      
      This will cause inconsistent backups.

.. option:: --no-backup-locks

	Do not use Percona Backup Locks

.. option:: --[skip-]tz-utc

   SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data 
   when a server has data in different time zones or data is being moved 
   between servers with different time zones, defaults to on use --skip-tz-utc 
   to disable.

.. option:: --less-locking

   Minimize locking time on InnoDB tables grabbing a LOCK TABLE ... READ 
   on all non-innodb tables.

.. option:: --chunk-filesize -F

   Split tables into chunks of this output file size. This value is in MB

.. option:: --success-on-1146

   Not increment error count and Warning instead of Critical in case of table doesn't exist

.. option:: --use-savepoints

   Use savepoints to reduce metadata locking issues, needs SUPER privilege
   
.. option:: --complete-insert

   Use complete INSERT statements that include column names.
