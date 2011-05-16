Myloader Usage
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

.. option:: --help, -?

   Show help text

.. option:: --host, -h

   Hostname of MySQL server to connect to (default localhost)

.. option:: --user, -u

   MySQL username with the correct privileges to execute the restoration

.. option:: --password, -p

   The corresponding password for the MySQL user

.. option:: --port, -P

   The port for the MySQL connection.

   .. note::

      For localhost TCP connections use 127.0.0.1 for :option:`--host`.

.. option:: --socket, -S

   The UNIX domain socket file to use for the connection

.. option:: --threads, -t

   The number of threads to use for restoring data, default is 4

.. option:: --version, -V

   Show the program version and exit

.. option:: --compress-protocol, -C

   Use client protocol compression for connections to the MySQL server

.. option:: --directory, -d

   The directory of the mydumper backup to restore

.. option:: --queries-per-transaction, -q

   Number of INSERT queries to execute per transaction during restore, default
   is 1000.

.. option:: --overwrite-tables, -o

   Drop any existing tables when restoring schemas
