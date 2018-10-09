Examples
========

Simple Usage
------------
Just running :program:`mydumper` without any options will try to connect to a
server using the default socket path.  It will then dump the tables from all
databases using 4 worker threads.

Regex
-----
To use :program:`mydumper`'s regex feature simply use the
:option:`--regex <mydumper --regex>` option.  In the following example mydumper
will ignore the ``test`` and ``mysql`` databases::

  mydumper --regex '^(?!(mysql\.|test\.))'

Restoring a dump
----------------
Mydumper now include myloader which is a multi-threaded restoration tool.  To
use myloader with a mydumper dump you simply need to pass it the directory of
the dump along with a user capable of restoring the schemas and data.  As an
example the following will restore a dump overwriting any existing tables::

  myloader --directory=export-20110614-094953 --overwrite-tables --user=root

Daemon mode
-----------
Mydumper has a daemon mode which will snapshot the dump data every so often
whilst continuously retreiving the binary log files.  This gives a continuous
consistent backup right up to the point where the database server fails.  To use
this you simply need to use the :option:`--daemon <mydumper --daemon>` option.

In the following example mydumper will use daemon mode, creating a snapshot
every half an hour and log to an output file::

  mydumper --daemon --snapshot-interval=30 --logfile=dump.log
