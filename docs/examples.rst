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

  mydumper --regex '^(?!(mysql|test))'
