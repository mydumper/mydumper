Output Files
============

mydumper generates several files during the generation of the dump.  Many of
these are for the table data itself since every table has at least one file.

Metadata
--------
When a dump is executed a file called ``.metadata`` is created in the output
directory.  This contains the start and end time of the dump as well as the
master binary log positions if applicable.

This is an example of the content of this file::

  Started dump at: 2011-05-05 13:57:17
  SHOW MASTER STATUS:
    Log: linuxjedi-laptop-bin.000001
    Pos: 106

  Finished dump at: 2011-05-05 13:57:17

Table Data
----------
The data from every table is written into a separate file, also if the 
:option:`--rows <mydumper --rows>` option is used then each chunk of table will
be in a separate file.  The file names for this are in the format::

  database.table.sql(.gz)

or if chunked::

  database.table.chunk.sql(.gz)

Where 'chunk' is a number padded with up to 5 zeros.

Table Schemas
-------------
When the :option:`--schemas <mydumper --schemas>` option is used mydumper will
create a file for the schema of every table it is writing data for.  The files
for this are in the following format::

  database.table-schema.sql(.gz)

Binary Logs
-----------
Binary logs are retrieved when :option:`--binlogs <mydumper --binlogs>` option
has been set.  This will store them in the the location specified in the
:option:`--binlog-outdir <mydumper --binlog-outdir>` option, by default this is
the ``binlogs/`` sub-directory inside the dump directory.

The binary log files have the same filename as the MySQL server that supplies them and will also have a .gz on the end if they are compressed.


