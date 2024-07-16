Output Files
============

mydumper generates several files during the generation of the dump.  Many of
these are for the table data itself since every table has at least one file.

Metadata
--------
When a dump is executed a file called ``metadata.partial`` is created in the output
directory and is renamed to ``metadata`` when mydumper finish without error.  
This contains the start and end time of the dump as well as the
master binary log positions if applicable.

This is an example of the content of this file::

  Started dump at: 2011-05-05 13:57:17
  SHOW MASTER STATUS:
    Log: linuxjedi-laptop-bin.000001
    Pos: 106

  Finished dump at: 2011-05-05 13:57:17
  
Since version 0.14.1-1 format has been changed to::

  # Started dump at: 2023-06-09 11:47:18
  [master]
  # Channel_Name = '' # It can be use to setup replication FOR CHANNEL
  File = mydumper1-bin.000017
  Position = 241149225
  Executed_Gtid_Set = 7b166a41-65a2-11ed-9de3-0800275ff74d:1-147115,7b166a41-65a2-11ed-9de3-0800275ff74e:1-61558

  [`sakila`.`store`]
  Rows = 2
  data_checksum = 3119812626
  schema_checksum = B7B99B4C
  indexes_checksum = B4D31E3

  [`sakila`]
  schema_checksum = FDF2173B
  post_checksum = 42085F07
  # Finished dump at: 2023-06-09 11:47:18
  

Table Data
----------
The data from every table is written into a separate file, also if the
:option:`--rows <mydumper --rows>` option is used then each chunk of table will
be in a separate file.  The file names for this are in the format::

  database.table.sql(.gz|.zst)

or if chunked::

  database.table.chunk.sql(.gz|.zst)

Where 'chunk' is a number padded with up to 5 zeros or:
  database.table.chunk.chunk2.sql(.gz|.zst)

Where 'chunk2' is a number padded with up to 5 zeros.


Table Schemas
-------------
As long as the :option:`--no-schemas <mydumper --no-schemas>` option is not specified, mydumper will
create a file for the schema of every table it is writing data for.  The files
for this are in the following format::

  database.table-schema.sql(.gz|.zst)

Compression (since 0.15.1-1)
----------------------------
By default, mydumper is not compressing backup. In order to compress the file you need to use -c which 
by default is going to use GZIP compression method. You can also use ZSTD to compress your backups.
The internal compression mechanisim has been removed from the code and we are using /usr/bin/gzip and 
/usr/bin/zstd. If you need to change to a different location or different compression software, you
need to set::

  --exec-per-thread
  --exec-per-thread-extension

For example::

  mydumper -o data --clear -T sakila.film --exec-per-thread="/usr/bin/bzip2" --exec-per-thread-extension=".bz2"
  -rw-r--r-- 1 circleci circleci   520 Jul 16 13:59 metadata
  -rw-r----- 1 circleci circleci 11500 Jul 16 13:59 sakila.film.00000.sql.bz2
  -rw-r----- 1 circleci circleci 11360 Jul 16 13:59 sakila.film.00001.sql.bz2
  -rw-r----- 1 circleci circleci   758 Jul 16 13:59 sakila.film-schema.sql.bz2
  -rw-r----- 1 circleci circleci   322 Jul 16 13:59 sakila-schema-create.sql.bz2
  myloader -d data -o --exec-per-thread="/usr/bin/bzip2 -d" --exec-per-thread-extension=".bz2"

Binary Logs
-----------
Binary logs are retrieved when :option:`--enable-binlog <myloader --enable-binlog>` option
has been set.  This will store them in the ``binlog_snapshot/`` sub-directory
inside the dump directory.

The binary log files have the same filename as the MySQL server that supplies them and will also have a .gz on the end if they are compressed.

Daemon mode
-----------
Daemon mode does things a little differently.  There are the directories ``0``
and ``1`` inside the dump directory.  These alternate when dumping so that if
mydumper fails for any reason there is still a good snapshot.  When a snapshot
dump is complete the ``last_dump`` symlink is updated to point to that dump.

If binary logging is enabled mydumper will connect as if it is a slave server
and constantly retreives the binary logs into the ``binlogs`` subdirectory.
