/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

#ifndef _src_mydumper_mydumper_jobs_h
#define _src_mydumper_mydumper_jobs_h

#include <stdio.h>
#include <glib.h>

struct db_table;
struct database;
struct thread_data;

enum job_type
{
  JOB_SHUTDOWN,
  JOB_RESTORE,
  JOB_DUMP,
  JOB_DUMP_NON_INNODB,
  JOB_DEFER,
  JOB_DETERMINE_CHUNK_TYPE,
  JOB_TABLE,
  JOB_CHECKSUM,
  JOB_SCHEMA,
  JOB_VIEW,
  JOB_SEQUENCE,
  JOB_TRIGGERS,
  JOB_SCHEMA_TRIGGERS,
  JOB_SCHEMA_POST,
  JOB_BINLOG,
  JOB_CREATE_DATABASE,
  JOB_CREATE_TABLESPACE,
  JOB_DUMP_DATABASE,
  JOB_DUMP_ALL_DATABASES,
  JOB_DUMP_TABLE_LIST,
  JOB_WRITE_SOURCE_AND_REPLICA_STATUS
};

struct job
{
  enum job_type type;
  void         *job_data;
};

struct schema_metadata_job
{
  FILE   *metadata_file;
  GMutex *release_binlog_mutex;
};

struct schema_job
{
  struct db_table *dbt;
  char            *filename;
};

struct sequence_job
{
  struct db_table *dbt;
  char            *filename;
};

struct table_checksum_job
{
  struct db_table *dbt;
  char            *filename;
};

struct create_tablespace_job
{
  char *filename;
};

struct database_job
{
  struct database *database;
  char            *filename;
};

struct view_job
{
  struct db_table *dbt;
  char            *tmp_table_filename;
  char            *view_filename;
};

void initialize_jobs();
void do_JOB_CREATE_DATABASE(struct thread_data *td, struct job *job);
void do_JOB_CREATE_TABLESPACE(struct thread_data *td, struct job *job);
void do_JOB_SCHEMA_POST(struct thread_data *td, struct job *job);
void do_JOB_VIEW(struct thread_data *td, struct job *job);
void do_JOB_SEQUENCE(struct thread_data *td, struct job *job);
void do_JOB_SCHEMA(struct thread_data *td, struct job *job);
void do_JOB_TRIGGERS(struct thread_data *td, struct job *job);
void do_JOB_SCHEMA_TRIGGERS(struct thread_data *td, struct job *job);
void do_JOB_CHECKSUM(struct thread_data *td, struct job *job);

#endif
