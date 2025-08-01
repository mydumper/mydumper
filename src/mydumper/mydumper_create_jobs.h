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


#ifndef _src_mydumper_create_jobs_h
#define _src_mydumper_create_jobs_h

enum job_type {
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
  JOB_WRITE_MASTER_STATUS
};

struct dump_table_job{
  gboolean is_view;
  gboolean is_sequence;
  struct database *database;
  gchar *table;
  gchar *collation;
  gchar *engine;
};

struct dump_database_job {
  struct database *database;
};

struct restore_job {
  char *database;
  char *table;
  char *filename;
};

struct dump_table_list_job{
  gchar **table_list;
};

struct binlog_job {
  char *filename;
  guint64 start_position;
  guint64 stop_position;
};

struct table_job_file{
  gchar *filename;
  int file;
};

// directory / database . table . first number . second number . extension
// first number : used when rows is used
// second number : when load data is used
struct table_job {
  char *partition;
  guint64 part;
  guint sub_part;
  GString *where;
  struct chunk_step_item *chunk_step_item;
  struct db_table *dbt;
//  gchar *sql_filename;
//  int sql_file;
//  gchar *dat_filename;
//  int dat_file;
  struct table_job_file *sql;
  struct table_job_file *rows;
  gchar *exec_out_filename;
  float filesize;
  guint st_in_file;
  int child_process;
  int char_chunk_part;
  struct thread_data *td;
  guint64 num_rows_of_last_run;
};

#endif

struct table_job * new_table_job(struct db_table *dbt, char *partition, guint64 part, struct chunk_step_item *chunk_step_item);
void create_job_to_dump_chunk(struct db_table *dbt, char *partition, guint64 part, struct chunk_step_item *chunk_step_item, void f(), GAsyncQueue *queue);
void create_job_defer(struct db_table *dbt, GAsyncQueue *queue);

void create_job_to_determine_chunk_type(struct db_table *dbt, void f(), GAsyncQueue *queue);
void free_table_job(struct table_job *tj);
struct job * create_job_to_dump_chunk_without_enqueuing(struct db_table *dbt, char *partition, guint64 part, char *order_by, struct chunk_step_item *chunk_step_item);

void create_job_to_dump_metadata(struct configuration *conf, FILE *mdfile);
void create_job_to_dump_tablespaces(struct configuration *conf);
void create_job_to_dump_post(struct database *database, struct configuration *conf);
void create_job_to_dump_table_schema(struct db_table *dbt, struct configuration *conf);
void create_job_to_dump_view(struct db_table *dbt, struct configuration *conf);
void create_job_to_dump_sequence(struct db_table *dbt, struct configuration *conf);
void create_job_to_dump_checksum(struct db_table * dbt, struct configuration *conf);
void create_job_to_dump_all_databases(struct configuration *conf);
void create_job_to_dump_database(struct database *database, struct configuration *conf);
void create_job_to_dump_schema(struct database* database, struct configuration *conf);
void create_job_to_dump_triggers(MYSQL *conn, struct db_table *dbt, struct configuration *conf);
void create_job_to_dump_schema_triggers(struct database *database, struct configuration *conf);
void create_job_to_dump_table(struct configuration *conf, gboolean is_view, gboolean is_sequence, struct database *database, gchar *table, gchar *collation, gchar *engine);
void create_job_to_dump_table_list(gchar **table_list, struct configuration *conf);
