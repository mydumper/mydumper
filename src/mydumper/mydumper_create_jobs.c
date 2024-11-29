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
#include <gio/gio.h>

#include "mydumper_start_dump.h"
#include "mydumper_common.h"
#include "mydumper_jobs.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_global.h"
#include "mydumper_arguments.h"

//
// Enqueueing in initial_queue
//

void create_job_to_dump_table(struct configuration *conf, gboolean is_view, gboolean is_sequence, struct database *database, gchar *table, gchar *collation, gchar *engine){
  struct job *j = g_new0(struct job, 1);
  struct dump_table_job *dtj= g_new0(struct dump_table_job, 1);
  dtj->is_view=is_view;
  dtj->is_sequence=is_sequence;
  dtj->database=database;
  dtj->table=table;
  dtj->collation=collation;
  dtj->engine=engine;
  j->job_data = dtj;
  j->type = JOB_TABLE;
  g_async_queue_push(conf->initial_queue, j);
}

void create_job_to_dump_metadata(struct configuration *conf, FILE *mdfile){
  struct job *j = g_new0(struct job, 1);
  j->job_data = (void *)mdfile;
  j->type = JOB_WRITE_MASTER_STATUS;
  g_async_queue_push(conf->initial_queue, j);
}

void create_job_to_dump_all_databases(struct configuration *conf) {
  g_atomic_int_inc(&database_counter);
  struct job *j = g_new0(struct job, 1);
  j->job_data = NULL;
  j->type = JOB_DUMP_ALL_DATABASES;
  g_async_queue_push(conf->initial_queue, j);
  return;
}

void create_job_to_dump_table_list(gchar **table_list, struct configuration *conf) {
  g_atomic_int_inc(&database_counter);
  struct job *j = g_new0(struct job, 1);
  struct dump_table_list_job *dtlj = g_new0(struct dump_table_list_job, 1);
  j->job_data = (void *)dtlj;
  dtlj->table_list = table_list;
  j->type = JOB_DUMP_TABLE_LIST;
  g_async_queue_push(conf->initial_queue, j);
  return;
}

void create_job_to_dump_database(struct database *database, struct configuration *conf) {
  g_atomic_int_inc(&database_counter);
  struct job *j = g_new0(struct job, 1);
  struct dump_database_job *ddj = g_new0(struct dump_database_job, 1);
  j->job_data = (void *)ddj;
  ddj->database = database;
  j->type = JOB_DUMP_DATABASE;
  g_async_queue_push(conf->initial_queue, j);
  return;
}

//
// Enqueueing in schema_queue
//

void create_job_to_dump_tablespaces(struct configuration *conf){
  struct job *j = g_new0(struct job, 1);
  struct create_tablespace_job *ctj = g_new0(struct create_tablespace_job, 1);
  j->job_data = (void *)ctj;
  j->type = JOB_CREATE_TABLESPACE;
  ctj->filename = build_tablespace_filename();
  g_async_queue_push(conf->schema_queue, j);
}

void create_database_related_job(struct database *database, struct configuration *conf, enum job_type type, const gchar *suffix) {
  struct job *j = g_new0(struct job, 1);
  struct database_job *dj = g_new0(struct database_job, 1);
  j->job_data = (void *)dj;
  dj->database = database;
  j->type = type;
  dj->filename = build_schema_filename(database->filename, suffix);
  dj->checksum_filename = schema_checksums;
  g_async_queue_push(conf->schema_queue, j);
  return;
}

void create_job_to_dump_table_schema(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct schema_job *sj = g_new0(struct schema_job, 1);
  j->job_data = (void *)sj;
  sj->dbt = dbt;
  j->type = JOB_SCHEMA;
  sj->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema");
  sj->checksum_filename=schema_checksums;
  sj->checksum_index_filename=schema_checksums;
  g_async_queue_push(conf->schema_queue, j);
}

void create_job_to_dump_schema(struct database *database, struct configuration *conf) {
  create_database_related_job(database, conf, JOB_CREATE_DATABASE, "schema-create");
}

void create_job_to_dump_post(struct database *database, struct configuration *conf) {
  create_database_related_job(database, conf, JOB_SCHEMA_POST, "schema-post");
}

//
// Enqueueing in post_data_queue
//

void create_job_to_dump_triggers(MYSQL *conn, struct db_table *dbt, struct configuration *conf) {
  char *query = NULL;
  MYSQL_RES *result = NULL;

  const char q= identifier_quote_character;
  query =
      g_strdup_printf("SHOW TRIGGERS FROM %c%s%c LIKE '%s'", q, dbt->database->name, q, dbt->escaped_table);
  if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
    g_critical("Error Checking triggers for %s.%s. Err: %s St: %s", dbt->database->name, dbt->table,
               mysql_error(conn),query);
    errors++;
  } else {
    if (mysql_num_rows(result)) {
      struct job *t = g_new0(struct job, 1);
      struct schema_job *st = g_new0(struct schema_job, 1);
      t->job_data = (void *)st;
      t->type = JOB_TRIGGERS;
      st->dbt = dbt;
      st->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-triggers");
      st->checksum_filename=routine_checksums;
      g_async_queue_push(conf->post_data_queue, t);
    }
  }
  g_free(query);
  if (result) {
    mysql_free_result(result);
  }
}

void create_job_to_dump_schema_triggers(struct database *database, struct configuration *conf) {
  struct job *t = g_new0(struct job, 1);
  struct database_job *st = g_new0(struct database_job, 1);
  t->job_data = (void *)st;
  t->type = JOB_SCHEMA_TRIGGERS;
  st->database = database;
  st->filename = build_schema_filename(database->filename, "schema-triggers");
  st->checksum_filename=routine_checksums;
  g_async_queue_push(conf->post_data_queue, t);
}

void create_job_to_dump_view(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct view_job *vj = g_new0(struct view_job, 1);
  j->job_data = (void *)vj;
  vj->dbt = dbt;
//  j->conf = conf;
  j->type = JOB_VIEW;
  vj->tmp_table_filename  = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema");
  vj->view_filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-view");
  vj->checksum_filename = schema_checksums;
  g_async_queue_push(conf->post_data_queue, j);
  return;
}

void create_job_to_dump_sequence(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct sequence_job *sj = g_new0(struct sequence_job, 1);
  j->job_data = (void *)sj;
  sj->dbt = dbt;
  j->type = JOB_SEQUENCE;
  sj->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-sequence");
  sj->checksum_filename=schema_checksums;
  g_async_queue_push(conf->post_data_queue, j);
  return;
}

void create_job_to_dump_checksum(struct db_table * dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct table_checksum_job *tcj = g_new0(struct table_checksum_job, 1);
  tcj->dbt=dbt;
  j->job_data = (void *)tcj;
  j->type = JOB_CHECKSUM;
  tcj->filename = build_meta_filename(dbt->database->filename, dbt->table_filename,"checksum");
  g_async_queue_push(conf->post_data_queue, j);
  return;
}

//
// Enqueueing in data tables queue
//

struct table_job * new_table_job(struct db_table *dbt, char *partition, guint64 nchunk, struct chunk_step_item *chunk_step_item){
  struct table_job *tj = g_new0(struct table_job, 1);
// begin Refactoring: We should review this, as dbt->database should not be free, so it might be no need to g_strdup.
  // from the ref table?? TODO
//  tj->database=dbt->database->name;
//  tj->table=g_strdup(dbt->table);
// end
//  g_message("new_table_job on %s.%s with nchuk: %"G_GUINT64_FORMAT, dbt->database->name, dbt->table,nchunk);
  tj->partition=g_strdup(partition);
  tj->chunk_step_item = chunk_step_item;
  tj->where=NULL;
  tj->nchunk=nchunk;
  tj->sub_part = 0;
  tj->rows=g_new0(struct table_job_file, 1);
  tj->rows->file = 0;
  tj->rows->filename = NULL;
  if (output_format==SQL_INSERT)
		tj->sql=NULL;
	else{
		tj->sql=g_new0(struct table_job_file, 1);
    tj->sql->file = 0;
    tj->sql->filename = NULL;
  }
  tj->exec_out_filename = NULL;
  tj->dbt=dbt;
  tj->st_in_file=0;
  tj->filesize=0;
  tj->char_chunk_part=char_chunk;
  tj->child_process=0;
  tj->where=g_string_new("");
  update_estimated_remaining_chunks_on_dbt(tj->dbt);
  return tj;
}

void free_table_job(struct table_job *tj){
  if (tj->sql){
    m_close(tj->td->thread_id, tj->sql->file, tj->sql->filename, tj->filesize, tj->dbt);
    tj->sql->file=0;
    tj->sql=NULL;
  }
  if (tj->rows){
    m_close(tj->td->thread_id, tj->rows->file, tj->rows->filename, tj->filesize, tj->dbt);
    tj->rows->file=0;
    tj->rows=NULL;
  }

  if (tj->where!=NULL)
    g_string_free(tj->where,TRUE);

  g_free(tj);
}

void create_job_to_dump_chunk(struct db_table *dbt, char *partition, guint64 nchunk, struct chunk_step_item *chunk_step_item, void f(GAsyncQueue *,struct job *), GAsyncQueue *queue){
  struct job *j = g_new0(struct job,1);
  struct table_job *tj = new_table_job(dbt, partition, nchunk, chunk_step_item);
  j->job_data=(void*) tj;
  j->type= dbt->is_transactional ? JOB_DUMP : JOB_DUMP_NON_INNODB;
  f(queue,j);
}

void create_job_defer(struct db_table *dbt, GAsyncQueue *queue){
  struct job *j = g_new0(struct job,1);
  j->type = JOB_DEFER;
  j->job_data=(void*) dbt;
  g_async_queue_push(queue,j);
}

void create_job_to_determine_chunk_type(struct db_table *dbt, void f(GAsyncQueue *,struct job *), GAsyncQueue *queue){
  struct job *j = g_new0(struct job,1);
  j->type = JOB_DETERMINE_CHUNK_TYPE;
  j->job_data=(void*) dbt;
  f(queue,j);
}

