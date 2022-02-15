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

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <string.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include "myloader_stream.h"
#include "common.h"
#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_job.h"
#include <errno.h>

extern guint errors;
extern guint commit_count;
extern gboolean serial_tbl_creation;
extern gboolean overwrite_tables;
extern gboolean no_delete;
extern gchar *directory;
extern gchar *compress_extension;
extern gchar *db;
extern gboolean skip_definer;
extern guint rows;

static GMutex *single_threaded_create_table = NULL;
static GMutex *progress_mutex = NULL;
enum purge_mode purge_mode;

unsigned long long int progress = 0;
unsigned long long int total_data_sql_files = 0;
extern gboolean stream;

void initialize_job(gchar * purge_mode_str){
  single_threaded_create_table = g_mutex_new();
  progress_mutex = g_mutex_new();
  if (purge_mode_str){
    if (!strcmp(purge_mode_str,"TRUNCATE")){
      purge_mode=TRUNCATE;
    } else if (!strcmp(purge_mode_str,"DROP")){
      purge_mode=DROP;
    } else if (!strcmp(purge_mode_str,"DELETE")){
      purge_mode=DELETE;
    } else if (!strcmp(purge_mode_str,"NONE")){
      purge_mode=NONE;
    } else {
      g_error("Purge mode unknown");
    }
  } else if (overwrite_tables)
    purge_mode=DROP; // Default mode is DROP when overwrite_tables is especified
  else purge_mode=NONE;
}

void free_restore_job(struct restore_job * rj){
  // We consider that
  if (rj->filename != NULL ) g_free(rj->filename);
  if (rj->statement != NULL ) g_string_free(rj->statement,TRUE);
  if (rj != NULL ) g_free(rj);
}

int overwrite_table(MYSQL *conn,gchar * database, gchar * table){
  int truncate_or_delete_failed=0;
  gchar *query=NULL;
  if (purge_mode == DROP) {
    g_message("Dropping table or view (if exists) `%s`.`%s`",
              database, table);
    query = g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`",
                            database, table);
    mysql_query(conn, query);
    query = g_strdup_printf("DROP VIEW IF EXISTS `%s`.`%s`", database,
                            table);
    mysql_query(conn, query);
  } else if (purge_mode == TRUNCATE) {
    g_message("Truncating table `%s`.`%s`", database, table);
    query= g_strdup_printf("TRUNCATE TABLE `%s`.`%s`", database, table);
    truncate_or_delete_failed= mysql_query(conn, query);
    if (truncate_or_delete_failed)
      g_warning("Truncate failed, we are going to try to create table or view");
  } else if (purge_mode == DELETE) {
    g_message("Deleting content of table `%s`.`%s`", database, table);
    query= g_strdup_printf("DELETE FROM `%s`.`%s`", database, table);
    truncate_or_delete_failed= mysql_query(conn, query);
    if (truncate_or_delete_failed)
      g_warning("Delete failed, we are going to try to create table or view");
  }
  g_free(query);
  return truncate_or_delete_failed;
}

int restore_data_in_gstring_by_statement(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter)
{
  if (mysql_real_query(td->thrconn, data->str, data->len)) {
    //g_critical("Error restoring: %s %s", data->str, mysql_error(conn));
    errors++;
    return 1;
  }
  *query_counter=*query_counter+1;
  if (!is_schema && (commit_count > 1) &&(*query_counter == commit_count)) {
    *query_counter= 0;
    if (mysql_query(td->thrconn, "COMMIT")) {
      errors++;
      return 2;
    }
    mysql_query(td->thrconn, "START TRANSACTION");
  }
  g_string_set_size(data, 0);
  return 0;
}

int restore_data_in_gstring(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter)
{
  int i=0;
  int r=0;
  if (data != NULL && data->len > 4){
    gchar** line=g_strsplit(data->str, ";\n", -1);
    for (i=0; i < (int)g_strv_length(line);i++){
       if (strlen(line[i])>2){
         GString *str=g_string_new(line[i]);
         g_string_append_c(str,';');
         r+=restore_data_in_gstring_by_statement(td, str, is_schema, query_counter);
       }
    }
  }
  return r;
}

void process_restore_job(struct thread_data *td, struct restore_job *rj){
  struct db_table *dbt=rj->dbt;
  dbt=rj->dbt;
  g_message("Thread %d restoring job", td->thread_id);
  switch (rj->type) {
    case JOB_RESTORE_STRING:
      g_message("Thread %d restoring %s `%s`.`%s` from %s", td->thread_id, rj->object,
                dbt->real_database, dbt->real_table, rj->filename);
      guint query_counter=0;
      restore_data_in_gstring(td, rj->statement, FALSE, &query_counter);
      break;
    case JOB_RESTORE_SCHEMA_STRING:
      if (serial_tbl_creation) g_mutex_lock(single_threaded_create_table);
      g_message("Thread %d restoring table `%s`.`%s` from %s", td->thread_id,
                dbt->real_database, dbt->real_table, rj->filename);
      int truncate_or_delete_failed=0;
      if (overwrite_tables)
        truncate_or_delete_failed=overwrite_table(td->thrconn,dbt->real_database, dbt->real_table);
      if ((purge_mode == TRUNCATE || purge_mode == DELETE) && !truncate_or_delete_failed){
        g_message("Skipping table creation `%s`.`%s` from %s", dbt->real_database, dbt->real_table, rj->filename);
      }else{
        g_message("Creating table `%s`.`%s` from content in %s", dbt->real_database, dbt->real_table, rj->filename);
        if (restore_data_in_gstring(td, rj->statement, FALSE, &query_counter)){
          g_critical("Thread %d issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
        }
      }
      dbt->schema_created=TRUE;
      if (serial_tbl_creation) g_mutex_unlock(single_threaded_create_table);
      break;
    case JOB_RESTORE_FILENAME:
      g_mutex_lock(progress_mutex);
      progress++;
      g_message("Thread %d restoring `%s`.`%s` part %d %d of %d from %s. Progress %llu of %llu .", td->thread_id,
                dbt->real_database, dbt->real_table, rj->part, rj->sub_part, dbt->count, rj->filename, progress,total_data_sql_files);
      g_mutex_unlock(progress_mutex);
      if (stream && !dbt->schema_created){
        // In a stream scenario we might need to wait until table is created to start executing inserts.
        int i=0;
        while (!dbt->schema_created && i<100){
          usleep(1000);
          i++;
        }
        if (!dbt->schema_created){
          g_critical("Table has not been created in more than 10 seconds");
          exit(EXIT_FAILURE);
        }
      }
      if (restore_data_from_file(td, dbt->real_database, dbt->real_table, rj->filename, FALSE) > 0){
        g_critical("Thread %d issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
      }
      break;
    case JOB_RESTORE_SCHEMA_FILENAME:
      g_message("Thread %d restoring %s on `%s` from %s", td->thread_id, rj->object,
                rj->database, rj->filename);
      restore_data_from_file(td, rj->database, NULL, rj->filename, TRUE );
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
  free_restore_job(rj);
}

struct job * new_job (enum job_type type, void *job_data, char *use_database) {
  struct job *j = g_new0(struct job, 1);
  j->type = type;
  j->use_database=use_database;
  switch (type){
    case JOB_WAIT:
      j->queue = (GAsyncQueue *)job_data;
    case JOB_SHUTDOWN:
      break;
    default:
      j->job_data = (struct restore_job *)job_data;
  }
  return j;
}

gboolean process_job(struct thread_data *td, struct job *job){
  switch (job->type) {
    case JOB_RESTORE:
      g_message("Restore Job");
      process_restore_job(td,job->job_data);
      break;
    case JOB_WAIT:
      g_message("Wait Job");
      g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
      GAsyncQueue *queue=job->queue;
      g_async_queue_pop(queue);
      break;
    case JOB_SHUTDOWN:
      g_message("Thread %d shutting down", td->thread_id);
      g_free(job);
      return FALSE;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
  }
  return TRUE;
}

int split_and_restore_data_in_gstring_by_statement(struct thread_data *td,
                  GString *data, gboolean is_schema, guint *query_counter, guint offset_line)
{
  char *next_line=g_strstr_len(data->str,-1,"VALUES") + 6;
  char *insert_statement_prefix=g_strndup(data->str,next_line - data->str);
  guint insert_statement_prefix_len=strlen(insert_statement_prefix);
  int r=0;
  guint tr=0,current_offset_line=offset_line-1;
  gchar *current_line=next_line;
  next_line=g_strstr_len(current_line, -1, "\n");
  GString * new_insert=g_string_sized_new(strlen(insert_statement_prefix));
  guint current_rows=0;
  do {
    current_rows=0;
    g_string_set_size(new_insert, 0);
    new_insert=g_string_append(new_insert,insert_statement_prefix);
    do {
      char *line=g_strndup(current_line, next_line - current_line);
      g_string_append(new_insert, line);
      g_free(line);
      current_rows++;
      current_line=next_line+1;
      next_line=g_strstr_len(current_line, -1, "\n");
      current_offset_line++;
    } while (current_rows < rows && next_line != NULL);
    if (new_insert->len > insert_statement_prefix_len)
      tr=restore_data_in_gstring_by_statement(td, new_insert, is_schema, query_counter);
    else
      tr=0;
    r+=tr;
    if (tr > 0){
      g_critical("Error occours between lines: %d and %d in a splited INSERT: %s",offset_line,current_offset_line,mysql_error(td->thrconn));
    }
    offset_line=current_offset_line+1;
    current_line++; // remove trailing ,
  } while (next_line != NULL);
  g_string_free(new_insert,TRUE);
  g_free(insert_statement_prefix);
  g_string_set_size(data, 0);
  return r;

}

int restore_data_from_file(struct thread_data *td, char *database, char *table,
                  const char *filename, gboolean is_schema){
  void *infile;
  int r=0;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  guint query_counter = 0;
  GString *data = g_string_sized_new(512);
  guint line=0,preline=0;
  gchar *path = g_build_filename(directory, filename, NULL);
  if (!g_str_has_suffix(path, compress_extension)) {
    infile = g_fopen(path, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(path, "r");
    is_compressed = TRUE;
  }

  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return 1;
  }
  if (!is_schema && (commit_count > 1) )
    mysql_query(td->thrconn, "START TRANSACTION");
  guint tr=0;
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof, &line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if ( skip_definer && g_str_has_prefix(data->str,"CREATE")){
          char * from=g_strstr_len(data->str,30," DEFINER")+1;
          if (from){
            char * to=g_strstr_len(from,30," ");
            if (to){
              while(from != to){
                from[0]=' ';
                from++;
              }
              g_message("It is a create statement %s: %s",filename,from);
            }
          }
        }
        if (rows > 0 && g_strrstr_len(data->str,6,"INSERT"))
          tr=split_and_restore_data_in_gstring_by_statement(td,
            data, is_schema, &query_counter,preline);
        else
          tr=restore_data_in_gstring_by_statement(td, data, is_schema, &query_counter);
        r+=tr;
        if (tr > 0){
            g_critical("Error occours between lines: %d and %d on file %s: %s",preline,line,filename,mysql_error(td->thrconn));
        }
        g_string_set_size(data, 0);
        preline=line+1;
      }
    } else {
      g_critical("error reading file %s (%d)", filename, errno);
      errors++;
      return r;
    }
  }
  if (!is_schema && (commit_count > 1) && mysql_query(td->thrconn, "COMMIT")) {
    g_critical("Error committing data for %s.%s from file %s: %s",
               db ? db : database, table, filename, mysql_error(td->thrconn));
    errors++;
  }
  g_string_free(data, TRUE);
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }

  if (stream && no_delete == FALSE){
    g_message("Removing file: %s", path);
    remove(path);
  }
  g_free(path);
  return r;
}

