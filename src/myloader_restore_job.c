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
#include "myloader_restore_job.h"
#include "myloader.h"
#include "myloader_restore.h"
#include <glib-unix.h>

#include "myloader_common.h"

extern gboolean serial_tbl_creation;
extern gboolean overwrite_tables;
extern gchar *db;
extern gboolean stream;
extern guint num_threads;

gboolean shutdown_triggered=FALSE;
GAsyncQueue *file_list_to_do=NULL;
static GMutex *progress_mutex = NULL;
static GMutex *single_threaded_create_table = NULL;
unsigned long long int progress = 0;
unsigned long long int total_data_sql_files = 0;

enum purge_mode purge_mode;

void initialize_restore_job(gchar * purge_mode_str){
  file_list_to_do = g_async_queue_new();
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

struct data_restore_job * new_data_restore_job_internal( guint index, guint part, guint sub_part){
  struct data_restore_job *drj = g_new(struct data_restore_job, 1);
  drj->index    = index;
  drj->part     = part;
  drj->sub_part = sub_part;
  return drj;
}

struct schema_restore_job * new_schema_restore_job_internal( char * database, GString * statement, const char *object){
  struct schema_restore_job *rj = g_new(struct schema_restore_job, 1);
  rj->database  = database;
  rj->statement = statement;
  rj->object    = object;
  return rj;
}

struct restore_job * new_restore_job( char * filename, struct db_table * dbt, enum restore_job_type type){
  struct restore_job *rj = g_new(struct restore_job, 1);
  rj->filename  = filename;
  rj->dbt       = dbt;
  rj->type      = type;
  return rj;
}

struct restore_job * new_data_restore_job( char * filename, enum restore_job_type type, struct db_table * dbt, guint part, guint sub_part){
  struct restore_job *rj = new_restore_job(filename, dbt, type);
  rj->data.drj=new_data_restore_job_internal( dbt->count, part, sub_part);
  return rj;
}

struct restore_job * new_schema_restore_job( char * filename, enum restore_job_type type, struct db_table * dbt, char * database, GString * statement, const char *object){
  struct restore_job *rj = new_restore_job(filename, dbt, type);
  rj->data.srj=new_schema_restore_job_internal(database, statement, object);
  return rj;
}

void free_restore_job(struct restore_job * rj){
  // We consider that
//  if ( !shutdown_triggered && rj->filename != NULL ) g_free(rj->filename);
//  if (rj->statement != NULL ) g_string_free(rj->statement,TRUE);
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

void process_restore_job(struct thread_data *td, struct restore_job *rj){
  if (td->conf->pause_resume){
    GMutex *resume_mutex = (GMutex *)g_async_queue_try_pop(td->conf->pause_resume);
    if (resume_mutex != NULL){
      g_mutex_lock(resume_mutex);
      g_mutex_unlock(resume_mutex);
      resume_mutex=NULL;
    }
  }
  if (shutdown_triggered){
//    g_message("file enqueued to allow resume: %s", rj->filename);
    g_async_queue_push(file_list_to_do,rj->filename);
    goto cleanup;
  }
  struct db_table *dbt=rj->dbt;
  switch (rj->type) {
    case JOB_RESTORE_STRING:
      g_message("Thread %d restoring %s `%s`.`%s` from %s", td->thread_id, rj->data.srj->object,
                dbt->real_database, dbt->real_table, rj->filename);
      guint query_counter=0;
      restore_data_in_gstring(td, rj->data.srj->statement, FALSE, &query_counter);
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
        if (restore_data_in_gstring(td, rj->data.srj->statement, FALSE, &query_counter)){
          g_critical("Thread %d issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
        }
      }
      dbt->schema_created=TRUE;
      if (serial_tbl_creation) g_mutex_unlock(single_threaded_create_table);
      break;
    case JOB_RESTORE_FILENAME:
      g_mutex_lock(progress_mutex);
      progress++;
      g_message("Thread %d restoring `%s`.`%s` part %d of %d from %s. Progress %llu of %llu.", td->thread_id,
                dbt->real_database, dbt->real_table, rj->data.drj->index, dbt->count, rj->filename, progress,total_data_sql_files);
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
      g_message("Thread %d restoring %s on `%s` from %s", td->thread_id, rj->data.srj->object,
                rj->data.srj->database, rj->filename);
      restore_data_from_file(td, rj->data.srj->database, NULL, rj->filename, TRUE );
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
cleanup:
  free_restore_job(rj);
}


GMutex **pause_mutex_per_thread=NULL;

gboolean sig_triggered(void * user_data, int signal) {
  guint i=0;
  GAsyncQueue *queue=NULL;
  if (signal == SIGTERM){
    shutdown_triggered = TRUE;
  }else{
    if (pause_mutex_per_thread == NULL){
      pause_mutex_per_thread=g_new(GMutex * , num_threads) ;
      for(i=0;i<num_threads;i++){
        pause_mutex_per_thread[i]=g_mutex_new();
      }
    }
    if (((struct configuration *)user_data)->pause_resume == NULL)
      ((struct configuration *)user_data)->pause_resume = g_async_queue_new();
    queue = ((struct configuration *)user_data)->pause_resume;
    for(i=0;i<num_threads;i++){
      g_mutex_lock(pause_mutex_per_thread[i]);
      g_async_queue_push(queue,pause_mutex_per_thread[i]);
    }
    g_critical("Ctrl+c detected! Are you sure you want to cancel(Y/N)?");
    int c=0;
    while (1){
      do{
        c=fgetc(stdin);
      }while (c=='\n');
      if ( c == 'N' || c == 'n'){
        for(i=0;i<num_threads;i++)
          g_mutex_unlock(pause_mutex_per_thread[i]);
        return TRUE;
      }
      if ( c == 'Y' || c == 'y'){
        shutdown_triggered = TRUE;
        for(i=0;i<num_threads;i++)
          g_mutex_unlock(pause_mutex_per_thread[i]);
        break;
      }
    }
  }

  g_message("Writing resume.partial file");
  gchar *filename;
  gchar *p=g_strdup("resume.partial"),*p2=g_strdup("resume");

  void *outfile = g_fopen(p, "w");
  filename=g_async_queue_pop(file_list_to_do);
  while(g_strcmp0(filename,"NO_MORE_FILES")!=0){
    fprintf(outfile, "%s\n", filename);
    filename=g_async_queue_pop(file_list_to_do);
  }
  fclose(outfile);
  if (g_rename(p, p2) != 0){
    g_critical("Error renaming resume.partial to resume");
  }
  g_free(p);
  g_free(p2);
  g_message("Shutting down gracefully completed.");
  return FALSE;
}

gboolean sig_triggered_int(void * user_data) {
  return sig_triggered(user_data,SIGINT);
}
gboolean sig_triggered_term(void * user_data) {
  return sig_triggered(user_data,SIGTERM);
}

void *signal_thread(void *data) {
  GMainLoop * loop=NULL;
  g_unix_signal_add(SIGINT, sig_triggered_int, data);
  g_unix_signal_add(SIGTERM, sig_triggered_term, data);
  loop = g_main_loop_new (NULL, TRUE);
  g_main_loop_run (loop);
  return NULL;
}

void restore_job_finish(){
  if (shutdown_triggered)
    g_async_queue_push(file_list_to_do, g_strdup("NO_MORE_FILES"));
}

