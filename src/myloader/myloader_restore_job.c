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
#include <mysqld_error.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <string.h>
#include <glib-unix.h>

#include "myloader.h"
#include "myloader_restore_job.h"
#include "myloader_restore.h"
#include "myloader_global.h"
#include "myloader_common.h"
#include "myloader_control_job.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_index.h"

unsigned long long int total_data_sql_files = 0;
gboolean shutdown_triggered=FALSE;
GAsyncQueue *file_list_to_do=NULL;
static GMutex *progress_mutex = NULL;
static GMutex *single_threaded_create_table = NULL;
GMutex *shutdown_triggered_mutex=NULL;
unsigned long long int progress = 0;
enum purge_mode purge_mode = FAIL;

void initialize_restore_job(){
  file_list_to_do = g_async_queue_new();
  single_threaded_create_table = g_mutex_new();
  progress_mutex = g_mutex_new();
  shutdown_triggered_mutex = g_mutex_new();
  if (!purge_mode_str){
    if (overwrite_tables)
      purge_mode=DROP; // Default mode is DROP when overwrite_tables is especified
    else
      purge_mode=FAIL; // This means that if -o is not set and CREATE TABLE statement fails, myloader will stop.
  }

}

struct data_restore_job * new_data_restore_job_internal( guint index, guint part, guint sub_part){
  struct data_restore_job *drj = g_new(struct data_restore_job, 1);
  drj->index    = index;
  drj->part     = part;
  drj->sub_part = sub_part;
  return drj;
}

struct schema_restore_job * new_schema_restore_job_internal( struct database * database, GString * statement, const char *object){
  struct schema_restore_job *srj = g_new(struct schema_restore_job, 1);
  srj->database  = database;
  srj->statement = statement;
  srj->object    = object;
  return srj;
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
  rj->data.drj=new_data_restore_job_internal( dbt->count + 1, part, sub_part);
  return rj;
}

struct restore_job * new_schema_restore_job( char * filename, enum restore_job_type type, struct db_table * dbt, struct database * database, GString * statement, const char *object){
  struct restore_job *srj = new_restore_job(filename, dbt, type);
  srj->data.srj=new_schema_restore_job_internal(database, statement, object);
  return srj;
}

void free_restore_job(struct restore_job * rj){
  // We consider that
  if (rj->filename != NULL ) g_free(rj->filename);
//  if ( !shutdown_triggered && rj->filename != NULL ) g_free(rj->filename);
//  if (rj->statement != NULL ) g_string_free(rj->statement,TRUE);
  if (rj != NULL ) g_free(rj);
}

void free_schema_restore_job(struct schema_restore_job *srj){
//  g_free(srj->database);
//  if (srj->statement!=NULL) g_string_free(srj->statement, TRUE);
//  srj->statement=NULL;
  g_free(srj);
};


void overwrite_table_message(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  va_arg ( args,gchar*);
  guint err = va_arg ( args, guint);
  if (overwrite_unsafe || (err != ER_LOCK_WAIT_TIMEOUT && err != ER_LOCK_DEADLOCK)) {
    g_critical("%s",c);
  } else {
    g_warning("%s",c);
  }
  g_free(c);

}

int overwrite_table(struct thread_data *td, struct db_table *dbt){
  int truncate_or_delete_failed=0;
  GString *data=g_string_new("");
  const char q= identifier_quote_character;
  if (purge_mode == DROP) {
    message("Dropping table or view (if exists) %s.%s",
        dbt->database->real_database, dbt->real_table);
    g_string_printf(data,"DROP TABLE IF EXISTS %c%s%c.%c%s%c",
        q, dbt->database->real_database, q, q, dbt->real_table, q);
    truncate_or_delete_failed = restore_data_in_gstring_extended(td, data, TRUE, dbt->database, overwrite_table_message, "Drop table %s.%s failed", dbt->database->real_database, dbt->real_table);
    g_string_printf(data,"DROP VIEW IF EXISTS %c%s%c.%c%s%c",
        q, dbt->database->real_database, q, q, dbt->real_table, q);
    if (restore_data_in_gstring(td, data, TRUE, dbt->database)){
      truncate_or_delete_failed = 1;
      g_critical("Drop view failed");
    }
  } else if (purge_mode == TRUNCATE) {
    message("Truncating table %s.%s", dbt->database->real_database, dbt->real_table);
    g_string_printf(data,"TRUNCATE TABLE %c%s%c.%c%s%c",
        q, dbt->database->real_database, q, q, dbt->real_table, q);
    truncate_or_delete_failed=restore_data_in_gstring(td, data, TRUE, dbt->database);
    if (truncate_or_delete_failed)
      g_warning("Truncate failed, we are going to try to create table or view");
  } else if (purge_mode == DELETE) {
    message("Deleting content of table %s.%s", dbt->database->real_database, dbt->real_table);
    g_string_printf(data,"DELETE FROM %c%s%c.%c%s%c ;\nCOMMIT",
        q, dbt->database->real_database, q, q, dbt->real_table, q);
    truncate_or_delete_failed=restore_data_in_gstring(td, data, TRUE, dbt->database);
    if (truncate_or_delete_failed)
      g_warning("Delete failed, we are going to try to create table or view");
    restore_data_in_gstring(td, data, TRUE, dbt->database);
  }
  return truncate_or_delete_failed;
}

void increse_object_error(const gchar *object){
        if (!g_strcmp0(object,SEQUENCE))
          g_atomic_int_inc(&(detailed_errors.sequence_errors));
        else if (!g_strcmp0(object,TRIGGER))
          g_atomic_int_inc(&(detailed_errors.trigger_errors));
        else if (!g_strcmp0(object,TABLESPACE))
          g_atomic_int_inc(&(detailed_errors.tablespace_errors));
        else if (!g_strcmp0(object,CREATE_DATABASE))
          g_atomic_int_inc(&(detailed_errors.schema_errors));
        else if (!g_strcmp0(object,VIEW))
          g_atomic_int_inc(&(detailed_errors.view_errors));
        else if (!g_strcmp0(object,POST))
          g_atomic_int_inc(&(detailed_errors.post_errors));
        else if (!g_strcmp0(object,INDEXES))
          g_atomic_int_inc(&(detailed_errors.index_errors));
        else if (!g_strcmp0(object,CONSTRAINTS))
          g_atomic_int_inc(&(detailed_errors.constraints_errors));
        else message("Failed object %s no place to save", object);
}

void schema_state_increment(gchar* _key, struct db_table* dbt, guint *total, enum schema_status schema_state){
  (void) _key;
  if (dbt->schema_state >= schema_state){
    *total = *total + 1;
  }
}

void is_all_done(gchar* _key, struct db_table* dbt, guint *total){
  schema_state_increment(_key, dbt, total, ALL_DONE);
}

void is_created(gchar* _key, struct db_table* dbt, guint *total){
  schema_state_increment(_key, dbt, total, CREATED);
}


void get_total_done(struct configuration * conf, guint *total){
  g_mutex_lock(conf->table_hash_mutex);
  g_hash_table_foreach(conf->table_hash,(void (*)(void *, void *, void *)) &is_all_done, total);
  g_mutex_unlock(conf->table_hash_mutex);
}

void get_total_created(struct configuration * conf, guint *total){
  g_mutex_lock(conf->table_hash_mutex);
  g_hash_table_foreach(conf->table_hash,(void (*)(void *, void *, void *)) &is_created, total);
  g_mutex_unlock(conf->table_hash_mutex);
}


int process_restore_job(struct thread_data *td, struct restore_job *rj){
  if (td->conf->pause_resume != NULL){
    GMutex *resume_mutex = (GMutex *)g_async_queue_try_pop(td->conf->pause_resume);
    if (resume_mutex != NULL){
      message("Thread %d: Stop", td->thread_id);
      g_mutex_lock(resume_mutex);
      g_mutex_unlock(resume_mutex);
      message("Thread %d: Resumming", td->thread_id);
      resume_mutex=NULL;
    }
  }
  if (shutdown_triggered){
//    message("file enqueued to allow resume: %s", rj->filename);
    g_async_queue_push(file_list_to_do,g_strdup(rj->filename));
    goto cleanup;
  }
  struct db_table *dbt=rj->dbt;
//  guint i=0;
  guint total=0;
  td->status=STARTED;
  switch (rj->type) {
    case JOB_RESTORE_STRING:
      if (!source_db || g_strcmp0(dbt->database->name,source_db)==0){
          get_total_done(td->conf, &total);
          message("Thread %d: restoring %s %s.%s from %s. Tables %d of %d completed", td->thread_id,
                    rj->data.srj->object, dbt->database->real_database, dbt->real_table, rj->filename, total , g_hash_table_size(td->conf->table_hash));
          if (restore_data_in_gstring(td, rj->data.srj->statement, FALSE, rj->data.srj->database)){
            increse_object_error(rj->data.srj->object);
            message("Failed %s: %s",rj->data.srj->object,rj->data.srj->statement->str);
          }
      }
      free_schema_restore_job(rj->data.srj);
      break;
    case JOB_TO_CREATE_TABLE:
      dbt->schema_state=CREATING;
      if ((!source_db || g_strcmp0(dbt->database->name,source_db)==0) && !no_schemas && !dbt->object_to_export.no_schema ){
        if (serial_tbl_creation) g_mutex_lock(single_threaded_create_table);
        message("Thread %d: restoring table %s.%s from %s", td->thread_id,
                dbt->database->real_database, dbt->real_table, rj->filename);
        int overwrite_error= 0;
        if (overwrite_tables) {
          overwrite_error= overwrite_table(td, dbt);
          if (overwrite_error) {
            if (dbt->retry_count) {
              dbt->retry_count--;
              dbt->schema_state= NOT_CREATED;
              m_warning("Drop table %s.%s failed: retry %u of %u", dbt->database->real_database, dbt->real_table, retry_count - dbt->retry_count, retry_count);
              return 1;
            } else {
              m_critical("Drop table %s.%s failed: exiting", dbt->database->real_database, dbt->real_table);
            }
          } else if (dbt->retry_count < retry_count) {
              m_warning("Drop table %s.%s succeeded!", dbt->database->real_database, dbt->real_table);
          }
        }
        if ((purge_mode == TRUNCATE || purge_mode == DELETE) && !overwrite_error) {
          message("Skipping table creation %s.%s from %s", dbt->database->real_database, dbt->real_table, rj->filename);
        }else{
          message("Thread %d: Creating table %s.%s from content in %s. On db: %s", td->thread_id, dbt->database->real_database, dbt->real_table, rj->filename, dbt->database->name);
          if (restore_data_in_gstring(td, rj->data.srj->statement, TRUE, rj->data.srj->database)){
            g_atomic_int_inc(&(detailed_errors.schema_errors));
            if (purge_mode == FAIL)
              g_error("Thread %d: issue restoring %s",td->thread_id,rj->filename);
            else 
              g_critical("Thread %d: issue restoring %s",td->thread_id,rj->filename);
          }else{
            get_total_created(td->conf, &total);
            message("Thread %d: Table %s.%s created. Tables that pass created stage: %d of %d", td->thread_id, dbt->database->real_database, dbt->real_table, total , g_hash_table_size(td->conf->table_hash));
          }
        }
        if (serial_tbl_creation) g_mutex_unlock(single_threaded_create_table);
      }
      dbt->schema_state=CREATED;
      free_schema_restore_job(rj->data.srj);
      break;
    case JOB_RESTORE_FILENAME:
      if (!source_db || g_strcmp0(dbt->database->name,source_db)==0){
          g_mutex_lock(progress_mutex);
          progress++;
          get_total_done(td->conf, &total);
          message("Thread %d: restoring %s.%s part %d of %d from %s | Progress %llu of %llu. Tables %d of %d completed", td->thread_id,
                    dbt->database->real_database, dbt->real_table, rj->data.drj->index, dbt->count, rj->filename, progress,total_data_sql_files, total , g_hash_table_size(td->conf->table_hash));
          g_mutex_unlock(progress_mutex);
          if (restore_data_from_file(td, rj->filename, FALSE, dbt->database) > 0){
            g_atomic_int_inc(&(detailed_errors.data_errors));
            g_critical("Thread : issue restoring %s", rj->filename);
          }
      }
      g_atomic_int_dec_and_test(&(dbt->remaining_jobs));
      g_free(rj->data.drj);
      break;
    case JOB_RESTORE_SCHEMA_FILENAME:
      if (!source_db || g_strcmp0(rj->data.srj->database->name,source_db)==0){
        if ( g_strcmp0(rj->data.srj->object,VIEW) || !no_schemas){
          get_total_done(td->conf, &total); 
          message("Thread %d: restoring %s on `%s` from %s. Tables %d of %d completed", td->thread_id, rj->data.srj->object,
                    rj->data.srj->database->real_database, rj->filename, total , g_hash_table_size(td->conf->table_hash));
          if (dbt)
            dbt->schema_state= CREATING;
          if ( restore_data_from_file(td, rj->filename, TRUE, g_strcmp0(rj->data.srj->object, CREATE_DATABASE) ? rj->data.srj->database : NULL ) > 0 ) {
            increse_object_error(rj->data.srj->object);
            if (dbt)
              dbt->schema_state= NOT_CREATED;
          } else if (dbt)
            dbt->schema_state= CREATED;
        }
      }
      free_schema_restore_job(rj->data.srj);
      break;
    default:
      m_critical("Something very bad happened!");
    }
cleanup:
  (void) rj;
//  if (rj != NULL ) free_restore_job(rj);
    td->status=COMPLETED;
  return 0;
}


GMutex **pause_mutex_per_thread=NULL;

gboolean sig_triggered(void * user_data, int signal) {
  struct configuration *conf=(struct configuration *)user_data;
  guint i=0;
  GAsyncQueue *queue=NULL;
  g_mutex_lock(shutdown_triggered_mutex);
  if (signal == SIGTERM){
    shutdown_triggered = TRUE;
  }else{
    if (pause_mutex_per_thread == NULL){
      pause_mutex_per_thread=g_new(GMutex * , num_threads) ;
      for(i=0;i<num_threads;i++){
        pause_mutex_per_thread[i]=g_mutex_new();
      }
    }
    if (conf->pause_resume == NULL)
      conf->pause_resume = g_async_queue_new();
    queue = conf->pause_resume;
    for(i=0;i<num_threads;i++){
      g_mutex_lock(pause_mutex_per_thread[i]);
      g_async_queue_push(queue,pause_mutex_per_thread[i]);
    }
    fprintf(stdout, "Ctrl+c detected! Are you sure you want to cancel(Y/N)?");
    int c=0;
    while (1){
      do{
        c=fgetc(stdin);
      }while (c=='\n');
      if ( c == 'N' || c == 'n'){
        for(i=0;i<num_threads;i++)
          g_mutex_unlock(pause_mutex_per_thread[i]);
        g_mutex_unlock(shutdown_triggered_mutex);
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
  inform_restore_job_running();
  create_index_shutdown_job(conf);
  message("Writing resume.partial file");
  gchar *filename;
  gchar *p=g_strdup("resume.partial"),*p2=g_strdup("resume");

  void *outfile = g_fopen(p, "w");
  filename = g_async_queue_pop(file_list_to_do);
  while(g_strcmp0(filename,"NO_MORE_FILES")!=0){
    g_debug("Adding %s to resume file", filename);
    fprintf(outfile, "%s\n", filename);
    filename=g_async_queue_pop(file_list_to_do);
  }
  fclose(outfile);
  if (g_rename(p, p2) != 0){
    g_critical("Error renaming resume.partial to resume");
  }
  g_free(p);
  g_free(p2);
  message("Shutting down gracefully completed.");
  g_mutex_unlock(shutdown_triggered_mutex);
  return FALSE;
}

gboolean sig_triggered_int(void * user_data) {
  return sig_triggered(user_data,SIGINT);
}
gboolean sig_triggered_term(void * user_data) {
  return sig_triggered(user_data,SIGTERM);
}

GMainLoop * loop=NULL;

void *signal_thread(void *data) {
  g_unix_signal_add(SIGINT, sig_triggered_int, data);
  g_unix_signal_add(SIGTERM, sig_triggered_term, data);
  loop = g_main_loop_new (NULL, TRUE);
  g_main_loop_run (loop);
  return NULL;
}

void stop_signal_thread(){
  g_mutex_lock(shutdown_triggered_mutex);
  g_mutex_unlock(shutdown_triggered_mutex);
  if (loop)
    g_main_loop_unref(loop);
//  g_main_loop_quit(loop);
}

void restore_job_finish(){
  if (shutdown_triggered)
    g_async_queue_push(file_list_to_do, g_strdup("NO_MORE_FILES"));
}

