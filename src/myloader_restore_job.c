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
#include "common.h"
#include "myloader_restore_job.h"
#include "myloader.h"
#include "myloader_restore.h"
#include <glib-unix.h>

unsigned long long int total_data_sql_files = 0;
#include "myloader_global.h"
#include "myloader_common.h"
#include "myloader_control_job.h"
#include "myloader_worker_loader.h"
gboolean shutdown_triggered=FALSE;
GAsyncQueue *file_list_to_do=NULL;
static GMutex *progress_mutex = NULL;
static GMutex *single_threaded_create_table = NULL;
GMutex *shutdown_triggered_mutex=NULL;
unsigned long long int progress = 0;
enum purge_mode purge_mode = FAIL;

void initialize_restore_job(gchar * pm_str){
  file_list_to_do = g_async_queue_new();
  single_threaded_create_table = g_mutex_new();
  progress_mutex = g_mutex_new();
  shutdown_triggered_mutex = g_mutex_new();
  if (pm_str){
    if (!strcmp(pm_str,"TRUNCATE")){
      purge_mode=TRUNCATE;
    } else if (!strcmp(pm_str,"DROP")){
      purge_mode=DROP;
    } else if (!strcmp(pm_str,"DELETE")){
      purge_mode=DELETE;
    } else if (!strcmp(pm_str,"NONE")){
      purge_mode=NONE;
    } else if (!strcmp(pm_str,"FAIL")){
      purge_mode=FAIL;
    } else {
      m_error("Purge mode unknown");
    }
  } else if (overwrite_tables)
    purge_mode=DROP; // Default mode is DROP when overwrite_tables is especified
  else purge_mode=FAIL; // This means that if -o is not set and CREATE TABLE statement fails, myloader will stop. 
}

struct data_restore_job * new_data_restore_job_internal( guint index, guint part, guint sub_part){
  struct data_restore_job *drj = g_new(struct data_restore_job, 1);
  drj->index    = index;
  drj->part     = part;
  drj->sub_part = sub_part;
  return drj;
}

struct schema_restore_job * new_schema_restore_job_internal( struct database * database, GString * statement, const char *object){
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
  rj->data.drj=new_data_restore_job_internal( dbt->count + 1, part, sub_part);
  return rj;
}

struct restore_job * new_schema_restore_job( char * filename, enum restore_job_type type, struct db_table * dbt, struct database * database, GString * statement, const char *object){
  struct restore_job *rj = new_restore_job(filename, dbt, type);
  rj->data.srj=new_schema_restore_job_internal(database, statement, object);
  return rj;
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

int overwrite_table(MYSQL *conn,gchar * database, gchar * table){
  int truncate_or_delete_failed=0;
  gchar *query=NULL;
  if (purge_mode == DROP) {
    g_message("Dropping table or view (if exists) `%s`.`%s`",
              database, table);
    query = g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`",
                            database, table);
    m_query(conn, query, m_critical, "Drop table failed");
//    mysql_query(conn, query);
    g_free(query);
    query = g_strdup_printf("DROP VIEW IF EXISTS `%s`.`%s`", database,
                            table);
    m_query(conn, query, m_critical, "Drop view failed");
//    mysql_query(conn, query);
  } else if (purge_mode == TRUNCATE) {
    g_message("Truncating table `%s`.`%s`", database, table);
    query= g_strdup_printf("TRUNCATE TABLE `%s`.`%s`", database, table);
    truncate_or_delete_failed= m_query(conn, query, m_warning, "TRUNCATE TABLE failed");
    if (truncate_or_delete_failed)
      g_warning("Truncate failed, we are going to try to create table or view");
  } else if (purge_mode == DELETE) {
    g_message("Deleting content of table `%s`.`%s`", database, table);
    query= g_strdup_printf("DELETE FROM `%s`.`%s`", database, table);
    truncate_or_delete_failed= m_query(conn, query, m_warning, "DELETE failed");
    if (truncate_or_delete_failed)
      g_warning("Delete failed, we are going to try to create table or view");
  }
  g_free(query);
  return truncate_or_delete_failed;
}

void is_all_done(void* key, void* dbt, void *total){
  (void) key;
  (void) dbt;
//  *((guint *)total)= 100;
  if (((struct db_table*)dbt)->schema_state >= ALL_DONE)
    *((guint *)total)= *((guint *)total) + 1;
//*((guint *)total) + 2+ ((struct db_table*)dbt)->schema_state >= CREATED /*ALL_DONE*/ ? 1 : 0;
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
        else g_message("Failed object %s no place to save", object);

}

void get_total_done(struct configuration * conf, void *total){
  g_mutex_lock(conf->table_hash_mutex);
  g_hash_table_foreach(conf->table_hash,&is_all_done, &total);
  g_mutex_unlock(conf->table_hash_mutex);
}


void process_restore_job(struct thread_data *td, struct restore_job *rj){
  if (td->conf->pause_resume != NULL){
    GMutex *resume_mutex = (GMutex *)g_async_queue_try_pop(td->conf->pause_resume);
    if (resume_mutex != NULL){
      g_message("Thread %d: Stop", td->thread_id);
      g_mutex_lock(resume_mutex);
      g_mutex_unlock(resume_mutex);
      g_message("Thread %d: Resumming", td->thread_id);
      resume_mutex=NULL;
    }
  }
  if (shutdown_triggered){
//    g_message("file enqueued to allow resume: %s", rj->filename);
    g_async_queue_push(file_list_to_do,g_strdup(rj->filename));
    goto cleanup;
  }
  struct db_table *dbt=rj->dbt;
  guint query_counter=0;
//  guint i=0;
  guint total=0;
  td->status=STARTED;
  switch (rj->type) {
    case JOB_RESTORE_STRING:
      get_total_done(td->conf, &total);
      g_message("Thread %d: restoring %s `%s`.`%s` from %s. Tables %d of %d completed", td->thread_id, rj->data.srj->object,
                dbt->database->real_database, dbt->real_table, rj->filename, total , g_hash_table_size(td->conf->table_hash));
      if (restore_data_in_gstring(td, rj->data.srj->statement, FALSE, &query_counter)){
        increse_object_error(rj->data.srj->object);
        g_message("Failed %s: %s",rj->data.srj->object,rj->data.srj->statement->str);
      }
      free_schema_restore_job(rj->data.srj);
      break;
    case JOB_TO_CREATE_TABLE:
      dbt->schema_state=CREATING;
      if (serial_tbl_creation) g_mutex_lock(single_threaded_create_table);
      g_message("Thread %d: restoring table `%s`.`%s` from %s", td->thread_id,
                dbt->database->real_database, dbt->real_table, rj->filename);
      int truncate_or_delete_failed=0;
      if (overwrite_tables)
        truncate_or_delete_failed=overwrite_table(td->thrconn,dbt->database->real_database, dbt->real_table);
      if ((purge_mode == TRUNCATE || purge_mode == DELETE) && !truncate_or_delete_failed){
        g_message("Skipping table creation `%s`.`%s` from %s", dbt->database->real_database, dbt->real_table, rj->filename);
      }else{
        g_message("Thread %d: Creating table `%s`.`%s` from content in %s.", td->thread_id, dbt->database->real_database, dbt->real_table, rj->filename);
        if (restore_data_in_gstring(td, rj->data.srj->statement, FALSE, &query_counter)){
          g_atomic_int_inc(&(detailed_errors.schema_errors));
          if (purge_mode == FAIL)
            g_error("Thread %d: issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
          else 
            g_critical("Thread %d: issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
        }else{
          get_total_done(td->conf, &total);
          g_message("Thread %d: Table `%s`.`%s` created. Tables %d of %d completed", td->thread_id, dbt->database->real_database, dbt->real_table, total , g_hash_table_size(td->conf->table_hash));
        }
      }
      dbt->schema_state=CREATED;
      if (serial_tbl_creation) g_mutex_unlock(single_threaded_create_table);
      free_schema_restore_job(rj->data.srj);
      break;
    case JOB_RESTORE_FILENAME:
      g_mutex_lock(progress_mutex);
      progress++;
      get_total_done(td->conf, &total);
      g_message("Thread %d: restoring `%s`.`%s` part %d of %d from %s. Progress %llu of %llu. Tables %d of %d completed", td->thread_id,
                dbt->database->real_database, dbt->real_table, rj->data.drj->index, dbt->count, rj->filename, progress,total_data_sql_files, total , g_hash_table_size(td->conf->table_hash));
      g_mutex_unlock(progress_mutex);
      if (restore_data_from_file(td, dbt->database->real_database, dbt->real_table, rj->filename, FALSE) > 0){
        g_atomic_int_inc(&(detailed_errors.data_errors));
        g_critical("Thread %d: issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
      }
      g_atomic_int_dec_and_test(&(dbt->remaining_jobs));
      g_free(rj->data.drj);
      break;
    case JOB_RESTORE_SCHEMA_FILENAME:
      get_total_done(td->conf, &total); 
      g_message("Thread %d: restoring %s on `%s` from %s. Tables %d of %d completed", td->thread_id, rj->data.srj->object,
                rj->data.srj->database->real_database, rj->filename, total , g_hash_table_size(td->conf->table_hash));
      if ( restore_data_from_file(td, rj->data.srj->database->real_database, NULL, rj->filename, TRUE ) > 0 )
        increse_object_error(rj->data.srj->object);
      free_schema_restore_job(rj->data.srj);
      break;
    default:
      m_critical("Something very bad happened!");
    }
cleanup:
  (void) rj;
//  if (rj != NULL ) free_restore_job(rj);
    td->status=COMPLETED;
}


GMutex **pause_mutex_per_thread=NULL;

gboolean sig_triggered(void * user_data, int signal) {
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
    if (((struct configuration *)user_data)->pause_resume == NULL)
      ((struct configuration *)user_data)->pause_resume = g_async_queue_new();
    queue = ((struct configuration *)user_data)->pause_resume;
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
  g_message("Writing resume.partial file");
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
  g_message("Shutting down gracefully completed.");
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
  g_main_loop_unref(loop);
//  g_main_loop_quit(loop);
}

void restore_job_finish(){
  if (shutdown_triggered)
    g_async_queue_push(file_list_to_do, g_strdup("NO_MORE_FILES"));
}

