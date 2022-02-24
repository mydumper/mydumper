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
//#include "common_options.h"
#include <glib-unix.h>
#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "connection.h"
#include <errno.h>

extern guint errors;
extern gboolean serial_tbl_creation;
extern gboolean overwrite_tables;
extern gboolean no_delete;
extern gchar *directory;
extern gchar *compress_extension;
extern gchar *db;
extern guint rows;
extern gchar *set_names_str;
extern GString *set_session;
extern guint num_threads;

static GMutex *init_mutex=NULL;
static GMutex *single_threaded_create_table = NULL;
static GMutex *progress_mutex = NULL;
enum purge_mode purge_mode;

unsigned long long int progress = 0;
unsigned long long int total_data_sql_files = 0;
extern gboolean stream;

GAsyncQueue *file_list_to_do=NULL;

gboolean shutdown_triggered=FALSE;

void initialize_job(gchar * purge_mode_str){
  single_threaded_create_table = g_mutex_new();
  init_mutex = g_mutex_new();
  progress_mutex = g_mutex_new();
  file_list_to_do = g_async_queue_new();
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
  if ( !shutdown_triggered && rj->filename != NULL ) g_free(rj->filename);
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
  dbt=rj->dbt;
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
cleanup:
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
//      g_message("Restore Job");
      process_restore_job(td,job->job_data);
      break;
    case JOB_WAIT:
//      g_message("Wait Job");
      g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
      GAsyncQueue *queue=job->queue;
      g_async_queue_pop(queue);
      break;
    case JOB_SHUTDOWN:
//      g_message("Thread %d shutting down", td->thread_id);
      g_free(job);
      return FALSE;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
  }
  return TRUE;
}

void *loader_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);
  td->current_database=NULL;

  m_connect(td->thrconn, "myloader", NULL);

  mysql_query(td->thrconn, set_names_str);
  mysql_query(td->thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
  mysql_query(td->thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
  mysql_query(td->thrconn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=db;
    execute_use(td, "Initializing thread");
  }
  g_debug("Thread %d: Starting import", td->thread_id);
  if (stream){
    process_stream_queue(td);
  }else{
    process_directory_queue(td);
  }
  struct job *job = NULL;
  gboolean cont=TRUE;

//  g_message("Thread %d: Starting post import task over table", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_table_queue);
//    g_message("%s",((struct restore_job *)job->job_data)->object);
    execute_use_if_needs_to(td, job->use_database, "Restoring post table");
    cont=process_job(td, job);
  }
//  g_message("Thread %d: Starting post import task: triggers, procedures and triggers", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_queue);
    execute_use_if_needs_to(td, job->use_database, "Restoring post tasks");
    cont=process_job(td, job);
  }

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  g_debug("Thread %d ending", td->thread_id);
  return NULL;
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

GThread **threads = NULL;
struct thread_data *td = NULL;

void initialize_loader_threads(struct configuration *conf){
  guint n=0;
  threads = g_new(GThread *, num_threads);
  td = g_new(struct thread_data, num_threads);
  for (n = 0; n < num_threads; n++) {
    td[n].conf = conf;
    td[n].thread_id = n + 1;
    threads[n] =
        g_thread_create((GThreadFunc)loader_thread, &td[n], TRUE, NULL);
    // Here, the ready queue is being used to serialize the connection to the database.
    // We don't want all the threads try to connect at the same time
    g_async_queue_pop(conf->ready);
  }
}

void wait_loader_threads_to_finish(){
  guint n=0;
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }

  if (shutdown_triggered)
    g_async_queue_push(file_list_to_do, g_strdup("NO_MORE_FILES"));
}

void free_loader_threads(){
  g_free(td);
  g_free(threads);
}
