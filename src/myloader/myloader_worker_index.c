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

#include <glib/gstdio.h>

#include "myloader_common.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "myloader_worker_loader_main.h"
#include "myloader_global.h"
#include "myloader_database.h"
#include "myloader_restore.h"
#include "../logging.h"

GAsyncQueue * optimize_keys_all_tables_queue=NULL;
GThread **index_threads = NULL;
struct thread_data *index_td = NULL;
static GMutex *init_connection_mutex=NULL;
#define INDEX_IDLE_WAIT_USEC G_USEC_PER_SEC
void *worker_index_thread(struct thread_data *td);

void initialize_worker_index(struct configuration *conf){
  guint n=0;
//  index_mutex = g_mutex_new();
  init_connection_mutex = g_mutex_new();
  index_threads = g_new(GThread *, max_threads_for_index_creation);
  index_td = g_new(struct thread_data, max_threads_for_index_creation);
  optimize_keys_all_tables_queue=g_async_queue_new();
  for (n = 0; n < max_threads_for_index_creation; n++) {
    initialize_thread_data(&(index_td[n]), conf, WAITING, n + 1 + num_threads + max_threads_for_schema_creation, NULL);
    index_threads[n] =
        m_thread_new("myloader_index",(GThreadFunc)worker_index_thread, &index_td[n], "Index thread could not be created");
  }
}

gboolean process_index(struct thread_data * td){
  /* Let idle index workers periodically drop pooled DB sessions while they wait
   * for more index jobs or the final shutdown signal. */
  struct control_job *job=g_async_queue_timeout_pop(td->conf->index_queue, INDEX_IDLE_WAIT_USEC);
  if (job == NULL) {
    release_idle_connection_if_possible();
    return TRUE;
  }
  if (job->type==JOB_SHUTDOWN)
  {
    trace("index_queue -> %s", jtype2str(job->type));
    return FALSE;
  }

  g_assert(job->type == JOB_RESTORE);
  struct db_table *dbt=job->data.restore_job->dbt;
  trace("index_queue -> %s: %s.%s", rjtype2str(job->data.restore_job->type), dbt->database->target_database, dbt->table_filename);
  dbt->start_index_time=g_date_time_new_now_local();
  process_job(td, job, NULL);
  dbt->finish_time=g_date_time_new_now_local();
  table_lock(dbt);
  dbt->schema_state=ALL_DONE;
  table_unlock(dbt);
  return TRUE;
}

void *worker_index_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_connection_mutex);
  g_mutex_unlock(init_connection_mutex);

  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));
  if (optimize_keys_all_tables){
    g_async_queue_pop(optimize_keys_all_tables_queue);
  }

  set_thread_name("I%02u", td->thread_id);
  trace("I-Thread %u: Starting import", td->thread_id);
  gboolean cont=TRUE;
  while (cont){
    cont=process_index(td);
//    enroute_into_the_right_queue_based_on_file_type(REQUEST_DATA_JOB);
    wake_data_threads();
  }

  trace("I-Thread %u: ending", td->thread_id);
  return NULL;
}

void create_index_shutdown_job(struct configuration *conf){
  guint n=0;
  trace("Sending SHUTDOWN to index threads");
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_async_queue_push(conf->index_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
  }
}

void wait_index_worker_to_finish(){
  guint n=0;
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_thread_join(index_threads[n]);
  }
  trace("Indexes completed");
}

void start_optimize_keys_all_tables(){
  guint n=0;
  trace("optimize_keys_all_tables_queue <- 1 (%u times)", max_threads_for_index_creation);
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_async_queue_push(optimize_keys_all_tables_queue, GINT_TO_POINTER(1));
  }
}

static
gboolean create_index_job(struct configuration *conf, struct db_table * dbt, guint tdid){
  if (machine_log_json_enabled()) {
    gchar *thread_id = g_strdup_printf("%u", tdid);
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "index job enqueued",
                      "EVENT", "index_job",
                      "PHASE", "restore_index",
                      "STATUS", "started",
                      "THREAD_ID", thread_id,
                      "DB", dbt->database->target_database,
                      "TABLE", dbt->table_filename,
                      NULL);
    g_free(thread_id);
  } else {
    message("Thread %d: Enqueuing index for table: %s.%s", tdid, dbt->database->target_database, dbt->table_filename);
  }
  struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->database,dbt->indexes, INDEXES);
  trace("index_queue <- %s: %s.%s", rjtype2str(rj->type), dbt->database->target_database, dbt->table_filename);
  g_async_queue_push(conf->index_queue, new_control_job(JOB_RESTORE,rj,dbt->database));
  dbt->schema_state=INDEX_ENQUEUED;
  return TRUE;
}

void enqueue_index_for_dbt_if_possible(struct configuration *conf, struct db_table * dbt){
  trace("Checking if index on %s %s is possible to enqueu", dbt->database->target_database, dbt->table_filename);
  if (dbt->schema_state==DATA_DONE){
    if (dbt->indexes == NULL){
      trace("Table %s %s is all done", dbt->database->target_database, dbt->table_filename);
      dbt->schema_state=ALL_DONE;
//      return FALSE;
    }else{
//      return 
      trace("Creating index on %s %s ", dbt->database->target_database, dbt->table_filename);
      create_index_job(conf, dbt, 0);
    }
  }else{
    trace("Indexes on %s %s are not possible yet dbt->schema_state %d %d ", dbt->database->target_database, dbt->table_filename,dbt->schema_state, DATA_DONE);
  
  }
//  return !(dbt->schema_state == ALL_DONE || dbt->schema_state == INDEX_ENQUEUED ) ;
}

void enqueue_indexes_if_possible(struct configuration *conf){
  (void )conf;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt = NULL;
  while (iter != NULL){
    dbt=iter->data;
    table_lock(dbt);
    enqueue_index_for_dbt_if_possible(conf,dbt);
    table_unlock(dbt);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}

void free_index_worker_threads(){
  g_free(index_td);
  g_free(index_threads);
}
