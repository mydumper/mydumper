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
#include <unistd.h>

#include "myloader_common.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "myloader_worker_loader.h"
#include "myloader_global.h"

extern GAsyncQueue *here_is_your_job;
extern GAsyncQueue *refresh_db_queue;
extern GAsyncQueue *data_queue;

GThread **threads = NULL;
struct thread_data *loader_td = NULL;
void *loader_thread(struct thread_data *td);

void initialize_loader_threads(struct configuration *conf){
  guint n=0;
  threads = g_new(GThread *, num_threads);
  loader_td = g_new(struct thread_data, num_threads);
  max_threads_per_table=max_threads_per_table>num_threads?num_threads:max_threads_per_table;
  for (n = 0; n < num_threads; n++) {
    initialize_thread_data(&(loader_td[n]), conf, WAITING, n + 1, NULL);
    threads[n] =
      m_thread_new("myloader_loader",(GThreadFunc)loader_thread, &loader_td[n], "Loader thread could not be created");
    // Here, the ready queue is being used to serialize the connection to the database.
    // We don't want all the threads try to connect at the same time
    g_async_queue_pop(conf->ready);
  }
}

gboolean create_index_job(struct configuration *conf, struct db_table * dbt, guint tdid){
  message("Thread %d: Enqueuing index for table: %s.%s", tdid, dbt->database->real_database, dbt->table);
  struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->database,dbt->indexes, INDEXES);
  trace("index_queue <- %s: %s.%s", rjtype2str(rj->type), dbt->database->real_database, dbt->table);
  g_async_queue_push(conf->index_queue, new_control_job(JOB_RESTORE,rj,dbt->database));
  dbt->schema_state=INDEX_ENQUEUED;
  return TRUE;
}

gboolean enqueue_index_for_dbt_if_possible(struct configuration *conf, struct db_table * dbt){
  if (dbt->schema_state==DATA_DONE){
    if (dbt->indexes == NULL){
      dbt->schema_state=ALL_DONE;
      return FALSE;
    }else{
      return create_index_job(conf, dbt, 0);
    }
  }
  return dbt->schema_state != ALL_DONE;
}

void enqueue_indexes_if_possible(struct configuration *conf){
  (void )conf;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt = NULL;
  while (iter != NULL){
    dbt=iter->data;
    g_mutex_lock(dbt->mutex);
    (void) enqueue_index_for_dbt_if_possible(conf,dbt);
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}



void *process_loader_thread(struct thread_data * td) {
  struct control_job *job = NULL;
  gboolean cont=TRUE;
  enum file_type ft=-1;
//  enum file_type ft;
//  int remaining_shutdown_pass=2*num_threads;
  struct restore_job *rj=NULL;
//  guint pass=0;
  struct db_table * dbt = NULL;
  while (cont){
    // control job threads needs to know that I'm ready to receive another job
    trace("refresh_db_queue <- %s", ft2str(THREAD));
    g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(THREAD));
    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(here_is_your_job));
    trace("here_is_your_job -> %s", ft2str(ft));
    switch (ft){
    case DATA:
      rj = (struct restore_job *)g_async_queue_pop(data_queue);
      dbt = rj->dbt;
      trace("data_queue -> %s: %s.%s, threads %u", rjtype2str(rj->type), dbt->database->real_database, dbt->real_table, dbt->current_threads);
      job=new_control_job(JOB_RESTORE,rj, dbt->database);
      td->dbt=dbt;
//      td->use_database=job->use_database;
//      execute_use_if_needs_to(&(td->connection_data), job->use_database, "Restoring tables (2)");
      cont=process_job(td, job, NULL);
      g_mutex_lock(dbt->mutex);
      dbt->current_threads--;
      trace("%s.%s: done job, threads %u", dbt->database->real_database, dbt->real_table, dbt->current_threads);
      g_mutex_unlock(dbt->mutex);
      break;
    case SHUTDOWN:
      cont=FALSE;
      break;
    case IGNORED:
      usleep(1000);
      break;
    default:
      NULL;
    }
  }
  enqueue_indexes_if_possible(td->conf);
  g_message("Thread %d: Data import ended", td->thread_id);
  maybe_shutdown_control_job();
//  process_index(td);
  return NULL;
}


void *loader_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  set_thread_name("T%02u", td->thread_id);
  trace("Thread %u: Starting import", td->thread_id);
  process_loader_thread(td);

  trace("Thread %u: ending", td->thread_id);
  return NULL;
}


void wait_loader_threads_to_finish(){
  guint n=0;
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  restore_job_finish();
}

void inform_restore_job_running(){
  if (shutdown_triggered){
    guint n=0, sum=0, prev_sum=0;
    for (n = 0; n < num_threads; n++) {
      sum+=loader_td[n].status == STARTED ? 1 : 0;
    }
    fprintf(stdout, "Printing remaining loader threads every %d seconds", RESTORE_JOB_RUNNING_INTERVAL);
    while (sum>0){
      if (prev_sum != sum){
        fprintf(stdout, "\nThere are %d loader thread still working", sum);
        fflush(stdout);
      }else{
        fprintf(stdout, ".");
        fflush(stdout);
      }
      sleep(RESTORE_JOB_RUNNING_INTERVAL);
      prev_sum=sum;
      sum=0;
      for (n = 0; n < num_threads; n++) {
        sum+=loader_td[n].status == STARTED ? 1 : 0;
      }
    }
    fprintf(stdout, "\nAll loader thread had finished\n");
  }
}


void free_loader_threads(){
  g_free(loader_td);
  g_free(threads);
}



