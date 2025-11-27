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
#include "myloader_worker_index.h"
#include "myloader_database.h"
#include "myloader_worker_loader_main.h"

GThread **threads = NULL;
struct thread_data *loader_td = NULL;
void *loader_thread(struct thread_data *td);
GAsyncQueue *data_job_queue = NULL;

void initialize_loader_threads(struct configuration *conf){
  guint n=0;
  data_job_queue = g_async_queue_new();
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

struct data_job * new_data_job(enum data_job_type type, struct restore_job *rj){
  struct data_job * dj = g_new0(struct data_job, 1);
  dj->type = type;
  dj->restore_job = rj;
  return dj;
}

void data_job_push(enum data_job_type type, struct restore_job *rj){
  trace("data_job_queue <- %s", data_job_type2str(type));
  g_async_queue_push(data_job_queue, new_data_job(type, rj) );
}

void data_ended(){
  data_job_push(DATA_PROCESS_ENDED, NULL);
}

gboolean process_loader(struct thread_data * td) {
  struct db_table * dbt = NULL;
  struct data_job *dj= (struct data_job *)g_async_queue_pop(data_job_queue);
  g_debug("[LOADER] Thread %d: Received data job type=%s", td->thread_id, data_job_type2str(dj->type));

  switch (dj->type){
    case DATA_JOB:
      dbt=dj->restore_job->dbt;
      td->dbt=dj->restore_job->dbt;

      // Log detailed information before processing
      g_message("[LOADER] Thread %d: Processing DATA_JOB for %s.%s file=%s schema_state=%s",
                td->thread_id,
                dbt->database->target_database,
                dbt->source_table_name,
                dj->restore_job->filename ? dj->restore_job->filename : "(null)",
                status2str(dbt->schema_state));

      process_restore_job(td, dj->restore_job);
      table_lock(dbt);
      dbt->current_threads--;
      g_debug("[LOADER] Thread %d: Completed job for %s.%s, threads now %u, remaining_jobs=%d",
              td->thread_id, dbt->database->target_database, dbt->source_table_name,
              dbt->current_threads, dbt->remaining_jobs);
      table_unlock(dbt);
      break;
    case DATA_PROCESS_ENDED:
      g_message("[LOADER] Thread %d: Received DATA_PROCESS_ENDED, propagating and exiting", td->thread_id);
      data_job_push(DATA_PROCESS_ENDED, NULL);
      return FALSE;
      break;
    case DATA_ENDED:
      g_message("[LOADER] Thread %d: Received DATA_ENDED, exiting", td->thread_id);
      return FALSE;
      break;
    }
//  maybe_shutdown_control_job();
//  process_index(td);
  return TRUE;
}

void *loader_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  gboolean cont=TRUE;
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  set_thread_name("T%02u", td->thread_id);
  g_message("L-Thread %u: Starting import", td->thread_id);
  while (cont){
    data_control_queue_push(REQUEST_DATA_JOB);
    cont=process_loader(td);
  }
//  process_loader_thread(td);
  enqueue_indexes_if_possible(td->conf);
  g_message("L-Thread %u: ending", td->thread_id);
  return NULL;
}


void wait_loader_threads_to_finish(){
  guint n=0;
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
//  restore_job_finish();
//  data_control_queue_push(SHUTDOWN);
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



