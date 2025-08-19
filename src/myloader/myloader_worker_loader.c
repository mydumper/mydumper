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
    ft=request_restore_data_job();
    switch (ft){
    case DATA:
      rj = request_next_data_job();
      dbt = rj->dbt;
      job=new_control_job(JOB_RESTORE,rj, dbt->database);
      td->dbt=dbt;
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



