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
#include "myloader_global.h"


GAsyncQueue * innodb_optimize_keys_all_tables_queue=NULL;
GThread **index_threads = NULL;
struct thread_data *index_td = NULL;
static GMutex *init_connection_mutex=NULL;
void *worker_index_thread(struct thread_data *td);

void initialize_worker_index(struct configuration *conf){
  guint n=0;
//  index_mutex = g_mutex_new();
  init_connection_mutex = g_mutex_new();
  index_threads = g_new(GThread *, max_threads_for_index_creation);
  index_td = g_new(struct thread_data, max_threads_for_index_creation);
  innodb_optimize_keys_all_tables_queue=g_async_queue_new();
  for (n = 0; n < max_threads_for_index_creation; n++) {
    initialize_thread_data(&(index_td[n]), conf, WAITING, n + 1 + num_threads + max_threads_for_schema_creation, NULL);
    index_threads[n] =
        g_thread_new("myloader_index",(GThreadFunc)worker_index_thread, &index_td[n]);
  }
}

gboolean process_index(struct thread_data * td){
  struct control_job *job=g_async_queue_pop(td->conf->index_queue);
  if (job->type==JOB_SHUTDOWN)
  {
    trace("index_queue -> %s", jtype2str(job->type));
    return FALSE;
  }

  g_assert(job->type == JOB_RESTORE);
  struct db_table *dbt=job->data.restore_job->dbt;
  trace("index_queue -> %s: %s.%s", rjtype2str(job->data.restore_job->type), dbt->database->real_database, dbt->table);
  dbt->start_index_time=g_date_time_new_now_local();
  g_message("restoring index: %s.%s", dbt->database->name, dbt->table);
  process_job(td, job, NULL);
  dbt->finish_time=g_date_time_new_now_local();
  g_mutex_lock(dbt->mutex);
  dbt->schema_state=ALL_DONE;
  g_mutex_unlock(dbt->mutex);
  return TRUE;
}

void *worker_index_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_connection_mutex);
  g_mutex_unlock(init_connection_mutex);

  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));
  if (innodb_optimize_keys_all_tables){
    g_async_queue_pop(innodb_optimize_keys_all_tables_queue);
  }

  set_thread_name("I%02u", td->thread_id);
  trace("I-Thread %u: Starting import", td->thread_id);
  gboolean cont=TRUE;
  while (cont){
    cont=process_index(td);
  }

  trace("I-Thread %u: ending", td->thread_id);
  return NULL;
}

void create_index_shutdown_job(struct configuration *conf){
  guint n=0;
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_async_queue_push(conf->index_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
  }
}

void wait_index_worker_to_finish(){
  guint n=0;
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_thread_join(index_threads[n]);
  }
}

void start_innodb_optimize_keys_all_tables(){
  guint n=0;
  trace("innodb_optimize_keys_all_tables_queue <- 1 (%u times)", max_threads_for_index_creation);
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_async_queue_push(innodb_optimize_keys_all_tables_queue, GINT_TO_POINTER(1));
  }
}

void free_index_worker_threads(){
  g_free(index_td);
  g_free(index_threads);
}
