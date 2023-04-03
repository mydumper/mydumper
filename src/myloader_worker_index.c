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
#include <stdlib.h>
#include <string.h>
#include "myloader_stream.h"
#include "common.h"
#include "server_detect.h"
#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "connection.h"
#include <errno.h>
#include "myloader_global.h"
#include "myloader_worker_index.h"


GRecMutex * innodb_optimize_keys_all_tables_mutex=NULL;

gboolean process_index(struct thread_data * td){
  struct control_job *job=g_async_queue_pop(td->conf->index_queue);
  if (job->type==JOB_SHUTDOWN)
    return FALSE;

  struct db_table *dbt=job->data.restore_job->dbt;
    execute_use_if_needs_to(td, job->use_database, "Restoring index");
    dbt->start_index_time=g_date_time_new_now_local();
    g_message("restoring index: %s.%s", dbt->database->name, dbt->table);
    process_job(td, job);
    dbt->finish_time=g_date_time_new_now_local();
    g_mutex_lock(dbt->mutex);
    dbt->schema_state=ALL_DONE;
    g_mutex_unlock(dbt->mutex);
    g_mutex_lock(index_mutex);
    index_threads_counter--;
    g_mutex_unlock(index_mutex);
  return TRUE;
}


void create_index_shutdown_job(struct configuration *conf){
  guint n=0;
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_async_queue_push(conf->index_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
}

static GMutex *init_connection_mutex=NULL;

void *worker_index_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_connection_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_connection_mutex);
  td->current_database=NULL;

  m_connect(td->thrconn, NULL);

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=db;
    if (execute_use(td)){
      m_critical("I-Thread %d: Error switching to database `%s` when initializing", td->thread_id, td->current_database);
    }
  }
  if (innodb_optimize_keys_all_tables){
    g_rec_mutex_lock(innodb_optimize_keys_all_tables_mutex);
    g_rec_mutex_unlock(innodb_optimize_keys_all_tables_mutex);
  }
    
  g_debug("I-Thread %d: Starting import", td->thread_id);
  gboolean cont=TRUE;
  while (cont){
    cont=process_index(td);
  }

  return NULL;
}

GThread **index_threads = NULL;
struct thread_data *index_td = NULL;

void initialize_worker_index(struct configuration *conf){
  guint n=0;
  init_connection_mutex = g_mutex_new();
  index_threads = g_new(GThread *, max_threads_for_index_creation);
  index_td = g_new(struct thread_data, max_threads_for_index_creation);
  innodb_optimize_keys_all_tables_mutex=g_rec_mutex_new();
  g_rec_mutex_lock(innodb_optimize_keys_all_tables_mutex);
  for (n = 0; n < max_threads_for_index_creation; n++) {
    index_td[n].conf = conf;
    index_td[n].thread_id = n + 1;
    index_threads[n] =
        g_thread_create((GThreadFunc)worker_index_thread, &index_td[n], TRUE, NULL);
  }
}

void wait_index_worker_to_finish(){
  guint n=0;
  for (n = 0; n < max_threads_for_index_creation; n++) {
    g_thread_join(index_threads[n]);
  }
}

void start_innodb_optimize_keys_all_tables(){
  g_rec_mutex_unlock(innodb_optimize_keys_all_tables_mutex);
}

void free_index_worker_threads(){
  g_free(index_td);
  g_free(index_threads);
}
