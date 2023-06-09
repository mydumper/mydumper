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
#include "myloader_worker_schema.h"


GAsyncQueue *refresh_db_queue2 = NULL;

void schema_queue_push(enum file_type current_ft){
  g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(current_ft));
}

void set_db_schema_state_to_created( struct database * database, GAsyncQueue * object_queue){
  database->schema_state=CREATED;
  struct control_job * cj = g_async_queue_try_pop(database->queue);
  while (cj != NULL){
    g_async_queue_push(object_queue, cj);
    g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(SCHEMA_TABLE));
    cj = g_async_queue_try_pop(database->queue);
  }
}


void set_table_schema_state_to_created (struct configuration *conf){
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt = NULL;
  while (iter != NULL){
    dbt=iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state == NOT_FOUND )
      dbt->schema_state = CREATED;
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}

gboolean second_round=FALSE;

gboolean process_schema(struct thread_data * td){


  enum file_type ft;
  GHashTableIter iter;
  gchar * lkey=NULL;
  struct database * real_db_name = NULL;
  struct control_job *job = NULL;
  gboolean ret=TRUE;
  ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(refresh_db_queue2));
  switch (ft){

    case SCHEMA_CREATE:
      job=g_async_queue_pop(td->conf->database_queue);
      real_db_name=job->data.restore_job->data.srj->database;
      g_mutex_lock(real_db_name->mutex);
      ret=process_job(td, job);
      set_db_schema_state_to_created(real_db_name, td->conf->table_queue);
      g_mutex_unlock(real_db_name->mutex);
      break;
    case SCHEMA_TABLE:
      job=g_async_queue_pop(td->conf->table_queue);
      execute_use_if_needs_to(td, job->use_database, "Restoring table structure");
      ret=process_job(td, job);
      refresh_db_and_jobs(DATA);
      break;
    case INTERMEDIATE_ENDED:
      if (!second_round){
        g_hash_table_iter_init ( &iter, db_hash );
        while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &real_db_name ) ) {
          g_mutex_lock(real_db_name->mutex);
          set_db_schema_state_to_created(real_db_name, td->conf->table_queue);
          g_mutex_unlock(real_db_name->mutex);
        }
        g_message("Schema creation enqueing completed");
        second_round=TRUE;
        g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(ft));
      }else{
        set_table_schema_state_to_created(td->conf);
        g_message("Table creation enqueing completed");
        guint n=0;
        for (n = 0; n < max_threads_for_schema_creation; n++) {
          g_async_queue_push(td->conf->table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));

          g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(SCHEMA_TABLE));
        }
      }
      break;
    default:
        g_message("Default in schema: %d", ft);
      break;
  }

  return ret;
}

static GMutex *init_connection_mutex=NULL;

void *worker_schema_thread(struct thread_data *td) {
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
      m_critical("S-Thread %d: Error switching to database `%s` when initializing", td->thread_id, td->current_database);
    }
  }
    
  g_message("S-Thread %d: Starting import", td->thread_id);
  gboolean cont=TRUE;
  while (cont){
    cont=process_schema(td);
  }
  g_message("S-Thread %d: Import completed", td->thread_id);

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  g_debug("S-Thread %d: ending", td->thread_id);
  return NULL;
}

GThread **schema_threads = NULL;
struct thread_data *schema_td = NULL;

void initialize_worker_schema(struct configuration *conf){
  guint n=0;
  init_connection_mutex = g_mutex_new();
  refresh_db_queue2 = g_async_queue_new();
  schema_threads = g_new(GThread *, max_threads_for_schema_creation);
  schema_td = g_new(struct thread_data, max_threads_for_schema_creation);
  g_message("Initializing initialize_worker_schema");
  for (n = 0; n < max_threads_for_schema_creation; n++) {
    schema_td[n].conf = conf;
    schema_td[n].thread_id = n + 1;
    schema_threads[n] =
        g_thread_create((GThreadFunc)worker_schema_thread, &schema_td[n], TRUE, NULL);
  }
}

void wait_schema_worker_to_finish(){
  guint n=0;
  for (n = 0; n < max_threads_for_schema_creation; n++) {
    g_thread_join(schema_threads[n]);
  }
}

void free_schema_worker_threads(){
  g_free(schema_td);
  g_free(schema_threads);
}
