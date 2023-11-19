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
//#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "connection.h"
#include <errno.h>
#include "myloader_global.h"
#include "myloader_worker_schema.h"

/* refresh_db_queue2 is for schemas creation */
GAsyncQueue *refresh_db_queue2 = NULL;
struct thread_data *schema_td = NULL;

void schema_queue_push(enum file_type current_ft){
  trace("refresh_db_queue2 <- %s", ft2str(current_ft));
  g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(current_ft));
}


static
void wait_db_schema_created(struct database * database)
{
  if (database->schema_state != CREATED) {
    trace("Waiting DB created: %s", database->name);
    g_mutex_lock(database->mutex);
    while (database->schema_state != CREATED)
      g_cond_wait(&database->state_cond, database->mutex);
    g_mutex_unlock(database->mutex);
  }
}


static
void set_db_schema_created(struct database * real_db_name, struct configuration *conf)
{
  struct control_job * cj;
  enum file_type ft;
  GAsyncQueue *queue;
  GAsyncQueue * object_queue= conf->table_queue;
  real_db_name->schema_state= CREATED;

  /* Until all sequences processed we requeue only sequences */
  if (sequences_processed < sequences) {
    ft= SCHEMA_SEQUENCE;
    queue= real_db_name->sequence_queue;
  } else {
    ft= SCHEMA_TABLE;
    queue= real_db_name->queue;
  }
  cj= g_async_queue_try_pop(queue);
  while (cj != NULL){
    g_async_queue_push(object_queue, cj);
    trace("refresh_db_queue2 <- %s (requeuing from db queue)", ft2str(ft));
    g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(ft));
    cj = g_async_queue_try_pop(queue);
  }

  // TODO: use g_cond_signal() after ensuring INTERMEDIATE_ENDED processed only in one thread
  g_cond_broadcast(&real_db_name->state_cond);
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
/* @return TRUE: continue worker_schema_thread() loop */
gboolean process_schema(struct thread_data * td){


  enum file_type ft;
  GHashTableIter iter;
  gchar * lkey=NULL;
  struct database * real_db_name = NULL;
  struct control_job *job = NULL;
  gboolean ret=TRUE;
  ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(refresh_db_queue2));
  trace("refresh_db_queue2 -> %s", ft2str(ft));
  switch (ft){

    case SCHEMA_CREATE:
      job=g_async_queue_pop(td->conf->database_queue);
      real_db_name=job->data.restore_job->data.srj->database;
      trace("database_queue -> %s: %s", ft2str(ft), real_db_name->name);
      g_mutex_lock(real_db_name->mutex);
      ret=process_job(td, job);
      set_db_schema_created(real_db_name, td->conf);
      trace("Set DB created: %s", real_db_name->name);
      g_mutex_unlock(real_db_name->mutex);
      break;
    case SCHEMA_TABLE:
    case SCHEMA_SEQUENCE: {
      job=g_async_queue_pop(td->conf->table_queue);
      const gboolean restore= (job->type == JOB_RESTORE);
      char *filename;
      if (restore) {
        filename= job->data.restore_job->filename;
        trace("table_queue -> %s: %s", ft2str(ft), filename);
        execute_use_if_needs_to(td, job->use_database, "Restoring table structure");
        trace("table_queue -> %s: %s", ft2str(ft), filename);
      } else
        trace("table_queue -> %s", jtype2str(job->type));
      ret=process_job(td, job);
      if (ft == SCHEMA_TABLE) /* TODO: for spoof view table don't do DATA */
        refresh_db_and_jobs(DATA);
      else if (restore) {
        g_assert(ft == SCHEMA_SEQUENCE && sequences_processed < sequences);
        g_mutex_lock(&sequences_mutex);
        ++sequences_processed;
        trace("Processed sequence: %s (%u of %u)", filename, sequences_processed, sequences);
        // TODO: use g_cond_signal() after ensuring INTERMEDIATE_ENDED processed only in one thread
        g_cond_broadcast(&sequences_cond);
        g_mutex_unlock(&sequences_mutex);
      }
      break;
    }
    case INTERMEDIATE_ENDED:
      if (!second_round){
        g_mutex_lock(&sequences_mutex);
        while (sequences_processed < sequences) {
          trace("INTERMEDIATE_ENDED waits %d sequences", sequences - sequences_processed);
          g_cond_wait(&sequences_cond, &sequences_mutex);
        }
        g_mutex_unlock(&sequences_mutex);
        g_hash_table_iter_init ( &iter, db_hash );
        while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &real_db_name ) ) {
          wait_db_schema_created(real_db_name);
          set_db_schema_created(real_db_name, td->conf);
        }
        message("Schema creation enqueing completed");
        second_round=TRUE;
        trace("refresh_db_queue2 <- %s (first round)", ft2str(ft));
        g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(ft));
      }else{
        set_table_schema_state_to_created(td->conf);
        message("Table creation enqueing completed");
        guint n=0;
        td= schema_td; /* we also sending to ourselves and upper loop of worker_schema_thread() will send us to SCHEMA_TABLE/JOB_SHUTDOWN */
        for (n = 0; n < max_threads_for_schema_creation; n++, td++) {
          trace("table_queue <- JOB_SHUTDOWN");
          g_async_queue_push(td->conf->table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
          trace("refresh_db_queue2 <- %s (second round)", ft2str(SCHEMA_TABLE));
          g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(SCHEMA_TABLE));
        }
      }
      break;
    default:
        message("Default in schema: %d", ft);
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

  m_connect(td->thrconn);

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=database_db;
    if (execute_use(td)){
      m_critical("S-Thread %d: Error switching to database `%s` when initializing", td->thread_id, td->current_database);
    }
  }

  message("S-Thread %d: Starting import", td->thread_id);
  gboolean cont=TRUE;
  while (cont){
    cont=process_schema(td);
  }
  message("S-Thread %d: Import completed", td->thread_id);

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  return NULL;
}

GThread **schema_threads = NULL;

void initialize_worker_schema(struct configuration *conf){
  guint n=0;
  init_connection_mutex = g_mutex_new();
  refresh_db_queue2 = g_async_queue_new();
  schema_threads = g_new(GThread *, max_threads_for_schema_creation);
  schema_td = g_new(struct thread_data, max_threads_for_schema_creation);
  g_message("Initializing initialize_worker_schema");
  for (n = 0; n < max_threads_for_schema_creation; n++) {
    schema_td[n].conf = conf;
    schema_td[n].thread_id = n + 1 + num_threads;
    schema_td[n].status=WAITING;
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
