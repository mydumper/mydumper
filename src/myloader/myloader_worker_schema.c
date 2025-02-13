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

/* refresh_db_queue2 is for schemas creation */
GAsyncQueue *refresh_db_queue2 = NULL;
struct thread_data *schema_td = NULL;

void schema_queue_push(enum file_type current_ft){
  trace("refresh_db_queue2 <- %s", ft2str(current_ft));
  g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(current_ft));
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
}

static
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
  const gboolean postpone_load= (overwrite_tables && !overwrite_unsafe);
  ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(refresh_db_queue2));
  trace("refresh_db_queue2 -> %s", ft2str(ft));
  switch (ft){

    case SCHEMA_CREATE:
      job=g_async_queue_pop(td->conf->database_queue);
      real_db_name=job->data.restore_job->data.srj->database;
      trace("database_queue -> %s: %s", ft2str(ft), real_db_name->name);
      g_mutex_lock(real_db_name->mutex);
      ret=process_job(td, job, NULL);
      set_db_schema_created(real_db_name, td->conf);
      trace("Set DB created: %s", real_db_name->name);
      g_mutex_unlock(real_db_name->mutex);
      break;
    case CJT_RESUME:
      cjt_resume();
      // fall through
    case SCHEMA_TABLE:
    case SCHEMA_SEQUENCE: {
      const char *qname;
      job= g_async_queue_pop(td->conf->table_queue);
      qname= "table_queue";
      if (job->type == JOB_SHUTDOWN) {
        struct control_job *rjob= g_async_queue_try_pop(td->conf->retry_queue);
        if (rjob) {
          g_async_queue_push(td->conf->table_queue, job);
          job= rjob;
          qname= "retry_queue";
        }
      }
      const gboolean restore= (job->type == JOB_RESTORE);
      gboolean retry= FALSE;
      char *filename;
      if (restore) {
        filename= job->data.restore_job->filename;
//        execute_use_if_needs_to(&(td->connection_data), job->use_database, "Restoring table structure");
        trace("%s -> %s: %s", qname, ft2str(ft), filename);
      } else
        trace("%s -> %s", qname, jtype2str(job->type));
      ret= process_job(td, job, &retry);
      if (retry) {
        g_assert(restore);
        trace("retry_queue <- %s: %s", ft2str(ft), filename);
        g_async_queue_push(td->conf->retry_queue, job);
        refresh_db_and_jobs(ft);
        break;
      }
      if (ft == SCHEMA_TABLE) { /* TODO: for spoof view table don't do DATA */
        refresh_db_and_jobs(DATA);
      } else if (restore) {
        g_assert(ft == SCHEMA_SEQUENCE && sequences_processed < sequences);
        g_mutex_lock(&sequences_mutex);
        ++sequences_processed;
        trace("Processed sequence: %s (%u of %u)", filename, sequences_processed, sequences);
        g_mutex_unlock(&sequences_mutex);
      }
      break;
    }
    case INTERMEDIATE_ENDED:
      if (!second_round){
        g_mutex_lock(&sequences_mutex);
        if (sequences_processed < sequences) {
          trace("INTERMEDIATE_ENDED waits %d sequences", sequences - sequences_processed);
          refresh_db_and_jobs(INTERMEDIATE_ENDED);
          g_mutex_unlock(&sequences_mutex);
          return TRUE;
        }
        g_mutex_unlock(&sequences_mutex);
        g_hash_table_iter_init ( &iter, db_hash );
        /* Wait while all DB created and go "second round" */
        while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &real_db_name ) ) {
          g_mutex_lock(real_db_name->mutex);
          if (real_db_name->schema_state != CREATED) {
            trace("INTERMEDIATE_ENDED waits %s created, current state: %s", real_db_name->name, status2str(real_db_name->schema_state));
            if (real_db_name->schema_state == NOT_FOUND)
              real_db_name->schema_state=NOT_FOUND_2;
            else if (real_db_name->schema_state == NOT_FOUND_2){
              g_warning("Schema file for `%s` not found, continue anyways",real_db_name->name);
              real_db_name->schema_state=CREATED;
            }
            refresh_db_and_jobs(INTERMEDIATE_ENDED);
            g_mutex_unlock(real_db_name->mutex);
            return TRUE;
          }
          g_mutex_unlock(real_db_name->mutex);
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
          g_async_queue_push(td->conf->table_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
          trace("refresh_db_queue2 <- %s (second round)", ft2str(SCHEMA_TABLE));
          if (!postpone_load || n < max_threads_for_schema_creation - 1)
            g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(SCHEMA_TABLE));
        }
        if (postpone_load)
          g_async_queue_push(refresh_db_queue2, GINT_TO_POINTER(CJT_RESUME));
      }
      break;
    default:
        message("Default in schema: %d", ft);
      break;
  }

  return ret;
}

void *worker_schema_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;

  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  set_thread_name("S%02u", td->thread_id);
  message("S-Thread %u: Starting import", td->thread_id);
  gboolean cont=TRUE;
  while (cont){
    cont=process_schema(td);
  }
  message("S-Thread %u: Import completed", td->thread_id);
  return NULL;
}

GThread **schema_threads = NULL;

void initialize_worker_schema(struct configuration *conf){
  guint n=0;
  refresh_db_queue2 = g_async_queue_new();
  schema_threads = g_new(GThread *, max_threads_for_schema_creation);
  schema_td = g_new(struct thread_data, max_threads_for_schema_creation);
  g_message("Initializing initialize_worker_schema");
  for (n = 0; n < max_threads_for_schema_creation; n++) 
    initialize_thread_data(&(schema_td[n]), conf, WAITING, n + 1 + num_threads, NULL);
}

void start_worker_schema(){
  guint n=0;
  for (n = 0; n < max_threads_for_schema_creation; n++)
    schema_threads[n] =
        m_thread_new("myloader_schema",(GThreadFunc)worker_schema_thread, &schema_td[n], "Schema thread could not be created");
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
