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
#include "myloader_worker_schema.h"

/* refresh_db_queue2 is for schemas creation */
static
GAsyncQueue *refresh_db_queue2 = NULL;
extern GHashTable *database_hash;
GAsyncQueue *schema_job_queue = NULL;
struct thread_data *schema_td = NULL;
GAsyncQueue *retry_queue = NULL;

gint schema_job_cmp(gconstpointer _a, gconstpointer _b, gpointer user_data){
   struct schema_job *a= ((struct schema_job *) _a);
   struct schema_job *b= ((struct schema_job *) _b);
   (void)user_data;
   return a->type > b->type;
}

struct schema_job * new_schema_job(enum schema_job_type type, struct restore_job *rj, struct database *use_database){
  struct schema_job * sj = g_new0(struct schema_job, 1);
  sj->type = type;
  sj->restore_job = rj;
  (void) use_database;
//  sj->use_database = use_database;
  return sj;
}

static
void schema_job_queue_push(struct schema_job *sj){
  trace("schema_job_queue <- %s", schema_job_type2str(sj->type));
  g_async_queue_push(schema_job_queue, sj);
}

gboolean schema_push( enum schema_job_type schema_worker_job, gchar * filename, enum restore_job_type rj_type, struct db_table * dbt, struct database * _database, GString * statement, enum restore_job_statement_type object, struct database *use_database ){
  struct restore_job *rj = new_schema_restore_job(filename, rj_type, dbt, _database, statement, object);
  struct schema_job *sj = new_schema_job(schema_worker_job, rj, use_database);
  g_mutex_lock(_database->mutex);
  trace("schema push %s", filename);
  if (_database->schema_state != CREATED && schema_worker_job != SCHEMA_CREATE_JOB){
    if (schema_worker_job == SCHEMA_SEQUENCE_JOB ) {
      trace("%s.sequence_queue <- %s", _database->target_database, schema_job_type2str(sj->type));
      trace("_database: %p; sequence_queue: %p", _database, _database->sequence_queue);
      g_async_queue_push(_database->sequence_queue, sj);
      g_mutex_unlock(_database->mutex);
      return FALSE;
    }else
    if (schema_worker_job == SCHEMA_TABLE_JOB ) {
      trace("%s.table_queue <- %s", _database->target_database, schema_job_type2str(sj->type));
      trace("_database: %p; table_queue: %p", _database, _database->table_queue);
      g_async_queue_push(_database->table_queue, sj);
      g_mutex_unlock(_database->mutex);
      return FALSE;
    }else{
      trace("not enqueuing %s %s", filename, schema_job_type2str(sj->type));
    
    }
  }else{
    trace("schema_job_queue <- %s (sorted)", schema_job_type2str(sj->type));
    g_async_queue_push_sorted(schema_job_queue, sj, &schema_job_cmp, NULL);
  }
  g_mutex_unlock(_database->mutex);
  return TRUE;
}

void schema_ended(){
  schema_job_queue_push(new_schema_job(SCHEMA_PROCESS_ENDED, NULL, NULL));
}

// _database is locked
static
void set_db_schema_created(struct database * _database)
{
//  struct control_job * cj;
//  enum file_type ft;
//  GAsyncQueue *queue;
  _database->schema_state= CREATED;

  /* Until all sequences processed we requeue only sequences */
/*
  if (sequences_processed < sequences) {
    trace("FT change to SCHEMA_SEQUENCE due %d < %d", sequences_processed, sequences );
//    ft= SCHEMA_SEQUENCE;
    queue= _database->sequence_queue;
  } else {
//    ft= SCHEMA_TABLE;
    queue= _database->table_queue;
  }
  cj= g_async_queue_try_pop(queue);
  while (cj != NULL){
    g_async_queue_push( conf->table_queue, cj);
//    schema_job_queue_push(ft, " (requeuing from db queue)");
    cj = g_async_queue_try_pop(queue);
  }
  */


  struct schema_job *sj = g_async_queue_try_pop(_database->sequence_queue);
  while (sj){
    schema_job_queue_push(sj);
    sj = g_async_queue_try_pop(_database->sequence_queue);  
  }
  sj = g_async_queue_try_pop(_database->table_queue);
  while (sj){
    schema_job_queue_push(sj);
    sj = g_async_queue_try_pop(_database->table_queue);
  }
}

static
void set_table_schema_state_to_created (struct configuration *conf){
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt = NULL;
  while (iter != NULL){
    dbt=iter->data;
    table_lock(dbt);
    if (dbt->schema_state == NOT_FOUND )
      dbt->schema_state = CREATED;
    table_unlock(dbt);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}

gboolean second_round=FALSE;
/* @return TRUE: continue worker_schema_thread() loop */
gboolean process_schema(struct thread_data * td){
  struct database * _database = NULL;
  struct control_job *job = NULL;

  struct schema_job * schema_job = g_async_queue_pop(schema_job_queue);
  trace("schema_job_queue -> %s", schema_job_type2str(schema_job->type));

  switch (schema_job->type){
    case SCHEMA_CREATE_JOB:
      _database=schema_job->restore_job->data.srj->database;
      trace("database_queue -> %s", _database->source_database);
      g_mutex_lock(_database->mutex);
      process_restore_job(td, schema_job->restore_job);
      //      ret=process_job(td, job, NULL);
      set_db_schema_created(_database);
      trace("Set DB created: %s", _database->source_database);
      g_mutex_unlock(_database->mutex);
      break;
    case SCHEMA_SEQUENCE_JOB:
    case SCHEMA_TABLE_JOB:
      if (process_restore_job(td, schema_job->restore_job)){
        trace("retry_queue <- ");
        g_async_queue_push(retry_queue, job);
      }
      wake_data_threads();
      break;
    case SCHEMA_PROCESS_ENDED:
      // set as created all database, 
      // which implies all tables are created
      trace("SCHEMA_PROCESS_ENDED ");
      schema_job = g_async_queue_try_pop(retry_queue);
      while (schema_job){
        schema_job_queue_push(schema_job);
        schema_job = g_async_queue_try_pop(retry_queue);
      }

      GHashTableIter iter;
      gpointer _key;
      g_hash_table_iter_init (&iter, database_hash);
      while (g_hash_table_iter_next (&iter, &_key, (gpointer) &_database)){
        set_db_schema_created(_database);
      }
      
      schema_job_queue_push(new_schema_job(SCHEMA_ENDED, NULL, NULL));

      //refresh_table_list(td->conf);
      break;

    case SCHEMA_ENDED:
      g_async_queue_push(schema_job_queue, schema_job);
      //refresh_table_list(td->conf);
      return FALSE;
      break;
                          
  }

  return TRUE;
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
  refresh_table_list(td->conf);
  message("S-Thread %u: Import completed", td->thread_id);
  return NULL;
}

GThread **schema_threads = NULL;

void initialize_worker_schema(struct configuration *conf){
  guint n=0;
  refresh_db_queue2 = g_async_queue_new();
  schema_job_queue = g_async_queue_new();
  retry_queue = g_async_queue_new();
  schema_threads = g_new(GThread *, max_threads_for_schema_creation);
  schema_td = g_new(struct thread_data, max_threads_for_schema_creation);
  g_message("Initializing initialize_worker_schema");
  for (n = 0; n < max_threads_for_schema_creation; n++) 
    initialize_thread_data(&(schema_td[n]), conf, WAITING, n + 1 + num_threads, NULL);

//  if (stream)
//    schema_job_queue_push(CJT_RESUME, "");
}

void start_worker_schema(){
  guint n=0;
  for (n = 0; n < max_threads_for_schema_creation; n++)
    schema_threads[n] =
        m_thread_new("myloader_schema",(GThreadFunc)worker_schema_thread, &schema_td[n], "Schema thread could not be created");
}


void wait_schema_worker_to_finish(struct configuration *conf){
  guint n=0;

  // In --no-data mode, index threads are started but never receive JOB_SHUTDOWN
  // because no data workers exist to send it. Send shutdown here to prevent deadlock.
  if (no_data && conf->index_queue != NULL) {
    guint num_idx_threads = max_threads_for_index_creation > 0
                          ? max_threads_for_index_creation
                          : num_threads;
    for (n = 0; n < num_idx_threads; n++) {
      g_async_queue_push(conf->index_queue, new_control_job(JOB_SHUTDOWN, NULL, NULL));
    }
  }

  trace("Waiting schema worker to finish");
  for (n = 0; n < max_threads_for_schema_creation; n++) {
    g_thread_join(schema_threads[n]);
  }
  // As all schema worker has finished, we need to let the process continue and set all tables as created
  set_table_schema_state_to_created(conf);
  data_control_queue_push(FILE_TYPE_ENDED);
  trace("Schema worker finished");
}

void free_schema_worker_threads(){
  g_free(schema_td);
  g_free(schema_threads);
}
