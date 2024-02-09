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
#include <glib.h>
#include <stdlib.h>
#include <unistd.h>

#include "myloader_control_job.h"
#include "myloader_restore_job.h"
#include "myloader_common.h"
#include "myloader_restore.h"
//#include "myloader_jobs_manager.h"
#include "myloader_global.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_index.h"
#include "myloader_worker_schema.h"

gboolean intermediate_queue_ended_local=FALSE;
gboolean dont_wait_for_schema_create=FALSE;
/* refresh_db_queue is for data loads */
GAsyncQueue *refresh_db_queue = NULL, *here_is_your_job=NULL, *data_queue=NULL;
//GAsyncQueue *give_me_another_job_queue = NULL;
static GThread *control_job_t = NULL;

gint last_wait=0;
//guint index_threads_counter = 0;
//GMutex *index_mutex=NULL;
void *control_job_thread(struct configuration *conf);

static GMutex *cjt_mutex= NULL;
static GCond *cjt_cond= NULL;
static gboolean cjt_paused= TRUE;

void cjt_resume()
{
  g_mutex_lock(cjt_mutex);
  cjt_paused= FALSE;
  g_cond_signal(cjt_cond);
  g_mutex_unlock(cjt_mutex);
}

void initialize_control_job (struct configuration *conf){
  refresh_db_queue = g_async_queue_new();
  here_is_your_job = g_async_queue_new();
  last_wait = num_threads;
  data_queue = g_async_queue_new();
  cjt_mutex= g_mutex_new();
  cjt_cond= g_cond_new();
//  give_me_another_job_queue = g_async_queue_new();
  control_job_t = g_thread_create((GThreadFunc)control_job_thread, conf, TRUE, NULL);

//  index_threads_counter = 0;
}

void wait_control_job()
{
  g_thread_join(control_job_t);
  g_mutex_free(cjt_mutex);
  g_cond_free(cjt_cond);
}

struct control_job * new_job (enum control_job_type type, void *job_data, struct database *use_database) {
  struct control_job *j = g_new0(struct control_job, 1);
  j->type = type;
  j->use_database=use_database;
  switch (type){
    case JOB_WAIT:
      j->data.queue = (GAsyncQueue *)job_data;
    case JOB_SHUTDOWN:
      break;
    default:
      j->data.restore_job = (struct restore_job *)job_data;
  }
  return j;
}

gboolean process_job(struct thread_data *td, struct control_job *job, gboolean *retry)
{
  switch (job->type) {
    case JOB_RESTORE: {
//      g_message("Restore Job");
      gboolean res= process_restore_job(td, job->data.restore_job);
      if (retry)
        *retry= res;
      return TRUE;
    }
    case JOB_WAIT:
//      g_message("Wait Job");
      g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
//      GAsyncQueue *queue=job->data.queue;
      g_async_queue_pop(job->data.queue);
      break;
    case JOB_SHUTDOWN: // TODO: do we need JOB_SHUTDOWN for data_queue as it is done in here_is_your_job queue?
//      g_message("Thread %d: shutting down", td->thread_id);
      g_free(job);
      return FALSE;
      break;
    default:
      g_free(job);
      g_critical("Something very bad happened!(1)");
      exit(EXIT_FAILURE);
  }
  g_free(job);
  return TRUE;
}
void schema_file_missed_lets_continue(struct thread_data * td){
  g_mutex_lock(td->conf->table_list_mutex);
  GList * iter=td->conf->table_list;
  guint i=0;
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    dbt->schema_state=CREATED;
    for(i=0; i<g_list_length(dbt->restore_job_list); i++){
      g_async_queue_push(td->conf->stream_queue, GINT_TO_POINTER(DATA));
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(td->conf->table_list_mutex);
}

gboolean are_we_waiting_for_schema_jobs_to_complete(struct thread_data * td){
  if (g_async_queue_length(td->conf->database_queue) > 0 ||
      g_async_queue_length(td->conf->table_queue) > 0 ||
      g_async_queue_length(td->conf->retry_queue) > 0)
    return TRUE;

  g_mutex_lock(td->conf->table_list_mutex);
  GList * iter=td->conf->table_list;
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state==CREATING){
      g_mutex_unlock(dbt->mutex);
      g_mutex_unlock(td->conf->table_list_mutex);
      return TRUE;
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(td->conf->table_list_mutex);
  return FALSE;
}

gboolean are_we_waiting_for_create_schema_jobs_to_complete(struct thread_data * td){
  if (g_async_queue_length(td->conf->database_queue)>0 )
    return TRUE;

  g_mutex_lock(td->conf->table_list_mutex);
  GList * iter=td->conf->table_list;
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state==CREATING){
      g_mutex_unlock(dbt->mutex);
      g_mutex_unlock(td->conf->table_list_mutex);
      return TRUE;
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(td->conf->table_list_mutex);
  return FALSE;
}

gboolean are_available_jobs(struct thread_data * td){
  g_mutex_lock(td->conf->table_list_mutex);
  GList * iter=td->conf->table_list;

  while (iter != NULL){
    struct db_table * dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state!=CREATED || g_list_length(dbt->restore_job_list) > 0){
      g_mutex_unlock(dbt->mutex);
      g_mutex_unlock(td->conf->table_list_mutex);
      return TRUE;
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(td->conf->table_list_mutex);
  return FALSE;
}

gboolean give_me_next_data_job_conf(struct configuration *conf, gboolean test_condition, struct restore_job ** rj){
  gboolean giveup = TRUE;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
//  GList * next = NULL;
  struct restore_job *job = NULL;
//  g_debug("Elements in table_list: %d",g_list_length(conf->table_list));
//  We are going to check every table and see if there is any missing job
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    if (dbt->database->schema_state == NOT_FOUND){
      iter=iter->next;
      /*
        TODO: make all "voting for finish" messages another debug level

        G_MESSAGES_DEBUG.  A space-separated list of log domains for which informational
        and debug messages should be printed. By default, these messages are not
        printed. You can also use the special value all. This environment variable only
        affects the default log handler, g_log_default_handler().
      */
      trace("%s.%s: %s, voting for finish", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
      continue;
    }
//    g_message("DB: %s Table: %s Schema State: %d remaining_jobs: %d", dbt->database->real_database,dbt->real_table, dbt->schema_state, dbt->remaining_jobs);
    if (dbt->schema_state >= DATA_DONE ||
        (dbt->schema_state == CREATED && (dbt->is_view || dbt->is_sequence)))
    {
//          g_message("DB: %s Table: %s Schema State: %d data done?", dbt->database->real_database,dbt->real_table, dbt->schema_state);
      trace("%s.%s done: %s, voting for finish", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      continue;
    }
//    g_message("DB: %s Table: %s len: %d state: %d", dbt->database->real_database,dbt->real_table,g_list_length(dbt->restore_job_list), dbt->schema_state);
    g_mutex_lock(dbt->mutex);
    if (!test_condition || (dbt->schema_state==CREATED && dbt->current_threads < dbt->max_threads)) {
      // I could do some job in here, do we have some for me?
//      g_message("DB: %s Table: %s max_threads: %d current: %d", dbt->database->real_database,dbt->real_table, dbt->max_threads,dbt->current_threads);
//      g_mutex_lock(dbt->mutex);
      if (!resume && dbt->schema_state<CREATED ){
        giveup=FALSE;
        trace("%s.%s not yet created: %s, waiting", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
        iter=iter->next;
        g_mutex_unlock(dbt->mutex);
        continue;
      }

      // TODO: can we do without double check (not under and under dbt->mutex)?
      if (dbt->schema_state >= DATA_DONE ||
          (dbt->schema_state == CREATED && (dbt->is_view || dbt->is_sequence)))
      {
//        g_message("DB: %s Table: %s DATA DONE %d", dbt->database->real_database,dbt->real_table, dbt->schema_state);
        trace("%s.%s done just now: %s, voting for finish", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
        iter=iter->next;
        g_mutex_unlock(dbt->mutex);
        continue;
      }

//      g_message("DB: %s Table: %s checking size %d", dbt->database->real_database,dbt->real_table, g_list_length(dbt->restore_job_list));
      if (g_list_length(dbt->restore_job_list) > 0){
        // We found a job that we can process!
        job = dbt->restore_job_list->data;
//        next = dbt->restore_job_list->next;
        dbt->restore_job_list=g_list_remove_link(dbt->restore_job_list,dbt->restore_job_list);
//        g_list_free_1(dbt->restore_job_list);
//        dbt->restore_job_list = next;
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        giveup=FALSE;
        trace("%s.%s sending %s: %s, threads: %u, prohibiting finish", dbt->database->real_database, dbt->real_table,
              rjtype2str(job->type), job->filename, dbt->current_threads);
        break;
      }else{
// AND CURRENT THREADS IS 0... if not we are seting DATA_DONE to unfinished tables
        if (intermediate_queue_ended_local && dbt->current_threads == 0 && (g_atomic_int_get(&(dbt->remaining_jobs))==0 )){
          dbt->schema_state = DATA_DONE;
          gboolean res= enqueue_index_for_dbt_if_possible(conf,dbt);
//          create_index_job(conf, dbt, -1);
          if (res) {
            giveup= FALSE;
            trace("%s.%s queuing indexes, prohibiting finish", dbt->database->real_database, dbt->real_table);
          } else {
            trace("%s.%s skipping indexes, voting for finish", dbt->database->real_database, dbt->real_table);
          }
        }
//        g_message("DB: %s Table: %s no more jobs in it", dbt->database->real_database,dbt->real_table);
      }
/*      if (intermediate_queue_ended_local){
        dbt->completed=TRUE;
        create_index_job(td->conf, dbt, td->thread_id);
      }
*/
//      g_mutex_unlock(dbt->mutex);
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
  *rj = job;
  return giveup;
}

void refresh_db_and_jobs(enum file_type current_ft){
  switch (current_ft){
    case SCHEMA_CREATE:
    case SCHEMA_TABLE:
    case SCHEMA_SEQUENCE:
      schema_queue_push(current_ft);
      break;
    case DATA:
      trace("refresh_db_queue <- %s", ft2str(current_ft));
      g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(current_ft));
      break;
    case INTERMEDIATE_ENDED:
      schema_queue_push(current_ft);
      trace("refresh_db_queue <- %s", ft2str(current_ft));
      g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(current_ft));
      break;
    case SHUTDOWN:
      trace("refresh_db_queue <- %s", ft2str(current_ft));
      g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(current_ft));
    default:
      break;
  }
}

void maybe_shutdown_control_job()
{
   if (g_atomic_int_dec_and_test(&last_wait)){
     trace("SHUTDOWN last_wait_control_job_to_shutdown");
     refresh_db_and_jobs(SHUTDOWN);
   }
}

void wake_threads_waiting(struct configuration *conf, guint *threads_waiting){
  struct restore_job *rj=NULL;
  if (*threads_waiting>0){
    gboolean giveup=FALSE;
    while ( 0<*threads_waiting && !giveup){
      giveup = give_me_next_data_job_conf(conf, TRUE, &rj);
      if (rj != NULL){
//          g_message("DATA pushing");
        *threads_waiting=*threads_waiting - 1;
        trace("data_queue <- %s: %s", rjtype2str(rj->type), rj->dbt ? rj->dbt->table : rj->filename);
        g_async_queue_push(data_queue,rj);
        trace("here_is_your_job <- %s", ft2str(DATA));
        g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
      }
    }
    if (intermediate_queue_ended_local && giveup) {
      trace("refresh_db_queue <- %s", ft2str(THREAD));
      g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(THREAD));
    }
  }
}

void *control_job_thread(struct configuration *conf){
  enum file_type ft;
  struct restore_job *rj=NULL;
  guint threads_waiting= num_threads;
//  GHashTableIter iter;
//  gchar * lkey=NULL;
  gboolean giveup;
//  struct database * real_db_name = NULL;
//  struct control_job *job = NULL;
  gboolean cont=TRUE;
  set_thread_name("CJT");
  if (overwrite_tables && !overwrite_unsafe && cjt_paused) {
    trace("Thread control_job_thread paused");
    g_mutex_lock (cjt_mutex);
    while (cjt_paused)
      g_cond_wait (cjt_cond, cjt_mutex);
    g_mutex_unlock (cjt_mutex);
  }
  trace("Thread control_job_thread started");
  while(cont){
    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(refresh_db_queue)); 
    trace("refresh_db_queue -> %s (%u loaders waiting)", ft2str(ft), threads_waiting);
    switch (ft){
    case DATA:
      wake_threads_waiting(conf, &threads_waiting);
      break;
    case THREAD:
//      g_message("Thread is asking for job");
      giveup = give_me_next_data_job_conf(conf, TRUE, &rj);
      if (rj != NULL){
//        g_message("job available in give_me_next_data_job_conf");
        trace("data_queue <- %s: %s", rjtype2str(rj->type), rj->dbt ? rj->dbt->table : rj->filename);
        g_async_queue_push(data_queue,rj);
        trace("here_is_your_job <- %s", ft2str(DATA));
        g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
      }else{
//        g_message("Thread is asking for job again");
        giveup = give_me_next_data_job_conf(conf, FALSE, &rj);
        if (rj != NULL){
//          g_message("job available in give_any_data_job");
          trace("data_queue <- %s: %s", rjtype2str(rj->type), rj->dbt ? rj->dbt->table : rj->filename);
          g_async_queue_push(data_queue,rj);
          trace("here_is_your_job <- %s", ft2str(DATA));
          g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
        }else{
//          g_message("No job available");


          if (intermediate_queue_ended_local){
            if (giveup){
              threads_waiting++;
              trace("here_is_your_job <- %s (%u times)", ft2str(SHUTDOWN), threads_waiting);
              for(; 0 < threads_waiting; threads_waiting--){
                g_async_queue_push(here_is_your_job, GINT_TO_POINTER(SHUTDOWN));
              }
            }else{
            //  g_message("Ignoring");
            //  g_async_queue_push(here_is_your_job, GINT_TO_POINTER(IGNORED));
//              g_message("Thread waiting");
              threads_waiting=threads_waiting<num_threads?threads_waiting+1:num_threads;
            }
          }else{
//            g_message("Thread waiting");
            threads_waiting=threads_waiting<num_threads?threads_waiting+1:num_threads;
          }
        }
      }
      break;
    case INTERMEDIATE_ENDED:
      enqueue_indexes_if_possible(conf);
      wake_threads_waiting(conf, &threads_waiting);        
      intermediate_queue_ended_local = TRUE;
      wake_threads_waiting(conf, &threads_waiting);
      break;
    case SHUTDOWN:
      cont=FALSE;
      break;
    default:
      trace("Thread control_job_thread: received Default: %d", ft);
      break;
    }
  }
  start_innodb_optimize_keys_all_tables();
  trace("Thread control_job_thread finished");
  return NULL;
}
