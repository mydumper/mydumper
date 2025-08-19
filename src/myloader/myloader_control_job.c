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

gboolean control_job_ended=FALSE;
gboolean all_jobs_are_enqueued=FALSE;
gboolean dont_wait_for_schema_create=FALSE;
/* control_job_queue is for data loads */
GAsyncQueue *control_job_queue = NULL, *data_job_queue=NULL, *data_queue=NULL;
static GThread *control_job_t = NULL;

gint last_wait=0;
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
  control_job_queue = g_async_queue_new();
  data_job_queue = g_async_queue_new();
  last_wait = num_threads;
  data_queue = g_async_queue_new();
  cjt_mutex= g_mutex_new();
  cjt_cond= g_cond_new();
  control_job_t = m_thread_new("myloader_ctr",(GThreadFunc)control_job_thread, conf, "Control job thread could not be created");
}

void wait_control_job()
{
  trace("Waiting control job to finish");
  g_thread_join(control_job_t);
  g_mutex_free(cjt_mutex);
  g_cond_free(cjt_cond);
  trace("Control job to finished");
}

struct control_job * new_control_job (enum control_job_type type, void *job_data, struct database *use_database) {
  struct control_job *j = g_new0(struct control_job, 1);
  j->type = type;
  j->use_database=use_database;
  switch (type){
    case JOB_SHUTDOWN:
      break;
    default:
      j->data.restore_job = (struct restore_job *)job_data;
  }
  return j;
}

void control_job_queue_push(enum file_type current_ft){
  trace("control_job_queue <- %s", ft2str(current_ft));
  g_async_queue_push(control_job_queue, GINT_TO_POINTER(current_ft));
}

enum file_type request_restore_data_job(){
  control_job_queue_push(REQUEST_DATA_JOB);
  enum file_type ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(data_job_queue));
  trace("data_job_queue -> %s", ft2str(ft));
  return ft;
}

struct restore_job * request_next_data_job(){
  struct restore_job *rj= (struct restore_job *)g_async_queue_pop(data_queue);
  trace("data_queue -> %s: %s.%s, threads %u", rjtype2str(rj->type), rj->dbt->database->real_database, rj->dbt->real_table, rj->dbt->current_threads);
  return rj;
}

gboolean give_me_next_data_job_conf(struct configuration *conf, struct restore_job ** rj){
  gboolean giveup = TRUE;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct restore_job *job = NULL;
//  g_debug("Elements in table_list: %d",g_list_length(conf->table_list));
//  We are going to check every table and see if there is any missing job
  struct db_table * dbt;
  while (iter != NULL){
    dbt = iter->data;
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
        (dbt->schema_state == CREATED && (dbt->is_view || dbt->is_sequence))){
      trace("%s.%s done: %s, voting for finish", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      continue;
    }
    g_mutex_lock(dbt->mutex);
    // I could do some job in here, do we have some for me?
    if (!resume && dbt->schema_state<CREATED ){
      giveup=FALSE;
      trace("%s.%s not yet created: %s, waiting", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      g_mutex_unlock(dbt->mutex);
      continue;
    }

      // TODO: can we do without double check (not under and under dbt->mutex)?
    if (dbt->schema_state >= DATA_DONE ||
        (dbt->schema_state == CREATED && (dbt->is_view || dbt->is_sequence))){
      trace("%s.%s done just now: %s, voting for finish", dbt->database->real_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      g_mutex_unlock(dbt->mutex);
      continue;
    }

    if (dbt->schema_state == CREATED && g_list_length(dbt->restore_job_list) > 0){
      if (dbt->current_threads >= dbt->max_threads ){
        giveup=FALSE;
        iter=iter->next;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
      // We found a job that we can process!
      job = dbt->restore_job_list->data;
      GList * current = dbt->restore_job_list;
//      dbt->restore_job_list = dbt->restore_job_list->next;
      dbt->restore_job_list = g_list_remove_link(dbt->restore_job_list, current);
      g_list_free_1(current);
      dbt->current_threads++;
      g_mutex_unlock(dbt->mutex);
      giveup=FALSE;
      trace("%s.%s sending %s: %s, threads: %u, prohibiting finish", dbt->database->real_database, dbt->real_table,
            rjtype2str(job->type), job->filename, dbt->current_threads);
      break;
    }else{
// AND CURRENT THREADS IS 0... if not we are seting DATA_DONE to unfinished tables
      trace("No remaining jobs on %s.%s", dbt->database->real_database, dbt->real_table); 
      if (all_jobs_are_enqueued && dbt->current_threads == 0 && (g_atomic_int_get(&(dbt->remaining_jobs))==0 )){
        dbt->schema_state = DATA_DONE;
        enqueue_index_for_dbt_if_possible(conf,dbt);
        trace("%s.%s queuing indexes, voting for finish", dbt->database->real_database, dbt->real_table);
      }
    }
    
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
  *rj = job;
  return giveup;
}

void enroute_into_the_right_queue_based_on_file_type(enum file_type current_ft){
  switch (current_ft){
    case SCHEMA_CREATE:
    case SCHEMA_TABLE:
    case SCHEMA_SEQUENCE:
      schema_queue_push(current_ft, "");
      break;
    case INTERMEDIATE_ENDED:
      schema_queue_push(current_ft, "");
      control_job_queue_push(current_ft);
      break;
    case REQUEST_DATA_JOB:
    case DATA:
    case SHUTDOWN:
      control_job_queue_push(current_ft);
    default:
      break;
  }
}

void maybe_shutdown_control_job()
{
   if (g_atomic_int_dec_and_test(&last_wait)){
     trace("SHUTDOWN last_wait_control_job_to_shutdown");
     enroute_into_the_right_queue_based_on_file_type(SHUTDOWN);
   }
}

void wake_threads_waiting(guint *threads_waiting){
  while(*threads_waiting>0){
    control_job_queue_push(REQUEST_DATA_JOB);
    *threads_waiting=*threads_waiting - 1;
  }
}

void *control_job_thread(struct configuration *conf){
  enum file_type ft;
  struct restore_job *rj=NULL;
  guint _num_threads = num_threads;
  guint threads_waiting = 0; //num_threads;
  gboolean giveup;
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
    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(control_job_queue));
    trace("control_job_queue -> %s (%u loaders waiting)", ft2str(ft), threads_waiting);
    switch (ft){
    case DATA:
      wake_threads_waiting(&threads_waiting);
      break;
    case REQUEST_DATA_JOB:
//      g_message("Thread is asking for job");
      giveup = give_me_next_data_job_conf(conf, &rj);
      if (rj != NULL){
        trace("job available in give_me_next_data_job_conf");
        trace("data_queue <- %s: %s", rjtype2str(rj->type), rj->dbt ? rj->dbt->table : rj->filename);
        g_async_queue_push(data_queue,rj);
        trace("data_job_queue <- %s", ft2str(DATA));
        g_async_queue_push(data_job_queue, GINT_TO_POINTER(DATA));
      }else{
        trace("No job available");
        if (all_jobs_are_enqueued && giveup){
          trace("Giving up...");
          control_job_ended = TRUE;
          guint i;
          for (i=0;i<num_threads;i++){
            trace("data_job_queue <- %s", ft2str(SHUTDOWN));
            g_async_queue_push(data_job_queue, GINT_TO_POINTER(SHUTDOWN));
          }
        }else{
          trace("Thread will be waiting | all_jobs_are_enqueued: %d | giveup: %d", all_jobs_are_enqueued, giveup);
          if (threads_waiting<_num_threads)
              threads_waiting++;
        }
      }
      break;
    case INTERMEDIATE_ENDED:
      enqueue_indexes_if_possible(conf);
      all_jobs_are_enqueued = TRUE;
      wake_threads_waiting(&threads_waiting);
      break;
    case SHUTDOWN:
      cont=FALSE;
      break;
    default:
      trace("Thread control_job_thread: received Default: %d", ft);
      break;
    }
  }
  start_optimize_keys_all_tables();
  trace("Thread control_job_thread finished");
  return NULL;
}
