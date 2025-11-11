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
#include "myloader_worker_loader_main.h"
#include "myloader_global.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_index.h"
#include "myloader_worker_schema.h"
#include "myloader_database.h"

gboolean control_job_ended=FALSE;
gboolean all_jobs_are_enqueued=FALSE;
/* data_control_queue is for data loads */
GAsyncQueue *data_control_queue = NULL; //, *data_queue=NULL;
static GThread *_worker_loader_main = NULL;
guint threads_waiting = 0;
static GMutex *threads_waiting_mutex= NULL;


void *worker_loader_main_thread(struct configuration *conf);

void initialize_worker_loader_main (struct configuration *conf){
  data_control_queue = g_async_queue_new();
//  data_job_queue = g_async_queue_new();
//  data_queue = g_async_queue_new();
  threads_waiting_mutex=g_mutex_new();
  _worker_loader_main = m_thread_new("myloader_ctr",(GThreadFunc)worker_loader_main_thread, conf, "Control job thread could not be created");
}

void wait_worker_loader_main()
{
  trace("Waiting control job to finish");
  g_thread_join(_worker_loader_main);
  trace("Control job to finished");
}

void data_control_queue_push(enum data_control_type current_ft){
  trace("data_control_queue <- %s", data_control_type2str(current_ft));
  g_async_queue_push(data_control_queue, GINT_TO_POINTER(current_ft));
}

gboolean give_me_next_data_job_conf(struct configuration *conf, struct restore_job ** rj){
  gboolean giveup = TRUE;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct restore_job *job = NULL;
//  g_mutex_lock(conf->table_list_mutex);
//  trace("Elements in table_list: %d",g_list_length(conf->table_list));
//  g_mutex_unlock(conf->table_list_mutex);
//  We are going to check every table and see if there is any missing job
  struct db_table * dbt;
  while (iter != NULL){
    dbt = iter->data;
    trace("DB: %s Table: %s Schema State: %d remaining_jobs: %d", dbt->database->target_database,dbt->real_table, dbt->schema_state, dbt->remaining_jobs);
    if (dbt->database->schema_state == NOT_FOUND){
      iter=iter->next;
      /*
        TODO: make all "voting for finish" messages another debug level

        G_MESSAGES_DEBUG.  A space-separated list of log domains for which informational
        and debug messages should be printed. By default, these messages are not
        printed. You can also use the special value all. This environment variable only
        affects the default log handler, g_log_default_handler().
      */
      trace("%s.%s: %s, voting for finish", dbt->database->target_database, dbt->real_table, status2str(dbt->schema_state));
      continue;
    }
//    g_message("DB: %s Table: %s Schema State: %d remaining_jobs: %d", dbt->database->target_database,dbt->real_table, dbt->schema_state, dbt->remaining_jobs);
    if (dbt->schema_state >= DATA_DONE ||
        (dbt->schema_state == CREATED && (dbt->is_view || dbt->is_sequence))){
      trace("%s.%s done: %s, voting for finish", dbt->database->target_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      continue;
    }
    g_mutex_lock(dbt->mutex);
    // I could do some job in here, do we have some for me?
    if (!resume && dbt->schema_state<CREATED ){
      giveup=FALSE;
      trace("%s.%s not yet created: %s, waiting", dbt->database->target_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      g_mutex_unlock(dbt->mutex);
      continue;
    }

      // TODO: can we do without double check (not under and under dbt->mutex)?
    if (dbt->schema_state >= DATA_DONE ||
        (dbt->schema_state == CREATED && (dbt->is_view || dbt->is_sequence))){
      trace("%s.%s done just now: %s, voting for finish", dbt->database->target_database, dbt->real_table, status2str(dbt->schema_state));
      iter=iter->next;
      g_mutex_unlock(dbt->mutex);
      continue;
    }

    if (dbt->schema_state == CREATED && g_list_length(dbt->restore_job_list) > 0){
      if (dbt->object_to_export.no_data){
        GList * current = dbt->restore_job_list;
        while (current){
          g_free(((struct restore_job *)current->data)->data.drj);
          current=current->next;
        }
        dbt->schema_state = ALL_DONE;

      }else{
        if (dbt->current_threads >= dbt->max_threads ){
          giveup=FALSE;
          trace("%s.%s Reached max thread %s", dbt->database->target_database, dbt->real_table, status2str(dbt->schema_state));
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
        trace("%s.%s sending %s: %s, threads: %u, prohibiting finish", dbt->database->target_database, dbt->real_table,
            rjtype2str(job->type), job->filename, dbt->current_threads);
        break;
      }
    }else{
// AND CURRENT THREADS IS 0... if not we are seting DATA_DONE to unfinished tables
      trace("No remaining jobs on %s.%s", dbt->database->target_database, dbt->real_table); 
      if (all_jobs_are_enqueued && dbt->current_threads == 0 && (g_atomic_int_get(&(dbt->remaining_jobs))==0 )){
        dbt->schema_state = DATA_DONE;
        enqueue_index_for_dbt_if_possible(conf,dbt);
        trace("%s.%s queuing indexes, voting for finish", dbt->database->target_database, dbt->real_table);
      }
    }
    
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
  *rj = job;
  return giveup;
}

static
void wake_threads_waiting(){
  g_mutex_lock(threads_waiting_mutex);
  while(threads_waiting>0){
    trace("Waking up threads");
    data_control_queue_push(REQUEST_DATA_JOB);
    threads_waiting=threads_waiting - 1;
  }
  g_mutex_unlock(threads_waiting_mutex);
}

void wake_data_threads(){
  g_mutex_lock(threads_waiting_mutex);
  if (threads_waiting>0){
    data_control_queue_push(WAKE_DATA_THREAD);
  }else 
    trace("No threads sleeping");
  g_mutex_unlock(threads_waiting_mutex);
}

void *worker_loader_main_thread(struct configuration *conf){
  enum data_control_type ft;
  struct restore_job *rj=NULL;
  guint _num_threads = num_threads;
//  guint threads_waiting = 0; //num_threads;
  gboolean giveup;
  gboolean cont=TRUE;
  set_thread_name("CJT");
  
  trace("Thread worker_loader_main_thread started");
  while(cont){
    ft=(enum data_control_type)GPOINTER_TO_INT(g_async_queue_pop(data_control_queue));
    g_mutex_lock(threads_waiting_mutex);
    trace("data_control_queue -> %s (%u loaders waiting)", data_control_type2str(ft), threads_waiting);
    g_mutex_unlock(threads_waiting_mutex);
    switch (ft){
    case WAKE_DATA_THREAD:
      wake_threads_waiting();
      break;
    case REQUEST_DATA_JOB:
      trace("Thread is asking for job");
      giveup = give_me_next_data_job_conf(conf, &rj);
      if (rj != NULL){
        trace("job available in give_me_next_data_job_conf");
        data_job_push(DATA_JOB, rj);
      }else{
        trace("No job available");
        if (all_jobs_are_enqueued && giveup){
          trace("Giving up...");
          control_job_ended = TRUE;
          data_ended();
//          cont=FALSE;
/*          guint i;

          for (i=0;i<num_threads;i++){
            trace("data_job_queue <- %s", ft2str(SHUTDOWN));
            g_async_queue_push(data_job_queue, GINT_TO_POINTER(SHUTDOWN));
          }
          */
        }else{
          trace("Thread will be waiting | all_jobs_are_enqueued: %d | giveup: %d", all_jobs_are_enqueued, giveup);
          g_mutex_lock(threads_waiting_mutex);
          if (threads_waiting<_num_threads)
            threads_waiting++;
          g_mutex_unlock(threads_waiting_mutex);
        }
      }
      break;
    case FILE_TYPE_ENDED:
      enqueue_indexes_if_possible(conf);
      all_jobs_are_enqueued = TRUE;
      wake_threads_waiting();
      break;
    case SHUTDOWN:
      cont=FALSE;
      trace("SHUTDOWN");
      break;
    case FILE_TYPE_SCHEMA_ENDED:
      wake_threads_waiting();
//      data_control_queue_push(REQUEST_DATA_JOB);
      break;
//    case SCHEMA_TABLE_JOB:
//    case SCHEMA_CREATE_JOB:
      trace("Thread control_job_thread received:  %d", ft);
      break;
    }
  }
//  data_ended();
  start_optimize_keys_all_tables();
  trace("Thread worker_loader_main_thread finished");
  return NULL;
}
