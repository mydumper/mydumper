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
#include "myloader_control_job.h"
#include "myloader_restore_job.h"
#include "myloader_common.h"
#include "myloader_restore.h"
#include "myloader_jobs_manager.h"
extern gboolean intermediate_queue_ended;
extern gboolean innodb_optimize_keys_per_table;

struct control_job * new_job (enum control_job_type type, void *job_data, char *use_database) {
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

gboolean process_job(struct thread_data *td, struct control_job *job){
  switch (job->type) {
    case JOB_RESTORE:
//      g_message("Restore Job");
      process_restore_job(td,job->data.restore_job);
      break;
    case JOB_WAIT:
//      g_message("Wait Job");
      g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
//      GAsyncQueue *queue=job->data.queue;
      g_async_queue_pop(job->data.queue);
      break;
    case JOB_SHUTDOWN:
//      g_message("Thread %d shutting down", td->thread_id);
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


struct restore_job * give_me_next_data_job(struct thread_data * td, gboolean test_condition){
  g_mutex_lock(td->conf->table_list_mutex);
  GList * iter=td->conf->table_list;
  GList * next = NULL;
  struct restore_job *job = NULL;
//  g_message("Elemetns in table_list: %d",g_list_length(stream_conf->table_list));
//  We are going to check every table and see if there is any missing job
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    if (dbt->completed){
      iter=iter->next;
      continue;
    }
//    g_message("DB: %s Table: %s len: %d", dbt->real_database,dbt->real_table,g_list_length(dbt->restore_job_list));
    if (!test_condition || (dbt->schema_created && dbt->current_threads < dbt->max_threads)){
      // I could do some job in here, do we have some for me?
      g_mutex_lock(dbt->mutex);
      if (dbt->completed){
        iter=iter->next;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
      if (g_list_length(dbt->restore_job_list) > 0){
        // We found a job that we can process!
        job = dbt->restore_job_list->data;
        next = dbt->restore_job_list->next;
        g_list_free_1(dbt->restore_job_list);
        dbt->restore_job_list = next;
        g_mutex_unlock(dbt->mutex);
        break;
      }
      if (intermediate_queue_ended)
        dbt->completed=TRUE;
      g_mutex_unlock(dbt->mutex);
    }
    iter=iter->next;
  }
  g_mutex_unlock(td->conf->table_list_mutex);
  return job;
}


struct restore_job * give_any_data_job(struct thread_data * td){
 return give_me_next_data_job(td,FALSE);
/*  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  GList * next = NULL;
 //  struct control_job *job = NULL;
  struct restore_job *rj =NULL;
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    if (dbt->schema_created){
      g_mutex_lock(dbt->mutex);
      if (g_list_length(dbt->restore_job_list) > 0){
        rj = dbt->restore_job_list->data;
        next = dbt->restore_job_list->next;
        g_list_free_1(dbt->restore_job_list);
        dbt->restore_job_list = next;
        g_mutex_unlock(dbt->mutex);
        break;
      }
      g_mutex_unlock(dbt->mutex);
    }
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);

  return rj;
*/
}

void enqueue_indexes_if_possible(struct configuration *conf){
  (void )conf;
  GList * iter=conf->table_list;
  struct db_table * dbt;
  while (iter != NULL){
    dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_created && !dbt->index_enqueued){
      if (intermediate_queue_ended){
        if (dbt->indexes != NULL){
          g_message("Enqueuing index for table: %s", dbt->table);
          struct restore_job *rj = new_schema_restore_job(strdup("index"),JOB_RESTORE_STRING, dbt, dbt->real_database,dbt->indexes,"indexes");
          g_async_queue_push(conf->index_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
        }
        dbt->index_enqueued=TRUE;
      }
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
}

void *process_stream_queue(struct thread_data * td) {
  struct control_job *job = NULL;
  gboolean cont=TRUE;
  enum file_type ft=0;
//  enum file_type ft;
  while (cont){
    if (ft==SHUTDOWN)
      g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(td->conf->stream_queue));
    job=g_async_queue_try_pop(td->conf->database_queue);
    if (job != NULL){
      g_debug("Restoring database");
      struct database * d=db_hash_lookup(job->data.restore_job->data.srj->database);
      cont=process_job(td, job);
      d->schema_created=TRUE;
      continue;
    }

    // Enqueue index to add
    enqueue_indexes_if_possible(td->conf);

   // Check if are index and can be added

    if (innodb_optimize_keys_per_table){
      enqueue_indexes_if_possible(td->conf);

      gboolean b=process_index(td);
      if (b){
        g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
        continue;
      }
    }

    job=g_async_queue_try_pop(td->conf->table_queue);
    if (job != NULL){
      struct database *real_db_name=db_hash_lookup(job->use_database); 
      if (real_db_name->schema_created){
        execute_use_if_needs_to(td, job->use_database, "Restoring table structure");
        cont=process_job(td, job);
      }else{
        g_async_queue_push(td->conf->table_queue,job);
        g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
      }
      continue;
    }
    struct restore_job *rj = give_me_next_data_job(td, TRUE);
    if (rj != NULL){
      job=new_job(JOB_RESTORE,rj,rj->dbt->database);
      execute_use_if_needs_to(td, job->use_database, "Restoring tables (1)");
      cont=process_job(td, job);
      continue;
    }
    rj=give_any_data_job(td);
    if (rj != NULL){
      job=new_job(JOB_RESTORE,rj,rj->dbt->database);
      execute_use_if_needs_to(td, job->use_database, "Restoring tables (2)");
      cont=process_job(td, job);
      continue;
    }else{
      if (ft==SHUTDOWN){
        cont=FALSE;
      }else
        g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
    }

  }
  enqueue_indexes_if_possible(td->conf);
  g_message("Thread %d: Data import ended", td->thread_id);
  return NULL;
}
