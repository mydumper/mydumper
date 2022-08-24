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
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
  }
  g_free(job);
  return TRUE;
}


struct restore_job * give_any_data_job(struct configuration * conf){
  g_mutex_lock(conf->table_list_mutex);
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
}

struct restore_job * give_me_next_data_job(struct configuration * conf){
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  GList * next = NULL;
  struct restore_job *job = NULL;
//  g_message("Elemetns in table_list: %d",g_list_length(stream_conf->table_list));
  while (iter != NULL){
    struct db_table * dbt = iter->data;
//    g_message("DB: %s Table: %s len: %d", dbt->real_database,dbt->real_table,g_list_length(dbt->restore_job_list));
    if (dbt->schema_created && dbt->current_threads < dbt->max_threads){
      // I could do some job in here, do we have some for me?
      g_mutex_lock(dbt->mutex);
      if (g_list_length(dbt->restore_job_list) > 0){
        job = dbt->restore_job_list->data;
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
  return job;
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
      cont=process_job(td, job);
      struct database * d=db_hash_lookup(job->use_database);
      d->schema_created=TRUE;      
      continue;
    }
    job=g_async_queue_try_pop(td->conf->table_queue);
    if (job != NULL){
      struct database *real_db_name=db_hash_lookup(job->use_database); 
      if (real_db_name->schema_created){
        execute_use_if_needs_to(td, job->use_database, "Restoring table structure");
        cont=process_job(td, job);
      }else
        g_async_queue_push(td->conf->table_queue,job);
      continue;
    }
    struct restore_job *rj = give_me_next_data_job(td->conf);
    if (rj != NULL){
      job=new_job(JOB_RESTORE,rj,rj->dbt->database);
      execute_use_if_needs_to(td, job->use_database, "Restoring tables (1)");
      cont=process_job(td, job);
      continue;
    }
    rj=give_any_data_job(td->conf);
    if (rj != NULL){
      job=new_job(JOB_RESTORE,rj,rj->dbt->database);
      execute_use_if_needs_to(td, job->use_database, "Restoring tables (2)");
      cont=process_job(td, job);
      continue;
    }else{
      if (ft==SHUTDOWN)
        cont=FALSE;
      else
        g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
    }

  }
  g_message("Shutting down stream thread %d", td->thread_id);
  return NULL;
}
