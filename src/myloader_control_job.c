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
gboolean intermediate_queue_ended_local=FALSE;
extern gboolean innodb_optimize_keys_per_table;
extern guint num_threads;
extern gboolean resume;
extern GHashTable *tbl_hash;

gboolean dont_wait_for_schema_create=FALSE;

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
  if (g_async_queue_length(td->conf->database_queue)>0 || g_async_queue_length(td->conf->table_queue)>0)
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

void create_index_job(struct configuration *conf, struct db_table * dbt, guint tdid){
  if (dbt->indexes != NULL && ! dbt->index_enqueued){
//    if (g_atomic_int_get(&(dbt->remaining_jobs)) == 0){
      g_message("Thread %d: Enqueuing index for table: `%s`.`%s`", tdid, dbt->database, dbt->table);
      struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->real_database,dbt->indexes,"indexes");
      g_async_queue_push(conf->index_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
      dbt->index_enqueued=TRUE;
    }
//  }
}
struct restore_job * give_me_next_data_job(struct thread_data * td, gboolean test_condition){
  g_mutex_lock(td->conf->table_list_mutex);
  GList * iter=td->conf->table_list;
  GList * next = NULL;
  struct restore_job *job = NULL;
//  g_debug("Elements in table_list: %d",g_list_length(td->conf->table_list));
//  We are going to check every table and see if there is any missing job
  while (iter != NULL){
    struct db_table * dbt = iter->data;
    if (dbt->completed){
      iter=iter->next;
      continue;
    }
//    g_debug("DB: %s Table: %s len: %d", dbt->real_database,dbt->real_table,g_list_length(dbt->restore_job_list));
    g_mutex_lock(dbt->mutex);
    if (!test_condition || (dbt->schema_state==CREATED && dbt->current_threads < dbt->max_threads)){
      // I could do some job in here, do we have some for me?
//      g_message("DB: %s Table: %s max_threads: %d current: %d", dbt->real_database,dbt->real_table, dbt->max_threads,dbt->current_threads);
//      g_mutex_lock(dbt->mutex);
      if (!resume && dbt->schema_state!=CREATED ){
        if (dont_wait_for_schema_create && dbt->schema_state!=CREATING){
//g_message("dont_wait_for_schema_create");
          g_hash_table_insert(tbl_hash, dbt->table, dbt->real_table);
          dbt->schema_state=CREATED;
        }else{
          iter=iter->next;
          g_mutex_unlock(dbt->mutex);
          continue;
        }
      }

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
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        break;
      }
      if (intermediate_queue_ended_local){
        dbt->completed=TRUE;
        create_index_job(td->conf, dbt, td->thread_id);
      }

//      g_mutex_unlock(dbt->mutex);
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(td->conf->table_list_mutex);
  return job;
}


struct restore_job * give_any_data_job(struct thread_data * td){
 return give_me_next_data_job(td,FALSE);
}

void enqueue_indexes_if_possible(struct configuration *conf){
  (void )conf;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt;
  while (iter != NULL){
    dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state==CREATED && !dbt->index_enqueued){
      if (intermediate_queue_ended_local && (g_atomic_int_get(&(dbt->remaining_jobs)) == 0)){
        create_index_job(conf, dbt, 0);
/*
        if (dbt->indexes != NULL){
          if (g_atomic_int_get(&(dbt->remaining_jobs)) == 0){
            g_message("Enqueuing index for table: %s", dbt->table);
            struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->real_database,dbt->indexes,"indexes");
            g_async_queue_push(conf->index_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
            dbt->index_enqueued=TRUE;
          }
        }
*/
      }
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}

void *process_stream_queue(struct thread_data * td) {
  struct control_job *job = NULL;
  gboolean cont=TRUE;
  enum file_type ft=-1;
//  enum file_type ft;
//  int remaining_shutdown_pass=2*num_threads;
  struct restore_job *rj=NULL;
  guint pass=0;
  guint max_jobs_to_wait=0;
  while (cont){
//    if (ft == SHUTDOWN)
//      g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));     


    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(td->conf->stream_queue));
    switch (ft){
    case SCHEMA_CREATE:
      job=g_async_queue_pop(td->conf->database_queue);
      if (job->type != JOB_SHUTDOWN){
        g_debug("Restoring database");
        struct database * d=get_db_hash(job->data.restore_job->data.srj->database, job->data.restore_job->data.srj->database);
        cont=process_job(td, job);
        d->schema_created=TRUE;
//        continue;
      }else{
        g_async_queue_push(td->conf->database_queue, job);
      }
      break;
    case SCHEMA_TABLE:

/*
    // Enqueue index to add
    enqueue_indexes_if_possible(td->conf);

   // Check if are index and can be added

    if (innodb_optimize_keys_per_table){
      enqueue_indexes_if_possible(td->conf);

      gboolean b=process_index(td);
      if (b){
        g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
        ft=-1;
//        continue;
      }
    }
*/

    job=g_async_queue_pop(td->conf->table_queue);
    if (job->type != JOB_SHUTDOWN){
    struct database *real_db_name=get_db_hash(job->use_database,job->use_database); 
    if (real_db_name->schema_created){
      execute_use_if_needs_to(td, job->use_database, "Restoring table structure");
      cont=process_job(td, job);
    }else{
      g_async_queue_push(td->conf->table_queue,job);
      g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));
      if (max_jobs_to_wait > 0 && pass>max_jobs_to_wait){
        g_message("Max jobs waited (%d) for schema: %s", max_jobs_to_wait, real_db_name->name);
        real_db_name->schema_created=TRUE;
//      ft=-1;
      }
    }
    
//      continue;
    }

      break;

    case DATA:
      rj = give_me_next_data_job(td, TRUE);
      if (rj != NULL){
        job=new_job(JOB_RESTORE,rj,rj->dbt->database);
        execute_use_if_needs_to(td, job->use_database, "Restoring tables (1)");
        cont=process_job(td, job);
        rj->dbt->current_threads--;
//        continue;
      }else{
        rj=give_any_data_job(td);
        if (rj != NULL){
          g_debug("Thread %d: Giving any data job", td->thread_id);
          job=new_job(JOB_RESTORE,rj,rj->dbt->database);
          execute_use_if_needs_to(td, job->use_database, "Restoring tables (2)");
          cont=process_job(td, job);
          rj->dbt->current_threads--;
//        continue;
        }
      }
      // NO DATA JOB available, no worries, there will be another one shortly...
      break;
      case SHUTDOWN:
        cont=FALSE;
        break;
      case INTERMEDIATE_ENDED:
        g_async_queue_push(td->conf->database_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
        if ( are_available_jobs(td) ){
          if (!are_we_waiting_for_schema_jobs_to_complete(td)){
            g_warning("Schema files missed, we continue...");
            schema_file_missed_lets_continue(td);
          }
          pass++;
        }
        max_jobs_to_wait=g_async_queue_length(td->conf->stream_queue)+1;
        intermediate_queue_ended_local=TRUE;
      default:
        NULL;
//        g_message("What do we do with: %d", ft);

    }
    enqueue_indexes_if_possible(td->conf);
    if (innodb_optimize_keys_per_table){
      process_index(td);
    }
    if (intermediate_queue_ended_local) {
      if ( are_available_jobs(td) ){
        if (!are_we_waiting_for_schema_jobs_to_complete(td)){
//          g_warning("Schema files missed, we continue...");
          schema_file_missed_lets_continue(td);
        }else{
//          g_message("Thread %d: There ar jobs %d", td->thread_id, ft);
        }     
        pass++;
      }else{
        g_async_queue_push(td->conf->stream_queue, GINT_TO_POINTER(SHUTDOWN));
      }
    }
  }
  enqueue_indexes_if_possible(td->conf);
  g_message("Thread %d: Data import ended", td->thread_id);
  return NULL;
}
