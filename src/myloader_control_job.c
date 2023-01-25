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
#include "myloader_jobs_manager.h"
gboolean intermediate_queue_ended_local=FALSE;
extern gboolean innodb_optimize_keys_per_table;
extern guint num_threads;
extern gboolean resume;
extern GHashTable *tbl_hash;
extern GHashTable *db_hash;

gboolean dont_wait_for_schema_create=FALSE;
GAsyncQueue *refresh_db_queue = NULL, *here_is_your_job=NULL, *data_queue=NULL;
//GAsyncQueue *give_me_another_job_queue = NULL;
GThread *control_job_t = NULL;

gint last_wait=0;
GMutex *last_wait_control_job_continue;

void *control_job_thread(struct configuration *conf);

void initialize_control_job (struct configuration *conf){
  refresh_db_queue = g_async_queue_new();
  here_is_your_job = g_async_queue_new();
  last_wait = num_threads;
  last_wait_control_job_continue = g_mutex_new();
  data_queue = g_async_queue_new();
//  give_me_another_job_queue = g_async_queue_new();
  control_job_t = g_thread_create((GThreadFunc)control_job_thread, conf, TRUE, NULL);
}

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
      g_message("Thread %d: Enqueuing index for table: `%s`.`%s`", tdid, dbt->database->real_database, dbt->table);
      struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->database,dbt->indexes,"indexes");
      g_async_queue_push(conf->index_queue, new_job(JOB_RESTORE,rj,dbt->database->real_database));
      dbt->index_enqueued=TRUE;
//      refresh_db_and_jobs(INDEX);
    }
//  }
}

gboolean give_me_next_data_job_conf(struct configuration *conf, gboolean test_condition, struct restore_job ** rj){
  gboolean giveup = TRUE;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  GList * next = NULL;
  struct restore_job *job = NULL;
//  g_debug("Elements in table_list: %d",g_list_length(conf->table_list));
//  We are going to check every table and see if there is any missing job
  while (iter != NULL){
    struct db_table * dbt = iter->data;
//    g_message("DB: %s Table: %s Schema State: %d remaining_jobs: %d", dbt->database->real_database,dbt->real_table, dbt->schema_state, dbt->remaining_jobs);
    if (dbt->schema_state>=DATA_DONE){
//          g_message("DB: %s Table: %s Schema State: %d data done?", dbt->database->real_database,dbt->real_table, dbt->schema_state);
      iter=iter->next;
      continue;
    }
//    g_message("DB: %s Table: %s len: %d state: %d", dbt->database->real_database,dbt->real_table,g_list_length(dbt->restore_job_list), dbt->schema_state);
    g_mutex_lock(dbt->mutex);
    if (!test_condition || (dbt->schema_state==CREATED && dbt->current_threads < dbt->max_threads)){
      // I could do some job in here, do we have some for me?
//      g_message("DB: %s Table: %s max_threads: %d current: %d", dbt->database->real_database,dbt->real_table, dbt->max_threads,dbt->current_threads);
//      g_mutex_lock(dbt->mutex);
      if (!resume && dbt->schema_state<CREATED ){
        giveup=FALSE;
//        g_message("DB: %s Table: %s NOT CREATED %d", dbt->database->real_database,dbt->real_table, dbt->schema_state);
        iter=iter->next;
        g_mutex_unlock(dbt->mutex);
        continue;
      }

      if (dbt->schema_state>=DATA_DONE){
//        g_message("DB: %s Table: %s DATA DONE %d", dbt->database->real_database,dbt->real_table, dbt->schema_state);
        iter=iter->next;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
//      g_message("DB: %s Table: %s checking size %d", dbt->database->real_database,dbt->real_table, g_list_length(dbt->restore_job_list));
      if (g_list_length(dbt->restore_job_list) > 0){
        // We found a job that we can process!
        job = dbt->restore_job_list->data;
        next = dbt->restore_job_list->next;
        g_list_free_1(dbt->restore_job_list);
        dbt->restore_job_list = next;
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        giveup=FALSE;
  //g_message("DB: %s Table: %s BREAKING", dbt->database->real_database,dbt->real_table);
        break;
      }else{
// AND CURRENT THREADS IS 0... if not we are seting DATA_DONE to unfinished tables
        if (intermediate_queue_ended_local && dbt->current_threads == 0){
          dbt->schema_state = DATA_DONE;
          create_index_job(conf, dbt, -1);
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

gboolean give_me_next_data_job(struct thread_data * td, gboolean test_condition, struct restore_job ** rj){
  return give_me_next_data_job_conf(td->conf, test_condition, rj);
}

gboolean give_any_data_job_conf(struct configuration *conf, struct restore_job ** rj){
 return give_me_next_data_job_conf(conf, FALSE, rj);
}

gboolean give_any_data_job(struct thread_data * td, struct restore_job ** rj){
 return give_me_next_data_job_conf(td->conf, FALSE, rj);
}

void enqueue_index_for_dbt_if_possible(struct configuration *conf, struct db_table * dbt){
//  g_mutex_lock(dbt->mutex);
  if (dbt->schema_state==DATA_DONE && !dbt->index_enqueued)
    if (intermediate_queue_ended_local && (g_atomic_int_get(&(dbt->remaining_jobs)) == 0))
      create_index_job(conf, dbt, -2);
//  g_mutex_unlock(dbt->mutex);
}

void enqueue_indexes_if_possible(struct configuration *conf){
  (void )conf;
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt = NULL;
  while (iter != NULL){
    dbt=iter->data;
    g_mutex_lock(dbt->mutex);
    enqueue_index_for_dbt_if_possible(conf,dbt);
    g_mutex_unlock(dbt->mutex);
/*    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state==CREATED && !dbt->index_enqueued){
      if (intermediate_queue_ended_local && (g_atomic_int_get(&(dbt->remaining_jobs)) == 0)){
        create_index_job(conf, dbt, 0);
*/
/*
        if (dbt->indexes != NULL){
          if (g_atomic_int_get(&(dbt->remaining_jobs)) == 0){
            g_message("Enqueuing index for table: %s", dbt->table);
            struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->database->real_database,dbt->indexes,"indexes");
            g_async_queue_push(conf->index_queue, new_job(JOB_RESTORE,rj,dbt->database->real_database));
            dbt->index_enqueued=TRUE;
          }
        }
*/
//      }
//    }
//    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}



void refresh_db_and_jobs(enum file_type current_ft){
  g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(current_ft));
}


void set_db_schema_state_to_created( struct database * database, GAsyncQueue * object_queue, GAsyncQueue * kind_queue){
  database->schema_state=CREATED;
  struct control_job * cj = g_async_queue_try_pop(database->queue);
  while (cj != NULL){
    g_async_queue_push(object_queue, cj);
    g_async_queue_push(kind_queue, GINT_TO_POINTER(SCHEMA_TABLE));
    cj = g_async_queue_try_pop(database->queue);
  }
}

void set_table_schema_state_to_created (struct configuration *conf){
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt = NULL;
  while (iter != NULL){
    dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (dbt->schema_state == NOT_FOUND )
      dbt->schema_state = CREATED;
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}

void last_wait_control_job_to_shutdown(){
   if (g_atomic_int_dec_and_test(&last_wait)){
     g_mutex_lock(last_wait_control_job_continue);
     refresh_db_and_jobs(SHUTDOWN);
     g_mutex_lock(last_wait_control_job_continue);
   }
}

void *control_job_thread(struct configuration *conf){
  enum file_type ft;
  struct restore_job *rj=NULL;
  guint threads_waiting=0;
  GHashTableIter iter;
  gchar * lkey=NULL;
  gboolean giveup;
  struct database * real_db_name = NULL;
//  struct control_job *job = NULL;
  gboolean cont=TRUE;
  while(cont){
    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(refresh_db_queue)); 
    switch (ft){
    case SCHEMA_CREATE:
      //review the db that can be created
//      g_message("SCHEMA_CREATE");
      g_async_queue_push(here_is_your_job, GINT_TO_POINTER(ft));
      break;
    case SCHEMA_TABLE: 
      //review all the table that can be created
//      g_message("SCHEMA_TABLE");
      g_async_queue_push(here_is_your_job, GINT_TO_POINTER(ft));
      break;
    case DATA:
    // If thread and I didn't enqueue anything, then enqueue data jobs
      giveup = give_me_next_data_job_conf(conf, TRUE, &rj);
      if (rj != NULL){
//        g_message("job available in give_me_next_data_job_conf");
        g_async_queue_push(data_queue,rj);
        g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
      } /* else{
        giveup=give_any_data_job_conf(conf,&rj);
        if (rj != NULL){
//          g_message("job available in give_any_data_job");
          g_async_queue_push(data_queue,rj);
          g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
        }else{
//          g_message("No job available");
          if (intermediate_queue_ended_local && giveup){
//            g_message("Enqueuing shutdown");
            g_async_queue_push(here_is_your_job, GINT_TO_POINTER(SHUTDOWN));
          }
        }
      } */
      // NO DATA JOB available, no worries, there will be another one shortly...
      break;
    case THREAD:
//      g_message("Thread is asking for job");

      giveup = give_me_next_data_job_conf(conf, TRUE, &rj);
      if (rj != NULL){
//        g_message("job available in give_me_next_data_job_conf");
        g_async_queue_push(data_queue,rj);
        g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
      }else{
//        g_message("Thread is asking for job again");
        giveup = give_any_data_job_conf(conf, &rj);
        if (rj != NULL){
//          g_message("job available in give_any_data_job");
          g_async_queue_push(data_queue,rj);
          g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
        }else{
//          g_message("No job available");
          if (intermediate_queue_ended_local){
            if (giveup){
              g_async_queue_push(here_is_your_job, GINT_TO_POINTER(SHUTDOWN));
              for(;0<threads_waiting;threads_waiting--){
//                g_message("Enqueuing shutdown");
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
//      intermediate_queue_ended_local = TRUE;
      g_hash_table_iter_init ( &iter, db_hash );
//      g_message("INTERMEDIATE_ENDED Starting");
      while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &real_db_name ) ) {
        g_mutex_lock(real_db_name->mutex);
        set_db_schema_state_to_created(real_db_name, conf->table_queue, here_is_your_job);
        g_mutex_unlock(real_db_name->mutex);
      }
//       g_message("INTERMEDIATE_ENDED ACA2");
      set_table_schema_state_to_created(conf);
//      g_message("INTERMEDIATE_ENDED ACA");
      enqueue_indexes_if_possible(conf);
//      g_message("INTERMEDIATE_ENDED FINISH");
      for (; 0<threads_waiting; threads_waiting--){
        giveup=give_any_data_job_conf(conf, &rj);
        if (rj != NULL){
          g_async_queue_push(data_queue,rj);
          g_async_queue_push(here_is_your_job, GINT_TO_POINTER(DATA));
        }else{
          if (giveup)
            g_async_queue_push(here_is_your_job, GINT_TO_POINTER(SHUTDOWN));
          else
            g_async_queue_push(here_is_your_job, GINT_TO_POINTER(IGNORED));
        }
        
      }
      intermediate_queue_ended_local = TRUE;
      break;
    case SHUTDOWN:
      cont=FALSE;
      g_mutex_unlock(last_wait_control_job_continue);
      break;
    default:
      g_debug("Thread control_job_thread: Default: %d", ft);
      break;
    }
  }

  return NULL;
}



void *process_stream_queue(struct thread_data * td) {
  struct control_job *job = NULL;
  gboolean cont=TRUE;
  enum file_type ft=-1;
//  enum file_type ft;
//  int remaining_shutdown_pass=2*num_threads;
  struct restore_job *rj=NULL;
//  guint pass=0;
  struct db_table * dbt = NULL; 
  while (cont){
//    if (ft == SHUTDOWN)
//      g_async_queue_push(td->conf->stream_queue,GINT_TO_POINTER(ft));     

    g_async_queue_push(refresh_db_queue, GINT_TO_POINTER(THREAD));
//    g_message("Sending THREAD");
    ft=(enum file_type)GPOINTER_TO_INT(g_async_queue_pop(here_is_your_job));
    switch (ft){
    case SCHEMA_CREATE:
//      g_message("SCHEMA_CREATE pop");
      job=g_async_queue_pop(td->conf->database_queue);
//      g_message("SCHEMA_CREATE poped");
      if (job->type != JOB_SHUTDOWN){
//        g_debug("Restoring database");
        struct database * real_db_name=job->data.restore_job->data.srj->database;
        g_mutex_lock(real_db_name->mutex);
        cont=process_job(td, job);
        set_db_schema_state_to_created(real_db_name, td->conf->table_queue, here_is_your_job);
        g_mutex_unlock(real_db_name->mutex);
      }else{
        g_async_queue_push(td->conf->database_queue, job);
      }
      break;
    case SCHEMA_TABLE:
      job=g_async_queue_pop(td->conf->table_queue);
      if (job->type != JOB_SHUTDOWN){
        execute_use_if_needs_to(td, job->use_database, "Restoring table structure");
        cont=process_job(td, job);
      }
      break;

    case DATA:
      // NO DATA JOB available, no worries, there will be another one shortly...
      rj = (struct restore_job *)g_async_queue_pop(data_queue);
//      g_message("REstinging JOB");
      dbt = rj->dbt;
      job=new_job(JOB_RESTORE,rj, dbt->database->real_database);
      execute_use_if_needs_to(td, job->use_database, "Restoring tables (2)");
      cont=process_job(td, job);
//      g_message("REstinging JOB completed before lock");
      g_mutex_lock(dbt->mutex);
//      g_message("REstinging JOB completed after lock");
      dbt->current_threads--;
      g_mutex_unlock(dbt->mutex);
//      g_message("REstinging JOB completed");
      break;
 //   case INDEX:
//      process_index(td);
//      break;
    case SHUTDOWN:
//      g_message("SHUTDOWN");
      cont=FALSE;
      break;
    case IGNORED:
      usleep(1000);
      break;
    default:
        NULL;
//        g_message("What do we do with: %d", ft);

    }
    if (innodb_optimize_keys_per_table)
      process_index(td);
  }
  enqueue_indexes_if_possible(td->conf);
  g_message("Thread %d: Data import ended", td->thread_id);
  return NULL;
}
