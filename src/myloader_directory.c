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

    Authors:        David Ducos, Percona (david dot ducos at percona dot com)

*/

#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <string.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include "myloader_stream.h"
#include "common.h"
#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_jobs_manager.h"
#include "myloader_restore.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"

extern guint num_threads;
extern gboolean innodb_optimize_keys;
extern gchar *directory;
extern guint errors;
extern gboolean skip_post;
extern gchar *source_db;
extern gboolean skip_triggers;
extern gboolean no_data;

gint compare_by_time(gconstpointer a, gconstpointer b){
  return
    g_date_time_difference(((struct db_table *)a)->finish_time,((struct db_table *)a)->start_time) >
    g_date_time_difference(((struct db_table *)b)->finish_time,((struct db_table *)b)->start_time);
}

gboolean append_filename_to_list (
    GList **schema_create_list, 
    GList **create_table_list, 
    GList **metadata_list, 
    GList **data_files_list, 
    GList **view_list, 
    GList **trigger_list, 
    GList **post_list, 
    GList **checksum_list, const gchar *filename, gboolean inside_resume){
  enum file_type ft= get_file_type(filename);
    if (ft == SCHEMA_POST){
        if (!skip_post)
          *post_list=g_list_insert(*post_list,g_strdup(filename),-1);
    } else if (ft ==  SCHEMA_CREATE ){
          *schema_create_list=g_list_insert(*schema_create_list,g_strdup(filename),-1);
    } else if (!source_db ||
      g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))||
      g_str_has_prefix(filename, "mydumper_")) {
        switch (ft){
          case INIT:
            break;
          case SCHEMA_TABLE:
            *create_table_list=g_list_append(*create_table_list,g_strdup(filename));
            break;
          case SCHEMA_VIEW:
            *view_list=g_list_append(*view_list,g_strdup(filename));
            break;
          case SCHEMA_TRIGGER:
            if (!skip_triggers)
              *trigger_list=g_list_append(*trigger_list,g_strdup(filename));
            break;
          case CHECKSUM:
            *checksum_list=g_list_append(*checksum_list,g_strdup(filename));
            break;
          case METADATA_GLOBAL:
            break;
          case METADATA_TABLE:
            // TODO: we need to process this info
            *metadata_list=g_list_append(*metadata_list,g_strdup(filename));
            break;
          case DATA:
            if (!no_data)
              *data_files_list=g_list_append(*data_files_list,g_strdup(filename));
            break;
          case LOAD_DATA:
            g_message("Load data file found: %s", filename);
            break;
          case RESUME:
            if (inside_resume){
              g_critical("resume file found inside resume processing. You need to manually edit resume file");
              exit(EXIT_FAILURE);
            }else{
              g_message("Using resume file");
              g_list_free_full(*create_table_list,g_free);
              g_list_free_full(*view_list,g_free);
              g_list_free_full(*trigger_list,g_free);
              g_list_free_full(*checksum_list,g_free);
              g_list_free_full(*metadata_list,g_free);
              g_list_free_full(*data_files_list,g_free);
              *create_table_list=NULL;
              *view_list=NULL;
              *trigger_list=NULL;
              *checksum_list=NULL;
              *metadata_list=NULL;
              *data_files_list=NULL;
              FILE *file = g_fopen(filename, "r");
              GString *data=g_string_sized_new(256);
              gboolean eof = FALSE;
              guint line=0;
              read_data(file, FALSE, data, &eof, &line);
              gchar **split=NULL;
              guint i=0;
              while (!eof){
                read_data(file, FALSE, data, &eof, &line);
                split=g_strsplit(data->str,"\n",0);
                for (i=0; i<g_strv_length(split);i++){
                  if (strlen(split[i])>2)
                    append_filename_to_list(schema_create_list,create_table_list,metadata_list,data_files_list,view_list,trigger_list,post_list,checksum_list,split[i],TRUE);
                }
                g_string_set_size(data, 0);
              } 
              fclose(file);
              g_remove(filename);
            }
            return FALSE;
            break;
          default:
            g_warning("File ignored: %s", filename);
            break;
        }
      }
  return TRUE;
}


void load_directory_information(struct configuration *conf) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;
  GList *create_table_list=NULL,
        *metadata_list= NULL,
        *data_files_list=NULL,
        *schema_create_list=NULL,
        *view_list=NULL,
        *trigger_list=NULL,
        *post_list=NULL;
  gboolean cont=TRUE;
  while (cont && (filename = g_dir_read_name(dir)))
    cont=append_filename_to_list(&schema_create_list,&create_table_list,&metadata_list,&data_files_list,&view_list,&trigger_list,&post_list,&(conf->checksum_list),filename,FALSE);
 
  g_dir_close(dir);

  gchar *f = NULL;
  // CREATE DATABASE
  while (schema_create_list){
    f = schema_create_list->data;
    process_database_filename(f, "create database");
    schema_create_list=schema_create_list->next;
  }

  // CREATE TABLE
  conf->table_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  while (create_table_list != NULL){
    f = create_table_list->data;
    process_table_filename(f);
    create_table_list=create_table_list->next;
  }

  // DATA FILES
  while (data_files_list != NULL){
    f = data_files_list->data;
    process_data_filename(f);

    data_files_list=data_files_list->next;
  }

  // METADATA FILES
  while (metadata_list != NULL){
    f = metadata_list->data;
    process_metadata_filename(conf->table_hash,f);
    metadata_list=metadata_list->next;
  }

  while (view_list != NULL){
    f = view_list->data;
    process_schema_filename(f,"view");
    view_list=view_list->next;
  }

  while (trigger_list != NULL){
    f = trigger_list->data;
    process_schema_filename(f, "trigger");
    trigger_list=trigger_list->next;
  }

  while (post_list != NULL){
    f = post_list->data;
    process_schema_filename(f,"post");
    post_list=post_list->next;
  }
  // SORT DATA FILES TO ENQUEUE
  // iterates over the dbt to create the jobs in the dbt->queue
  // and sorts the dbt for the conf->table_list
  // in stream mode, it is not possible to sort the tables as 
  // we don't know the amount the rows, .metadata are sent at the end.
  GList * table_list=NULL;
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, conf->table_hash );
  struct db_table *dbt=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
    table_list=g_list_insert_sorted_with_data (table_list,dbt,&compare_dbt,conf->table_hash);
    GList *i=dbt->restore_job_list; 
    while (i) {
      g_async_queue_push(dbt->queue, new_job(JOB_RESTORE ,i->data,dbt->real_database));
      i=i->next;
    }
    dbt->count=g_async_queue_length(dbt->queue);
//    g_debug("Setting count to: %d", dbt->count);
  }
  conf->table_list=table_list;
  // conf->table needs to be set.
}

void *process_directory_queue(struct thread_data * td) {
  struct db_table *dbt=NULL;
  struct control_job *job = NULL;
  gboolean cont=TRUE;

  // Step 1: creating databases
  while (cont){
    job = (struct control_job *)g_async_queue_pop(td->conf->database_queue);
    cont=process_job(td, job);
  }
  // Step 2: Create tables
  cont=TRUE;
  while (cont){
    job = (struct control_job *)g_async_queue_pop(td->conf->table_queue);
    execute_use_if_needs_to(td, job->use_database, "Restoring tables");
    cont=process_job(td, job);
  }

  // Is this correct in a streaming scenario ?
  GList *table_list=td->conf->table_list;
  if (table_list == NULL ) {
    dbt=NULL;
  }else{
    dbt=table_list->data;
    g_mutex_lock(dbt->mutex);
    dbt->current_threads++;
    if (dbt->start_time==NULL)
      dbt->start_time=g_date_time_new_now_local();
    g_mutex_unlock(dbt->mutex);
  }


  // Step 3: Load data
  cont=TRUE;
  while (cont){
    if (dbt != NULL){
      g_mutex_lock(dbt->mutex);
      if (dbt->current_threads > dbt->max_threads){
        dbt->current_threads--;
        g_mutex_unlock(dbt->mutex);
        table_list=table_list->next;
        if (table_list == NULL ){
          dbt=NULL;
          continue;
        }
        dbt=table_list->data;
        g_mutex_lock(dbt->mutex);
        if (dbt->start_time==NULL) dbt->start_time=g_date_time_new_now_local();
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
      g_mutex_unlock(dbt->mutex);

      job = (struct control_job *)g_async_queue_try_pop(dbt->queue);

      if (job == NULL){
        g_mutex_lock(dbt->mutex);
        dbt->current_threads--;
        if (dbt->current_threads == 0){
          dbt->current_threads--;
          dbt->start_index_time=g_date_time_new_now_local();
          g_mutex_unlock(dbt->mutex);
          if (dbt->indexes != NULL) {
            g_message("Thread %d restoring indexes `%s`.`%s`", td->thread_id,
                  dbt->real_database, dbt->real_table);
            guint query_counter=0;
            restore_data_in_gstring(td, dbt->indexes, FALSE, &query_counter);
          }
          dbt->finish_time=g_date_time_new_now_local();
        }else{
          g_mutex_unlock(dbt->mutex);
        }
        guint max=dbt->max_threads;
        table_list=table_list->next;
        if (table_list == NULL ){
          dbt=NULL;
          continue;
        }
        dbt=table_list->data;
        g_mutex_lock(dbt->mutex);
        if (dbt->start_time==NULL) dbt->start_time=g_date_time_new_now_local();
        dbt->max_threads = max;
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
    }else{
     job = (struct control_job *)g_async_queue_pop(td->conf->data_queue);
    }
    execute_use_if_needs_to(td, job->use_database, "Restoring data");
    cont=process_job(td, job);
  }
  return NULL;
}

void sync_threads_on_queue(GAsyncQueue *ready_queue,GAsyncQueue *comm_queue,const gchar *msg){
  guint n;
  GAsyncQueue * queue = g_async_queue_new();
  for (n = 0; n < num_threads; n++){
    g_async_queue_push(comm_queue, new_job(JOB_WAIT, queue, NULL));
  }
  for (n = 0; n < num_threads; n++)
    g_async_queue_pop(ready_queue);
  g_debug("%s",msg);
  for (n = 0; n < num_threads; n++)
    g_async_queue_push(queue, GINT_TO_POINTER(1));
}

void sync_threads(struct configuration * conf){
  sync_threads_on_queue(conf->ready, conf->data_queue,"Syncing");
}

gchar * print_time(GTimeSpan timespan){
  GTimeSpan days=timespan/G_TIME_SPAN_DAY;
  GTimeSpan hours=(timespan-(days*G_TIME_SPAN_DAY))/G_TIME_SPAN_HOUR;
  GTimeSpan minutes=(timespan-(days*G_TIME_SPAN_HOUR)-(hours*G_TIME_SPAN_HOUR))/G_TIME_SPAN_MINUTE;
  GTimeSpan seconds=(timespan-(days*G_TIME_SPAN_MINUTE)-(hours*G_TIME_SPAN_HOUR)-(minutes*G_TIME_SPAN_MINUTE))/G_TIME_SPAN_SECOND;
  return g_strdup_printf("%ld %02ld:%02ld:%02ld",days,hours,minutes,seconds);
}

void restore_from_directory(struct configuration *conf){
  // Leaving just on thread to execute the add constraints as it might cause deadlocks
  guint n=0;
  for (n = 0; n < num_threads-1; n++) {
    g_async_queue_push(conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  load_directory_information(conf);
  sync_threads_on_queue(conf->ready,conf->database_queue,"Step 1 completed, Databases created");
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->database_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  sync_threads_on_queue(conf->ready,conf->table_queue,"Step 2 completed, Tables created");
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  // We need to sync all the threads before continue
  sync_threads_on_queue(conf->ready,conf->data_queue,"Step 3 completed, load data finished");
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->data_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  g_async_queue_push(conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  g_debug("Step 4 completed");

  GList * t=g_list_sort(conf->table_list, compare_by_time);
  g_message("Import timings:");
  g_message("Data      \t| Index    \t| Total   \t| Table");
  while (t != NULL){
    struct db_table * dbt=t->data;
    GTimeSpan diff1=g_date_time_difference(dbt->start_index_time,dbt->start_time);
    GTimeSpan diff2=g_date_time_difference(dbt->finish_time,dbt->start_index_time);
    g_message("%s\t| %s\t| %s\t| `%s`.`%s`",print_time(diff1),print_time(diff2),print_time(diff1+diff2),dbt->real_database,dbt->real_table);
    t=t->next;
  }
  innodb_optimize_keys=FALSE;

  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->post_table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  // Step 5: Create remaining objects.
  // TODO: is it possible to do it in parallel? Actually, why aren't we queuing this files?
  g_debug("Step 5 started");

}
