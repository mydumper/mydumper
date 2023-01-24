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
#include <mysql.h>
#include <glib/gstdio.h>
#include "common.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "myloader_intermediate_queue.h"
#include "myloader_restore.h"
extern gchar *db;
extern gchar *directory;
extern gchar *source_db;
extern gboolean no_data;
extern gboolean skip_triggers;
extern gboolean skip_post;
extern guint num_threads;
extern guint total_data_sql_files;
extern gboolean innodb_optimize_keys_all_tables;
extern gboolean stream;
gboolean intermediate_queue_ended = FALSE;
GAsyncQueue *intermediate_queue = NULL;
GThread *stream_intermediate_thread = NULL;

void *intermediate_thread();
struct configuration *intermediate_conf = NULL;

void initialize_intermediate_queue (struct configuration *c){
  intermediate_conf=c;
  intermediate_queue = g_async_queue_new();
  intermediate_queue_ended=FALSE;
  stream_intermediate_thread = g_thread_create((GThreadFunc)intermediate_thread, NULL, TRUE, NULL);
  initialize_control_job(c);
}

void intermediate_queue_new(gchar *filename){
  struct intermediate_filename * iflnm=g_new0(struct intermediate_filename, 1);
  iflnm->filename = filename;
  iflnm->iterations=0;
  g_async_queue_push(intermediate_queue, iflnm);
}

void intermediate_queue_end(){
  gchar *e=g_strdup("END");
  intermediate_queue_new(e);
  g_message("Intermediate queue: Sending END job");
  g_thread_join(stream_intermediate_thread);
  g_message("Intermediate thread: SHUTDOWN");
  intermediate_queue_ended=TRUE;
}

void intermediate_queue_incomplete(struct intermediate_filename * iflnm){
// TODO: we need to add the filelist and check the filename iterations
// the idea is to keep track how many times a filename was incomplete and
// at what stage
  iflnm->iterations++;
  g_async_queue_push(intermediate_queue, iflnm);
}

enum file_type process_filename(char *filename){
//  g_message("Filename: %s", filename);
  enum file_type ft= get_file_type(filename);
  if (!source_db ||
    g_str_has_prefix(filename, g_strdup_printf("%s.", source_db)) ||
    g_str_has_prefix(filename, g_strdup_printf("%s-schema-post.sql", source_db)) ||
    g_str_has_prefix(filename, g_strdup_printf("%s-schema-create.sql", source_db) )
    ) {
    switch (ft){
      case INIT:
        break;
      case SCHEMA_TABLESPACE:
        break;
      case SCHEMA_CREATE:
        process_database_filename(filename, "create database");
        if (db)
          ft=DO_NOT_ENQUEUE;
        //m_remove(directory,filename);
        break;
      case SCHEMA_TABLE:
        // filename is free
        if (!process_table_filename(filename)){
          return DO_NOT_ENQUEUE;
        }else{
          g_free(filename);
          refresh_table_list(intermediate_conf);
        }
        break;
      case SCHEMA_VIEW:
        if (!process_schema_view_filename(filename))
          return DO_NOT_ENQUEUE;
        break;
      case SCHEMA_TRIGGER:
        if (!skip_triggers)
          if (!process_schema_filename(filename,"trigger"))
            return DO_NOT_ENQUEUE;
        break;
      case SCHEMA_POST:
        // can be enqueued in any order
        if (!skip_post)
          if (!process_schema_filename(filename,"post"))
            return DO_NOT_ENQUEUE;
        break;
      case CHECKSUM:
        if (!process_checksum_filename(filename))
          return DO_NOT_ENQUEUE;
        intermediate_conf->checksum_list=g_list_insert(intermediate_conf->checksum_list,filename,-1);
        break;
      case METADATA_GLOBAL:
        break;
      case METADATA_TABLE:
        intermediate_conf->metadata_list=g_list_insert(intermediate_conf->metadata_list,filename,-1);
        if (!process_metadata_filename(filename))
          return DO_NOT_ENQUEUE;
        refresh_table_list(intermediate_conf);
        break;
      case DATA:
        if (!no_data){
          if (!process_data_filename(filename))
            return DO_NOT_ENQUEUE;
        }else
          m_remove(directory,filename);
        total_data_sql_files++;
        break;
      case RESUME:
        if (stream){
          m_critical("We don't expect to find resume files in a stream scenario");
        }
        break;
      case IGNORED:
        g_warning("Filename %s has been ignored", filename);
        break;
      case LOAD_DATA:
        release_load_data_as_it_is_close(filename);
        break;
      case SHUTDOWN:
        break;
      case INCOMPLETE:
        break;
      default:
        break;
    }
  }else{
    ft=DO_NOT_ENQUEUE;
  }
  return ft;
}

void process_stream_filename(struct intermediate_filename  * iflnm){
  enum file_type current_ft=process_filename(iflnm->filename);
  if (current_ft == INCOMPLETE ){
    if (iflnm->iterations > 5){
      g_warning("Max renqueing reached for: %s", iflnm->filename);
    }else{
      g_debug("Requeuing in intermediate queue %u: %s", iflnm->iterations, iflnm->filename);
      intermediate_queue_incomplete(iflnm);
    }
//    g_async_queue_push(intermediate_queue, filename);
    return;
  }
  if (current_ft != SCHEMA_VIEW &&
      current_ft != SCHEMA_TRIGGER &&
      current_ft != SCHEMA_POST &&
      current_ft != CHECKSUM &&
      current_ft != METADATA_TABLE &&
      current_ft != DO_NOT_ENQUEUE )
    refresh_db_and_jobs(current_ft);
//    g_async_queue_push(intermediate_conf->stream_queue, GINT_TO_POINTER(current_ft));
}

void enqueue_all_index_jobs(struct configuration *conf){
  g_mutex_lock(conf->table_list_mutex);
  GList * iter=conf->table_list;
  struct db_table * dbt;
  while (iter != NULL){
    dbt = iter->data;
    g_mutex_lock(dbt->mutex);
    if (!dbt->index_enqueued){
      struct restore_job *rj = new_schema_restore_job(g_strdup("index"),JOB_RESTORE_STRING, dbt, dbt->database,dbt->indexes,"indexes");
      g_async_queue_push(conf->index_queue, new_job(JOB_RESTORE,rj,dbt->database->name));
      dbt->index_enqueued=TRUE;
    }
    g_mutex_unlock(dbt->mutex);
    iter=iter->next;
  }
  g_mutex_unlock(conf->table_list_mutex);
}

void *intermediate_thread(){
  struct intermediate_filename  * iflnm=NULL;
  do{
    iflnm = (struct intermediate_filename  *)g_async_queue_pop(intermediate_queue);
    if ( g_strcmp0(iflnm->filename,"END") == 0 ){
      if (g_async_queue_length(intermediate_queue)>0){
        g_async_queue_push(intermediate_queue,iflnm);
        continue;
      }
      g_free(iflnm->filename);
      g_free(iflnm);
      iflnm=NULL;
      break;
    }
    process_stream_filename(iflnm);
  } while (iflnm != NULL);
  guint n=0;
  for (n = 0; n < num_threads; n++){
//    g_async_queue_push(intermediate_conf->stream_queue, GINT_TO_POINTER(SHUTDOWN));
  }
  if (innodb_optimize_keys_all_tables)
    enqueue_all_index_jobs(intermediate_conf);
  g_message("Intermediate thread ended");
  refresh_db_and_jobs(INTERMEDIATE_ENDED);
  return NULL;
}

