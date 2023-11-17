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
#include "myloader_global.h"
//GAsyncQueue * schema_counter=NULL;
guint schema_counter = 0;
gboolean intermediate_queue_ended = FALSE;
GAsyncQueue *intermediate_queue = NULL;
GThread *stream_intermediate_thread = NULL;
gchar *exec_per_thread = NULL;
gchar *exec_per_thread_extension = NULL;
gchar **exec_per_thread_cmd=NULL;
gchar ** zstd_decompress_cmd = NULL; //[3][15]={"              ","-c","-d"};
gchar ** gzip_decompress_cmd = NULL; //[3][15]={"              ","-c","-d"};

GHashTable * exec_process_id = NULL;
GMutex *exec_process_id_mutex = NULL;
GMutex *start_intermediate_thread=NULL;
void *intermediate_thread();
struct configuration *intermediate_conf = NULL;

void initialize_intermediate_queue (struct configuration *c){
//  schema_counter = g_async_queue_new();
  intermediate_conf=c;
  intermediate_queue = g_async_queue_new();
  exec_process_id=g_hash_table_new ( g_str_hash, g_str_equal );
  exec_process_id_mutex=g_mutex_new();
  start_intermediate_thread = g_mutex_new();
  g_mutex_lock(start_intermediate_thread);
  if (stream)
    g_mutex_unlock(start_intermediate_thread);
  intermediate_queue_ended=FALSE;
  stream_intermediate_thread = g_thread_create((GThreadFunc)intermediate_thread, NULL, TRUE, NULL);
  if (exec_per_thread_extension != NULL){
    if(exec_per_thread == NULL)
      m_error("--exec-per-thread needs to be set when --exec-per-thread-extension is used");
  }

  if (exec_per_thread!=NULL){
    if (exec_per_thread[0]!='/'){
      m_error("Absolute path is only allowed when --exec-per-thread is used");
    }
    exec_per_thread_cmd=g_strsplit(exec_per_thread, " ", 0);
  }
  gchar *cmd=NULL, *tmp=NULL;
  if ((cmd=get_zstd_cmd()) == NULL){
    g_warning("zstd command not found on any static location, use --exec-per-thread for non default locations");
  }else {
    zstd_decompress_cmd = g_strsplit( tmp=g_strdup_printf("%s -c -d", cmd)," ",0);
    g_free(tmp);
  }
  if ((cmd=get_gzip_cmd()) == NULL){
    g_warning("gzip command not found on any static location, use --exec-per-thread for non default locations");
  }else{
    gzip_decompress_cmd = g_strsplit( tmp=g_strdup_printf("%s -c -d", cmd)," ",0);
    g_free(tmp);
  }

  initialize_control_job(c);
}

void intermediate_queue_new(const gchar *filename){
  struct intermediate_filename * iflnm=g_new0(struct intermediate_filename, 1);
  iflnm->filename = g_strdup(filename);
  iflnm->iterations=0;
  trace("intermediate_queue <- %s (%u)", iflnm->filename, iflnm->iterations);
  g_async_queue_push(intermediate_queue, iflnm);
}

void intermediate_queue_end(){
  g_mutex_unlock(start_intermediate_thread);
  gchar *e=g_strdup("END");
  intermediate_queue_new(e);
  message("Intermediate queue: Sending END job");
  g_thread_join(stream_intermediate_thread);
  message("Intermediate thread: SHUTDOWN");
  intermediate_queue_ended=TRUE;
}

void intermediate_queue_incomplete(struct intermediate_filename * iflnm){
// TODO: we need to add the filelist and check the filename iterations
// the idea is to keep track how many times a filename was incomplete and
// at what stage
  iflnm->iterations++;
  trace("intermediate_queue <- %s (%u) incomplete", iflnm->filename, iflnm->iterations);
  g_async_queue_push(intermediate_queue, iflnm);
}

enum file_type process_filename(char *filename){
//  g_message("Filename: %s", filename);
  enum file_type ft= get_file_type(filename);
  switch (ft){
    case INIT:
      break;
    case SCHEMA_TABLESPACE:
      break;
    case SCHEMA_CREATE:
//      g_async_queue_push(schema_counter,GINT_TO_POINTER(1));
      g_atomic_int_inc(&schema_counter);
      process_database_filename(filename);
      if (db){
        ft=DO_NOT_ENQUEUE;
        m_remove(directory,filename);
      }
      break;
    case SCHEMA_TABLE:
      // filename is free
//      g_async_queue_push(schema_counter,GINT_TO_POINTER(1));
      g_atomic_int_inc(&schema_counter);
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
    case SCHEMA_SEQUENCE:
      if (!process_schema_sequence_filename(filename)){
        return DO_NOT_ENQUEUE;
      }
      break;
    case SCHEMA_TRIGGER:
      if (!skip_triggers)
        if (!process_schema_filename(filename, TRIGGER))
          return DO_NOT_ENQUEUE;
      break;
    case SCHEMA_POST:
      // can be enqueued in any order
      if (!skip_post)
        if (!process_schema_filename(filename, POST))
          return DO_NOT_ENQUEUE;
      break;
    case CHECKSUM:
      if (!process_checksum_filename(filename))
        return DO_NOT_ENQUEUE;
      intermediate_conf->checksum_list=g_list_insert(intermediate_conf->checksum_list,filename,-1);
      break;
    case METADATA_GLOBAL:
      process_metadata_global(filename);
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
      g_message("Ignoring file %s", filename);
      break;
  }
  return ft;
}

void remove_fifo_file(gchar *fifo_name){
  g_mutex_lock(exec_process_id_mutex);
  gchar *filename=g_hash_table_lookup(exec_process_id,fifo_name);
  g_mutex_unlock(exec_process_id_mutex);
  if (filename!=NULL)
    m_remove(directory,filename);

}

void process_stream_filename(struct intermediate_filename  * iflnm){
  enum file_type current_ft=process_filename(iflnm->filename);
  if (current_ft == INCOMPLETE ){
    if (iflnm->iterations > 5){
      g_warning("Max renqueing reached for: %s", iflnm->filename);
    }else{
      intermediate_queue_incomplete(iflnm);
    }
//    g_async_queue_push(intermediate_queue, filename);
    return;
  }
  if (current_ft != SCHEMA_VIEW &&
      current_ft != SCHEMA_TRIGGER &&
      current_ft != SCHEMA_POST &&
      current_ft != CHECKSUM &&
      current_ft != IGNORED &&
      current_ft != DO_NOT_ENQUEUE )
    refresh_db_and_jobs(current_ft);
//    g_async_queue_push(intermediate_conf->stream_queue, GINT_TO_POINTER(current_ft));
}

void *intermediate_thread(){
  struct intermediate_filename  * iflnm=NULL;
  g_mutex_lock(start_intermediate_thread);
  do{
    iflnm = (struct intermediate_filename  *)g_async_queue_pop(intermediate_queue);
    trace("intermediate_queue -> %s (%u)", iflnm->filename, iflnm->iterations);
    if ( g_strcmp0(iflnm->filename,"END") == 0 ){
      if (g_async_queue_length(intermediate_queue)>0){
        trace("intermediate_queue <- %s (%u)", iflnm->filename, iflnm->iterations);
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
//  if (innodb_optimize_keys_all_tables)
//    enqueue_all_index_jobs(intermediate_conf);
  message("Intermediate thread ended");
  refresh_db_and_jobs(INTERMEDIATE_ENDED);
  return NULL;
}

