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
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include "common.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_jobs_manager.h"
#include "myloader_stream.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"

extern gchar *compress_extension;
extern gchar *db;
extern gchar *directory;
extern gchar *source_db;
extern gboolean no_data;
extern gboolean skip_triggers;
extern gboolean skip_post;
extern guint num_threads;
extern int (*m_close)(void *file);
extern int (*m_write)(FILE * file, const char * buff, int len);
extern guint total_data_sql_files;

GAsyncQueue *intermediate_queue = NULL;
GThread *stream_intermediate_thread = NULL;
static GMutex *table_list_mutex = NULL;

void *intermediate_thread();
struct configuration *intermediate_conf = NULL;

void initialize_intermediate_queue (struct configuration *c){
  intermediate_conf=c;
  intermediate_queue = g_async_queue_new();
  table_list_mutex = g_mutex_new();
  stream_intermediate_thread = g_thread_create((GThreadFunc)intermediate_thread, NULL, TRUE, NULL);
//  stream_thread = g_thread_create((GThreadFunc)process_stream_directory, NULL, TRUE, NULL);
}

void intermediate_queue_new(gchar *filename){
  g_async_queue_push(intermediate_queue, filename);
}
void intermediate_queue_end(){
  gchar *e=g_strdup("END");
  intermediate_queue_new(e);
  g_message("Intermediate queue ending");
  g_thread_join(stream_intermediate_thread);
  g_message("Intermediate thread ended");
}

void intermediate_queue_incomplete(gchar *filename){
// TODO: we need to add the filelist and check the filename iterations
// the idea is to keep track how many times a filename was incomplete and
// at what stage
  g_async_queue_push(intermediate_queue, filename);
}

enum file_type process_filename(char *filename){
  enum file_type ft= get_file_type(filename);
  if (!source_db ||
    g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
    switch (ft){
      case INIT:
        break;
      case SCHEMA_TABLESPACE:
        break;
      case SCHEMA_CREATE:
        process_database_filename(filename, "create database");
        //m_remove(directory,filename);
        break;
      case SCHEMA_TABLE:
        // filename is free
        if (!process_table_filename(filename)){
          return INCOMPLETE;
        }else{
          g_free(filename);
          g_mutex_lock(table_list_mutex);
          refresh_table_list(intermediate_conf);
          g_mutex_unlock(table_list_mutex);
        }
        break;
      case SCHEMA_VIEW:
        process_schema_filename(filename,"view");
        break;
      case SCHEMA_TRIGGER:
        if (!skip_triggers)
          process_schema_filename(filename,"trigger");
        break;
      case SCHEMA_POST:
        // can be enqueued in any order
        if (!skip_post)
          process_schema_filename(filename,"post");
        break;
      case CHECKSUM:
        intermediate_conf->checksum_list=g_list_insert(intermediate_conf->checksum_list,filename,-1);
        break;
      case METADATA_GLOBAL:
        break;
      case METADATA_TABLE:
        intermediate_conf->metadata_list=g_list_insert(intermediate_conf->metadata_list,filename,-1);
        if (!process_metadata_filename(filename))
          return INCOMPLETE;
        g_mutex_lock(table_list_mutex);
        refresh_table_list(intermediate_conf);
        g_mutex_unlock(table_list_mutex);
        break;
      case DATA:
        if (!no_data){
          if (!process_data_filename(filename))
            return INCOMPLETE;
        }else
          m_remove(directory,filename);
        total_data_sql_files++;
        break;
      case RESUME:
        g_critical("We don't expect to find resume files in a stream scenario");
        exit(EXIT_FAILURE);
        break;
      case IGNORED:
        g_warning("Filename %s has been ignored", filename);
        break;
      case LOAD_DATA:
        g_message("Load data file found: %s", filename);
        break;
      case SHUTDOWN:
        break;
      case INCOMPLETE:
        break;
    }
  }
  return ft;
}

void process_stream_filename(gchar * filename){
  enum file_type current_ft=process_filename(filename);
  if (current_ft == INCOMPLETE ){
    g_debug("Requeuing in intermediate queue: %s", filename);
    g_async_queue_push(intermediate_queue, filename);
    return;
  }
  if (current_ft != SCHEMA_VIEW &&
      current_ft != SCHEMA_TRIGGER &&
      current_ft != SCHEMA_POST &&
      current_ft != CHECKSUM &&
      current_ft != METADATA_TABLE )
    g_async_queue_push(intermediate_conf->stream_queue, GINT_TO_POINTER(current_ft));
}

void *intermediate_thread(){
  char * filename=NULL;
  do{
    filename = (gchar *)g_async_queue_pop(intermediate_queue);
    if ( g_strcmp0(filename,"END") == 0 ){
      if (g_async_queue_length(intermediate_queue)>0){
        g_async_queue_push(intermediate_queue,filename);
        continue;
      }
      g_free(filename);
      break;
    }
    process_stream_filename(filename);
  } while (filename != NULL);
  guint n=0;
  for (n = 0; n < num_threads; n++)
    g_async_queue_push(intermediate_conf->stream_queue, GINT_TO_POINTER(SHUTDOWN));
  return NULL;
}

