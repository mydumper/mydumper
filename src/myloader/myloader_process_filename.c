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

#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "myloader_process_filename.h"
#include "myloader_restore.h"
#include "myloader_global.h"
#include "myloader_database.h"
#include "myloader_process_file_type.h"
#include "myloader_worker_loader_main.h"
guint schema_counter = 0;
guint sequence_counter = 0;


gboolean process_filename_queue_ended = FALSE;
GAsyncQueue *process_filename_queue = NULL;
//GThread *stream_process_filename_thread = NULL;
gchar *exec_per_thread = NULL;
gchar *exec_per_thread_extension = NULL;
gchar **exec_per_thread_cmd=NULL;

GHashTable * exec_process_id = NULL;
GMutex *exec_process_id_mutex = NULL;
GMutex *start_process_filename_thread=NULL;
struct configuration *process_filename_conf = NULL;
guint process_filename_num_threads=4;
GThread *process_filename_thread=NULL;


void *process_filename_worker(void *data);

void initialize_process_filename (struct configuration *c){
//  schema_counter = g_async_queue_new();
  process_filename_conf=c;
  process_filename_queue = g_async_queue_new();
  exec_process_id=g_hash_table_new ( g_str_hash, g_str_equal );
  exec_process_id_mutex=g_mutex_new();
  start_process_filename_thread = g_mutex_new();
  g_mutex_lock(start_process_filename_thread);
  if (stream)
    g_mutex_unlock(start_process_filename_thread);
  process_filename_queue_ended=FALSE;
  process_filename_thread =
      m_thread_new("myloader_process_filename",(GThreadFunc)process_filename_worker, NULL, "Intermediate worker could not be created");
  initialize_process_file_type(c);
  initialize_worker_loader_main(c);
}

void process_filename_push(const gchar *filename){
  struct process_filename_filename * iflnm=g_new0(struct process_filename_filename, 1);
  iflnm->filename = g_strdup(filename);
  iflnm->iterations=0;
  trace("process_filename_queue <- %s (%u)", iflnm->filename, iflnm->iterations);
  g_async_queue_push(process_filename_queue, iflnm);
}

void process_filename_queue_end(){
  if (!stream)
    g_mutex_unlock(start_process_filename_thread);
  gchar *e=g_strdup("END");
  process_filename_push(e);
  message("Intermediate queue: Sending END job");
  g_thread_join(process_filename_thread);
  message("Intermediate thread: SHUTDOWN");
  process_filename_queue_ended=TRUE;
}

void process_filename_queue_incomplete(struct process_filename_filename * iflnm){
// TODO: we need to add the filelist and check the filename iterations
// the idea is to keep track how many times a filename was incomplete and
// at what stage
  iflnm->iterations++;
  trace("process_filename_queue <- %s (%u) incomplete", iflnm->filename, iflnm->iterations);
  g_async_queue_push(process_filename_queue, iflnm);
}

static
enum file_type get_file_type (const char * filename){
  if ( !g_strcmp0(filename,"END"))
    return FILENAME_ENDED; 

  if ((!strcmp(filename,          "metadata") ||
       !strcmp(filename,          "metadata.header") ||
       g_str_has_prefix(filename, "metadata.partial"))
      &&
      !( g_str_has_suffix(filename, ".sql") ||
         has_exec_per_thread_extension(filename)))
    return METADATA_GLOBAL;

  if ((
       g_str_has_prefix(filename, "metadata.partial"))
      &&
      !( g_str_has_suffix(filename, ".sql") ||
         has_exec_per_thread_extension(filename)))
    return METADATA_PARTIAL;


  if (source_db && !(g_str_has_prefix(filename, source_db) && strlen(filename) > strlen(source_db) && (filename[strlen(source_db)] == '.' || filename[strlen(source_db)] == '-') ) && !g_str_has_prefix(filename, "mydumper_"))
    return IGNORED;

  if (m_filename_has_suffix(filename, "-schema.sql")){
    g_atomic_int_inc(&schema_counter);
    return SCHEMA_TABLE;
  }

  if ( strcmp(filename, "all-schema-create-tablespace.sql") == 0 )
    return SCHEMA_TABLESPACE;

  if ( strcmp(filename, "resume") == 0 ){
    if (!resume){
      m_critical("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.");
    }
    return RESUME;
  }

  if ( strcmp(filename, "resume.partial") == 0 )
    m_critical("resume.partial file found. Remove it and restart process if you consider that it will be safe.");

  if (m_filename_has_suffix(filename, "-schema-view.sql") )
    return SCHEMA_VIEW;

  if (m_filename_has_suffix(filename, "-schema-sequence.sql") ){
    g_atomic_int_inc(&sequence_counter);
    return SCHEMA_SEQUENCE;
  }

  if (m_filename_has_suffix(filename, "-schema-triggers.sql") )
    return SCHEMA_TRIGGER;

  if (m_filename_has_suffix(filename, "-schema-post.sql") )
    return SCHEMA_POST;

  if (m_filename_has_suffix(filename, "-schema-create.sql") ){
    g_atomic_int_inc(&schema_counter);
    return SCHEMA_CREATE;
  }

  if (m_filename_has_suffix(filename, ".sql") )
    return DATA;

  if (m_filename_has_suffix(filename, ".dat"))
    return LOAD_DATA;

  return IGNORED;
}

void *process_filename_worker(void *data){
  (void)data;

  struct process_filename_filename  * iflnm=NULL;
  set_thread_name("IQT_worker");
  enum file_type ft;
  do{
    iflnm = (struct process_filename_filename *)g_async_queue_pop(process_filename_queue);
    ft = get_file_type(iflnm->filename);
    trace("process_filename_queue -> %s (%u)", iflnm->filename, iflnm->iterations);
    if ( ft == FILENAME_ENDED ){
//      if (g_async_queue_length(process_filename_queue)>0){
//        file_type_push(ft, iflnm->filename);
//        continue;
//      }
      file_type_push(ft, iflnm->filename);
      iflnm=NULL;
      break;
    }
    file_type_push(ft, iflnm->filename);
  } while (iflnm != NULL);
  g_message("Process filename ended");
  wait_file_type_to_complete();
//  refresh_table_list(process_filename_conf);
  return NULL;
}

