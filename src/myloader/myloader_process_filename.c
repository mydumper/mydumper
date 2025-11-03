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

guint schema_counter = 0;
guint schema_processed_counter = 0;
gboolean process_filename_queue_ended = FALSE;
gboolean process_filename_ended_send = FALSE;
GAsyncQueue *process_file_type_queue = NULL;
GAsyncQueue *process_filename_queue = NULL;
//GThread *stream_process_filename_thread = NULL;
gchar *exec_per_thread = NULL;
gchar *exec_per_thread_extension = NULL;
gchar **exec_per_thread_cmd=NULL;

GHashTable * exec_process_id = NULL;
GMutex *exec_process_id_mutex = NULL;
GMutex *start_process_filename_thread=NULL;
void *process_filename_thread();
struct configuration *process_filename_conf = NULL;
guint process_filename_num_threads=4;
GThread *_process_filename_thread=NULL;



void initialize_process_filename_queue (struct configuration *c){
//  schema_counter = g_async_queue_new();
  process_filename_conf=c;
  process_filename_queue = g_async_queue_new();
  process_file_type_queue = g_async_queue_new();
  exec_process_id=g_hash_table_new ( g_str_hash, g_str_equal );
  exec_process_id_mutex=g_mutex_new();
  start_process_filename_thread = g_mutex_new();
  g_mutex_lock(start_process_filename_thread);
  if (stream)
    g_mutex_unlock(start_process_filename_thread);
  process_filename_queue_ended=FALSE;
//  stream_process_filename_thread = m_thread_new("myloader_process_filename",(GThreadFunc)process_filename_thread, NULL, "Intermediate thread could not be created");

  _process_filename_thread = m_thread_new("myloader_process_filename",(GThreadFunc)process_filename_thread, NULL, "Intermediate thread could not be created");

  initialize_control_job(c);
}

void process_filename_queue_new(const gchar *filename){
  struct process_filename_filename * iflnm=g_new0(struct process_filename_filename, 1);
  iflnm->filename = g_strdup(filename);
  iflnm->iterations=0;
  trace("process_filename_queue <- %s (%u)", iflnm->filename, iflnm->iterations);
  g_async_queue_push(process_filename_queue, iflnm);
}

struct filetype_item *file_type_new(gchar * filename,enum file_type file_type){
  struct filetype_item* fti=g_new0(struct filetype_item, 1);
  fti->filename=g_strdup(filename);
  fti->file_type=file_type;
  return fti;
}


void process_filename_queue_end(){
  if (!stream)
    g_mutex_unlock(start_process_filename_thread);
  gchar *e=g_strdup("END");
  process_filename_queue_new(e);
  message("Intermediate queue: Sending END job");
  g_thread_join(_process_filename_thread);
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
    return FILE_TYPE_ENDED; 

  if ((!strcmp(filename,          "metadata") ||
       !strcmp(filename,          "metadata.header") ||
       g_str_has_prefix(filename, "metadata.partial"))
      &&
      !( g_str_has_suffix(filename, ".sql") ||
         has_exec_per_thread_extension(filename)))
    return METADATA_GLOBAL;

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

  if (m_filename_has_suffix(filename, "-schema-sequence.sql") )
    return SCHEMA_SEQUENCE;

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


/*
//static
enum file_type process_filename(char *filename){
  enum file_type ft= get_file_type(filename);
  switch (ft){
    case METADATA_GLOBAL:
      process_metadata_global_filename(filename, process_filename_conf->context);
      refresh_table_list(process_filename_conf);
      break;
    case SCHEMA_TABLESPACE:
      g_warning("Tablespace file %s has been ignored. It should be imported manually before restoring", filename);
      break;
    case SCHEMA_CREATE:
//      g_atomic_int_inc(&schema_counter);
      process_database_filename(filename);
      if (has_been_defined_a_target_database()){
        ft=DO_NOT_ENQUEUE;
        m_remove(directory,filename);
      }
      break;
    case SCHEMA_SEQUENCE:
      if (!process_schema_sequence_filename(filename)){
        return DO_NOT_ENQUEUE;
      }
      break;
    case SCHEMA_TABLE:
      g_atomic_int_inc(&schema_counter);
      g_message("SCHEMA_TABLE %s", filename);
      if (!process_table_filename(filename)){
        return DO_NOT_ENQUEUE;
      }
      break;
    case DATA:
      if (!no_data){
        if (!process_data_filename(filename))
          return DO_NOT_ENQUEUE;
      }else
        m_remove(directory,filename);
      total_data_sql_files++;
      break;
    case LOAD_DATA:
      release_load_data_as_it_is_close(filename);
      break;
    case SCHEMA_VIEW:
      if (!process_schema_view_filename(filename))
        return DO_NOT_ENQUEUE;
      break;
    case SCHEMA_TRIGGER:
      if (!skip_triggers)
        if (!process_schema_post_filename(filename, TRIGGER))
          return DO_NOT_ENQUEUE;
      break;
    case SCHEMA_POST:
      if (!skip_post)
        if (!process_schema_post_filename(filename, POST))
          return DO_NOT_ENQUEUE;
      break;
    case IGNORED:
      g_warning("Filename %s has been ignored", filename);
      break;
    case RESUME:
      if (stream){
        m_critical("We don't expect to find resume files in a stream scenario");
      }
      break;
    default:
      g_message("Ignoring file %s", filename);
      break;
  }
  return ft;
}
*/


enum file_type last_file_type_popped;

void *process_file_type_worker(void *data){
  (void) data;
struct filetype_item* fti=NULL;
enum file_type ft;
while (TRUE){
  fti = g_async_queue_pop(process_file_type_queue);
  ft=fti->file_type;
  last_file_type_popped=ft!=FILE_TYPE_ENDED?ft:last_file_type_popped;
  switch (ft){
    case METADATA_GLOBAL:
      process_metadata_global_filename(fti->filename, process_filename_conf->context);
      refresh_table_list(process_filename_conf);
      break;
    case SCHEMA_TABLESPACE:
      g_warning("Tablespace file %s has been ignored. It should be imported manually before restoring", fti->filename);
      break;
    case SCHEMA_CREATE:
      g_atomic_int_inc(&schema_processed_counter);
      process_database_filename(fti->filename);
      if (has_been_defined_a_target_database()){
        ft=DO_NOT_ENQUEUE;
        m_remove(directory,fti->filename);
      }
      break;
    case SCHEMA_SEQUENCE:
      if (!process_schema_sequence_filename(fti->filename)){
        ft=DO_NOT_ENQUEUE;
      }
      break;
    case SCHEMA_TABLE:
      if (!process_table_filename(fti->filename)){
        ft=DO_NOT_ENQUEUE;
      }
      g_atomic_int_inc(&schema_processed_counter);
      break;
    case DATA:
      if (!no_data){
        if (!process_data_filename(fti->filename))
          ft= DO_NOT_ENQUEUE;
      }else
        m_remove(directory,fti->filename);
      total_data_sql_files++;
      if (process_filename_ended_send && g_atomic_int_get(&schema_counter) > 0 &&g_atomic_int_get(&schema_counter) == g_atomic_int_get (&schema_processed_counter)){
        process_filename_ended_send=FALSE;
        refresh_table_list(process_filename_conf);
        enroute_into_the_right_queue_based_on_file_type(FILE_TYPE_SCHEMA_ENDED);
      }
      break;
    case LOAD_DATA:
      release_load_data_as_it_is_close(fti->filename);
      break;
    case SCHEMA_VIEW:
      if (!process_schema_view_filename(fti->filename))
        ft = DO_NOT_ENQUEUE;
      break;
    case SCHEMA_TRIGGER:
      if (!skip_triggers)
        if (!process_schema_post_filename(fti->filename, TRIGGER))
          ft = DO_NOT_ENQUEUE;
      break;
    case SCHEMA_POST:
      if (!skip_post)
        if (!process_schema_post_filename(fti->filename, POST))
          ft = DO_NOT_ENQUEUE;
      break;
    case IGNORED:
      g_warning("Filename %s has been ignored", fti->filename);
      break;
    case RESUME:
      if (stream){
        m_critical("We don't expect to find resume files in a stream scenario");
      }
      break;

    case FILE_TYPE_ENDED:
      g_async_queue_push(process_file_type_queue,fti);
      return NULL;
      break;
    case INIT:
    case CJT_RESUME:
    case SHUTDOWN:
    case DO_NOT_ENQUEUE:
    case REQUEST_DATA_JOB:
    case FILE_TYPE_SCHEMA_ENDED:
      g_warning("process_file_type_worker: received a wrong kind of file type");
      break;
  }
  enroute_into_the_right_queue_based_on_file_type(ft);
}
return NULL;
}

/*
void process_stream_filename(struct process_filename_filename  * iflnm){
  enum file_type current_ft=process_filename(iflnm->filename);
  enroute_into_the_right_queue_based_on_file_type(current_ft);
}
*/

 gint file_type_cmp (gconstpointer _a, gconstpointer _b, gpointer user_data){
   struct filetype_item *a= (struct filetype_item *) _a;
   struct filetype_item * b = (struct filetype_item *)_b;
   (void)user_data;
   return a->file_type>b->file_type;
 }

void *process_filename_worker(void *data){
  (void)data;
  struct process_filename_filename  * iflnm=NULL;
  set_thread_name("IQT_worker");
  enum file_type ft;
  do{
    iflnm = (struct process_filename_filename  *)g_async_queue_pop(process_filename_queue);
    ft = get_file_type(iflnm->filename);
    trace("process_filename_queue -> %s (%u)", iflnm->filename, iflnm->iterations);
    if ( ft == FILE_TYPE_ENDED ){
      process_filename_ended_send=TRUE;
      if (g_async_queue_length(process_filename_queue)>0){
        trace("process_filename_queue <- %s (%u)", iflnm->filename, iflnm->iterations);
        g_async_queue_push( process_file_type_queue,file_type_new(iflnm->filename, ft));
        continue;
      }
      g_async_queue_push( process_file_type_queue,file_type_new(iflnm->filename, ft));
      iflnm=NULL;
      break;
    }
    g_async_queue_push_sorted( process_file_type_queue,file_type_new(iflnm->filename, ft), &file_type_cmp, NULL);
  } while (iflnm != NULL);
  return NULL;
}

void *process_filename_thread(){
  set_thread_name("IQT");
//  g_mutex_lock(start_process_filename_thread);

  
  guint n=0;
  GThread * process_filename_threads = 
      m_thread_new("myloader_process_filename",(GThreadFunc)process_filename_worker, NULL, "Intermediate worker could not be created");

  GThread ** process_file_type_workers = g_new(GThread *, process_filename_num_threads);
  for (n = 0; n < process_filename_num_threads; n++) {
    process_file_type_workers[n] =
      m_thread_new("myloader_process_filetype",(GThreadFunc)process_file_type_worker, NULL, "File type worker could not be created");
  }


//  for (n = 0; n < process_filename_num_threads; n++) {
  g_thread_join(process_filename_threads);
//  }
  g_message("Process filename ended");
  for (n = 0; n < process_filename_num_threads; n++) {
    g_thread_join(process_file_type_workers[n]);
  }
  g_message("Process filetype ended");

  message("Intermediate thread ended");
  refresh_table_list(process_filename_conf);
  enroute_into_the_right_queue_based_on_file_type(FILE_TYPE_ENDED);
  return NULL;
}

