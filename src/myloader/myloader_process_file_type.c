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
#include "myloader_worker_loader_main.h"
#include "myloader_process_file_type.h"
#include "myloader_restore.h"
#include "myloader_global.h"
#include "myloader_database.h"
#include "myloader_worker_schema.h"

extern guint schema_counter;
guint schema_processed_counter = 0;
GAsyncQueue *process_file_type_queue = NULL;
//GThread *stream_process_filename_thread = NULL;
struct configuration *process_file_type_conf;
guint process_file_type_num_threads=4;
GThread ** process_file_type_workers;
gboolean process_filename_ended_send = FALSE;
void *process_file_type_worker(void *data);
guint amount_of_files=0;
GMutex *metadata_global_mutex=NULL;

void initialize_process_file_type(struct configuration *c){
  metadata_global_mutex=g_mutex_new();
  process_file_type_conf=c;
  process_file_type_queue = g_async_queue_new();
  process_file_type_workers = g_new(GThread *, process_file_type_num_threads);
  guint n=0;
  for (n = 0; n < process_file_type_num_threads; n++) {
    process_file_type_workers[n] =
      m_thread_new("myloader_process_filetype",(GThreadFunc)process_file_type_worker, NULL, "File type worker could not be created");
  }
}

static
struct filetype_item *file_type_new(gchar * filename,enum file_type file_type){
  struct filetype_item* fti=g_new0(struct filetype_item, 1);
  fti->filename=g_strdup(filename);
  fti->file_type=file_type;
  return fti;
}

static
void file_type_free(struct filetype_item * fti){
//  g_free(fti->filename); we leave this to be managed by the process_*_filename
  g_free(fti);
}

gint file_type_cmp (gconstpointer _a, gconstpointer _b, gpointer user_data){
   struct filetype_item *a= (struct filetype_item *) _a;
   struct filetype_item * b = (struct filetype_item *)_b;
   (void)user_data;
   trace("comparing %s > %s = %d",ft2str(a->file_type),ft2str(b->file_type), a->file_type > b->file_type);
   return a->file_type > b->file_type;
}

void file_type_push( enum file_type ft, gchar *filename){
  trace("process_file_type_queue <- %s (%s)", filename, ft2str(ft));
  g_atomic_int_inc(&amount_of_files);
  g_async_queue_push_sorted(process_file_type_queue, file_type_new(filename, ft), &file_type_cmp, NULL);
}

void *process_file_type_worker(void *data){
  (void) data;
  struct filetype_item* fti=NULL;
  while (TRUE){
    fti = g_async_queue_pop(process_file_type_queue);
    trace("process_file_type_queue -> %s (%s)", fti->filename, ft2str(fti->file_type));
    switch (fti->file_type){
      case METADATA_GLOBAL:
        g_mutex_lock(metadata_global_mutex);
        process_metadata_global_filename(fti->filename, process_file_type_conf->context);
        g_mutex_unlock(metadata_global_mutex);
        refresh_table_list(process_file_type_conf);
        break;
      case SCHEMA_TABLESPACE:
        g_warning("Tablespace file %s has been ignored. It should be imported manually before restoring", fti->filename);
        break;
      case SCHEMA_CREATE:
        process_database_filename(fti->filename);  // pushed to database_queue
        g_atomic_int_inc(&schema_processed_counter);
        if (has_been_defined_a_target_database())
         m_remove(directory,fti->filename);
        break;
      case SCHEMA_SEQUENCE:
        process_schema_sequence_filename(fti->filename); // pushed to table_queue if database is created, _database->sequence_queue otherwise 
        break;
      case SCHEMA_TABLE:
        process_table_filename(fti->filename); // pushed to table_queue if database is created, _database->table_queue otherwise
        g_atomic_int_inc(&schema_processed_counter);
        break;
      case DATA:
        if (!no_data){
          if (process_data_filename(fti->filename)) // added to dbt->restore_job_list 
            wake_data_threads();
        }else
          m_remove(directory,fti->filename);
        total_data_sql_files++;
        if (process_filename_ended_send && g_atomic_int_get(&schema_counter) > 0 &&g_atomic_int_get(&schema_counter) == g_atomic_int_get (&schema_processed_counter)){
          process_filename_ended_send=FALSE;
          refresh_table_list(process_file_type_conf);
        }
        break;
      case LOAD_DATA:
        // LOAD_DATA files are not processed/executed as the DATA filename has the execution statement
        release_load_data_as_it_is_close(fti->filename);
        // nevertheless, we need to sync DATA execution with LOAD_DATA, as DATA can not be execute if LOAD_DATA hasn't been received
        break;
      case SCHEMA_VIEW:
        process_schema_view_filename(fti->filename); // pushed to view_queue 
        break;
      case SCHEMA_TRIGGER:
        if (!skip_triggers)
          process_schema_post_filename(fti->filename, TRIGGER); // pushed to post_queue
        break;
      case SCHEMA_POST:
        if (!skip_post)
          process_schema_post_filename(fti->filename, POST); // pushed to post_queue
        break;
      case IGNORED:
        g_warning("Filename %s has been ignored", fti->filename);
        break;
      case RESUME:
        if (stream){
          m_critical("We don't expect to find resume files in a stream scenario");
        }
        break;
      case FILENAME_ENDED:
        process_filename_ended_send=TRUE;
  //      schema_ended();
        trace("process_file_type_queue <- %s", ft2str(fti->file_type));
        g_async_queue_push(process_file_type_queue,fti);
        return NULL;
        break;
    }
    file_type_free(fti);
    fti=NULL;
  }
  return NULL;
}

void wait_file_type_to_complete(){
  guint n;
  for (n = 0; n < process_file_type_num_threads; n++) {
    g_thread_join(process_file_type_workers[n]);
  }
  schema_ended();
}
