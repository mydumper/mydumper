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

extern gchar *compress_extension;
extern gchar *db;
extern gchar *directory;
extern gchar *source_db;
extern gboolean no_data;
extern gboolean skip_triggers;
extern gboolean skip_post;
extern guint num_threads;
extern GAsyncQueue *stream_queue;
extern int (*m_close)(void *file);
extern int (*m_write)(FILE * file, const char * buff, int len);

GAsyncQueue *intermidiate_queue = NULL;
GThread *stream_thread = NULL;
GThread *stream_intermidiate_thread = NULL;
static GMutex *table_list_mutex = NULL;

struct configuration *stream_conf = NULL;

void *process_stream();
void *intermidiate_thread();

void initialize_stream (struct configuration *c){
  stream_conf = c;
  stream_queue = g_async_queue_new();
  intermidiate_queue = g_async_queue_new();
  table_list_mutex = g_mutex_new();
  stream_intermidiate_thread = g_thread_create((GThreadFunc)intermidiate_thread, NULL, TRUE, NULL);
  stream_thread = g_thread_create((GThreadFunc)process_stream, NULL, TRUE, NULL);
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}

enum file_type process_filename(char *filename){
  enum file_type ft= get_file_type(filename);
  if (!source_db ||
    g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
    switch (ft){
      case INIT:
      case SCHEMA_CREATE:
        if (db){
          g_warning("Skipping database creation on file: %s",filename);
          m_remove(directory,filename);
        }else{
          process_database_filename(filename, "create database");
        }
        break;
      case SCHEMA_TABLE:
        process_table_filename(filename);
        g_mutex_lock(table_list_mutex);
        refresh_table_list(stream_conf);
        g_mutex_unlock(table_list_mutex);        
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
        stream_conf->checksum_list=g_list_insert(stream_conf->checksum_list,g_strdup(filename),-1);
        break;
      case METADATA_GLOBAL:
        break;
      case METADATA_TABLE:
        stream_conf->metadata_list=g_list_insert(stream_conf->metadata_list,g_strdup(filename),-1);
        process_metadata_filename(stream_conf->table_hash,filename);
        g_mutex_lock(table_list_mutex);
        refresh_table_list(stream_conf);
        g_mutex_unlock(table_list_mutex);
        break;
      case DATA:
        if (!no_data)
          process_data_filename(filename);
        else
          m_remove(directory,filename);
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
    }
  }
  return ft;
}

gboolean has_mydumper_suffix(gchar *line){
  return g_str_has_suffix(line,".sql") || g_str_has_suffix(line,".sql.gz") || g_str_has_suffix(line,"metadata") || g_str_has_suffix(line,".checksum") || g_str_has_suffix(line,".checksum.gz") ;
}

void process_stream_filename(gchar * filename){
  enum file_type current_ft=process_filename(filename);
  if (current_ft != SCHEMA_VIEW &&
      current_ft != SCHEMA_TRIGGER &&
      current_ft != SCHEMA_POST &&
      current_ft != CHECKSUM &&
      current_ft != METADATA_TABLE )
  g_async_queue_push(stream_queue, GINT_TO_POINTER(current_ft));
}

struct job * give_any_data_job(){
  g_mutex_lock(table_list_mutex);
  GList * iter=stream_conf->table_list;
  GList * next = NULL;
  struct job *job = NULL;

  while (iter != NULL){
    struct db_table * dbt = iter->data;
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
    iter=iter->next;
  }
  g_mutex_unlock(table_list_mutex);

  return job;
}

struct restore_job * give_me_next_data_job(){
  g_mutex_lock(table_list_mutex);
  GList * iter=stream_conf->table_list;
  GList * next = NULL;
  struct restore_job *job = NULL;
//  g_message("Elemetns in table_list: %d",g_list_length(stream_conf->table_list));
  while (iter != NULL){
    struct db_table * dbt = iter->data;
//    g_message("DB: %s Table: %s len: %d", dbt->real_database,dbt->real_table,g_list_length(dbt->restore_job_list));
    if (dbt->current_threads < dbt->max_threads){
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
  g_mutex_unlock(table_list_mutex);
  return job;
}

void *process_stream_queue(struct thread_data * td) {
  struct job *job = NULL;
  gboolean cont=TRUE;
  enum file_type ft=0;
//  enum file_type ft;
  while (cont){
    ft=(enum file_type)g_async_queue_pop(stream_queue);
    job=g_async_queue_try_pop(stream_conf->database_queue);
    if (job != NULL){
      g_debug("Restoring database");
      cont=process_job(td, job);
      continue;
    }
    job=g_async_queue_try_pop(stream_conf->table_queue);
    if (job != NULL){
      execute_use_if_needs_to(td, job->use_database, "Restoring tables");
      cont=process_job(td, job);
      continue;
    }
    struct restore_job *rj = give_me_next_data_job();
    if (rj != NULL){
      job=new_job(JOB_RESTORE,rj,rj->dbt->real_database);
      execute_use_if_needs_to(td, job->use_database, "Restoring tables");
      cont=process_job(td, job);
      continue;
    }
    job=give_any_data_job();
    if (job != NULL){
      execute_use_if_needs_to(td, job->use_database, "Restoring tables");
      cont=process_job(td, job);
      continue;
    }else{
      if (ft==SHUTDOWN)
        cont=FALSE;
      else
        g_async_queue_push(stream_queue,GINT_TO_POINTER(ft));
    }
    
  }
  g_message("Shutting down stream thread");
  return NULL;
}

void *intermidiate_thread(){
  char * filename=NULL;
  do{
    filename = (gchar *)g_async_queue_pop(intermidiate_queue);
    if ( g_strcmp0(filename,"END") ==0 ) break;
    process_stream_filename(filename);
  } while (filename != NULL);
  return NULL;
}

void *process_stream(){
  char * filename=NULL,*real_filename=NULL;
  char buffer[STREAM_BUFFER_SIZE];
  FILE *file=NULL;
  gboolean eof=FALSE;
  stream_conf->table_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  do {
    if(fgets(buffer, STREAM_BUFFER_SIZE, stdin) == NULL){
      if (file && feof(file)){
        eof = TRUE;
        buffer[0] = '\0';
        m_close(file);
      }else{
        break;
      }
    }else{
      if (g_str_has_prefix(buffer,"-- ")){
        gchar lastchar = buffer[strlen(buffer)-1];
        buffer[strlen(buffer)-1]='\0';
        if (has_mydumper_suffix(buffer)){
          if (file){
            m_close(file);
            g_async_queue_push(intermidiate_queue, filename);
          }
          real_filename = g_build_filename(directory,&(buffer[3]),NULL);
          filename = g_build_filename(&(buffer[3]),NULL);
          if (!g_str_has_suffix(filename, compress_extension)) {
            file = g_fopen(real_filename, "w");
            m_write=(void *)&write_file;
            m_close=(void *) &fclose;
          } else {
            file = (void *)gzopen(real_filename, "w");
            m_write=(void *)&gzwrite;
            m_close=(void *) &gzclose;
          }
        }else{
          buffer[strlen(buffer)-1]=lastchar;
          if (file) m_write(file,buffer,strlen(buffer));
        }
      }else{
        if (file) m_write(file,buffer,strlen(buffer));
      }
    }
  } while (eof == FALSE);
  m_close(file);
  g_async_queue_push(intermidiate_queue, filename);
  gchar *e=g_strdup("END");
  g_async_queue_push(intermidiate_queue, e);
  guint n=0;
  for (n = 0; n < num_threads *2 ; n++) {
    g_async_queue_push(stream_conf->data_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->post_table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_queue, GINT_TO_POINTER(SHUTDOWN));
  }

  return NULL;
}

