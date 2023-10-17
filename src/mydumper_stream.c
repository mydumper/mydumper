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
#include "string.h"
#include <mysql.h>
#include <glib/gstdio.h>
#include <stdlib.h>
#include <glib.h>
#include <stdio.h>
#include "common.h"
#include "mydumper_global.h"
#include "mydumper_start_dump.h"
#include "mydumper_stream.h"
#include <sys/file.h>

extern GAsyncQueue *stream_queue;

GThread *stream_thread = NULL;
GThread *metadata_partial_writer_thread = NULL;
gboolean metadata_partial_writer_alive = TRUE;
GAsyncQueue *metadata_partial_queue = NULL;
GAsyncQueue * initial_metadata_lock_queue = NULL;
GAsyncQueue * initial_metadata_queue = NULL;
void metadata_partial_queue_push (struct db_table *dbt){
  if (dbt)
    g_async_queue_push(metadata_partial_queue, dbt);
}

struct stream_queue_element * new_stream_queue_element(struct db_table *dbt,gchar *filename,GAsyncQueue *done){
  struct stream_queue_element *sf=g_new0(struct stream_queue_element, 1);
  sf->dbt=dbt;
  sf->filename=filename;
  sf->done=done;
  return sf;
}

guint get_stream_queue_length(){
  return g_async_queue_length(stream_queue);
}

void stream_queue_push(struct db_table *dbt,gchar *filename){
/*  if (dbt)
    g_message("New stream file: %s for dbt: %s ", filename, dbt->table);
  else
    g_message("New stream file: %s with null dbt: ", filename);
*/
  GAsyncQueue *done = g_async_queue_new();
  g_async_queue_push(stream_queue, new_stream_queue_element(dbt,filename,done));
  g_async_queue_pop(done);
  g_async_queue_unref(done);
  metadata_partial_queue_push(dbt);
}

void *process_stream(void *data){
  (void)data;
  int f=0;
  char buf[STREAM_BUFFER_SIZE];
  int buflen;
  guint64 total_size=0;
  GDateTime *total_start_time=g_date_time_new_now_local();
  GTimeSpan diff=0,total_diff=0;
//  gboolean not_compressed = FALSE;
//  guint sz=0;
  ssize_t len=0;
  GDateTime *datetime;
  struct stream_queue_element *sf = NULL;
  for(;;){
    sf = g_async_queue_pop(stream_queue);
    if (sf->done != NULL) {
      g_async_queue_push(sf->done, GINT_TO_POINTER(1));
    }

    if (strlen(sf->filename) == 0){
      break;
    }
    char *used_filemame=g_path_get_basename(sf->filename);
    len=write(fileno(stdout), "\n-- ", 4);
    len=write(fileno(stdout), used_filemame, strlen(used_filemame));
    len=write(fileno(stdout), " ", 1);
    total_size+=5;
    total_size+=strlen(used_filemame);
    free(used_filemame);
    if (no_stream == FALSE){
//      g_message("Stream Opening: %s",sf->filename);
      f=open(sf->filename,O_RDONLY|O_DSYNC);
      if (!f){
        m_error("File failed to open: %s",sf->filename);
      }else{
/*
      	      if (flock(fileno(f),LOCK_EX)){
          g_async_queue_push(stream_queue,sf);
	  g_message("File not possible to lock %s",sf->filename);
	  continue;
	}
	flock(fileno(f),LOCK_UN);
*/
	if (!f){
          g_critical("File failed to open: %s. Reetrying",sf->filename);
          f=open(sf->filename,O_RDONLY);
          if (!f){
            m_error("File failed to open: %s. Cancelling",sf->filename);
            exit(EXIT_FAILURE);
          }
        }
        struct stat st;
        fstat(f, &st);
        off_t size = st.st_size;
        
//        g_message("File size of %s is %"G_GINT64_FORMAT, sf->filename, size);
//        g_message("Streaming file %s", sf->filename);
        gchar *c = g_strdup_printf("%"G_GINT64_FORMAT,size);
        len=write(fileno(stdout), c, strlen(c));
        len=write(fileno(stdout), "\n", 1);
        total_size+=strlen(c) + 1;
        g_free(c);

        guint total_len=0;
        GDateTime *start_time=g_date_time_new_now_local();
        buflen = read(f, buf, STREAM_BUFFER_SIZE);
        while(buflen > 0){
          len=write(fileno(stdout), buf, buflen);
          total_len=total_len + buflen;
          if (len != buflen)
            m_error("Stream failed during transmition of file: %s",sf->filename);
          buflen = read(f, buf, STREAM_BUFFER_SIZE);
        }
//        g_message("Bytes readed of %s is %d", filename, total_len);
        datetime = g_date_time_new_now_local();
        diff=g_date_time_difference(datetime,start_time)/G_TIME_SPAN_SECOND;
        g_date_time_unref(start_time);
        total_diff=g_date_time_difference(datetime,total_start_time)/G_TIME_SPAN_SECOND;
        g_date_time_unref(datetime);
        if (diff > 0){
          g_message("File %s transferred in %" G_GINT64_FORMAT " seconds at %" G_GINT64_FORMAT " MB/s | Global: %" G_GINT64_FORMAT " MB/s",sf->filename,diff,total_len/1024/1024/diff,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }else{
          g_message("File %s transferred | Global: %" G_GINT64_FORMAT "MB/s",sf->filename,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }
        total_size+=total_len;
        close(f);
      }
    }
    if (no_delete == FALSE){
      remove(sf->filename);
    }
    g_free(sf->filename);
    g_free(sf);
  }
  datetime = g_date_time_new_now_local();
  total_diff=g_date_time_difference(datetime,total_start_time)/G_TIME_SPAN_SECOND;
  g_date_time_unref(total_start_time);
  g_date_time_unref(datetime);
  g_message("All data transferred was %" G_GINT64_FORMAT " at a rate of %" G_GINT64_FORMAT " MB/s",total_size,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
  metadata_partial_writer_alive = FALSE;
  metadata_partial_queue_push(GINT_TO_POINTER(1));
  g_thread_join(metadata_partial_writer_thread);
  return NULL;
}



void send_initial_metadata(){
  g_async_queue_push(initial_metadata_queue, GINT_TO_POINTER(1) );
  g_async_queue_pop(initial_metadata_lock_queue);
}

void *metadata_partial_writer(void *data){
  (void) data;
  struct db_table *dbt=NULL;
  GList *dbt_list = NULL;
  GString *output=g_string_sized_new(256);
  guint i=0;
  gchar *filename = NULL;
  GError* gerror = NULL;
  for(i=0;i<num_threads;i++){
    g_async_queue_pop(initial_metadata_queue);
  }
  dbt=g_async_queue_try_pop(metadata_partial_queue);   
  while (dbt != NULL ){
    dbt_list=g_list_append(dbt_list,dbt);
    dbt=g_async_queue_try_pop(metadata_partial_queue);
  }
  g_string_set_size(output,0);
  g_list_foreach(dbt_list,(GFunc)(&print_dbt_on_metadata_gstring),output);
  filename=g_strdup_printf("metadata.partial.%d",0);
  g_file_set_contents(filename, output->str,output->len,&gerror);
  stream_queue_push(NULL, filename);
  for(i=0;i<num_threads;i++){
    g_async_queue_push(initial_metadata_lock_queue, GINT_TO_POINTER(1));
  }

  i=1;
  GDateTime *prev_datetime = g_date_time_new_now_local();
  GDateTime *current_datetime = NULL;
  GTimeSpan diff=0;
  g_string_set_size(output,0);
  filename=NULL;
  dbt=g_async_queue_timeout_pop(metadata_partial_queue, METADATA_PARTIAL_INTERVAL * 1000000);
  while (metadata_partial_writer_alive){
    if (dbt != NULL && g_list_find(dbt_list, dbt)==NULL){
      dbt_list=g_list_append(dbt_list,dbt);
    }
    current_datetime = g_date_time_new_now_local();
    diff=g_date_time_difference(current_datetime,prev_datetime)/G_TIME_SPAN_SECOND;
    if (diff > METADATA_PARTIAL_INTERVAL){
      if (g_list_length(dbt_list) > 0){  
        filename=g_strdup_printf("metadata.partial.%d",i);
        i++;
        g_list_foreach(dbt_list,(GFunc)(&print_dbt_on_metadata_gstring),output);
        g_file_set_contents(filename,output->str,output->len,&gerror);
        stream_queue_push(NULL, filename);
        filename = NULL;
        g_string_set_size(output,0);
        dbt_list=NULL;        
      }
      g_date_time_unref(prev_datetime);
      prev_datetime=current_datetime;
    }else{
      g_date_time_unref(current_datetime);
    }
    dbt=g_async_queue_timeout_pop(metadata_partial_queue, METADATA_PARTIAL_INTERVAL * 1000000);
  }
  return NULL;
}

void initialize_stream(){
  initial_metadata_queue = g_async_queue_new();
  initial_metadata_lock_queue = g_async_queue_new();
  stream_queue = g_async_queue_new();
  metadata_partial_queue = g_async_queue_new();
  stream_thread = g_thread_create((GThreadFunc)process_stream, stream_queue, TRUE, NULL);
  metadata_partial_writer_thread = g_thread_create((GThreadFunc)metadata_partial_writer, NULL, TRUE, NULL);
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}
