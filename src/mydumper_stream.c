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
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include <stdlib.h>
#include <glib.h>
#include <stdio.h>
#include "common.h"

extern FILE * (*m_open)(const char *filename, const char *);
extern int (*m_close)(void *file);
extern gchar *compress_extension;
extern GAsyncQueue *stream_queue;
extern gboolean no_delete;

GThread *stream_thread = NULL;

void *process_stream(void *data){
  (void)data;
  char * filename=NULL;
  FILE * f=NULL;
  char buf[STREAM_BUFFER_SIZE];
  int buflen;
  guint64 total_size=0;
  GDateTime *total_start_time=g_date_time_new_now_local();
  GTimeSpan diff=0,total_diff=0;
  gboolean not_compressed = FALSE;
  guint sz=0;
  ssize_t len=0;
  for(;;){
    filename=(char *)g_async_queue_pop(stream_queue);
    if (strlen(filename) == 0){
      break;
    }
    char *used_filemame=g_path_get_basename(filename);
    len=write(fileno(stdout), "\n-- ", 4);
    len=write(fileno(stdout), used_filemame, strlen(used_filemame));
    len=write(fileno(stdout), "\n", 1);
    total_size+=5;
    total_size+=strlen(used_filemame);
    free(used_filemame);
    g_message("Opening: %s",filename);
    f=m_open(filename,"r");
    not_compressed= g_str_has_suffix(filename, compress_extension);
    if (not_compressed)
      f=g_fopen(filename,"r");
    else
      f=m_open(filename,"r");
    if (!f){
      g_error("File failed to open: %s",filename);
    }else{
      if (not_compressed){
        fseek(f, 0, SEEK_END);
        sz = ftell(f);
        m_close(f);
        f=g_fopen(filename,"r");
      }
      guint total_len=0;
      GDateTime *start_time=g_date_time_new_now_local();
      buflen = not_compressed ? read(fileno(f), buf, STREAM_BUFFER_SIZE): gzread((gzFile)f, buf, STREAM_BUFFER_SIZE);
      while(buflen > 0){
        len=write(fileno(stdout), buf, buflen);
        total_len=total_len + buflen;
        if (len != buflen){
          g_critical("Stream failed during transmition of file: %s",filename);
          exit(EXIT_FAILURE);
        }
        buflen = not_compressed ? read(fileno(f), buf, STREAM_BUFFER_SIZE): gzread((gzFile)f, buf, STREAM_BUFFER_SIZE);
      }
      if (not_compressed && total_len != sz){
        g_critical("Data transmited for %s doesn't match. File size: %d Transmited: %d",filename,sz,total_len);
        exit(EXIT_FAILURE);
      }else{
        diff=g_date_time_difference(g_date_time_new_now_local(),start_time)/G_TIME_SPAN_SECOND;
        total_diff=g_date_time_difference(g_date_time_new_now_local(),total_start_time)/G_TIME_SPAN_SECOND;
        if (diff > 0){
          g_message("File %s transfered in %ld seconds at %ld MB/s | Global: %ld MB/s",filename,diff,sz/1024/1024/diff,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }else{
          g_message("File %s transfered | Global: %ld MB/s",filename,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }
        total_size+=sz;
      }
      m_close(f);
    }
    if (no_delete == FALSE){
      remove(filename);
    }
  }
  total_diff=g_date_time_difference(g_date_time_new_now_local(),total_start_time)/G_TIME_SPAN_SECOND;
  g_message("All data transfered was %ld at a rate of %ld MB/s",total_size,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
  return NULL;
}

void initialize_stream(){
  stream_queue = g_async_queue_new();
  stream_thread = g_thread_create((GThreadFunc)process_stream, stream_queue, TRUE, NULL);
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}
