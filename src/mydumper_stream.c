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
#include "mydumper_global.h"
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
//  gboolean not_compressed = FALSE;
//  guint sz=0;
  ssize_t len=0;
  GDateTime *datetime;
  for(;;){
    filename=(char *)g_async_queue_pop(stream_queue);
    if (strlen(filename) == 0){
      break;
    }
    char *used_filemame=g_path_get_basename(filename);
    len=write(fileno(stdout), "\n-- ", 4);
    len=write(fileno(stdout), used_filemame, strlen(used_filemame));
    len=write(fileno(stdout), " ", 1);
    total_size+=5;
    total_size+=strlen(used_filemame);
    free(used_filemame);

    if (no_stream == FALSE){
//      g_message("Opening: %s",filename);
      f=g_fopen(filename,"r");
      if (!f){
        m_error("File failed to open: %s",filename);
      }else{
        if (!f){
          g_critical("File failed to open: %s. Reetrying",filename);
          f=g_fopen(filename,"r");
          if (!f){
            m_error("File failed to open: %s. Cancelling",filename);
            exit(EXIT_FAILURE);
          }
        }
        struct stat st;
        int fd = fileno(f);
        fstat(fd, &st);
        off_t size = st.st_size;
        
        g_message("File size of %s is %"G_GINT64_FORMAT, filename, size);
        gchar *c = g_strdup_printf("%"G_GINT64_FORMAT,size);
        len=write(fileno(stdout), c, strlen(c));
        len=write(fileno(stdout), "\n", 1);
        total_size+=strlen(c) + 1;
        g_free(c);

        guint total_len=0;
        GDateTime *start_time=g_date_time_new_now_local();
        buflen = read(fileno(f), buf, STREAM_BUFFER_SIZE);
        while(buflen > 0){
          len=write(fileno(stdout), buf, buflen);
          total_len=total_len + buflen;
          if (len != buflen)
            m_error("Stream failed during transmition of file: %s",filename);
          buflen = read(fileno(f), buf, STREAM_BUFFER_SIZE);
        }
        g_message("Bytes readed of %s is %d", filename, total_len);
        datetime = g_date_time_new_now_local();
        diff=g_date_time_difference(datetime,start_time)/G_TIME_SPAN_SECOND;
        g_date_time_unref(start_time);
        total_diff=g_date_time_difference(datetime,total_start_time)/G_TIME_SPAN_SECOND;
        g_date_time_unref(datetime);
        if (diff > 0){
          g_message("File %s transferred in %" G_GINT64_FORMAT " seconds at %" G_GINT64_FORMAT " MB/s | Global: %" G_GINT64_FORMAT " MB/s",filename,diff,total_len/1024/1024/diff,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }else{
          g_message("File %s transferred | Global: %" G_GINT64_FORMAT "MB/s",filename,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }
        total_size+=total_len;
        fclose(f);
      }
    }
    if (no_delete == FALSE){
      remove(filename);
    }
    g_free(filename);
  }
  g_free(filename);
  datetime = g_date_time_new_now_local();
  total_diff=g_date_time_difference(datetime,total_start_time)/G_TIME_SPAN_SECOND;
  g_date_time_unref(total_start_time);
  g_date_time_unref(datetime);
  g_message("All data transferred was %" G_GINT64_FORMAT " at a rate of %" G_GINT64_FORMAT " MB/s",total_size,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
  return NULL;
}

void initialize_stream(){
  stream_queue = g_async_queue_new();
  stream_thread = g_thread_create((GThreadFunc)process_stream, stream_queue, TRUE, NULL);
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}
