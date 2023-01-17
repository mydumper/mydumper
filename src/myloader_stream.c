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
#include "myloader_control_job.h"
#include "myloader_intermediate_queue.h"

extern gchar *directory;
extern guint num_threads;
extern int (*m_close)(void *file);
extern int (*m_write)(FILE * file, const char * buff, int len);

GThread *stream_thread = NULL;

void *process_stream();

void initialize_stream (struct configuration *c){
  stream_thread = g_thread_create((GThreadFunc)process_stream, c, TRUE, NULL);
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}

int read_stream_line(char *buffer, gboolean *eof,FILE *file,int c_to_read){
    size_t bytes = fread(buffer, sizeof(char), c_to_read, stdin);
    if( !bytes ){
      if (file != NULL && feof(file)){
        *eof = TRUE;
        buffer[0] = '\0';
        m_close(file);
      }
    }
    return bytes;
}

void flush(char *buffer, int from, int to, FILE *file){
  if (file) 
    if (m_write(file,&(buffer[from]),to-from+1) != to-from+1) 
      g_critical("error on writing");
}


gboolean has_mydumper_suffix(gchar *line){
  return
    g_str_has_suffix(line,".dat") ||
    g_str_has_suffix(line,".dat.gz") ||
    g_str_has_suffix(line,".dat.zst") ||
    g_str_has_suffix(line,".sql") ||
    g_str_has_suffix(line,".sql.gz") ||
    g_str_has_suffix(line,".sql.zst") ||
    g_str_has_suffix(line,"metadata") ||
    g_str_has_suffix(line,"-checksum") ||
    g_str_has_suffix(line,"-checksum.gz") ||
    g_str_has_suffix(line,"-checksum.zst");
}

void *process_stream(struct configuration *stream_conf){
  char * filename=NULL,*real_filename=NULL,* previous_filename=NULL;
  char *buffer=g_new(char, STREAM_BUFFER_SIZE);
  FILE *file=NULL;
  gboolean eof=FALSE;
  int pos=0,buffer_len=0;
  int diff=0, i=0, line_from=0, line_end=0, last_pos=0, next_line_from=0;
  for(i=0;i<STREAM_BUFFER_SIZE;i++){
    buffer[i]='\0';
  }
  do {
read_more:    buffer_len=read_stream_line(&(buffer[diff]),&eof,file,STREAM_BUFFER_SIZE-1-diff)+diff;

    next_line_from=0;
    pos=0;
    diff=0;
//g_message("Buffer_len %d", buffer_len);
    if (!buffer_len){ 
      break;
    }else{
      while (pos < buffer_len){
        if (buffer[pos] =='\n')
          pos++;
        line_from=next_line_from;
        while (pos < buffer_len && buffer[pos] !='\n' ){
          pos++;
        }
        last_pos=pos;
        line_end=pos-1;
        // Is a header?
        if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
          if (buffer[last_pos] == '\n'){
            previous_filename=g_strdup(filename);
            g_free(filename);
            gchar a=buffer[last_pos-(line_from+4)];
            buffer[last_pos-(line_from)]='\0';
//g_message("Pos: %d Line_end: %d line_from %d last_pos: %d next_line_from: %d", pos,line_end, line_from, last_pos, next_line_from);
//            if (line_from==last_pos)
//m_error("Pos: %d Line_end: %d line_from %d last_pos: %d next_line_from: %d", pos,line_end, line_from, last_pos, next_line_from);
            filename=g_strndup(&(buffer[line_from+4]),last_pos-(line_from+4));
            buffer[last_pos-(line_from+4)]=a;
            real_filename = g_build_filename(directory,filename,NULL);
            if (has_mydumper_suffix(filename)){
              if (file){
                m_close(file);
              }
              if (previous_filename)
                intermediate_queue_new(previous_filename); 
              if (g_file_test(real_filename, G_FILE_TEST_EXISTS)){
                g_debug("Stream Thread: File exists in datadir: %s", real_filename);
                last_pos++;
                file = NULL;
              }else{
                file = g_fopen(real_filename, "w");
                m_write=(void *)&write_file;
                m_close=(void *) &fclose;
              }
            }else{
              g_debug("Not a mydumper file: %s", filename);
            }
            next_line_from=last_pos+1;
            continue;
          }

          if (pos == buffer_len){

            diff=buffer_len-line_from;
            g_strlcpy(buffer,&(buffer[line_from]),buffer_len-line_from+2);
            goto read_more;
          }
        }
        flush(buffer,line_from,line_end,file);
        next_line_from=last_pos;
      }
    }
  } while (eof == FALSE);
  if (file) 
    m_close(file);
  if (filename)
    intermediate_queue_new(g_strdup(filename));
  g_free(filename);
  intermediate_queue_end();
  guint n=0;
  for (n = 0; n < num_threads ; n++) {
//    g_async_queue_push(stream_conf->data_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->post_table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->view_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  return NULL;
}

