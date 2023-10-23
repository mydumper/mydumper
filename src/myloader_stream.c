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
#include "myloader_global.h"

GThread *stream_thread = NULL;
void *process_stream();

void initialize_stream (struct configuration *c){
  stream_thread = g_thread_create((GThreadFunc)process_stream, c, TRUE, NULL);
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}

size_t read_stream_line(char *buffer, gboolean *eof,FILE *file,int c_to_read){
(void) file;
(void) eof;
    size_t bytes = fread(buffer, sizeof(char), c_to_read, stdin);
/*    if (file != NULL && feof(stdin)){
      g_message("EOF!!");
      *eof = TRUE;
      buffer[0] = '\0';
//      m_close(file);
      return 0;
    }
*/
    return bytes;
}

void flush(char *buffer, int from, int to, FILE *file, guint *total_size){
//  char * tmp=g_strndup(&(buffer[from]),to-from);
//  g_message("Flushing data %d: %s",to-from, tmp);
  if (file){ 
    if (write_file(file,&(buffer[from]),to-from+1) != to-from+1) 
      g_critical("error on writing");
    *total_size=*total_size+(to-from)+1;
  }
}

gboolean has_mydumper_suffix(gchar *line){
  return
    m_filename_has_suffix(line,".dat") ||
    m_filename_has_suffix(line,".sql") ||
    g_strstr_len(line,-1,"metadata.partial") ||
    g_str_has_prefix(line,"metadata");
}

void *process_stream(struct configuration *stream_conf){
  char * filename=NULL,*real_filename=NULL,* previous_filename=NULL;
  char *buffer=g_new(char, STREAM_BUFFER_SIZE);
  FILE *file=NULL;
  gboolean eof=FALSE;
  int pos=0,buffer_len=0;
  int diff=0, i=0, line_from=0, line_end=0; 
  int initial_pos=0;
  guint total_size=0;
  guint b=0;
  for(i=0;i<STREAM_BUFFER_SIZE;i++){
    buffer[i]='\0';
  }
  do {
read_more:    buffer_len=read_stream_line(&(buffer[diff]),&eof,file,STREAM_BUFFER_SIZE-1-diff)+diff;
//    g_message("Reading more byte(%d) with diff %d : |%s|\nWith buffer: |%s|", buffer_len, diff, &(buffer[diff]), buffer);
    if (buffer_len==diff){
//      g_message("Char in byte1 = %d %d == %d", buffer[0], buffer_len, diff);
//      g_message("ENDING Writing missing data ");
      flush(buffer,0,buffer_len-1,file, &total_size);
      break;
    }
    pos=0;
    diff=0;
    if (!buffer_len){ 
      break;
    }else{
      while (pos < buffer_len && !eof ){
        initial_pos=pos;
        while (buffer[pos] == '\n')
          pos++;
//        g_message("POS: %d", pos);
        if (initial_pos != pos) {
          line_from=pos-1;
        }else{
          line_from=pos;
        }

        // Reading line
//        g_message("Reading line initial_pos=%d pos=%d of buffer_len=%d is \\0? %d eof=%d", initial_pos, pos, buffer_len, buffer[pos]=='\0', eof);
        while (pos < buffer_len && buffer[pos] !='\n' ){
          pos++;
        }
//	g_message("DATA: %d %d  %s",line_from, pos, &(buffer[line_from]));
        line_end=pos;



        // is it a line?
        if (buffer[line_end] == '\n'){
//          g_message("Processing line from: %d to: %d", line_from, line_end);
          if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
//            g_message("FOUND -- at: %d", line_from);
            if (file != NULL ){
              if (initial_pos < line_from ){
//                g_message("Writing missing new_lines, initial_pos: %d  line_from: %d", initial_pos, line_from );
                flush(buffer,initial_pos,line_from-1,file, &total_size);
              }

              if (total_size < b){
                g_message("Different file size in %s. Should be: %d | Written: %d", filename, b, total_size);
                total_size=0;
              }else if (total_size > b) {
                m_critical("Different file size in %s. Should be: %d | Written: %d", filename, b, total_size);
              }else{
                total_size=0;
              }
              previous_filename=g_strdup(filename);
              g_free(filename);
            }
//g_message("Pos: %d Line_end: %d line_from %d last_pos: %d next_line_from: %d filename_sapce: %d", pos,line_end, line_from, last_pos, next_line_from, GPOINTER_TO_INT(filename_space->next->data));
            gchar ** sp=g_strsplit(&(buffer[line_from]), " ", 3);
            filename=g_strdup(sp[1]);
            b = g_ascii_strtoull(sp[2], NULL, 10);
            g_strfreev(sp);
//          g_message("Raaded Size from file is %d", b);
            real_filename = g_build_filename(directory,filename,NULL);
//            g_message("FILENAME: %s", filename);
            if (file){
              m_close(file);
            }
            if (previous_filename){
              intermediate_queue_new(previous_filename);
	      previous_filename=NULL;
	    }
            if (g_file_test(real_filename, G_FILE_TEST_EXISTS)){
              g_warning("Stream Thread: File %s exists in datadir, we are not replacing", real_filename);
              file = NULL;
            }else{
              file = g_fopen(real_filename, "w");
//              m_write=(void *)&write_file;
              m_close=(void *) &fclose;
            }
            if (!has_mydumper_suffix(filename)){
              g_debug("Not a mydumper file: %s", filename);
            }
            pos++;
//            g_message("Seting next_line_from=%d and pos=%d",next_line_from, pos);
            continue;
          }
          flush(buffer,initial_pos,line_end-1,file, &total_size);
          continue;
        }else{
          // It reached end of buffer
          //
          // this data doesn't end with new line
          // but we need to check if starts with --

          if (line_end-line_from >= 4){
            if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
            }else{
              flush(buffer,initial_pos,line_end-1,file, &total_size);
              diff=0;
            }
          }else{
            gchar *tmp=g_strndup(&(buffer[line_from]),line_end-line_from >= 4 ? 4:line_end-line_from);
//          g_message("TMP: |%s| %c", tmp, tmp[0]);
            if  (strlen(tmp)>=1 && g_strstr_len("\n-- ", -1 ,tmp) != NULL ){
              // we need to move to the begining of the buffer and reprocess 
//              g_message("Coping data %d %d: %s", initial_pos,  buffer_len, &(buffer[initial_pos])  );
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
//              g_message("After copy data: %s | new len should be: %d", buffer , diff);
            }else{
              flush(buffer,initial_pos,line_end-1,file, &total_size);
              diff=0;
//  g_message("Checking for partial filename failed");
            }
            g_free(tmp);
          }
          goto read_more;

        }


        g_error("This should not happen");
      }
    }
  } while (eof == 0);
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

