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
#include "myloader_control_job.h"
#include "myloader_intermediate_queue.h"
#include "myloader_global.h"

GThread *stream_thread = NULL;
void *process_stream(struct configuration *stream_conf);

void initialize_stream (struct configuration *c){
  stream_thread = m_thread_new("myloader_stream",(GThreadFunc)process_stream, c, "Stream thread could not be created");
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}

size_t read_stream_line(char *buffer, int c_to_read){
    size_t bytes = fread(buffer, sizeof(char), c_to_read, stdin);
    return bytes;
}

void flush(char *buffer, int from, int to, FILE *file, guint *total_size){
//  char * tmp=g_strndup(&(buffer[from]),to-from);
//  g_message("Flushing data %d: %s",to-from, tmp);
  if (file){ 
    if (write_file(file,&(buffer[from]),to-from+1) != to-from+1) 
      g_critical("Error on writing");
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
  set_thread_name("STT");
  char * filename=NULL,*real_filename=NULL,* previous_filename=NULL;
  char *buffer=g_new(char, STREAM_BUFFER_SIZE);
  FILE *file=NULL;
  int pos=0,buffer_len=0;
  int diff=0, i=0, line_from=0, line_end=0; 
  int initial_pos=0;
  guint total_size=0;
  guint file_size_from_stream=0;
  GString *set_buffer=g_string_new_len("", 1000);
  g_string_set_size(set_buffer,0);
  gboolean writing_set=TRUE;
  gchar *database_name=g_strdup("sakila");
  gchar *table_name=NULL;
  for(i=0;i<STREAM_BUFFER_SIZE;i++){
    buffer[i]='\0';
  }
  gchar *new_filename,*new_real_filename,*kind=NULL;
  int num=0;
  while (TRUE){
    // Reads from stdin and fills the buffer from last position
read_more:
    buffer_len=fread(&(buffer[diff]), sizeof(char), STREAM_BUFFER_SIZE-1-diff, stdin)+diff;

    if (buffer_len==diff){
      // This menas that there is nothing else to read from stdin
      // so, we need to flush and EXIT.
      flush(buffer,0,buffer_len-1,file, &total_size);
      break;
    }
  
    if (!buffer_len)
      // We read nothing, we have to EXIT
      break;

    if (mysqldump){
      // We have data to process
      // we always start reading from the begining of the buffer
      pos=0;
      diff=0;
      while (pos < buffer_len){

        initial_pos=pos;
        if (buffer[pos] == '\n'){
          // new lines means new file header, new header of file content or new file content
          pos++;

          if (set_buffer->len > 0){
            // SET has been written
            writing_set=FALSE;
            if (file == NULL ){
              if (g_str_has_prefix(&(buffer[line_from]),"--")){
                // after writing the SET and when file is NULL, we should be reading the header of the file
                // we create a temporary filename 
                filename=g_strdup_printf("mydumper_tmp.table_%d.sql",num);
                num++;

                real_filename = g_build_filename(directory,filename,NULL);

                file = g_fopen(real_filename, "w");
                table_name=NULL;
                kind=NULL;

                flush(set_buffer->str,0,set_buffer->len -1,file, &total_size);
                flush(buffer,initial_pos,line_end-1,file, &total_size);
              }

            }else{
              // File content was being written, we might need to flush from initial_pos to line_from
              if (initial_pos < line_from ){
                // flushing from initial_pos to line_from - 1
                flush(buffer,initial_pos,line_from-1,file, &total_size);
              }
              fclose(file);
              file=NULL;
              if (table_name){
                new_filename=g_strdup_printf("%s%s%s%s.sql",database_name?database_name:"",table_name?".":"",table_name?table_name:"",kind?kind:"");
                new_real_filename=g_build_filename(directory,new_filename,NULL);
                g_rename(real_filename,new_real_filename);
              }else{
                new_filename=g_strdup(filename);
              }
              g_free(filename);
              filename=NULL;
              // sending previous file for processing
              intermediate_queue_new(new_filename);
              new_filename=NULL;
            }
          }
        }

        // we need to determine the right line_from
        if (initial_pos != pos) {
          line_from=pos-1;
        }else{
          line_from=pos;
        }

        // We process by line to correctly detect the new file
        while (pos < buffer_len && buffer[pos] !='\n' ){
          pos++;
        }

        line_end=pos;    
        // At this point we know:
        // - line_from
        // - line_end

        if (file){
          // is it a complete line?
          if (buffer[line_end] == '\n'){
            // this was a common line, flushing to disk
//            if (!g_str_has_prefix(&(buffer[line_from]),"--"))

            // Can we get relevant info?
            if (g_str_has_prefix(&(buffer[line_from]),"CREATE TABLE ") || g_str_has_prefix(&(buffer[line_from]),"/*!50001 CREATE VIEW") || g_str_has_prefix(&(buffer[line_from]),"/*!50001 VIEW")){
              g_free(table_name);
              gchar ** sp=g_strsplit(&(buffer[line_from]), "`", 3);
              table_name=g_strdup(sp[1]);
              g_strfreev(sp);
              kind=g_strdup("-schema");
            } else if (g_str_has_prefix(&(buffer[line_from]),"INSERT INTO ")){
              g_free(table_name);
              gchar ** sp=g_strsplit(&(buffer[line_from]), "`", 3);
              table_name=g_strdup(sp[1]);
              g_strfreev(sp);
              kind=g_strdup_printf(".000%d",num);
            } 

            char c=buffer[line_end];
            buffer[line_end]='\0';
            buffer[line_end]=c;
            flush(buffer,initial_pos,line_end,file, &total_size);
            pos++;
            continue;
          }else{
            // this was not a common line, flushing to disk anyways
            flush(buffer,initial_pos,line_end-1,file, &total_size);
            continue;      
          }
        }else{
          if (writing_set){
            // file was NULL, this must be the header of the mysqldump
            if (buffer[line_end] == '\n'){
              if (!g_str_has_prefix(&(buffer[line_from]),"--") && initial_pos!=line_end){
                g_string_append_len(set_buffer,&(buffer[initial_pos]),line_end-initial_pos+1);
              }
              pos++;        
            }else{
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
              goto read_more;
            }
          }else{
            pos++;
          }
        }
      }
    }else{
      //mydumper stream

      // We have data to process
      // we always start reading from the begining of the buffer
      pos=0;
      diff=0;
      while (pos < buffer_len){
        initial_pos=pos;
        while (buffer[pos] == '\n')
          // local new lines are ignored at this point, it will be written
          pos++;

        // we need to determine the right line_from
        if (initial_pos != pos) {
          line_from=pos-1;
        }else{
          line_from=pos;
        }

        // We process by line to correctly detect the header
        while (pos < buffer_len && buffer[pos] !='\n' ){
          pos++;
        }

        line_end=pos;

        // At this point we know:
        // - line_from
        // - line_end

        // is it a line?
        if (buffer[line_end] == '\n'){
          // As it is a line we need to detect if it is a header
          if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
            // header tag detected 
            if (file != NULL ){
              // Another file was being written, we might need to flush from initial_pos to line_from
              if (initial_pos < line_from ){
                // flushing from initial_pos to line_from - 1
                flush(buffer,initial_pos,line_from-1,file, &total_size);
              }
              if (!no_stream){
                // Content of the file are comming from stdin, it is not sharing the backup dir
                if (total_size < file_size_from_stream){
                  // The file size reported in the header is not the same that the amount of data written
                  // this means that the content of the file has the header tag
                  // we need to flush and continue 
                  flush(buffer,line_from,line_end-1,file, &total_size);
                  g_message("Different file size in %s. Should be: %d | Written: %d. But continuing", filename, file_size_from_stream, total_size);
                  continue;
                }else if (total_size > file_size_from_stream) {
                  // we wrote on the file more data than the file size reported in the header
                  m_critical("Different file size in %s. Should be: %d | Written: %d", filename, file_size_from_stream, total_size);
                }else{
                  // The amount of data written and the file size reported in the header match!
                  total_size=0;
                }
              }else{
                // we do not expect file size reported on the header in this case
                if (total_size>0)
                  m_critical("Different file size in %s. Should be: 0 | Written: %d", filename, total_size);
              }
              previous_filename=g_strdup(filename);
              g_free(filename);
            }
            // processing header
            gchar ** sp=g_strsplit(&(buffer[line_from]), " ", 3);
            filename=g_strdup(sp[1]);
            // detecting file size reported on the header
            file_size_from_stream = g_ascii_strtoull(sp[2], NULL, 10);
            g_strfreev(sp);

            // sending previous file for processing
            real_filename = g_build_filename(directory,filename,NULL);
            if (file)
              m_close(file);
            if (previous_filename){
              intermediate_queue_new(previous_filename);
              previous_filename=NULL;
            }
            if (g_file_test(real_filename, G_FILE_TEST_EXISTS)){
              if (no_stream){
                 if (total_size>0)
                   m_critical("Different file size in %s. Should be: 0 | Written: %d", filename, total_size);
                 intermediate_queue_new(filename);
              }else{
                g_warning("Stream Thread: File %s exists in datadir, we are not replacing", real_filename);
                file = NULL;
              }
            }else{
              if (no_stream){
                m_critical("File %s not found in backup dir when using NO_STREAM.", filename);
              }
              file = g_fopen(real_filename, "w");
              m_close=(void *) &fclose;
            }
            if (!has_mydumper_suffix(filename)){
              g_debug("Not a mydumper file: %s", filename);
            }
            pos++;
            continue;
          }
          // this was a common line, flushing to disk
          flush(buffer,initial_pos,line_end-1,file, &total_size);
          continue;
        }else{
          // It reached end of buffer
          //
          // this data doesn't end with new line
          // but we need to check if starts with --

          if (line_end-line_from >= 4){
            // In the buffer remains more than 4 chars
            if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
              // It could be a header, so we copied to the begining of the buffer
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
              // diff remains set to do not overwrite the buffer
            }else{
              // it is safe to flush it all the content of the buffer
              flush(buffer,initial_pos,line_end-1,file, &total_size);
              diff=0;
              // the buffer will start empty
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
            }
            g_free(tmp);
          }
          goto read_more;

        }


        g_error("This should not happen");
      }
    }
  }
  if (mysqldump){
    if (file)
      fclose(file);
    if (!no_stream && filename)
      intermediate_queue_new(g_strdup(filename));
    g_free(filename);
  }else{
    if (file) 
      m_close(file);
    if (!no_stream && filename)
      intermediate_queue_new(g_strdup(filename));
    g_free(filename);
  }
  intermediate_queue_end();
  guint n=0;
  for (n = 0; n < num_threads ; n++) {
//    g_async_queue_push(stream_conf->data_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->post_table_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->post_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(stream_conf->view_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
  }
  return NULL;
}

