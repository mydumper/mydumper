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
#include <unistd.h>
#include "common.h"
#include <sys/wait.h>
#include <errno.h>

extern int errno;

extern FILE * (*m_open)(const char *filename, const char *);
extern gchar *compress_extension;
extern GAsyncQueue *stream_queue;
extern gboolean no_delete;
extern guint num_threads;
GThread **exec_command_thread = NULL;
extern gchar *exec_command;

void *process_exec_command(void *data){
  (void)data;
  gchar *space=g_strstr_len(exec_command,-1," ");
  guint len = strlen(exec_command) - strlen(space);
  char * filename=NULL;
  char * bin=g_strndup(exec_command,len);
  gchar ** arguments=g_strsplit(space," ", 0);
  gchar ** volatile c_arg=NULL;
  guint i=0;
  GList *filename_pos=NULL;
  GList *iter;
  c_arg=g_strdupv(arguments);
  for(i=0; i<g_strv_length(c_arg); i++){
    if (g_strcmp0(c_arg[i],"FILENAME") == 0){
      int *c=g_new(int, 1);
      *c=i;
      filename_pos=g_list_prepend(filename_pos,c);
    }
  }

  for(;;){
    filename=(char *)g_async_queue_pop(stream_queue);
    if (strlen(filename) == 0){
      break;
    }
    char *used_filemame=g_path_get_basename(filename);
    iter=filename_pos;
    while (iter!=NULL){
      c_arg[(*((guint *)(iter->data)))]=filename;
      iter=iter->next;
    } 
    int childpid=vfork();
    if(!childpid)
      i=execv(bin,c_arg);
    wait(&childpid);
    free(used_filemame);
    if (no_delete == FALSE)
      remove(filename);
  }
  return NULL;
}

void initialize_exec_command(){
  g_message("Initializing Execcommand");
  stream_queue = g_async_queue_new();
  exec_command_thread=g_new(GThread * , num_threads) ;
  guint i;
  for(i=0;i<num_threads;i++){
    exec_command_thread[i]=g_thread_create((GThreadFunc)process_exec_command, stream_queue, TRUE, NULL);
  }
}

void wait_exec_command_to_finish(){
  guint i;
  for(i=0;i<num_threads;i++){
    g_async_queue_push(stream_queue, g_strdup(""));
  }
  for(i=0;i<num_threads;i++){
    g_thread_join(exec_command_thread[i]);
  }
}
