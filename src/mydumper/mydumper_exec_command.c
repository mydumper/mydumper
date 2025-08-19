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
#include <stdlib.h>
#include <glib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>

#include "mydumper.h"
#include "mydumper_global.h"
#include "mydumper_start_dump.h"
#include "mydumper_stream.h"
#include "mydumper_file_handler.h"

extern GAsyncQueue *stream_queue;

GAsyncQueue *exec_queue;
GThread **exec_command_thread = NULL;
guint num_exec_threads = 4;
gchar * global_bin = NULL;
GHashTable* pid_file_table=NULL;
GMutex *exec_mutex=NULL;
void exec_this_command(gchar **c_arg,struct filename_queue_element * sqe){
  int childpid=vfork();
  if(!childpid){
    int fd=0;
    for (fd=3; fd<256; fd++) (void) close(fd);
    execv(global_bin,c_arg);
  }else{
    gchar *_key=NULL;
    g_mutex_lock(exec_mutex);
    _key=g_strdup_printf("%d",childpid);
    g_hash_table_insert(pid_file_table,g_strdup(_key),sqe);
    g_mutex_unlock(exec_mutex);
    g_free(_key);
    int wstatus;
    int waitchildpid=wait(&wstatus);
    // TODO: do we want to keep the file depending og the wstatus ??
    if (no_delete == FALSE){
      _key=g_strdup_printf("%d",waitchildpid);
      g_mutex_lock(exec_mutex);
      struct filename_queue_element *sqe2=g_hash_table_lookup(pid_file_table, g_strdup(_key));
      g_mutex_unlock(exec_mutex);
      if (sqe2!=NULL){
        remove(sqe2->filename);
        if (g_hash_table_contains(pid_file_table, _key))
          g_hash_table_remove(pid_file_table, _key);
        if (sqe2->done)
          g_async_queue_push(sqe2->done, GINT_TO_POINTER(1));
      }else{
        g_error("pid not found: %s", _key);
      }
      g_free(_key);
    }
  }
}


void exec_queue_push(struct db_table *dbt, gchar *filename){
  GAsyncQueue *done = g_async_queue_new();
  g_async_queue_push(exec_queue, new_filename_queue_element(dbt,filename,done));
  g_async_queue_pop(done);
  g_async_queue_unref(done);
}

void *process_exec_command(void *data){
  (void)data;
  char * filename=NULL;
  gchar *space=g_strstr_len(exec_command,-1," ");
  gchar ** arguments=g_strsplit(space," ", 0);
  gchar ** volatile c_arg=NULL;
  guint i=0;
  GList *filename_pos=NULL;
  GList *iter;
  c_arg=g_strdupv(arguments);
  if (strlen(c_arg[g_strv_length(c_arg)-1])==0)
    c_arg[g_strv_length(c_arg)-1]=NULL;
  for(i=0; i<g_strv_length(c_arg); i++){
    if (g_strcmp0(c_arg[i],"FILENAME") == 0){
      int *c=g_new(int, 1);
      *c=i;
      filename_pos=g_list_prepend(filename_pos,c);
    }
  }

  if (!filename_pos)
    g_warning("Common use case requires FILENAME on --exec.");

  for(;;){
    struct filename_queue_element * sqe=g_async_queue_pop(exec_queue);
    filename=sqe->filename;
    if (strlen(filename) == 0){
      break;
    }
    iter=filename_pos;
    while (iter!=NULL){
      c_arg[(*((guint *)(iter->data)))]=filename;
      iter=iter->next;
    }
    exec_this_command(c_arg,sqe);
  }
  return NULL;
}

void initialize_exec_command(){
  exec_queue = g_async_queue_new();
  exec_mutex=g_mutex_new();
  exec_command_thread=g_new(GThread * , num_exec_threads) ;
  gchar *space=g_strstr_len(exec_command,-1," ");
  guint len = strlen(exec_command) - strlen(space);
  while (len==0){
    exec_command++;
    space=g_strstr_len(exec_command,-1," ");
    len = strlen(exec_command) - strlen(space);
  }
  global_bin=g_strndup(exec_command,len);
  if (!g_file_test(global_bin, G_FILE_TEST_EXISTS)){
    g_error("Command not found: %s", global_bin);
  }
  guint i;
  pid_file_table=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free ); 
  for(i=0;i<num_exec_threads;i++){
    exec_command_thread[i]=m_thread_new("exec_command", (GThreadFunc)process_exec_command, stream_queue, "Exec command thread could not be created");
  }
}

void wait_exec_command_to_finish(){
  guint i;
  for(i=0;i<num_exec_threads;i++){
    g_async_queue_push(stream_queue, g_strdup(""));
  }
  for(i=0;i<num_exec_threads;i++){
    g_thread_join(exec_command_thread[i]);
  }
}
