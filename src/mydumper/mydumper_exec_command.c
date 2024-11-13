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

extern GAsyncQueue *stream_queue;

GThread **exec_command_thread = NULL;
guint num_exec_threads = 4;

GHashTable* pid_file_table=NULL;

void exec_this_command(gchar * bin,gchar **c_arg,gchar *filename){
  int childpid=vfork();
  if(!childpid){
    g_hash_table_insert(pid_file_table,g_strdup_printf("%d",getpid()),g_strdup(filename));
    execv(bin,c_arg);
  }else{
    int wstatus;
    int waitchildpid=wait(&wstatus);
    // TODO: do we want to keep the file depending og the wstatus ??
    if (no_delete == FALSE){
      gchar *_key=g_strdup_printf("%d",waitchildpid);
      filename=g_hash_table_lookup(pid_file_table, _key);
      remove(filename);
      g_hash_table_remove(pid_file_table, _key);
    }
  }
}


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
//    char *used_filemame=g_path_get_basename(filename);
    iter=filename_pos;
    while (iter!=NULL){
      c_arg[(*((guint *)(iter->data)))]=filename;
      iter=iter->next;
    } 
    exec_this_command(bin,c_arg,filename);
  }
  return NULL;
}

/*
static GOptionEntry exec_entries[] = {
    {"exec-threads", 0, 0, G_OPTION_ARG_INT, &num_exec_threads,
     "Amount of threads to use with --exec", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

void load_exec_entries(GOptionGroup *main_group){
  g_option_group_add_entries(main_group, exec_entries);
}
*/

void initialize_exec_command(){
  g_warning("initialize_exec_command: Started");
  stream_queue = g_async_queue_new();
  exec_command_thread=g_new(GThread * , num_exec_threads) ;
  guint i;
  pid_file_table=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free ); 
  for(i=0;i<num_exec_threads;i++){
    exec_command_thread[i]=g_thread_create((GThreadFunc)process_exec_command, stream_queue, TRUE, NULL);
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
