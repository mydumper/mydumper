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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include "string.h"
#include <stdlib.h>
#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <pcre.h>
#include "regex.h"
#include <errno.h>
#include "server_detect.h"
#include "mydumper_global.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include "mydumper_common.h"
//#include <sys/wait.h>
#include "mydumper_start_dump.h"
#include "mydumper_stream.h"
#include <sys/wait.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <sys/file.h>

GAsyncQueue *close_file_queue=NULL;
GAsyncQueue *available_pids=NULL;
GHashTable *fifo_hash=NULL;
//GHashTable *fifo_hash_by_pid=NULL;
GMutex *fifo_table_mutex=NULL;
GMutex *pipe_creation=NULL;
GThread * cft = NULL;
guint open_pipe=0;

int (*m_close)(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt) = NULL;

// FILE open/close

int m_open_file(char **filename, const char *type ){
  (void) type;
  return open(*filename, O_CREAT|O_WRONLY|O_TRUNC, 0660 );
}

int m_close_file(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt){
  int r=close(file);
  if (size > 0){
    if (stream) stream_queue_push(dbt, g_strdup(filename));
  }else if (!build_empty_files){
    if (remove(filename)) {
      g_warning("Thread %d: Failed to remove empty file : %s", thread_id, filename);
    }else{
      g_debug("Thread %d: File removed: %s", thread_id, filename);
    }
  }
  return r;
}

// 

void close_file_queue_push(struct fifo *f){
  g_async_queue_push(close_file_queue, f);
  if (f->child_pid>0){
  int status;
  int pid;
  gboolean b=TRUE;
  do {
    do {
      g_mutex_lock(pipe_creation);
      pid=waitpid(f->child_pid, &status, WNOHANG);
      g_mutex_unlock(pipe_creation);
      if (pid > 0){
        b=FALSE;
        break;
      }else if (pid == -1 && errno == ECHILD){
        b=FALSE;
        break;
      }
    } while (pid == -1 && errno == EINTR); 
  }while (b);
//g_message("close_file_queue_push:: %s child pid %d ended with %d and error: %d | EINTR=%d ECHILD=%d EINVAL=%d | b=%d",f->filename?f->filename:"NOFILENAME", f->child_pid, pid , errno, EINTR, ECHILD, EINVAL, b);
    f->error_number=errno;
    g_mutex_unlock(f->out_mutex);
  }
}

void wait_close_files(){
  struct fifo f;
  f.gpid=-10;
  f.child_pid=-10;
  f.filename=NULL;
  close_file_queue_push(&f);
  g_thread_join(cft);
}

void release_pid(){
  g_async_queue_push(available_pids, GINT_TO_POINTER(1));
}

int execute_file_per_thread( int p_in[2], int out){
  int childpid=fork();
  if(!childpid){
    dup2(p_in[0], STDIN_FILENO);
    close(p_in[1]);
    dup2(out, STDOUT_FILENO);
    close(out);
    int fd=3;
    for (fd=3; fd<256; fd++) (void) close(fd);
    execv(exec_per_thread_cmd[0],exec_per_thread_cmd);
  }
  return childpid;
}

// PIPE open/close

// filename must never use the compression extension. .fifo files should be deprecated
int m_open_pipe(gchar **filename, const char *type){
  (void)type;
  g_atomic_int_inc(&open_pipe);

  gchar *new_filename = g_strdup_printf("%s%s", *filename, exec_per_thread_extension);
  (void)type;
  struct fifo *f=NULL;

  g_mutex_lock(fifo_table_mutex);
  f=g_hash_table_lookup(fifo_hash,*filename);
  g_mutex_unlock(fifo_table_mutex);
  if (f){
    g_error("file already open: %s", *filename);
  }
  f=g_new0(struct fifo, 1);
  f->out_mutex=g_mutex_new();
  g_mutex_lock(f->out_mutex);
  f->fdout = open(new_filename, O_CREAT|O_WRONLY|O_TRUNC, 0660);
  if (!f->fdout){
    g_error("opening file: %s", new_filename);
  }
  g_async_queue_pop(available_pids);
  f->queue = g_async_queue_new();
  f->filename=g_strdup(*filename);
  f->stdout_filename=new_filename;
  guint e=0;
  g_mutex_lock(pipe_creation);
  gint status=pipe(f->pipe);
  if (status != 0){
    g_error("Not able to create pipe (%d)", e);
  }
  
  f->child_pid=execute_file_per_thread(f->pipe, f->fdout);

  g_mutex_unlock(pipe_creation);
  g_mutex_lock(fifo_table_mutex);
  g_hash_table_insert(fifo_hash,f->filename,f);
  g_mutex_unlock(fifo_table_mutex);
  return f->pipe[1];
}

int m_close_pipe(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt){
  release_pid();
  (void)file;
  (void)thread_id;
  g_mutex_lock(fifo_table_mutex);
  struct fifo *f=g_hash_table_lookup(fifo_hash,filename);
  g_mutex_unlock(fifo_table_mutex);
  if (f){
    f->size=size;
    f->dbt=dbt;
    close_file_queue_push(f);
    return 0;
  }else{
    g_warning("pipe %s not closed", filename);
  }
  return 1;
}

// close_file_thread

void final_step_close_file(guint thread_id, gchar *filename, struct fifo *f, float size, struct db_table * dbt) {
  if (size > 0){
    if (stream) stream_queue_push(dbt,g_strdup(f->stdout_filename));
  }else if (!build_empty_files){
    if (remove(f->stdout_filename)) {
      g_warning("Thread %d: Failed to remove empty file : %s", thread_id, f->stdout_filename);
    }else{
      g_debug("Thread %d: File removed: %s", thread_id, filename);
    }
  }
}

void * close_file_thread(void *data){
  (void)data;
  struct fifo *f=NULL;
  for (;;){
    f=g_async_queue_pop(close_file_queue);
    if (f->gpid == -10)
      break;
    g_mutex_lock(pipe_creation);
    close(f->pipe[1]);
    close(f->pipe[0]);
    g_mutex_unlock(pipe_creation);
    g_mutex_lock(f->out_mutex);
    if (f->error_number==EAGAIN){
      usleep(1000);
    }
    if (fsync(f->fdout))
      g_error("while syncing file %s (%d)",f->filename, errno);
    close(f->fdout);

    release_pid();
    final_step_close_file(0, f->filename, f, f->size, f->dbt);
    g_atomic_int_dec_and_test(&open_pipe);
 }
  return NULL;
}

void initialize_file_handler(gboolean is_pipe){
  if (is_pipe){
    m_open  = &m_open_pipe;
    m_close = &m_close_pipe;
  }else{
    m_open  = &m_open_file;
    m_close = &m_close_file;
  }
  available_pids = g_async_queue_new();
  close_file_queue=g_async_queue_new();
  guint i=0;
  for (i=0; i < (num_threads * 2); i++){
    release_pid();
  }
  pipe_creation = g_mutex_new();
  fifo_hash=g_hash_table_new(g_str_hash, g_str_equal);
  fifo_table_mutex = g_mutex_new();

  cft=g_thread_create((GThreadFunc)close_file_thread, NULL, TRUE, NULL);
}
