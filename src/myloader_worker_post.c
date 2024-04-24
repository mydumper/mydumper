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
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "myloader_stream.h"
#include "common.h"
#include "server_detect.h"
#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
//#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "connection.h"
#include <errno.h>
#include "myloader_global.h"
#include "myloader_worker_post.h"


GThread **post_threads = NULL;
struct thread_data *post_td = NULL;
void *worker_post_thread(struct thread_data *td);
GMutex *sync_mutex;
GMutex *sync_mutex1;
GMutex *sync_mutex2;
guint sync_threads_remaining;
guint sync_threads_remaining1;
guint sync_threads_remaining2;

void initialize_post_loding_threads(struct configuration *conf){
  guint n=0;
//  post_mutex = g_mutex_new();
  post_threads = g_new(GThread *, max_threads_for_post_creation);
  post_td = g_new(struct thread_data, max_threads_for_post_creation);
  sync_threads_remaining=max_threads_for_post_creation;
  sync_threads_remaining1=max_threads_for_post_creation;
  sync_threads_remaining2=max_threads_for_post_creation;
  sync_mutex = g_mutex_new();
  sync_mutex1 = g_mutex_new();
  sync_mutex2 = g_mutex_new();
  g_mutex_lock(sync_mutex);
  g_mutex_lock(sync_mutex1);
  g_mutex_lock(sync_mutex2);

  for (n = 0; n < max_threads_for_post_creation; n++) {
    initialize_thread_data(&(post_td[n]), conf, WAITING, n + 1 + num_threads + max_threads_for_schema_creation + max_threads_for_index_creation, NULL);
    post_threads[n] =
        g_thread_new("myloader_post",(GThreadFunc)worker_post_thread, &post_td[n]);
  }
}


void sync_threads(guint *counter, GMutex *mutex){
  if (g_atomic_int_dec_and_test(counter)){
    g_mutex_unlock(mutex);
  }else{
    g_mutex_lock(mutex);
    g_mutex_unlock(mutex);
  }
}

void *worker_post_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;

  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));
  gboolean cont=TRUE;
  struct control_job *job = NULL;

  set_thread_name("T%02u", td->thread_id);
  g_message("Thread %u: Starting post import task over table", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct control_job *)g_async_queue_pop(conf->post_table_queue);
    cont=process_job(td, job, NULL);
  }

  cont=TRUE;
  while (cont){
    job = (struct control_job *)g_async_queue_pop(conf->post_queue);
    cont=process_job(td, job, NULL);
  }
  sync_threads(&sync_threads_remaining2,sync_mutex2);
  cont=TRUE;
  while (cont){
    job = (struct control_job *)g_async_queue_pop(conf->view_queue);
    cont=process_job(td, job, NULL);
  }

  trace("Thread %u: ending", td->thread_id);
  return NULL;
}

void create_post_shutdown_job(struct configuration *conf){
  guint n=0;
  for (n = 0; n < max_threads_for_post_creation; n++) {
    g_async_queue_push(conf->post_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->post_table_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->view_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
  }
}

void wait_post_worker_to_finish(){
  guint n=0;
  for (n = 0; n < max_threads_for_post_creation; n++) {
    g_thread_join(post_threads[n]);
  }
}

void free_post_worker_threads(){
  g_free(post_td);
  g_free(post_threads);
}
