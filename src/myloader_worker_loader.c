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
#include "myloader_worker_loader.h"
#include "connection.h"
#include <errno.h>
#include "myloader_global.h"
#include "myloader_worker_index.h"
#include <unistd.h>


GThread **threads = NULL;
struct thread_data *loader_td = NULL;
void *loader_thread(struct thread_data *td);
static GMutex *init_mutex=NULL;

void initialize_loader_threads(struct configuration *conf){
  init_mutex = g_mutex_new();
  guint n=0;
  threads = g_new(GThread *, num_threads);
  loader_td = g_new(struct thread_data, num_threads);
  for (n = 0; n < num_threads; n++) {
    loader_td[n].conf = conf;
    loader_td[n].thread_id = n + 1;
    threads[n] =
        g_thread_create((GThreadFunc)loader_thread, &loader_td[n], TRUE, NULL);
    // Here, the ready queue is being used to serialize the connection to the database.
    // We don't want all the threads try to connect at the same time
    g_async_queue_pop(conf->ready);
  }
}

void *loader_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);
  td->current_database=NULL;

  m_connect(td->thrconn, NULL);

//  mysql_query(td->thrconn, set_names_statement);

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=db;
    if (execute_use(td)){
      m_critical("Thread %d: Error switching to database `%s` when initializing", td->thread_id, td->current_database);
    }
  }

  g_debug("Thread %d: Starting import", td->thread_id);
  process_stream_queue(td);

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  g_debug("Thread %d: ending", td->thread_id);
  return NULL;
}


void wait_loader_threads_to_finish(){
  guint n=0;
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  restore_job_finish();
}

void inform_restore_job_running(){
  if (shutdown_triggered){
    guint n=0, sum=0, prev_sum=0;
    for (n = 0; n < num_threads; n++) {
      sum+=loader_td[n].status == STARTED ? 1 : 0;
    }
    fprintf(stdout, "Printing remaining loader threads every %d seconds", RESTORE_JOB_RUNNING_INTERVAL);
    while (sum>0){
      if (prev_sum != sum){
        fprintf(stdout, "\nThere are %d loader thread still working", sum);
        fflush(stdout);
      }else{
        fprintf(stdout, ".");
        fflush(stdout);
      }
      sleep(RESTORE_JOB_RUNNING_INTERVAL);
      prev_sum=sum;
      sum=0;
      for (n = 0; n < num_threads; n++) {
        sum+=loader_td[n].status == STARTED ? 1 : 0;
      }
    }
    fprintf(stdout, "\nAll loader thread had finished\n");
  }
}


void free_loader_threads(){
  g_free(loader_td);
  g_free(threads);
}



