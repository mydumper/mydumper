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
#include <string.h>
#include "myloader_stream.h"
#include "common.h"
#include "myloader.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "connection.h"
#include <errno.h>

extern gchar *db;
extern gchar *set_names_str;
extern GString *set_session;
extern guint num_threads;
extern gboolean stream;

static GMutex *init_mutex=NULL;

void initialize_job(gchar * purge_mode_str){
  initialize_restore_job(purge_mode_str);
  init_mutex = g_mutex_new();
}

void *loader_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);
  td->current_database=NULL;

  m_connect(td->thrconn, "myloader", NULL);

  mysql_query(td->thrconn, set_names_str);
  mysql_query(td->thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
  mysql_query(td->thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
  mysql_query(td->thrconn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=db;
    if (execute_use(td, "Initializing thread")){
      g_critical("Changing to database: %s %s", td->current_database,db);
      exit(EXIT_FAILURE);
    }
  }

  g_debug("Thread %d: Starting import", td->thread_id);
  if (stream){
    process_stream_queue(td);
  }else{
    process_directory_queue(td);
  }
  struct control_job *job = NULL;
  gboolean cont=TRUE;

//  g_message("Thread %d: Starting post import task over table", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct control_job *)g_async_queue_pop(conf->post_table_queue);
//    g_message("%s",((struct restore_job *)job->job_data)->object);
    execute_use_if_needs_to(td, job->use_database, "Restoring post table");
    cont=process_job(td, job);
  }
//  g_message("Thread %d: Starting post import task: triggers, procedures and triggers", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct control_job *)g_async_queue_pop(conf->post_queue);
    execute_use_if_needs_to(td, job->use_database, "Restoring post tasks");
    cont=process_job(td, job);
  }

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  g_debug("Thread %d ending", td->thread_id);
  return NULL;
}

GThread **threads = NULL;
struct thread_data *td = NULL;

void initialize_loader_threads(struct configuration *conf){
  guint n=0;
  threads = g_new(GThread *, num_threads);
  td = g_new(struct thread_data, num_threads);
  for (n = 0; n < num_threads; n++) {
    td[n].conf = conf;
    td[n].thread_id = n + 1;
    threads[n] =
        g_thread_create((GThreadFunc)loader_thread, &td[n], TRUE, NULL);
    // Here, the ready queue is being used to serialize the connection to the database.
    // We don't want all the threads try to connect at the same time
    g_async_queue_pop(conf->ready);
  }
}

void wait_loader_threads_to_finish(){
  guint n=0;
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  restore_job_finish(); 
/*  if (shutdown_triggered)
    g_async_queue_push(file_list_to_do, g_strdup("NO_MORE_FILES"));
    */
}

void free_loader_threads(){
  g_free(td);
  g_free(threads);
}
