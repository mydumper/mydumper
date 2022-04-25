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
#include <glib.h>
#include "myloader_control_job.h"
#include "myloader_restore_job.h"

struct control_job * new_job (enum control_job_type type, void *job_data, char *use_database) {
  struct control_job *j = g_new0(struct control_job, 1);
  j->type = type;
  j->use_database=use_database;
  switch (type){
    case JOB_WAIT:
      j->data.queue = (GAsyncQueue *)job_data;
    case JOB_SHUTDOWN:
      break;
    default:
      j->data.restore_job = (struct restore_job *)job_data;
  }
  return j;
}

gboolean process_job(struct thread_data *td, struct control_job *job){
  switch (job->type) {
    case JOB_RESTORE:
//      g_message("Restore Job");
      process_restore_job(td,job->data.restore_job);
      break;
    case JOB_WAIT:
//      g_message("Wait Job");
      g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
//      GAsyncQueue *queue=job->data.queue;
      g_async_queue_pop(job->data.queue);
      break;
    case JOB_SHUTDOWN:
//      g_message("Thread %d shutting down", td->thread_id);
      g_free(job);
      return FALSE;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
  }
  return TRUE;
}

