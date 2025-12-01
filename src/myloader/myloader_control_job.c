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
#include <stdlib.h>
#include <unistd.h>

#include "myloader_control_job.h"
#include "myloader_restore_job.h"
#include "myloader_common.h"
#include "myloader_restore.h"
//#include "myloader_jobs_manager.h"
#include "myloader_global.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_index.h"
#include "myloader_worker_schema.h"
#include "myloader_database.h"

/* control_job_queue is for data loads */
GAsyncQueue *control_job_queue = NULL;

struct control_job * new_control_job (enum control_job_type type, void *job_data, struct database *use_database) {
  struct control_job *j = g_new0(struct control_job, 1);
  j->type = type;
(void) use_database;
  //  j->use_database=use_database;
  switch (type){
    case JOB_SHUTDOWN:
      break;
    default:
      j->data.restore_job = (struct restore_job *)job_data;
  }
  return j;
}



