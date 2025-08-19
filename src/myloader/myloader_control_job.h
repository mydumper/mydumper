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

#ifndef _src_myloader_control_job_h
#define _src_myloader_control_job_h

#include "myloader.h"

enum control_job_type { JOB_RESTORE, JOB_SHUTDOWN };
static inline const char *jtype2str(enum control_job_type jtype)
{
  switch (jtype) {
  case JOB_RESTORE:
    return "JOB_RESTORE";
  case JOB_SHUTDOWN:
    return "JOB_SHUTDOWN";
  }
  g_assert(0);
  return NULL;
}

union control_job_data {
  struct restore_job *restore_job;
  GAsyncQueue *queue;
};

struct control_job {
  enum control_job_type type;
  union control_job_data data;
  struct database* use_database;
};

struct control_job * new_control_job (enum control_job_type type, void *job_data, struct database *use_database);
//gboolean process_job(struct thread_data *td, struct control_job *job, gboolean *retry);
void enroute_into_the_right_queue_based_on_file_type(enum file_type current_ft);
void cjt_resume();
void initialize_control_job (struct configuration *conf);
void wait_control_job();
void maybe_shutdown_control_job();
enum file_type request_restore_data_job();
struct restore_job * request_next_data_job();
#endif
