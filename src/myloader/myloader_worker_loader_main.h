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

#ifndef _src_myloader_worker_loader_main_h
#define _src_myloader_worker_loader_main_h

#include "myloader.h"


enum data_control_type{
  SHUTDOWN =1 ,
  REQUEST_DATA_JOB,
  WAKE_DATA_THREAD,
  FILE_TYPE_SCHEMA_ENDED,
  FILE_TYPE_ENDED
};


static inline
const char *data_control_type2str(enum data_control_type ft){
  switch (ft) {
  case SHUTDOWN:
    return "SHUTDOWN";
  case REQUEST_DATA_JOB:
    return "REQUEST_DATA_JOB";
  case WAKE_DATA_THREAD:
    return "WAKE_DATA_THREAD";
  case FILE_TYPE_SCHEMA_ENDED:
    return "FILE_TYPE_SCHEMA_ENDED";
  case FILE_TYPE_ENDED:
    return "FILE_TYPE_ENDED";
  }
  g_assert(0);
  return NULL;
}

void wait_worker_loader_main();
void initialize_worker_loader_main (struct configuration *conf);
void maybe_shutdown_control_job();
enum data_job_type request_restore_data_job();
void data_control_queue_push(enum data_control_type current_ft);

struct restore_job * request_next_data_job();
void wake_data_threads();

#endif
