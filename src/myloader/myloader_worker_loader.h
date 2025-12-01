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
#include "myloader.h"
#define RESTORE_JOB_RUNNING_INTERVAL 10


enum data_job_type {DATA_JOB, DATA_PROCESS_ENDED, DATA_ENDED};

struct data_job{
  enum data_job_type type;
  struct restore_job *restore_job;
//  struct database *use_database;
};


static inline
const char *data_job_type2str(enum data_job_type ft){
  switch (ft) {
  case DATA_JOB:
    return "DATA_JOB";
  case DATA_PROCESS_ENDED:
    return "DATA_PROCESS_ENDED";
  case DATA_ENDED:
    return "DATA_ENDED";
  }
  g_assert(0);
  return NULL;
}


void initialize_loader_threads(struct configuration *conf);
void wait_loader_threads_to_finish();
void free_loader_threads();
void inform_restore_job_running();
void data_ended();
void data_job_push(enum data_job_type type, struct restore_job *rj);
