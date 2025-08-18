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

#ifndef _src_myloader_restore_job_h
#define _src_myloader_restore_job_h
#include "myloader.h"
#include "myloader_control_job.h"
enum restore_job_type { JOB_RESTORE_SCHEMA_FILENAME, JOB_RESTORE_FILENAME, JOB_TO_CREATE_TABLE, JOB_RESTORE_STRING };
static inline
const char * rjtype2str(enum restore_job_type rjtype)
{
  switch (rjtype) {
  case JOB_RESTORE_SCHEMA_FILENAME:
    return "JOB_RESTORE_SCHEMA_FILENAME";
  case JOB_RESTORE_FILENAME:
    return "JOB_RESTORE_FILENAME";
  case JOB_TO_CREATE_TABLE:
    return "JOB_TO_CREATE_TABLE";
  case JOB_RESTORE_STRING:
    return "JOB_RESTORE_STRING";
  }
  g_assert(0);
  return 0;
}

struct data_restore_job{
  guint index;
  guint part;
  guint sub_part;
};

struct schema_restore_job{
  struct database *database;
  GString *statement;
  const char *object;
};


union restore_job_data {
  struct data_restore_job *drj;
  struct schema_restore_job *srj;
};

struct restore_job {
  enum restore_job_type type;
  union restore_job_data data;
  char *filename;
  struct db_table *dbt;
};

void initialize_restore_job();
//struct restore_job * new_restore_job( char * filename, /*char * database,*/ struct db_table * dbt, GString * statement, guint part, guint sub_part, enum restore_job_type type, const char *object);
struct restore_job * new_data_restore_job( char * filename, enum restore_job_type type, struct db_table * dbt, guint part, guint sub_part);
struct restore_job * new_schema_restore_job( char * filename, enum restore_job_type type, struct db_table * dbt, struct database * database, GString * statement, const char *object);
int process_restore_job(struct thread_data *td, struct restore_job *rj);
void restore_job_finish();
void stop_signal_thread();
void *signal_thread(void *data);
void execute_drop_database(struct thread_data *td, gchar *database);
gboolean process_job(struct thread_data *td, struct control_job *job, gboolean *retry);
#endif
