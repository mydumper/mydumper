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

enum schema_job_type {SCHEMA_CREATE_JOB, SCHEMA_SEQUENCE_JOB, SCHEMA_TABLE_JOB, SCHEMA_PROCESS_ENDED, SCHEMA_ENDED};

struct schema_job{
  enum schema_job_type type;
  struct restore_job *restore_job;
//  struct database *use_database;
};

void initialize_worker_schema(struct configuration *conf);
void start_worker_schema();
void wait_schema_worker_to_finish();
gboolean schema_push( enum schema_job_type type, gchar * filename, enum restore_job_type rj_type, struct db_table * dbt, struct database * _database, GString * statement, enum restore_job_statement_type object, struct database *use_database );
void schema_ended();
