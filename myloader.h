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

        Authors: 	Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle
   dot com) Andrew Hutchings, SkySQL (andrew at skysql dot com)

*/

#ifndef _myloader_h
#define _myloader_h

enum restore_job_type { JOB_RESTORE_FILENAME, JOB_RESTORE_SCHEMA_STRING, JOB_RESTORE_STRING };
enum job_type { JOB_RESTORE, JOB_WAIT, JOB_SHUTDOWN};
enum purge_mode { NONE, DROP, TRUNCATE, DELETE };

struct configuration {
  GAsyncQueue *pre_queue;
  GAsyncQueue *queue;
  GAsyncQueue *post_queue;
  GAsyncQueue *ready;
  GAsyncQueue *constraints_queue;
  GList *table_list;
  GList *schema_view_list;
  GList *schema_triggers_list;
  GList *schema_post_list;
  GList *schema_create_list;
  GList *checksum_list;
  GMutex *mutex;
  int done;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
};

struct job {
  enum job_type type;
  void *job_data;
  struct configuration *conf;
};

struct restore_job {
  enum restore_job_type type;
  struct db_table * dbt;
  char *filename;
  GString *statement;
  guint part;
};

struct db_table {
  char *database;
  char *real_database;
  char *table;
  char *real_table;
  guint64 rows;
  GAsyncQueue * queue;
  GList * restore_job_list;
  guint current_threads;
  guint max_threads;
  GMutex *mutex;
  GString *indexes;
  GString *constraints;
  guint count;
  GDateTime * start_time;
  GDateTime * start_index_time;
  GDateTime * finish_time;
};
#endif
