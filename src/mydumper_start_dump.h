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


enum job_type {
  JOB_SHUTDOWN,
  JOB_RESTORE,
  JOB_DUMP,
  JOB_DUMP_NON_INNODB,
  JOB_CHECKSUM,
  JOB_SCHEMA,
  JOB_VIEW,
  JOB_TRIGGERS,
  JOB_SCHEMA_POST,
  JOB_BINLOG,
  JOB_LOCK_DUMP_NON_INNODB,
  JOB_CREATE_DATABASE,
  JOB_DUMP_DATABASE
};

struct configuration {
  char use_any_index;
  GAsyncQueue *queue;
  GAsyncQueue *queue_less_locking;
  GAsyncQueue *ready;
  GAsyncQueue *ready_less_locking;
  GAsyncQueue *ready_database_dump;
  GAsyncQueue *unlock_tables;
  GAsyncQueue *pause_resume;
  GMutex *mutex;
  int done;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
  MYSQL *thrconn;
  GAsyncQueue *queue;
  GAsyncQueue *ready;
  gboolean less_locking_stage;
};

struct job {
  enum job_type type;
  void *job_data;
  struct configuration *conf;
};

// directory / database . table . first number . second number . extension
// first number : used when rows is used
// second number : when load data is used
struct table_job {
  char *database;
  char *table;
  char *partition;
  guint nchunk;
  char *filename;
  char *where;
  gboolean has_generated_fields;
  char *order_by;
  struct db_table *dbt;
};

struct table_checksum_job {
  char *database;
  char *table;
  char *filename;
};

struct tables_job {
  GList *table_job_list;
};

struct dump_database_job {
  struct database *database;
};

struct create_database_job {
  char *database;
  char *filename;
};

struct restore_job {
  char *database;
  char *table;
  char *filename;
};

struct binlog_job {
  char *filename;
  guint64 start_position;
  guint64 stop_position;
};

struct db_table {
  struct database *database;
  char *table;
  char *table_filename;
  char *escaped_table;
  guint64 datalength;
  guint rows;
  GMutex *rows_lock;
  GList *anonymized_function;
};

struct schema_post {
  struct database *database;
};

void load_start_dump_entries(GOptionGroup *main_group);
void initialize_start_dump();
void start_dump(MYSQL *conn);
MYSQL *create_main_connection();
void *exec_thread(void *data);
gboolean sig_triggered_int(void * user_data);
gboolean sig_triggered_term(void * user_data);
void set_disk_limits(guint p_at, guint r_at);
gboolean write_data(FILE *, GString *);



