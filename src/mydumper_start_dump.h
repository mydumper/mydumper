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
  JOB_DETERMINE_CHUNK_TYPE,
  JOB_TABLE,
  JOB_CHECKSUM,
  JOB_SCHEMA,
  JOB_VIEW,
  JOB_TRIGGERS,
  JOB_SCHEMA_POST,
  JOB_BINLOG,
  JOB_CREATE_DATABASE,
  JOB_CREATE_TABLESPACE,
  JOB_DUMP_DATABASE,
  JOB_DUMP_ALL_DATABASES,
  JOB_DUMP_TABLE_LIST,
  JOB_WRITE_MASTER_STATUS
};

enum chunk_type{
  UNDEFINED,
  DEFINING,
  NONE,
  INTEGER,
  CHAR,
  PARTITION
};

struct configuration {
  char use_any_index;
  GAsyncQueue *initial_queue;
  GAsyncQueue *schema_queue;
  GAsyncQueue *non_innodb_queue;
  GAsyncQueue *innodb_queue;
  GAsyncQueue *post_data_queue;
  GAsyncQueue *ready;
  GAsyncQueue *ready_non_innodb_queue;
  GAsyncQueue *unlock_tables;
  GAsyncQueue *pause_resume;
  GString *lock_tables_statement;
  GMutex *mutex;
  int done;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
  MYSQL *thrconn;
  gboolean less_locking_stage;
  gchar *binlog_snapshot_gtid_executed;
};

struct job {
  enum job_type type;
  void *job_data;
//  struct configuration *conf;
};

union chunk_step;

struct integer_step {
  gchar *prefix;
  gchar *field;
  guint64 nmin;
  guint64 cursor;
  guint64 step;
  guint64 nmax;
  guint number;
  guint deep;
  GMutex *mutex;
  gboolean assigned;
  gboolean check_max;
  gboolean check_min;
};

struct char_step {
  gchar *prefix;
  gchar *field;

  gchar *cmin;
  guint cmin_len;
  guint cmin_clen;
  gchar *cmin_escaped;

  gchar *cursor;
  guint cursor_len;
  guint cursor_clen;
  gchar *cursor_escaped;

  gchar *cmax;
  guint cmax_len;
  guint cmax_clen;
  gchar *cmax_escaped;

  guint number;
  guint deep;
  GList *list;
  GMutex *mutex;
  gboolean assigned;
  guint64 step;
  union chunk_step *previous;

  guint status;
};

struct partition_step{
  GList *list;
  gchar *current_partition;
  guint number;
  guint deep;
  GMutex *mutex;
  gboolean assigned;
};

union chunk_step {
  struct integer_step integer_step;
  struct char_step char_step;
  struct partition_step partition_step;
};

// directory / database . table . first number . second number . extension
// first number : used when rows is used
// second number : when load data is used
struct table_job {
//  char *database;
//  char *table;
  char *partition;
  guint nchunk;
  guint sub_part;
//  char *filename;
  char *where;
  union chunk_step *chunk_step;  
  char *order_by;
  struct db_table *dbt;
  gchar *sql_filename;
  FILE *sql_file;
  gchar *dat_filename;
  FILE *dat_file;
  float filesize;
  guint st_in_file;
  int char_chunk_part;
};

struct tables_job {
  GList *table_job_list;
};

struct dump_database_job {
  struct database *database;
};

struct dump_table_list_job{
  gchar **table_list;
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
  char *min;
  char *max;
  char *field;
  GString *select_fields;
  gboolean complete_insert;
  GString *insert_statement;
  gboolean is_innodb;
  char *character_set;
  guint64 datalength;
  guint64 rows;
  GMutex *rows_lock;
  GList *anonymized_function;
  gchar *where;
  gchar *limit;
  guint num_threads;
  enum chunk_type chunk_type;
  GList *chunks;
  GMutex *chunks_mutex;
  GAsyncQueue *chunks_queue;
  gchar *primary_key;
};

struct schema_post {
  struct database *database;
};

void load_start_dump_entries(GOptionContext *context, GOptionGroup * filter_group);
void initialize_start_dump();
void start_dump();
MYSQL *create_main_connection();
void *exec_thread(void *data);
gboolean sig_triggered_int(void * user_data);
gboolean sig_triggered_term(void * user_data);
void set_disk_limits(guint p_at, guint r_at);
//gboolean write_data(FILE *, GString *);



