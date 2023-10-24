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
#include <pcre.h>

struct MList;

#ifndef _mydumper_start_dump_h 
#define _mydumper_start_dump_h
#define MAX_START_TRANSACTION_RETRIES 5
#define MYDUMPER "mydumper"
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
  JOB_SEQUENCE,
  JOB_TRIGGERS,
  JOB_SCHEMA_TRIGGERS,
  JOB_SCHEMA_POST,
  JOB_BINLOG,
  JOB_CREATE_DATABASE,
  JOB_CREATE_TABLESPACE,
  JOB_DUMP_DATABASE,
  JOB_DUMP_ALL_DATABASES,
  JOB_DUMP_TABLE_LIST,
  JOB_WRITE_MASTER_STATUS
};

struct MList{
  GList *list;
  GMutex *mutex;
};

enum chunk_type{
  NONE,
  INTEGER,
  CHAR,
  PARTITION,
  MULTICOLUMN_INTEGER
};

enum db_table_states{
  UNDEFINED,
  DEFINING,
  READY
};


enum chunk_states{
  UNASSIGNED,
  ASSIGNED,
  DUMPING_CHUNK,
  COMPLETED
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
  GAsyncQueue *db_ready;
  GAsyncQueue *binlog_ready;
  GAsyncQueue *unlock_tables;
  GAsyncQueue *pause_resume;
  GAsyncQueue *gtid_pos_checked;
  GAsyncQueue *are_all_threads_in_same_pos;
  GMainLoop * loop;
  GString *lock_tables_statement;
  GMutex *mutex;
  int done;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
  char *table_name;
  MYSQL *thrconn;
  gboolean less_locking_stage;
  gchar *binlog_snapshot_gtid_executed;
  GMutex *pause_resume_mutex;
};

struct job {
  enum job_type type;
  void *job_data;
//  struct configuration *conf;
};

union chunk_step;


struct unsigned_int{
  guint64 min;
  guint64 cursor;
  guint64 max;
};

struct signed_int{
  gint64 min;
  gint64 cursor;
  gint64 max;
};


union type {
  struct unsigned_int unsign;
  struct signed_int   sign;
};

struct table_job;
struct db_table;
struct chunk_step_item;


struct chunk_functions{
  void (*process)(struct table_job *tj, struct chunk_step_item *csi);
//  gchar *(*update_where)(union chunk_step * chunk_step);
  struct chunk_step_item *(*get_next)(struct db_table *dbt);
};

struct integer_step {
  gboolean is_unsigned;
  union type type; 
  gboolean is_step_fixed_length;
  guint64 step;
  guint64 min_chunk_step_size;
  guint64 max_chunk_step_size;
  guint64 estimated_remaining_steps;
//  guint64 number;
//  guint deep;
  gboolean check_max;
  gboolean check_min;
};

struct char_step {
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

//  guint number;
  guint deep;
  GList *list;
  guint64 step;
  union chunk_step *previous;

  guint64 estimated_remaining_steps;
  guint status;

  GMutex *mutex;
};

struct partition_step{
  GList *list;
  gchar *current_partition;
//  guint number;
//  guint deep;
};

union chunk_step {
  struct integer_step integer_step;
  struct char_step char_step;
  struct partition_step partition_step;
};


struct chunk_step_item{
  union chunk_step *chunk_step;
  enum chunk_type chunk_type;
  struct chunk_step_item *next;
  struct chunk_functions chunk_functions;
  GString *where;
  gboolean include_null;
  GString *prefix;
  gchar *field;
  guint64 number;
  guint deep;
  guint position;
  GMutex *mutex;
  gboolean needs_refresh;
//  gboolean assigned;
  enum chunk_states status;
};


// directory / database . table . first number . second number . extension
// first number : used when rows is used
// second number : when load data is used
struct table_job {
  char *partition;
  guint64 nchunk;
  guint sub_part;
  GString *where;
//  union chunk_step *chunk_step;  
  struct chunk_step_item *chunk_step_item;
  char *order_by;
  struct db_table *dbt;
  gchar *sql_filename;
  int sql_file;
  gchar *dat_filename;
  int dat_file;
  gchar *exec_out_filename;
  float filesize;
  guint st_in_file;
  int child_process;
  int char_chunk_part;
  struct thread_data *td;
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
//  char *field;
  guint64 rows_in_sts;
  GString *select_fields;
  gboolean complete_insert;
  GString *insert_statement;
  gboolean is_innodb;
  gboolean has_json_fields;
  char *character_set;
  guint64 datalength;
  guint64 rows;
  guint64 estimated_remaining_steps;
  GMutex *rows_lock;
  struct function_pointer ** anonymized_function;
  gchar *where;
  gchar *limit;
  gchar *columns_on_select;
  gchar *columns_on_insert;
  pcre *partition_regex;
  guint num_threads;
//  enum chunk_type chunk_type;
  GList *chunks;
//  struct chunk_step_item * initial_chunk_step;
  GMutex *chunks_mutex;
  GAsyncQueue *chunks_queue;
  GList *primary_key;
  gchar *primary_key_separated_by_comma;
  gboolean multicolumn;
  gint * chunks_completed;
  gchar *data_checksum;
  gchar *schema_checksum;
  gchar *indexes_checksum;
  gchar *triggers_checksum;
  guint chunk_filesize;  
  guint64 min_chunk_step_size;
  guint64 starting_chunk_step_size;
  guint64 max_chunk_step_size;
// struct chunk_functions chunk_functions;
  enum db_table_states status;
};


struct stream_queue_element{
  struct db_table *dbt;
  gchar *filename;
  GAsyncQueue *done;
};

struct schema_post {
  struct database *database;
};

struct fifo{
  gchar *filename;
  gchar *stdout_filename;
  GAsyncQueue * queue;
  float size;
  struct db_table *dbt;
  int fdout;
  GPid gpid;
  int child_pid;
  int pipe[2];
  GMutex *out_mutex;
  int error_number;
};

#endif

void load_start_dump_entries(GOptionContext *context, GOptionGroup * filter_group);
void initialize_start_dump();
void start_dump();
void *exec_thread(void *data);
gboolean sig_triggered_int(void * user_data);
gboolean sig_triggered_term(void * user_data);
void set_disk_limits(guint p_at, guint r_at);
void print_dbt_on_metadata(FILE *mdfile, struct db_table *dbt);
void print_dbt_on_metadata_gstring(struct db_table *dbt, GString *data);
int m_close_pipe(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt);
int m_close_file(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt);
