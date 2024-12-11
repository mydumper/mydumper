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

#include "../config.h"
#include "../connection.h"
#include "../common_options.h"
#include "../common.h"
#include "../logging.h"
#include "../set_verbose.h"
#include "../tables_skiplist.h"
#include "../regex.h"
#include "../server_detect.h"

#ifndef _src_myloader_h
#define _src_myloader_h
#include <mysql.h>
#define MYLOADER "myloader"

enum purge_mode { FAIL, NONE, DROP, TRUNCATE, DELETE };

struct restore_errors {
  guint data_errors;
  guint index_errors;
  guint schema_errors;
  guint trigger_errors;
  guint view_errors;
  guint sequence_errors;
  guint tablespace_errors;
  guint post_errors;
  guint constraints_errors;
  guint retries;
};
struct database;

enum thread_states { WAITING, STARTED, COMPLETED };

struct io_restore_result{
  GAsyncQueue *restore;
  GAsyncQueue *result;
};

struct connection_data{
  MYSQL *thrconn;
  struct database *current_database;
  long unsigned int thread_id;
  struct io_restore_result *queue;
  GAsyncQueue * ready;
  gboolean transaction;
  GMutex *in_use;
};

struct thread_data {
  struct configuration *conf;
//  MYSQL *thrconn;
//  struct database *current_database;
  guint thread_id;
//  struct connection_data connection_data;
  enum thread_states status;
  guint granted_connections;
//  GAsyncQueue *connection_pool;
  struct db_table*dbt;
//  struct database* use_database;
};

struct configuration {
  GAsyncQueue *database_queue;
  GAsyncQueue *table_queue;
  GAsyncQueue *retry_queue;
  GAsyncQueue *data_queue;
  GAsyncQueue *post_table_queue;
  GAsyncQueue *view_queue;
  GAsyncQueue *post_queue;
  GAsyncQueue *ready;
  GAsyncQueue *pause_resume;
  GAsyncQueue *stream_queue;
  GList *table_list;
  GMutex * table_list_mutex;
  GHashTable *table_hash;
  GMutex *table_hash_mutex;
//  GList *schema_create_list;
  GList *checksum_list;
  GMutex *mutex;
  GAsyncQueue *index_queue;
  int done;
};


enum schema_status { NOT_FOUND, NOT_FOUND_2, NOT_CREATED, CREATING, CREATED, DATA_DONE, INDEX_ENQUEUED, ALL_DONE};
static inline
const char * status2str(enum schema_status status)
{
  switch (status) {
  case NOT_FOUND:
    return "NOT_FOUND";
  case NOT_FOUND_2:
    return "NOT_FOUND_2";
  case NOT_CREATED:
    return "NOT_CREATED";
  case CREATING:
    return "CREATING";
  case CREATED:
    return "CREATED";
  case DATA_DONE:
    return "DATA_DONE";
  case INDEX_ENQUEUED:
    return "INDEX_ENQUEUED";
  case ALL_DONE:
    return "ALL_DONE";
  }
  g_assert(0);
  return 0;
}

struct database {
  gchar *name; // aka: the logical schema name, that could be different of the filename.
  char *real_database; // aka: the output schema name this can change when use -B.
  gchar *filename; // aka: the key of the schema. Useful if you have mydumper_ filenames.
  enum schema_status schema_state;
  GAsyncQueue *sequence_queue;
  GAsyncQueue *queue;
  GMutex * mutex; // TODO: use g_mutex_init() instead of g_mutex_new()
  gchar *schema_checksum;
  gchar *post_checksum;
  gchar *triggers_checksum;
};

struct db_table {
//  char *database;
//  char *real_database;
  struct database * database;
  char *table;
  char *real_table;
  struct object_to_export object_to_export;
	guint64 rows;
//  GAsyncQueue * queue;
  GList * restore_job_list;
  guint current_threads;
  guint max_threads;
  guint max_connections_per_job;
  guint retry_count;
  GMutex *mutex;
  GString *indexes;
  GString *constraints;
  guint count;
  enum schema_status schema_state;
  gboolean index_enqueued;
  GDateTime * start_data_time;
  GDateTime * finish_data_time;
  GDateTime * start_index_time;
  GDateTime * finish_time;
//  gboolean completed;
  gint remaining_jobs;
  gchar *data_checksum;
  gchar *schema_checksum;
  gchar *indexes_checksum;
  gchar *triggers_checksum;
  gboolean is_view;
  gboolean is_sequence;
};

enum file_type { 
  INIT, 
  SCHEMA_TABLESPACE, 
  SCHEMA_CREATE, 
  CJT_RESUME,
  SCHEMA_TABLE,
  DATA,
  SCHEMA_VIEW, 
  SCHEMA_SEQUENCE,
  SCHEMA_TRIGGER, 
  SCHEMA_POST, 
  CHECKSUM, 
//  METADATA_TABLE,
  METADATA_GLOBAL, 
  RESUME, 
  IGNORED, 
  LOAD_DATA, 
  SHUTDOWN, 
  INCOMPLETE,
  DO_NOT_ENQUEUE,
  THREAD,
  INDEX,
  INTERMEDIATE_ENDED };

static inline
const char *ft2str(enum file_type ft)
{
  switch (ft) {
  case INIT:
    return "INIT";
  case SCHEMA_TABLESPACE:
    return "SCHEMA_TABLESPACE";
  case SCHEMA_CREATE:
    return "SCHEMA_CREATE";
  case CJT_RESUME:
    return "CJT_RESUME";
  case SCHEMA_TABLE:
    return "SCHEMA_TABLE";
  case DATA:
    return "DATA";
  case SCHEMA_VIEW:
    return "SCHEMA_VIEW";
  case SCHEMA_SEQUENCE:
    return "SCHEMA_SEQUENCE";
  case SCHEMA_TRIGGER:
    return "SCHEMA_TRIGGER";
  case SCHEMA_POST:
    return "SCHEMA_POST";
  case CHECKSUM:
    return "CHECKSUM";
  case METADATA_GLOBAL:
    return "METADATA_GLOBAL";
  case RESUME:
    return "RESUME";
  case IGNORED:
    return "IGNORED";
  case LOAD_DATA:
    return "LOAD_DATA";
  case SHUTDOWN:
    return "SHUTDOWN";
  case INCOMPLETE:
    return "INCOMPLETE";
  case DO_NOT_ENQUEUE:
    return "DO_NOT_ENQUEUE";
  case THREAD:
    return "THREAD";
  case INDEX:
    return "INDEX";
  case INTERMEDIATE_ENDED:
    return "INTERMEDIATE_ENDED";
  }
  g_assert(0);
  return NULL;
}
#endif
