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



#include <mysql.h>

#ifndef _src_myloader_h
#define _src_myloader_h

#define MYLOADER "myloader"

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

enum thread_states { WAITING, STARTED, COMPLETED };
struct thread_data {
  struct configuration *conf;
  MYSQL *thrconn;
  gchar *current_database;
  guint thread_id;
  enum thread_states status;
};

struct configuration {
  GAsyncQueue *database_queue;
  GAsyncQueue *table_queue;
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


enum schema_status { NOT_FOUND, NOT_CREATED, CREATING, CREATED, DATA_DONE, INDEX_ENQUEUED, ALL_DONE};

struct database {
  gchar *name;
  char *real_database;
  enum schema_status schema_state;
  GAsyncQueue *queue;
  GMutex * mutex;
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
  guint64 rows;
//  GAsyncQueue * queue;
  GList * restore_job_list;
  guint current_threads;
  guint max_threads;
  guint max_threads_hard;
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
};

enum file_type { 
  INIT, 
  SCHEMA_TABLESPACE, 
  SCHEMA_CREATE, 
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

#endif
