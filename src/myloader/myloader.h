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
#include "../pmm_thread.h"
#include "../checksum.h"
#include "myloader_table.h"
#ifndef _src_myloader_h
#define _src_myloader_h
#include <mysql.h>
#define MYLOADER "myloader"

enum purge_mode { FAIL, NONE, DROP, TRUNCATE, DELETE, PM_SKIP};

struct restore_errors {
  guint data_errors;
  guint data_warnings;
  guint index_errors;
  guint schema_errors;
  guint trigger_errors;
  guint view_errors;
  guint sequence_errors;
  guint tablespace_errors;
  guint post_errors;
  guint constraints_errors;
  guint skip_errors;
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
  long unsigned int connection_id;
  long unsigned int thread_id;
  struct io_restore_result *queue;
  GAsyncQueue * ready;
  gboolean transaction;
  GMutex *in_use;
  
};

struct replication_statements {
  GString *gtid_purge;
  GString *stop_replica;
  GString *reset_replica;
  GString *start_replica_until;
  GString *change_replication_source;
  GString *start_replica;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
  enum thread_states status;
  guint granted_connections;
  struct db_table*dbt;
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
//  GAsyncQueue *stream_queue;
  GList *table_list;
  GList *loading_table_list;
  GMutex * table_list_mutex;
  GHashTable *table_hash;
  GMutex *table_hash_mutex;
//  GList *schema_create_list;
  GList *checksum_list;
  GMutex *mutex;
  GAsyncQueue *index_queue;
  // O(1) ready table queue: tables with pending jobs ready for dispatch
  GAsyncQueue *ready_table_queue;
  int done;
  GOptionContext * context;
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

enum file_type { 
  METADATA_GLOBAL,
  RESUME,
  SCHEMA_TABLESPACE, 
  SCHEMA_SEQUENCE,
  SCHEMA_CREATE, 
  SCHEMA_TABLE,
  DATA,
  LOAD_DATA,
  SCHEMA_VIEW, 
  SCHEMA_TRIGGER, 
  SCHEMA_POST, 
  IGNORED,
  FILENAME_ENDED
};

static inline
const char *ft2str(enum file_type ft){
  switch (ft) {
  case METADATA_GLOBAL:
    return "METADATA_GLOBAL";
  case RESUME:
    return "RESUME";
  case SCHEMA_TABLESPACE:
    return "SCHEMA_TABLESPACE";
  case SCHEMA_SEQUENCE:
    return "SCHEMA_SEQUENCE";
  case SCHEMA_CREATE:
    return "SCHEMA_CREATE";
  case SCHEMA_TABLE:
    return "SCHEMA_TABLE";
  case DATA:
    return "DATA";
  case LOAD_DATA:
    return "LOAD_DATA";
  case SCHEMA_VIEW:
    return "SCHEMA_VIEW";
  case SCHEMA_TRIGGER:
    return "SCHEMA_TRIGGER";
  case SCHEMA_POST:
    return "SCHEMA_POST";
  case IGNORED:
    return "IGNORED";
  case FILENAME_ENDED:
    return "FILENAME_ENDED";
  }
  g_assert(0);
  return NULL;
}

#endif
