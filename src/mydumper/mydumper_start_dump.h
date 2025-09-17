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

struct MList;
struct configuration;

#ifndef _mydumper_start_dump_h 
#define _mydumper_start_dump_h
#define MAX_START_TRANSACTION_RETRIES 5
#define MYDUMPER "mydumper"

#include "mydumper.h"
#include "mydumper_create_jobs.h"
#include "mydumper_table.h"

enum sync_thread_lock_mode {
  AUTO,
  FTWRL,
  LOCK_ALL,
  GTID,
  NO_LOCK,
  SAFE_NO_LOCK
};

struct MList{
  GList *list;
  GMutex *mutex;
};

struct table_queuing {
  GAsyncQueue *queue;
  GAsyncQueue *defer;
  GAsyncQueue *request_chunk;
  struct MList *table_list;
  const char *descr;
};

struct configuration {
  char use_any_index;
  GAsyncQueue *initial_queue;
  GAsyncQueue *initial_completed_queue;
  GAsyncQueue *schema_queue;
  struct table_queuing non_transactional;
  struct table_queuing transactional;
  GAsyncQueue *post_data_queue;
  GAsyncQueue *ready;
  GAsyncQueue *ready_non_transactional_queue;
  GAsyncQueue *db_ready;
  GAsyncQueue *source_and_replica_status_queue;
  GAsyncQueue *unlock_tables;
  GAsyncQueue *pause_resume;
  GAsyncQueue *gtid_pos_checked;
  GAsyncQueue *are_all_threads_in_same_pos;
  GMainLoop * loop;
  GString *lock_tables_statement;
  GMutex *mutex;
  int done;
};

#endif

void load_start_dump_entries(GOptionContext *context, GOptionGroup * filter_group);
void start_dump(struct configuration *conf);
void *exec_thread(void *data);
gboolean sig_triggered_int(void * user_data);
gboolean sig_triggered_term(void * user_data);
void set_disk_limits(guint p_at, guint r_at);
//void print_dbt_on_metadata(FILE *mdfile, struct db_table *dbt);
void print_dbt_on_metadata_gstring(struct db_table *dbt, GString *data);
