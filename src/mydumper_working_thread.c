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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <mysql.h>

#if defined MARIADB_CLIENT_VERSION_STR && !defined MYSQL_SERVER_VERSION
#define MYSQL_SERVER_VERSION MARIADB_CLIENT_VERSION_STR
#endif

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include <pcre.h>
#include <signal.h>
#include <glib/gstdio.h>
#include <glib/gerror.h>
#include <gio/gio.h>
#include "config.h"
#include "server_detect.h"
#include "connection.h"
//#include "common_options.h"
#include "common.h"
#include <glib-unix.h>
#include <math.h>
#include "logging.h"
#include "set_verbose.h"
#include "locale.h"
#include <sys/statvfs.h>

#include "tables_skiplist.h"
#include "regex.h"

#include "mydumper_start_dump.h"
#include "mydumper_jobs.h"
#include "mydumper_common.h"
#include "mydumper_stream.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

GMutex *init_mutex = NULL;
/* Program options */
extern GAsyncQueue *stream_queue;
guint complete_insert = 0;
gboolean load_data = FALSE;
gboolean csv = FALSE;
gchar *fields_enclosed_by=NULL;
gchar *fields_escaped_by=NULL;
gchar *fields_terminated_by=NULL;
gchar *lines_starting_by=NULL;
gchar *lines_terminated_by=NULL;
gchar *statement_terminated_by=NULL;

gchar *fields_enclosed_by_ld=NULL;
gchar *lines_starting_by_ld=NULL;
gchar *lines_terminated_by_ld=NULL;
gchar *statement_terminated_by_ld=NULL;
gchar *fields_terminated_by_ld=NULL;
guint rows_per_file = 0;
gboolean use_savepoints = FALSE;
const gchar *insert_statement=INSERT;
extern gboolean dump_triggers;
extern gboolean stream;
extern int detected_server;
extern gboolean no_data;
extern FILE * (*m_open)(const char *filename, const char *);
extern int (*m_close)(void *file);
extern int (*m_write)(FILE * file, const char * buff, int len);
extern gchar *compress_extension;
extern gchar *db;
extern GString *set_session;
extern guint num_threads;
extern char **tables;
extern gchar *tables_skiplist_file;
extern GMutex *ready_database_dump_mutex;
gint database_counter = 0;
gchar *ignore_engines = NULL;
char **ignore = NULL;
extern gchar *tidb_snapshot;
extern GList *no_updated_tables;
int skip_tz = 0;
extern int need_dummy_read;
extern int need_dummy_toku_read;
extern int compress_output;
int sync_wait = -1;
extern gboolean ignore_generated_fields;
extern gboolean no_schemas;
gboolean dump_events = FALSE;
gboolean dump_routines = FALSE;
gboolean no_dump_views = FALSE;
extern gboolean less_locking;
gboolean success_on_1146 = FALSE;
gboolean insert_ignore = FALSE;
gboolean replace = FALSE;

extern GList *innodb_tables;
GMutex *innodb_tables_mutex = NULL;
extern GList *non_innodb_table;
GMutex *non_innodb_table_mutex = NULL;
extern GList *table_schemas;
GMutex *table_schemas_mutex = NULL;
extern GList *trigger_schemas;
GMutex *trigger_schemas_mutex = NULL;
extern GList *view_schemas;
GMutex *view_schemas_mutex = NULL;
extern GList *schema_post;
extern gint non_innodb_table_counter;
extern gint non_innodb_done;
guint less_locking_threads = 0;
extern guint trx_consistency_only;
extern gchar *set_names_str;
gchar *where_option=NULL;
extern GHashTable *all_anonymized_function;

gboolean dump_checksums = FALSE;
gboolean data_checksums = FALSE;
gboolean schema_checksums = FALSE;
gboolean routine_checksums = FALSE;
gboolean exit_if_broken_table_found = FALSE;
// For daemon mode
extern guint dump_number;
extern gboolean shutdown_triggered;
extern GAsyncQueue *start_scheduled_dump;
GCond *ll_cond = NULL;
GMutex *ll_mutex = NULL;

extern guint errors;
guint statement_size = 1000000;
guint chunk_filesize = 0;
int build_empty_files = 0;

static GOptionEntry working_thread_entries[] = {
    {"events", 'E', 0, G_OPTION_ARG_NONE, &dump_events, "Dump events. By default, it do not dump events", NULL},
    {"routines", 'R', 0, G_OPTION_ARG_NONE, &dump_routines,
     "Dump stored procedures and functions. By default, it do not dump stored procedures nor functions", NULL},
    {"no-views", 'W', 0, G_OPTION_ARG_NONE, &no_dump_views, "Do not dump VIEWs",
     NULL},
    {"checksum-all", 'M', 0, G_OPTION_ARG_NONE, &dump_checksums,
     "Dump checksums for all elements", NULL},
    {"data-checksums", 0, 0, G_OPTION_ARG_NONE, &data_checksums,
     "Dump table checksums with the data", NULL},
    {"schema-checksums", 0, 0, G_OPTION_ARG_NONE, &schema_checksums,
     "Dump schema table and view creation checksums", NULL},
    {"routine-checksums", 0, 0, G_OPTION_ARG_NONE, &routine_checksums,
     "Dump triggers, functions and routines checksums", NULL},
    {"tz-utc", 0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, &skip_tz,
     "SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data "
     "when a server has data in different time zones or data is being moved "
     "between servers with different time zones, defaults to on use "
     "--skip-tz-utc to disable.",
     NULL},
    {"complete-insert", 0, 0, G_OPTION_ARG_NONE, &complete_insert,
     "Use complete INSERT statements that include column names", NULL},
    {"skip-tz-utc", 0, 0, G_OPTION_ARG_NONE, &skip_tz, "", NULL},
    {"tidb-snapshot", 'z', 0, G_OPTION_ARG_STRING, &tidb_snapshot,
     "Snapshot to use for TiDB", NULL},
    {"insert-ignore", 'N', 0, G_OPTION_ARG_NONE, &insert_ignore,
     "Dump rows with INSERT IGNORE", NULL},
    {"replace", 0, 0 , G_OPTION_ARG_NONE, &replace,
     "Dump rows with REPLACE", NULL},
    {"exit-if-broken-table-found", 0, 0, G_OPTION_ARG_NONE, &exit_if_broken_table_found,
      "Exits if a broken table has been found", NULL},
    {"success-on-1146", 0, 0, G_OPTION_ARG_NONE, &success_on_1146,
     "Not increment error count and Warning instead of Critical in case of "
     "table doesn't exist",
     NULL},
    {"use-savepoints", 0, 0, G_OPTION_ARG_NONE, &use_savepoints,
     "Use savepoints to reduce metadata locking issues, needs SUPER privilege",
     NULL},
    {"statement-size", 's', 0, G_OPTION_ARG_INT, &statement_size,
     "Attempted size of INSERT statement in bytes, default 1000000", NULL},
    {"chunk-filesize", 'F', 0, G_OPTION_ARG_INT, &chunk_filesize,
     "Split tables into chunks of this output file size. This value is in MB",
     NULL},
    {"build-empty-files", 'e', 0, G_OPTION_ARG_NONE, &build_empty_files,
     "Build dump files even if no data available from table", NULL},
    { "where", 0, 0, G_OPTION_ARG_STRING, &where_option,
      "Dump only selected records.", NULL },
    {"ignore-engines", 'i', 0, G_OPTION_ARG_STRING, &ignore_engines,
     "Comma delimited list of storage engines to ignore", NULL},
    {"load-data", 0, 0, G_OPTION_ARG_NONE, &load_data,
     "", NULL },
    { "csv", 0, 0, G_OPTION_ARG_NONE, &csv,
      "Automatically enables --load-data and set variables to export in CSV format.", NULL },
    {"fields-terminated-by", 0, 0, G_OPTION_ARG_STRING, &fields_terminated_by_ld,"", NULL },
    {"fields-enclosed-by", 0, 0, G_OPTION_ARG_STRING, &fields_enclosed_by_ld,"", NULL },
    {"fields-escaped-by", 0, 0, G_OPTION_ARG_STRING, &fields_escaped_by,
      "Single character that is going to be used to escape characters in the"
      "LOAD DATA stament, default: '\\' ", NULL },
    {"lines-starting-by", 0, 0, G_OPTION_ARG_STRING, &lines_starting_by_ld,
      "Adds the string at the begining of each row. When --load-data is used"
      "it is added to the LOAD DATA statement. Its affects INSERT INTO statements"
      "also when it is used.", NULL },
    {"lines-terminated-by", 0, 0, G_OPTION_ARG_STRING, &lines_terminated_by_ld,
      "Adds the string at the end of each row. When --load-data is used it is"
       "added to the LOAD DATA statement. Its affects INSERT INTO statements"
       "also when it is used.", NULL },
    {"statement-terminated-by", 0, 0, G_OPTION_ARG_STRING, &statement_terminated_by_ld,
      "This might never be used, unless you know what are you doing", NULL },
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows_per_file,
     "Try to split tables into chunks of this many rows. This option turns off "
     "--chunk-filesize",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

void dump_database_thread(MYSQL *, struct configuration*, struct database *);
gchar *get_primary_key_string(MYSQL *conn, char *database, char *table);
GList *get_chunks_for_table(MYSQL *, char *, char *,
                            struct configuration *conf);
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field,
                       char *from, char *to);
guint64 write_table_data_into_file(MYSQL *conn, FILE *file, struct table_job *tj);
void write_table_job_into_file(MYSQL *conn, struct table_job * tj);

void load_working_thread_entries(GOptionGroup *main_group){
  g_option_group_add_entries(main_group, working_thread_entries);
}


void initialize_working_thread(){
  non_innodb_table_mutex = g_mutex_new();
  innodb_tables_mutex = g_mutex_new();
  view_schemas_mutex = g_mutex_new();
  table_schemas_mutex = g_mutex_new();
  trigger_schemas_mutex = g_mutex_new();
  init_mutex = g_mutex_new();
  ll_mutex = g_mutex_new();
  ll_cond = g_cond_new();
  if (less_locking)
    less_locking_threads = num_threads;
  initialize_dump_into_file();

  if (csv){
    load_data=TRUE;
    if (!fields_terminated_by_ld) fields_terminated_by_ld=g_strdup(",");
    if (!fields_enclosed_by_ld) fields_enclosed_by_ld=g_strdup("\"");
    if (!fields_escaped_by) fields_escaped_by=g_strdup("\\");
    if (!lines_terminated_by_ld) lines_terminated_by_ld=g_strdup("\n");
  }
  if (load_data){
    if (!fields_enclosed_by_ld){
      fields_enclosed_by=g_strdup("");
      fields_enclosed_by_ld=fields_enclosed_by;
    }else if(strlen(fields_enclosed_by_ld)>1){
      g_error("--fields-enclosed-by must be a single character");
      exit(EXIT_FAILURE);
    }else{
      fields_enclosed_by=fields_enclosed_by_ld;
    }

    if (fields_escaped_by){
      if(strlen(fields_escaped_by)>1){
        g_error("--fields-escaped-by must be a single character");
        exit(EXIT_FAILURE);
      }else if (strcmp(fields_escaped_by,"\\")==0){
        fields_escaped_by=g_strdup("\\\\");
      }
    }else{
      fields_escaped_by=g_strdup("\\\\");
    }
  }

  if (!fields_terminated_by_ld){
    if (load_data){
      fields_terminated_by=g_strdup("\t");
      fields_terminated_by_ld=g_strdup("\\t");
    }else
      fields_terminated_by=g_strdup(",");
  }else
    fields_terminated_by=replace_escaped_strings(g_strdup(fields_terminated_by_ld));
  if (!lines_starting_by_ld){
    if (load_data){
      lines_starting_by=g_strdup("");
      lines_starting_by_ld=lines_starting_by;
    }else
      lines_starting_by=g_strdup("(");
  }else
    lines_starting_by=replace_escaped_strings(g_strdup(lines_starting_by_ld));
  if (!lines_terminated_by_ld){
    if (load_data){
      lines_terminated_by=g_strdup("\n");
      lines_terminated_by_ld=g_strdup("\\n");
    }else
      lines_terminated_by=g_strdup(")\n");
  }else
    lines_terminated_by=replace_escaped_strings(g_strdup(lines_terminated_by_ld));
  if (!statement_terminated_by_ld){
    if (load_data){
      statement_terminated_by=g_strdup("");
      statement_terminated_by_ld=statement_terminated_by;
    }else
      statement_terminated_by=g_strdup(";\n");
  }else
    statement_terminated_by=replace_escaped_strings(g_strdup(statement_terminated_by_ld));


  // rows chunks have precedence over chunk_filesize
  if (rows_per_file > 0 && chunk_filesize > 0) {
//    chunk_filesize = 0;
//    g_warning("--chunk-filesize disabled by --rows option");
    g_warning("We are going to chunk by row and by filesize");
  }

  /* savepoints workaround to avoid metadata locking issues
     doesnt work for chuncks */
  if (rows_per_file && use_savepoints) {
    use_savepoints = FALSE;
    g_warning("--use-savepoints disabled by --rows");
  }

  /* Give ourselves an array of engines to ignore */
  if (ignore_engines)
    ignore = g_strsplit(ignore_engines, ",", 0);

  if (!compress_output) {
    m_open=&g_fopen;
    m_close=(void *) &fclose;
    m_write=(void *)&write_file;
    compress_extension=g_strdup("");
  } else {
    m_open=(void *) &gzopen;
    m_close=(void *) &gzclose;
    m_write=(void *)&gzwrite;
#ifdef ZWRAP_USE_ZSTD
    compress_extension = g_strdup(".zst");
#else
    compress_extension = g_strdup(".gz");
#endif
  }
  if (dump_checksums){
    data_checksums = TRUE;
    schema_checksums = TRUE;
    routine_checksums = TRUE;
  }

  if ( insert_ignore && replace ){
    g_critical("You can't use --insert-ignore and --replace at the same time");
    exit(EXIT_FAILURE);
  }

  if (insert_ignore)
    insert_statement=INSERT_IGNORE;

  if (replace)
    insert_statement=REPLACE;
}


// Free structures
void free_table_job(struct table_job *tj){
  if (tj->table)
    g_free(tj->table);
  if (tj->where)
    g_free(tj->where);
  if (tj->order_by)
    g_free(tj->order_by);
  if (tj->filename)
    g_free(tj->filename);
//  g_free(tj);
}

void message_dumping_data(struct thread_data *td, struct table_job *tj){
  g_message("Thread %d dumping data for `%s`.`%s`%s%s%s%s%s%s | Remaining jobs: %d",
                    td->thread_id, tj->database, tj->table, 
		    (tj->where || where_option ) ? " WHERE " : "", tj->where ? tj->where : "",
		    (tj->where && where_option ) ? " AND " : "", where_option ? where_option : "", 
                    tj->order_by ? " ORDER BY " : "", tj->order_by ? tj->order_by : "", g_async_queue_length(td->queue));
}

void thd_JOB_DUMP_DATABASE(struct configuration *conf, struct thread_data *td, struct job *job){
  struct dump_database_job * ddj = (struct dump_database_job *)job->job_data;
  g_message("Thread %d dumping db information for `%s`", td->thread_id,
            ddj->database->name);
  dump_database_thread(td->thrconn, conf, ddj->database);
  g_free(ddj);
  g_free(job);
  if (g_atomic_int_dec_and_test(&database_counter)) {
//   g_async_queue_push(conf->ready_database_dump, GINT_TO_POINTER(1));
    g_mutex_unlock(ready_database_dump_mutex);
  }
}

void thd_JOB_DUMP(struct thread_data *td, struct job *job){
  struct table_job *tj = (struct table_job *)job->job_data;
  message_dumping_data(td,tj);
  if (use_savepoints && mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
    g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
  }
  write_table_job_into_file(td->thrconn, tj);
  if (use_savepoints &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }
  free_table_job(tj);
  g_free(job);
}

void thd_JOB_LOCK_DUMP_NON_INNODB(struct configuration *conf, struct thread_data *td, struct job *job, gboolean *first,GString *prev_database,GString *prev_table){
  GString *query=g_string_new(NULL);
  GList *glj;
  struct table_job *tj;
  struct tables_job *mj = (struct tables_job *)job->job_data;
  for (glj = mj->table_job_list; glj != NULL; glj = glj->next) {
    tj = (struct table_job *)glj->data;
    if (*first) {
      g_string_printf(query, "LOCK TABLES `%s`.`%s` READ LOCAL",
                      tj->database, tj->table);
      *first = 0;
    } else {
      if (g_ascii_strcasecmp(prev_database->str, tj->database) ||
          g_ascii_strcasecmp(prev_table->str, tj->table)) {
        g_string_append_printf(query, ", `%s`.`%s` READ LOCAL",
            tj->database, tj->table);
      }
    }
    g_string_printf(prev_table, "%s", tj->table);
    g_string_printf(prev_database, "%s", tj->database);
  }
  *first = 1;
  if (mysql_query(td->thrconn, query->str)) {
    g_critical("Non Innodb lock tables fail: %s", mysql_error(td->thrconn));
    exit(EXIT_FAILURE);
  }
  if (g_atomic_int_dec_and_test(&non_innodb_table_counter) &&
      g_atomic_int_get(&non_innodb_done)) {
    g_async_queue_push(conf->unlock_tables, GINT_TO_POINTER(1));
  }
  for (glj = mj->table_job_list; glj != NULL; glj = glj->next) {
    tj = (struct table_job *)glj->data;
    message_dumping_data(td,tj);
    write_table_job_into_file(td->thrconn, tj);
    free_table_job(tj);
    g_free(tj);
  }
  mysql_query(td->thrconn, "UNLOCK TABLES /* Non Innodb */");
  g_list_free(mj->table_job_list);
  g_free(mj);
  g_free(job);
}

void initialize_thread(struct thread_data *td){
  m_connect(td->thrconn, "mydumper", NULL);
  g_message("Thread %d connected using MySQL connection ID %lu",
            td->thread_id, mysql_thread_id(td->thrconn));
}

void initialize_consistent_snapshot(struct thread_data *td){
  if ( sync_wait != -1 && mysql_query(td->thrconn, g_strdup_printf("SET SESSION WSREP_SYNC_WAIT = %d",sync_wait))){
    g_critical("Failed to set wsrep_sync_wait for the thread: %s",
               mysql_error(td->thrconn));
    exit(EXIT_FAILURE);
  }
  set_transaction_isolation_level_repeatable_read(td->thrconn);
  if (mysql_query(td->thrconn,
                  "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")) {
    g_critical("Failed to start consistent snapshot: %s", mysql_error(td->thrconn));
    exit(EXIT_FAILURE);
  }
}

void check_connection_status(struct thread_data *td){
  if (detected_server == SERVER_TYPE_TIDB) {
    // Worker threads must set their tidb_snapshot in order to be safe
    // Because no locking has been used.
    gchar *query =
        g_strdup_printf("SET SESSION tidb_snapshot = '%s'", tidb_snapshot);
    if (mysql_query(td->thrconn, query)) {
      g_critical("Failed to set tidb_snapshot: %s", mysql_error(td->thrconn));
      exit(EXIT_FAILURE);
    }
    g_free(query);
    g_message("Thread %d set to tidb_snapshot '%s'", td->thread_id,
              tidb_snapshot);
  }

  /* Unfortunately version before 4.1.8 did not support consistent snapshot
   * transaction starts, so we cheat */
  if (need_dummy_read) {
    mysql_query(td->thrconn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy");
    MYSQL_RES *res = mysql_store_result(td->thrconn);
    if (res)
      mysql_free_result(res);
  }
  if (need_dummy_toku_read) {
    mysql_query(td->thrconn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy");
    MYSQL_RES *res = mysql_store_result(td->thrconn);
    if (res)
      mysql_free_result(res);
  }
}

void *working_thread(struct thread_data *td) {
  struct configuration *conf = td->conf;
  // mysql_init is not thread safe, especially in Connector/C
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);

  initialize_thread(td);
  execute_gstring(td->thrconn, set_session);

  // Initialize connection 
  if (!skip_tz && mysql_query(td->thrconn, "/*!40103 SET TIME_ZONE='+00:00' */")) {
    g_critical("Failed to set time zone: %s", mysql_error(td->thrconn));
  }
  if (!td->less_locking_stage){
    if (use_savepoints && mysql_query(td->thrconn, "SET SQL_LOG_BIN = 0")) {
      g_critical("Failed to disable binlog for the thread: %s",
                 mysql_error(td->thrconn));
      exit(EXIT_FAILURE);
    }
    initialize_consistent_snapshot(td);
    check_connection_status(td);
  }
  if (set_names_str)
    mysql_query(td->thrconn, set_names_str);

  g_async_queue_push(td->ready, GINT_TO_POINTER(1));
  // Thread Ready to process jobs
 
  struct job *job = NULL;
  int first = 1;
  GString *query = g_string_new(NULL);
  GString *prev_table = g_string_new(NULL);
  GString *prev_database = g_string_new(NULL);
  /* if less locking we need to wait until that threads finish
      progressively waking up these threads */
  if (!td->less_locking_stage && less_locking) {
    g_mutex_lock(ll_mutex);

    while (less_locking_threads >= td->thread_id) {
      g_cond_wait(ll_cond, ll_mutex);
    }

    g_mutex_unlock(ll_mutex);
  }

  GMutex *resume_mutex=NULL;

  for (;;) {
    if (conf->pause_resume){
      resume_mutex = (GMutex *)g_async_queue_try_pop(conf->pause_resume);
      if (resume_mutex != NULL){
        g_mutex_lock(resume_mutex);
        g_mutex_unlock(resume_mutex);
        resume_mutex=NULL;
        continue;
      }
    }

    job = (struct job *)g_async_queue_pop(td->queue);
    if (shutdown_triggered && (job->type != JOB_SHUTDOWN)) {
      continue;
    }

    switch (job->type) {
    case JOB_LOCK_DUMP_NON_INNODB:
      thd_JOB_LOCK_DUMP_NON_INNODB(conf, td, job, &first, prev_database, prev_table);
      break;
    case JOB_DUMP:
      thd_JOB_DUMP(td, job);
      break;
    case JOB_DUMP_NON_INNODB:
      thd_JOB_DUMP(td, job);
      if (g_atomic_int_dec_and_test(&non_innodb_table_counter) &&
          g_atomic_int_get(&non_innodb_done)) {
        g_async_queue_push(conf->unlock_tables, GINT_TO_POINTER(1));
      }
      break;
    case JOB_CHECKSUM:
      do_JOB_CHECKSUM(td,job);
      break;
    case JOB_DUMP_DATABASE:
      thd_JOB_DUMP_DATABASE(conf,td,job);
      break;
    case JOB_CREATE_DATABASE:
      do_JOB_CREATE_DATABASE(td,job);
      break;
    case JOB_CREATE_TABLESPACE:
      do_JOB_CREATE_TABLESPACE(td,job);
      break;
    case JOB_SCHEMA:
      do_JOB_SCHEMA(td,job);
      break;
    case JOB_VIEW:
      do_JOB_VIEW(td,job);
      break;
    case JOB_TRIGGERS:
      do_JOB_TRIGGERS(td,job);
      break;
    case JOB_SCHEMA_POST:
      do_JOB_SCHEMA_POST(td,job);
      break;
    case JOB_SHUTDOWN:
      g_message("Thread %d shutting down", td->thread_id);
      if (td->less_locking_stage){
        g_mutex_lock(ll_mutex);
        less_locking_threads--;
        g_cond_broadcast(ll_cond);
        g_mutex_unlock(ll_mutex);
        g_string_free(query, TRUE);
        g_string_free(prev_table, TRUE);
        g_string_free(prev_database, TRUE);
      }
      if (td->thrconn)
        mysql_close(td->thrconn);
      g_free(job);
      mysql_thread_end();
      return NULL;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
  }
  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  return NULL;
}

GString *get_insertable_fields(MYSQL *conn, char *database, char *table) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra "
                      "not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%'",
                      database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else {
      g_string_append(field_list, ",");
    }

    gchar *tb = g_strdup_printf("`%s`", row[0]);
    g_string_append(field_list, tb);
    g_free(tb);
  }
  mysql_free_result(res);

  return field_list;
}

GList *get_anonymized_function_for(MYSQL *conn, gchar *database, gchar *table){
  // TODO #364: this is the place where we need to link the column between file loaded and dbt.
  // Currently, we are using identity_function, which return the same data.
  // Key: `database`.`table`.`column`

  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;",
                      database, table);
  mysql_query(conn, query);
  g_free(query);

  GList *anonymized_function_list=NULL;
  res = mysql_store_result(conn);
  gchar * k = g_strdup_printf("`%s`.`%s`",database,table);
  GHashTable *ht = g_hash_table_lookup(all_anonymized_function,k);
  fun_ptr2 f;
  if (ht){
    while ((row = mysql_fetch_row(res))) {
      f=(fun_ptr2)g_hash_table_lookup(ht,row[0]);
      if (f  != NULL){
        anonymized_function_list=g_list_append(anonymized_function_list,f);
      }else{
        anonymized_function_list=g_list_append(anonymized_function_list,&identity_function);
      }
    }
  }
  mysql_free_result(res);
  g_free(k);
  return anonymized_function_list;
}

gboolean detect_generated_fields(MYSQL *conn, gchar *database, gchar* table) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  gboolean result = FALSE;
  if (ignore_generated_fields)
    return FALSE;

  gchar *query = g_strdup_printf(
      "select COLUMN_NAME from information_schema.COLUMNS where "
      "TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra like '%%GENERATED%%' and extra not like '%%DEFAULT_GENERATED%%'",
      database, table);

  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  if (res == NULL){
    return FALSE;
  }

  if ((row = mysql_fetch_row(res))) {
    result = TRUE;
  }
  mysql_free_result(res);

  return result;
}

struct db_table *new_db_table( MYSQL *conn, struct database *database, char *table, char *datalength){
  struct db_table *dbt = g_new(struct db_table, 1);
  dbt->database = database;
  dbt->table = g_strdup(table);
  dbt->table_filename = get_ref_table(dbt->table);
  dbt->rows_lock= g_mutex_new();
  dbt->escaped_table = escape_string(conn,dbt->table);
  dbt->anonymized_function=get_anonymized_function_for(conn, dbt->database->name, dbt->table);
  dbt->has_generated_fields = detect_generated_fields(conn, dbt->database->escaped, dbt->escaped_table);
  if (dbt->has_generated_fields) {
    dbt->select_fields = get_insertable_fields(conn, dbt->database->escaped, dbt->escaped_table);
  } else {
    dbt->select_fields = g_string_new("*");
  }

  dbt->rows=0;
  if (!datalength)
    dbt->datalength = 0;
  else
    dbt->datalength = g_ascii_strtoull(datalength, NULL, 10);
  return dbt; 
}

void new_table_to_dump(MYSQL *conn, struct configuration *conf, gboolean is_view, struct database * database, char *table, char *datalength, gchar *ecol){
    /* Green light! */
  g_mutex_lock(database->ad_mutex);
  if (!database->already_dumped){
    create_job_to_dump_schema(database->name, conf);
    database->already_dumped=TRUE;
  }
  g_mutex_unlock(database->ad_mutex);

  struct db_table *dbt = new_db_table( conn, database, table, datalength);// (*row)[0], (*row)[6]);

 // if is a view we care only about schema
  if (!is_view) {
  // with trx_consistency_only we dump all as innodb_tables
    if (!no_schemas) {
//      write_table_metadata_into_file(dbt);
      table_schemas=g_list_append( table_schemas, dbt) ;
      create_job_to_dump_table_schema( dbt, conf, less_locking ? conf->queue_less_locking : conf->queue);
    }
    if (dump_triggers) {
      create_job_to_dump_triggers(conn, dbt, conf);
    }
    if (!no_data) {
      if (ecol != NULL && g_ascii_strcasecmp("MRG_MYISAM",ecol)) {
        if (data_checksums) {
          create_job_to_dump_checksum(dbt, conf);
        }
        if (trx_consistency_only ||
          (ecol != NULL && (!g_ascii_strcasecmp("InnoDB", ecol) || !g_ascii_strcasecmp("TokuDB", ecol)))) {
          create_job_to_dump_table(conn, dbt, conf, TRUE);
        } else {
          g_mutex_lock(non_innodb_table_mutex);
          non_innodb_table = g_list_prepend(non_innodb_table, dbt);
          g_mutex_unlock(non_innodb_table_mutex);
        }
      }
    }
  } else {
    if (!no_schemas) {
      create_job_to_dump_view(dbt, conf);
    }
  }
}

gboolean determine_if_schema_is_elected_to_dump_post(MYSQL *conn, struct database *database){
  char *query;
  MYSQL_RES *result = mysql_store_result(conn);
  MYSQL_ROW row;
  // Store Procedures and Events
  // As these are not attached to tables we need to define when we need to dump
  // or not Having regex filter make this hard because we dont now if a full
  // schema is filtered or not Also I cant decide this based on tables from a
  // schema being dumped So I will use only regex to dump or not SP and EVENTS I
  // only need one match to dump all

  gboolean post_dump = FALSE;

  if (dump_routines) {
    // SP
    query = g_strdup_printf("SHOW PROCEDURE STATUS WHERE CAST(Db AS BINARY) = '%s'", database->escaped);
    if (mysql_query(conn, (query))) {
      g_critical("Error showing procedure on: %s - Could not execute query: %s", database->name,
                 mysql_error(conn));
      errors++;
      g_free(query);
      return FALSE;
    }
    result = mysql_store_result(conn);
    while ((row = mysql_fetch_row(result)) && !post_dump) {
      /* Checks skip list on 'database.sp' string */
      if (tables_skiplist_file && check_skiplist(database->name, row[1]))
        continue;

      /* Checks PCRE expressions on 'database.sp' string */
      if (!eval_regex(database->name, row[1]))
        continue;

      post_dump = TRUE;
    }

    if (!post_dump) {
      // FUNCTIONS
      query = g_strdup_printf("SHOW FUNCTION STATUS WHERE CAST(Db AS BINARY) = '%s'", database->escaped);
      if (mysql_query(conn, (query))) {
        g_critical("Error showing function on: %s - Could not execute query: %s", database->name,
                   mysql_error(conn));
        errors++;
        g_free(query);
        return FALSE;
      }
      result = mysql_store_result(conn);
      while ((row = mysql_fetch_row(result)) && !post_dump) {
        /* Checks skip list on 'database.sp' string */
        if (tables_skiplist_file && check_skiplist(database->name, row[1]))
          continue;
        /* Checks PCRE expressions on 'database.sp' string */
        if ( !eval_regex(database->name, row[1]))
          continue;

        post_dump = TRUE;
      }
    }
    mysql_free_result(result);
  }

  if (dump_events && !post_dump) {
    // EVENTS
    query = g_strdup_printf("SHOW EVENTS FROM `%s`", database->name);
    if (mysql_query(conn, (query))) {
      g_critical("Error showing events on: %s - Could not execute query: %s", database->name,
                 mysql_error(conn));
      errors++;
      g_free(query);
      return FALSE;
    }
    result = mysql_store_result(conn);
    while ((row = mysql_fetch_row(result)) && !post_dump) {
      /* Checks skip list on 'database.sp' string */
      if (tables_skiplist_file && check_skiplist(database->name, row[1]))
        continue;
      /* Checks PCRE expressions on 'database.sp' string */
      if ( !eval_regex(database->name, row[1]))
        continue;

      post_dump = TRUE;
    }
    mysql_free_result(result);
  }
  return post_dump;
}

void dump_database_thread(MYSQL *conn, struct configuration *conf, struct database *database) {

  char *query;
  mysql_select_db(conn, database->name);
  if (detected_server == SERVER_TYPE_MYSQL ||
      detected_server == SERVER_TYPE_TIDB)
    query = g_strdup("SHOW TABLE STATUS");
  else
    query =
        g_strdup_printf("SELECT TABLE_NAME, ENGINE, TABLE_TYPE as COMMENT FROM "
                        "DATA_DICTIONARY.TABLES WHERE TABLE_SCHEMA='%s'",
                        database->escaped);

  if (mysql_query(conn, (query))) {
      g_critical("Error showing tables on: %s - Could not execute query: %s", database->name,
               mysql_error(conn));
    errors++;
    g_free(query);
    return;
  }

  MYSQL_RES *result = mysql_store_result(conn);
  guint ecol = -1;
  guint ccol = -1;
  determine_ecol_ccol(result, &ecol, &ccol);
  if (!result) {
    g_critical("Could not list tables for %s: %s", database->name, mysql_error(conn));
    errors++;
    return;
  }
  guint i=0;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result))) {

    int dump = 1;
    int is_view = 0;

    /* We now do care about views!
            num_fields>1 kicks in only in case of 5.0 SHOW FULL TABLES or SHOW
       TABLE STATUS row[1] == NULL if it is a view in 5.0 'SHOW TABLE STATUS'
            row[1] == "VIEW" if it is a view in 5.0 'SHOW FULL TABLES'
    */
    if ((detected_server == SERVER_TYPE_MYSQL) &&
        (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
      is_view = 1;

    /* Check for broken tables, i.e. mrg with missing source tbl */
    if (!is_view && row[ecol] == NULL) {
      g_warning("Broken table detected, please review: %s.%s", database->name,
                row[0]);
      if (exit_if_broken_table_found)
        exit(EXIT_FAILURE);
      dump = 0;
    }

    /* Skip ignored engines, handy for avoiding Merge, Federated or Blackhole
     * :-) dumps */
    if (dump && ignore && !is_view) {
      for (i = 0; ignore[i] != NULL; i++) {
        if (g_ascii_strcasecmp(ignore[i], row[ecol]) == 0) {
          dump = 0;
          break;
        }
      }
    }

    /* Skip views */
    if (is_view && no_dump_views)
      dump = 0;

    if (!dump)
      continue;

    /* In case of table-list option is enabled, check if table is part of the
     * list */
    if (tables) {
/*      int table_found = 0;
      for (i = 0; tables[i] != NULL; i++)
        if (g_ascii_strcasecmp(tables[i], row[0]) == 0)
          table_found = 1;
*/
      if (!is_table_in_list(row[0], tables))
        dump = 0;
    }
    if (!dump)
      continue;

    /* Special tables */
    if (g_ascii_strcasecmp(database->name, "mysql") == 0 &&
        (g_ascii_strcasecmp(row[0], "general_log") == 0 ||
         g_ascii_strcasecmp(row[0], "slow_log") == 0 ||
         g_ascii_strcasecmp(row[0], "innodb_index_stats") == 0 ||
         g_ascii_strcasecmp(row[0], "innodb_table_stats") == 0)) {
      dump = 0;
      continue;
    }

    /* Checks skip list on 'database.table' string */
    if (tables_skiplist_file && check_skiplist(database->name, row[0]))
      continue;

    /* Checks PCRE expressions on 'database.table' string */
    if (!eval_regex(database->name, row[0]))
      continue;

    /* Check if the table was recently updated */
    if (no_updated_tables && !is_view) {
      GList *iter;
      for (iter = no_updated_tables; iter != NULL; iter = iter->next) {
        if (g_ascii_strcasecmp(
                iter->data, g_strdup_printf("%s.%s", database->name, row[0])) == 0) {
          g_message("NO UPDATED TABLE: %s.%s", database->name, row[0]);
          dump = 0;
        }
      }
    }

    if (!dump)
      continue;

    new_table_to_dump(conn, conf, is_view, database, row[0], row[6], row[ecol]);

  }

  mysql_free_result(result);

  if (determine_if_schema_is_elected_to_dump_post(conn,database)) {
    create_job_to_dump_post(database, conf);
//    struct schema_post *sp = g_new(struct schema_post, 1);
//    sp->database = database;
//    schema_post = g_list_prepend(schema_post, sp);
  }

  g_free(query);

  return;
}

void write_table_job_into_file(MYSQL *conn, struct table_job *tj) {
  void *outfile = NULL;

  outfile = m_open(tj->filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s TABLE: %s Could not create output file %s (%d)",
               tj->database, tj->table, tj->filename, errno);
    errors++;
    return;
  }
  guint64 rows_count =
      write_table_data_into_file(conn, (FILE *)outfile, tj);

  if (!rows_count)
    g_message("Empty table %s.%s", tj->database, tj->table);
  
}

void append_columns (GString *statement, MYSQL_FIELD *fields, guint num_fields){
  guint i = 0;
  for (i = 0; i < num_fields; ++i) {
    if (i > 0) {
      g_string_append_c(statement, ',');
    }
    g_string_append_printf(statement, "`%s`", fields[i].name);
  }
}

void append_insert (gboolean condition, GString *statement, char *table, MYSQL_FIELD *fields, guint num_fields){
  if (condition) {
    g_string_printf(statement, "%s INTO `%s` (", insert_statement, table);
    append_columns(statement,fields,num_fields);
    g_string_append(statement, ") VALUES");
  } else {
    g_string_printf(statement, "%s INTO `%s` VALUES", insert_statement, table);
  }
}

gboolean write_data(FILE *file, GString *data) {
  size_t written = 0;
  ssize_t r = 0;
  gboolean second_write_zero = FALSE;
  while (written < data->len) {
    r=m_write(file, data->str + written, data->len);
    if (r < 0) {
      g_critical("Couldn't write data to a file: %s", strerror(errno));
      errors++;
      return FALSE;
    }
    if ( r == 0 ) {
      if (second_write_zero){
        g_critical("Couldn't write data to a file: %s", strerror(errno));
        errors++;
        return FALSE;        
      }
      second_write_zero=TRUE;
    }else{
      second_write_zero=FALSE;
    }
    written += r;
  }

  return TRUE;
}

/* Do actual data chunk reading/writing magic */
guint64 write_table_data_into_file(MYSQL *conn, FILE *file, struct table_job * tj){
  // There are 2 possible options to chunk the files:
  // - no chunk: this means that will be just 1 data file
  // - chunk_filesize: this function will be spliting the per filesize, this means that multiple files will be created
  // Split by row is before this step
  // It could write multiple INSERT statments in a data file if statement_size is reached
  guint i;
  guint fn = 0;
  guint sub_part=0;
  guint st_in_file = 0;
  guint num_fields = 0;
  guint64 num_rows = 0;
  guint64 num_rows_st = 0;
  MYSQL_RES *result = NULL;
  char *query = NULL;
  gchar *fcfile = NULL;
  gchar *load_data_fn=NULL;
//  gchar *filename_prefix = NULL;
  struct db_table * dbt = tj->dbt;
  /* Buffer for escaping field values */
  GString *escaped = g_string_sized_new(3000);
  FILE *main_file=file;
//  if (chunk_filesize) {
//    fcfile = build_data_filename(dbt->database->filename, dbt->table_filename, fn, sub_part);
//  }else{
    fcfile = g_strdup(tj->filename);
//  }

  /* Ghm, not sure if this should be statement_size - but default isn't too big
   * for now */
  GString *statement = g_string_sized_new(statement_size);
  GString *statement_row = g_string_sized_new(0);

  /* Poor man's database code */
  query = g_strdup_printf(
      "SELECT %s %s FROM `%s`.`%s` %s %s %s %s %s %s %s",
      (detected_server == SERVER_TYPE_MYSQL) ? "/*!40001 SQL_NO_CACHE */" : "",
      tj->dbt->select_fields->str, tj->database, tj->table, tj->partition?tj->partition:"", (tj->where || where_option ) ? "WHERE" : "",
      tj->where ? tj->where : "",  (tj->where && where_option ) ? "AND" : "", where_option ? where_option : "", tj->order_by ? "ORDER BY" : "",
      tj->order_by ? tj->order_by : "");
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    // ERROR 1146
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping table (%s.%s) data: %s ", tj->database, tj->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping table (%s.%s) data: %s ", tj->database, tj->table,
                 mysql_error(conn));
      errors++;
    }
    goto cleanup;
  }

  num_fields = mysql_num_fields(result);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);

  MYSQL_ROW row;

  g_string_set_size(statement, 0);

  gboolean first_time=TRUE;
  /* Poor man's data dump code */
  while ((row = mysql_fetch_row(result))) {
    gulong *lengths = mysql_fetch_lengths(result);
    num_rows++;

    if (!statement->len) {
	    
      // A file can be chunked by amount of rows or file size. 
      //
      if (!st_in_file) { 
        // File Header
        if (detected_server == SERVER_TYPE_MYSQL) {
          if (set_names_str)
            g_string_printf(statement,"%s;\n",set_names_str);
          g_string_append(statement, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n");
          if (!skip_tz) {
            g_string_append(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
          }
        } else if (detected_server == SERVER_TYPE_TIDB) {
          if (!skip_tz) {
            g_string_printf(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
          }
        } else {
          g_string_printf(statement, "SET FOREIGN_KEY_CHECKS=0;\n");
        }

        if (!write_data(file, statement)) {
          g_critical("Could not write out data for %s.%s", tj->database, tj->table);
          goto cleanup;
        }
      }
      if ( load_data ){
        if (first_time){
          load_data_fn=build_filename(dbt->database->filename, dbt->table_filename, tj->nchunk, sub_part, "dat");
          char * basename=g_path_get_basename(load_data_fn);
  	      g_string_printf(statement, "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE `%s` ",basename,tj->table);
          g_free(basename);
	        if (fields_terminated_by_ld)
	        	g_string_append_printf(statement, "FIELDS TERMINATED BY '%s' ",fields_terminated_by_ld);
	        if (fields_enclosed_by_ld)
  	        g_string_append_printf(statement, "ENCLOSED BY '%s' ",fields_enclosed_by_ld);
	        if (fields_escaped_by)
            g_string_append_printf(statement, "ESCAPED BY '%s' ",fields_escaped_by);
	        g_string_append(statement, "LINES ");
	        if (lines_starting_by_ld)
  	        g_string_append_printf(statement, "STARTING BY '%s' ",lines_starting_by_ld);
	        g_string_append_printf(statement, "TERMINATED BY '%s' (", lines_terminated_by_ld);

          append_columns(statement,fields,num_fields);
          g_string_append(statement,");\n");
	        if (!write_data(main_file, statement)) {
		        g_critical("Could not write out data for %s.%s", tj->database, tj->table);
		        goto cleanup;
  	      }else{
            g_string_set_size(statement, 0);
            g_free(fcfile);
            fcfile=load_data_fn;
  
            if (!compress_output) {
              fclose((FILE *)file);
              file = g_fopen(fcfile, "a");
            } else {
              gzclose((gzFile)file);
              file = (void *)gzopen(fcfile, "a");
            }
	        }
          first_time=FALSE;
        }
      }else{
        append_insert ((complete_insert || tj->dbt->has_generated_fields), statement, tj->table, fields, num_fields);
      }
      num_rows_st = 0;
    }

    if (statement_row->len) {
      g_string_append(statement, statement_row->str);
      g_string_set_size(statement_row, 0);
      num_rows_st++;
    }

    g_string_append(statement_row, lines_starting_by);
    GList *f = dbt->anonymized_function;
    gchar * (*fun_ptr)(gchar **) = &identity_function;
    for (i = 0; i < num_fields; i++) {
      if (f){
      fun_ptr=f->data;
      f=f->next;
      }
      if (load_data){
        if (!row[i]) {
//          g_string_append(statement_row,fields_enclosed_by);
          g_string_append(statement_row, "\\N");
//          g_string_append(statement_row,fields_enclosed_by);
        }else if (fields[i].type != MYSQL_TYPE_LONG && fields[i].type != MYSQL_TYPE_LONGLONG  && fields[i].type != MYSQL_TYPE_INT24  && fields[i].type != MYSQL_TYPE_SHORT ){
          g_string_append(statement_row,fields_enclosed_by);
          g_string_set_size(escaped, lengths[i] * 2 + 1);
          mysql_real_escape_string(conn, escaped->str, fun_ptr(&(row[i])), lengths[i]);
          g_string_append(statement_row,escaped->str);
          g_string_append(statement_row,fields_enclosed_by);
        }else
          g_string_append(statement_row,fun_ptr(&(row[i])));
      }else{
        /* Don't escape safe formats, saves some time */
        if (!row[i]) {
          g_string_append(statement_row, "NULL");
        } else if (fields[i].flags & NUM_FLAG) {
          g_string_append(statement_row, fun_ptr(&(row[i])));
        } else {
          /* We reuse buffers for string escaping, growing is expensive just at
           * the beginning */
          g_string_set_size(escaped, lengths[i] * 2 + 1);
          mysql_real_escape_string(conn, escaped->str, fun_ptr(&(row[i])), lengths[i]);
          if (fields[i].type == MYSQL_TYPE_JSON)
            g_string_append(statement_row, "CONVERT(");
          g_string_append_c(statement_row, '\"');
          g_string_append(statement_row, escaped->str);
          g_string_append_c(statement_row, '\"');
          if (fields[i].type == MYSQL_TYPE_JSON)
            g_string_append(statement_row, " USING UTF8MB4)");
        }
      }
      if (i < num_fields - 1) {
        g_string_append(statement_row, fields_terminated_by);
      } else {
        g_string_append_printf(statement_row,"%s", lines_terminated_by);

        /* INSERT statement is closed before over limit */
        if (statement->len + statement_row->len + 1 > statement_size) {
          if (num_rows_st == 0) {
            g_string_append(statement, statement_row->str);
            g_string_set_size(statement_row, 0);
            g_warning("Row bigger than statement_size for %s.%s", tj->database,
                      tj->table);
          }
          g_string_append(statement, statement_terminated_by);

          if (!write_data(file, statement)) {
            g_critical("Could not write out data for %s.%s", tj->database, tj->table);
            goto cleanup;
          } else {
            st_in_file++;
            if (chunk_filesize &&
                st_in_file * (guint)ceil((float)statement_size / 1024 / 1024) >
                    chunk_filesize) {
              if (tj->where == NULL){
                fn++;
              }else{
                sub_part++;
              }
              m_close(file);
              if (stream) { 
                g_async_queue_push(stream_queue, g_strdup(fcfile));
		if (load_data){
                  g_async_queue_push(stream_queue, g_strdup(load_data_fn));
		}
	      }
              g_free(fcfile);
              fcfile = build_data_filename(dbt->database->filename, dbt->table_filename, fn, sub_part);
              file = m_open(fcfile,"w");
              st_in_file = 0;
            }
          }
          g_string_set_size(statement, 0);
        } else {
          if (num_rows_st && ! load_data)
            g_string_append_c(statement, ',');
          g_string_append(statement, statement_row->str);
          num_rows_st++;
          g_string_set_size(statement_row, 0);
        }
      }
    }
  }
  if (mysql_errno(conn)) {
    g_critical("Could not read data from %s.%s: %s", tj->database, tj->table,
               mysql_error(conn));
    errors++;
  }

  if (statement_row->len > 0) {
    /* this last row has not been written out */
    if (statement->len > 0) {
      /* strange, should not happen */
      g_string_append(statement, statement_row->str);
    } else {
      if (load_data)
        append_insert (complete_insert, statement, tj->table, fields, num_fields); 
      g_string_append(statement, statement_row->str);
    }
  }

  if (statement->len > 0) {
    g_string_append(statement, statement_terminated_by);
    if (!write_data(file, statement)) {
      g_critical(
          "Could not write out closing newline for %s.%s, now this is sad!",
          tj->database, tj->table);
      goto cleanup;
    }
    st_in_file++;
  }

cleanup:
  g_free(query);

  g_string_free(escaped, TRUE);
  g_string_free(statement, TRUE);
  g_string_free(statement_row, TRUE);

  if (result) {
    mysql_free_result(result);
  }

  if (file) {
    m_close(file);
  }

  if (!st_in_file && !build_empty_files) {
    // dropping the useless file
    if (remove(fcfile)) {
      g_warning("Failed to remove empty file : %s\n", fcfile);
    }
  } else if (chunk_filesize) {
    if (stream) {
      g_async_queue_push(stream_queue, g_strdup(fcfile));
      if (load_data && load_data_fn !=NULL){
        g_async_queue_push(stream_queue, g_strdup(load_data_fn));
      }
    }
  }else{
    if (stream) {
      g_async_queue_push(stream_queue, g_strdup(tj->filename));
      if (load_data && load_data_fn !=NULL){
        g_async_queue_push(stream_queue, g_strdup(load_data_fn));
      }
    }
  }

  g_mutex_lock(dbt->rows_lock);
  dbt->rows+=num_rows;
  g_mutex_unlock(dbt->rows_lock);

  g_free(fcfile);

  return num_rows;
}


