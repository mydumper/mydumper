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
#include "mydumper_masquerade.h"
#include "mydumper_jobs.h"
#include "mydumper_chunks.h"

/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

GMutex *init_mutex = NULL;
/* Program options */
extern gboolean no_locks;
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
extern GHashTable *all_where_per_table;
gint database_counter = 0;
//gint table_counter = 0;
gchar *ignore_engines = NULL;
gchar *binlog_snapshot_gtid_executed = NULL;
gboolean binlog_snapshot_gtid_executed_status = FALSE;
guint binlog_snapshot_gtid_executed_count = 0;
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

GList  *innodb_table = NULL;
GMutex *innodb_table_mutex = NULL;
GList  *non_innodb_table = NULL;
GMutex *non_innodb_table_mutex = NULL;
extern GList *table_schemas;
GMutex *table_schemas_mutex = NULL;
extern GList *trigger_schemas;
GMutex *trigger_schemas_mutex = NULL;
extern GList *view_schemas;
GMutex *view_schemas_mutex = NULL;
extern GList *schema_post;
extern gint non_innodb_done;
guint less_locking_threads = 0;
extern guint trx_consistency_only;
extern gchar *set_names_str;
gchar *where_option=NULL;

extern struct configuration_per_table conf_per_table;

GHashTable *character_set_hash=NULL;
GMutex *character_set_hash_mutex = NULL;

gboolean hex_blob = FALSE;
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

extern guint errors;
guint statement_size = 1000000;
guint chunk_filesize = 0;
int build_empty_files = 0;

GMutex *consistent_snapshot = NULL;
GMutex *consistent_snapshot_token_I = NULL;
GMutex *consistent_snapshot_token_II = NULL;

gchar *exec_per_thread = NULL;
gchar *exec_per_thread_extension = NULL;
gboolean use_fifo = FALSE;
gchar **exec_per_thread_cmd=NULL;

unsigned long (*escape_function)(MYSQL *, char *, const char *, unsigned long );

gboolean split_partitions = FALSE;

static GOptionEntry working_thread_entries[] = {
    {"events", 'E', 0, G_OPTION_ARG_NONE, &dump_events, "Dump events. By default, it do not dump events", NULL},
    {"routines", 'R', 0, G_OPTION_ARG_NONE, &dump_routines,
     "Dump stored procedures and functions. By default, it do not dump stored procedures nor functions", NULL},
    {"no-views", 'W', 0, G_OPTION_ARG_NONE, &no_dump_views, "Do not dump VIEWs",
     NULL},
    { "split-partitions", 0, 0, G_OPTION_ARG_NONE, &split_partitions,
      "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.", NULL},
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
    {"hex-blob", 0, 0, G_OPTION_ARG_NONE, &hex_blob,
      "Dump binary columns using hexadecimal notation", NULL},
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows_per_file,
     "Try to split tables into chunks of this many rows. This option turns off "
     "--chunk-filesize",
     NULL},
    {"exec-per-thread",0, 0, G_OPTION_ARG_STRING, &exec_per_thread, 
     "Set the command that will receive by STDIN and write in the STDOUT into the output file", NULL},
    {"exec-per-thread-extension",0, 0, G_OPTION_ARG_STRING, &exec_per_thread_extension,
     "Set the extension for the STDOUT file when --exec-per-thread is used", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

void dump_database_thread(MYSQL *, struct configuration*, struct database *);
gchar *get_primary_key_string(MYSQL *conn, char *database, char *table);
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field,
                       char *from, char *to);
guint64 write_table_data_into_file(MYSQL *conn, struct table_job *tj);
void write_table_job_into_file(MYSQL *conn, struct table_job * tj);

void load_working_thread_entries(GOptionGroup *main_group){
  g_option_group_add_entries(main_group, working_thread_entries);
}


void initialize_working_thread(){
  character_set_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free);
  character_set_hash_mutex = g_mutex_new();
  escape_function=&mysql_real_escape_string;
  non_innodb_table_mutex = g_mutex_new();
  innodb_table_mutex = g_mutex_new();
  view_schemas_mutex = g_mutex_new();
  table_schemas_mutex = g_mutex_new();
  trigger_schemas_mutex = g_mutex_new();
  innodb_table_mutex = g_mutex_new();
  init_mutex = g_mutex_new();
  ll_cond = g_cond_new();
  consistent_snapshot = g_mutex_new();
  g_mutex_lock(consistent_snapshot);
  consistent_snapshot_token_I = g_mutex_new();
  consistent_snapshot_token_II = g_mutex_new();
  g_mutex_lock(consistent_snapshot_token_II);
  if (less_locking)
    less_locking_threads = num_threads;
  initialize_dump_into_file();
  initialize_chunk();
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
      }else{
        escape_function=&m_real_escape_string;
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

  if (exec_per_thread_extension != NULL){
    if(exec_per_thread == NULL)
      g_error("--exec-per-thread needs to be set when --exec-per-thread-extension is used");
  }

  if (exec_per_thread!=NULL){
    use_fifo=TRUE;
    exec_per_thread_cmd=g_strsplit(exec_per_thread, " ", 0);
  }
}


void finalize_working_thread(){
  g_free(fields_terminated_by);
  g_free(lines_starting_by);
  g_free(lines_terminated_by);
  g_free(statement_terminated_by);
  g_hash_table_destroy(character_set_hash);
  g_mutex_free(character_set_hash_mutex);
  g_mutex_free(innodb_table_mutex);
  g_mutex_free(non_innodb_table_mutex);
  g_mutex_free(view_schemas_mutex);
  g_mutex_free(table_schemas_mutex);
  g_mutex_free(trigger_schemas_mutex);
  g_mutex_free(init_mutex);
  g_mutex_unlock(consistent_snapshot);
  g_mutex_free(consistent_snapshot);
  g_mutex_free(consistent_snapshot_token_I);
  g_mutex_unlock(consistent_snapshot_token_II);
  g_mutex_free(consistent_snapshot_token_II);
}


// Free structures
void free_table_job(struct table_job *tj){
  if (tj->where)
    g_free(tj->where);
  if (tj->order_by)
    g_free(tj->order_by);
  if (tj->chunk_step){
    switch (tj->dbt->chunk_type){
     case INTEGER:
       free_integer_step(tj->chunk_step);
       break;
     case CHAR:
       free_char_step(tj->chunk_step);
       break;
     default:
       break;
    };
   
  }
  if (tj->sql_file){
    m_close(tj->sql_file);
    tj->sql_file=NULL;
  }
//    g_free(tj->filename);
  g_free(tj);
}

void message_dumping_data(struct thread_data *td, struct table_job *tj){
  g_message("Thread %d dumping data for `%s`.`%s` %s %s %s %s %s %s %s %s %s into %s| Remaining jobs: %d",
                    td->thread_id, 
                    tj->dbt->database->name, tj->dbt->table, tj->partition?tj->partition:"",
                     (tj->where || where_option   || tj->dbt->where) ? "WHERE" : "" ,      tj->where ?      tj->where : "",
                     (tj->where && where_option )                    ? "AND"   : "" ,   where_option ?   where_option : "",
                    ((tj->where || where_option ) && tj->dbt->where) ? "AND"   : "" , tj->dbt->where ? tj->dbt->where : "",
                    tj->order_by ? "ORDER BY" : "", tj->order_by ? tj->order_by : "",
                    tj->sql_filename,
                    g_async_queue_length(td->conf->innodb_queue) + g_async_queue_length(td->conf->non_innodb_queue) + g_async_queue_length(td->conf->schema_queue));
}



void thd_JOB_DUMP_ALL_DATABASES( struct thread_data *td, struct job *job){
  // TODO: This should be in a job as needs to be done by a thread.
  MYSQL_RES *databases;
  MYSQL_ROW row;
  if (mysql_query(td->thrconn, "SHOW DATABASES") ||
      !(databases = mysql_store_result(td->thrconn))) {
    g_critical("Unable to list databases: %s", mysql_error(td->thrconn));
    exit(EXIT_FAILURE);
  }

  while ((row = mysql_fetch_row(databases))) {
    if (!strcasecmp(row[0], "information_schema") ||
        !strcasecmp(row[0], "performance_schema") ||
        (!strcasecmp(row[0], "data_dictionary")))
      continue;
    struct database * db_tmp=NULL;
    if (get_database(td->thrconn,row[0],&db_tmp) && !no_schemas && (eval_regex(row[0], NULL))){
      g_mutex_lock(db_tmp->ad_mutex);
      if (!db_tmp->already_dumped){
        create_job_to_dump_schema(db_tmp->name, td->conf);
        db_tmp->already_dumped=TRUE;
      }
      g_mutex_unlock(db_tmp->ad_mutex);
    }
    create_job_to_dump_database(db_tmp, td->conf);
  }
  if (g_atomic_int_dec_and_test(&database_counter)) {
    g_mutex_unlock(ready_database_dump_mutex);
  }
  mysql_free_result(databases);
  g_free(job);
}

void thd_JOB_DUMP_DATABASE(struct thread_data *td, struct job *job){
  struct dump_database_job * ddj = (struct dump_database_job *)job->job_data;
  g_message("Thread %d dumping db information for `%s`", td->thread_id,
            ddj->database->name);
  dump_database_thread(td->thrconn, td->conf, ddj->database);
  g_free(ddj);
  g_free(job);
  if (g_atomic_int_dec_and_test(&database_counter)) {
    g_mutex_unlock(ready_database_dump_mutex);
  }
}

union chunk_step *new_partition_step(gchar *partition){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  (void)partition;
//  cs->partition_step.partition = g_strdup(partition);
  return cs;
}

void m_async_queue_push_conservative(GAsyncQueue *queue, struct job *element){
  // Each job weights 500 bytes aprox.
  // if we reach to 200k of jobs, which is 100MB of RAM, we are going to wait 5 seconds
  // which is not too much considering that it will impossible to proccess 200k of jobs
  // in 5 seconds.
  // I don't think that we need to this values as parameters, unless that a user needs to
  // set hundreds of threads
  while (g_async_queue_length(queue)>200000){
    g_warning("Too many jobs in the queue. We are pausing the jobs creation for 5 seconds.");
    sleep(5);
  }
  g_async_queue_push(queue, element);
}

void thd_JOB_TABLE(struct thread_data *td, struct job *job){
//  char *database = dbt->database;
//  char *table = dbt->table;
  struct db_table * dbt = job->job_data;
  GList * partitions = NULL;
  GAsyncQueue *queue = dbt->is_innodb ? td->conf->innodb_queue : td->conf->non_innodb_queue;
  if (split_partitions)
    partitions = get_partitions_for_table(td->thrconn, dbt->database->name, dbt->table);

  if (partitions){
    int npartition=0;
    dbt->chunk_type=PARTITION;
    for (partitions = g_list_first(partitions); partitions; partitions=g_list_next(partitions)) {
      create_job_to_dump_chunk(dbt, (char *) g_strdup_printf(" PARTITION (%s) ", (char *)partitions->data), npartition, get_primary_key_string(td->thrconn, dbt->database->name, dbt->table), new_partition_step(partitions->data), &g_async_queue_push, queue);
      npartition++;
    }
    g_list_free_full(g_list_first(partitions), (GDestroyNotify)g_free);
  }else{
    GList *chunks = NULL;
    if (rows_per_file)
      chunks = get_chunks_for_table(td->thrconn, dbt, td->conf);
    if (chunks) {
      int nchunk = 0;
      GList *iter;
      for (iter = chunks; iter != NULL; iter = iter->next) {
        create_job_to_dump_chunk( dbt, NULL, nchunk, get_primary_key_string(td->thrconn, dbt->database->name, dbt->table), iter->data, &m_async_queue_push_conservative, queue);
        nchunk++;
      }
      g_list_free(chunks);
    } else {
      create_job_to_dump_chunk( dbt, NULL, 0, get_primary_key_string(td->thrconn, dbt->database->name, dbt->table), NULL, &g_async_queue_push, queue);
    }
  }
  if (g_atomic_int_dec_and_test(&database_counter)) {
    g_mutex_unlock(ready_database_dump_mutex);
  }
  g_free(job);
}

void get_where_clause_for_chunk_step(struct table_job * tj){
  switch (tj->dbt->chunk_type){
    case INTEGER:
      tj->where=g_strdup_printf("%s(`%s` >= %"G_GUINT64_FORMAT" AND `%s` < %"G_GUINT64_FORMAT")",
                          tj->chunk_step->integer_step.prefix?tj->chunk_step->integer_step.prefix:"",
                          tj->chunk_step->integer_step.field, tj->chunk_step->integer_step.nmin,
                          tj->chunk_step->integer_step.field, tj->chunk_step->integer_step.nmax);
    break;
  case CHAR:
    tj->where=g_strdup_printf("%s(`%s` BETWEEN BINARY '%s' AND BINARY '%s')",
                          tj->chunk_step->char_step.prefix?tj->chunk_step->char_step.prefix:"",
                          tj->chunk_step->char_step.field,
                          tj->chunk_step->char_step.cmin,tj->chunk_step->char_step.cmax
                          );
     break;

  default: break;
  }
}


void process_integer_chunk(struct thread_data *td, struct table_job *tj);
void process_char_chunk(struct thread_data *td, struct table_job *tj);
void process_partition_chunk(struct thread_data *td, struct table_job *tj);

void thd_JOB_DUMP(struct thread_data *td, struct job *job){
  struct table_job *tj = (struct table_job *)job->job_data;
//  get_where_clause_for_chunk_step(tj);
  if (use_savepoints && mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
    g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
  }
  message_dumping_data(td,tj);
  switch (tj->dbt->chunk_type) {
    case INTEGER:
      process_integer_chunk(td, tj);
      break;
    case CHAR:
      process_char_chunk(td, tj);
      break;
    case PARTITION:
      process_partition_chunk(td, tj);
      break;
    case NONE:
      write_table_job_into_file(td->thrconn, tj);
      break;
  }
  if (tj->sql_file){
    m_close(tj->sql_file);
    tj->sql_file=NULL;
  }
  if (tj->dat_file){
    m_close(tj->dat_file);
    tj->dat_file=NULL;
  }
  if (tj->filesize == 0 && !build_empty_files) {
    // dropping the useless file
    if (remove(tj->sql_filename)) {
      g_warning("Failed to remove empty file : %s", tj->sql_filename);
    }else{
      g_message("File removed: %s", tj->sql_filename);
    }
  } else if (stream) {
      g_async_queue_push(stream_queue, g_strdup(tj->sql_filename));
  }

  if (use_savepoints &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }
  free_table_job(tj);
  g_free(job);
}

void initialize_thread(struct thread_data *td){
  m_connect(td->thrconn, "mydumper", NULL);
  g_message("Thread %d connected using MySQL connection ID %lu",
            td->thread_id, mysql_thread_id(td->thrconn));
}


gboolean are_all_threads_in_same_pos(struct thread_data *td){
  if (g_strcmp0(td->binlog_snapshot_gtid_executed,"")==0)
    return TRUE;
  gboolean binlog_snapshot_gtid_executed_status_local=FALSE;
  g_mutex_lock(consistent_snapshot_token_I);
  g_message("Thread %d: All threads in same pos check",td->thread_id);
  if (binlog_snapshot_gtid_executed == NULL){
    binlog_snapshot_gtid_executed_count=0;
    binlog_snapshot_gtid_executed=g_strdup(td->binlog_snapshot_gtid_executed);
    binlog_snapshot_gtid_executed_status=TRUE;
  }else 
    if (!(( binlog_snapshot_gtid_executed_status) && (g_strcmp0(td->binlog_snapshot_gtid_executed,binlog_snapshot_gtid_executed) == 0))){
      binlog_snapshot_gtid_executed_status=FALSE;
    }
  binlog_snapshot_gtid_executed_count++;
  if (binlog_snapshot_gtid_executed_count < num_threads){
    g_debug("Thread %d: Consistent_snapshot_token_I trying unlock",td->thread_id);
    g_mutex_unlock(consistent_snapshot_token_I);
    g_debug("Thread %d: Consistent_snapshot_token_I unlocked",td->thread_id);
    g_mutex_lock(consistent_snapshot_token_II);
    g_debug("Thread %d: Consistent_snapshot_token_II locked",td->thread_id);
    binlog_snapshot_gtid_executed_status_local=binlog_snapshot_gtid_executed_status;
    binlog_snapshot_gtid_executed_count--;
    if (binlog_snapshot_gtid_executed_count == 1){
      if (!binlog_snapshot_gtid_executed_status_local)
        binlog_snapshot_gtid_executed=NULL;
      g_debug("Thread %d: Consistent_snapshot trying unlock",td->thread_id);
      g_mutex_unlock(consistent_snapshot);
      g_debug("Thread %d: Consistent_snapshot unlocked",td->thread_id);
    }else{
      g_debug("Thread %d: 1- Consistent_snapshot_token_II trying unlock",td->thread_id);
      g_mutex_unlock(consistent_snapshot_token_II);
      g_debug("Thread %d: 1- Consistent_snapshot_token_II unlocked",td->thread_id);
    }
  }else{
    binlog_snapshot_gtid_executed_status_local=binlog_snapshot_gtid_executed_status;
    g_debug("Thread %d: 2- Consistent_snapshot_token_II trying unlock",td->thread_id);
    g_mutex_unlock(consistent_snapshot_token_II);
    g_debug("Thread %d: 2- Consistent_snapshot_token_II unlocked",td->thread_id);
    g_mutex_lock(consistent_snapshot);
    g_debug("Thread %d: Consistent_snapshot locked",td->thread_id);
    g_debug("Thread %d: Consistent_snapshot_token_I trying unlock",td->thread_id);
    g_mutex_unlock(consistent_snapshot_token_I);
    g_debug("Thread %d: Consistent_snapshot_token_I unlocked",td->thread_id);
  }
  g_message("Thread %d: binlog_snapshot_gtid_executed_status_local %s with gtid: '%s'.", td->thread_id, binlog_snapshot_gtid_executed_status_local?"succeeded":"failed", td->binlog_snapshot_gtid_executed);
  return binlog_snapshot_gtid_executed_status_local;
}

void initialize_consistent_snapshot(struct thread_data *td){
  if ( sync_wait != -1 && mysql_query(td->thrconn, g_strdup_printf("SET SESSION WSREP_SYNC_WAIT = %d",sync_wait))){
    g_critical("Failed to set wsrep_sync_wait for the thread: %s",
               mysql_error(td->thrconn));
    exit(EXIT_FAILURE);
  }
  set_transaction_isolation_level_repeatable_read(td->thrconn);
  guint start_transaction_retry=0;
  gboolean cont = FALSE; 
  while ( !cont && (start_transaction_retry < 5)){
//  Uncommenting the sleep will cause inconsitent scenarios always, which is useful for debugging 
//    sleep(td->thread_id);
    g_debug("Thread %d: Start trasaction #%d", td->thread_id, start_transaction_retry);
    if (mysql_query(td->thrconn,
                  "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")) {
      g_critical("Failed to start consistent snapshot: %s", mysql_error(td->thrconn));
      exit(EXIT_FAILURE);
    }
    if (mysql_query(td->thrconn,
                  "SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'")) {
      g_warning("Failed to get binlog_snapshot_gtid_executed: %s", mysql_error(td->thrconn));
    }else{
      MYSQL_RES *res = mysql_store_result(td->thrconn);
      MYSQL_ROW row = mysql_fetch_row(res);
      if (row!=NULL)
        td->binlog_snapshot_gtid_executed=g_strdup(row[1]);
      else
        td->binlog_snapshot_gtid_executed=g_strdup("");
      mysql_free_result(res);
    }
    start_transaction_retry++;
    cont=are_all_threads_in_same_pos(td);
  } 

  if (g_strcmp0(td->binlog_snapshot_gtid_executed,"")==0){
    if (no_locks){
      g_warning("We are not able to determine if the backup will be consistent.");
    }
  }else{
    if (cont){
        g_message("All threads in the same position. This will be a consistent backup.");
    }else{
      if (no_locks){ 
        g_warning("Backup will not be consistent, but we are continuing because you use --no-locks.");
      }else{
        g_error("Backup will not be consistent. Threads are in different points in time. Use --no-locks if you expect inconsistent backups.");
        exit(EXIT_FAILURE);
      }
    }
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


/* Write some stuff we know about snapshot, before it changes */
void write_snapshot_info(MYSQL *conn, FILE *file) {
  MYSQL_RES *master = NULL, *slave = NULL, *mdb = NULL;
  MYSQL_FIELD *fields;
  MYSQL_ROW row;

  char *masterlog = NULL;
  char *masterpos = NULL;
  char *mastergtid = NULL;

  char *connname = NULL;
  char *slavehost = NULL;
  char *slavelog = NULL;
  char *slavepos = NULL;
  char *slavegtid = NULL;
  guint isms;
  guint i;

  mysql_query(conn, "SHOW MASTER STATUS");
  master = mysql_store_result(conn);
  if (master && (row = mysql_fetch_row(master))) {
    masterlog = row[0];
    masterpos = row[1];
    /* Oracle/Percona GTID */
    if (mysql_num_fields(master) == 5) {
      mastergtid = row[4];
    } else {
      /* Let's try with MariaDB 10.x */
      /* Use gtid_binlog_pos due to issue with gtid_current_pos with galera
 *        * cluster, gtid_binlog_pos works as well with normal mariadb server
 *               * https://jira.mariadb.org/browse/MDEV-10279 */
      mysql_query(conn, "SELECT @@gtid_binlog_pos");
      mdb = mysql_store_result(conn);
      if (mdb && (row = mysql_fetch_row(mdb))) {
        mastergtid = row[0];
      }
    }
  }

  if (masterlog) {
    fprintf(file, "SHOW MASTER STATUS:\n\tLog: %s\n\tPos: %s\n\tGTID:%s\n\n",
            masterlog, masterpos, mastergtid);
    g_message("Written master status");
  }

  isms = 0;
  mysql_query(conn, "SELECT @@default_master_connection");
  MYSQL_RES *rest = mysql_store_result(conn);
  if (rest != NULL && mysql_num_rows(rest)) {
    mysql_free_result(rest);
    g_message("Multisource slave detected.");
    isms = 1;
  }

  if (isms)
    mysql_query(conn, "SHOW ALL SLAVES STATUS");
  else
    mysql_query(conn, "SHOW SLAVE STATUS");

  guint slave_count=0;
  slave = mysql_store_result(conn);
  while (slave && (row = mysql_fetch_row(slave))) {
    fields = mysql_fetch_fields(slave);
    for (i = 0; i < mysql_num_fields(slave); i++) {
      if (isms && !strcasecmp("connection_name", fields[i].name))
        connname = row[i];
      if (!strcasecmp("exec_master_log_pos", fields[i].name)) {
        slavepos = row[i];
      } else if (!strcasecmp("relay_master_log_file", fields[i].name)) {
        slavelog = row[i];
      } else if (!strcasecmp("master_host", fields[i].name)) {
        slavehost = row[i];
      } else if (!strcasecmp("Executed_Gtid_Set", fields[i].name) ||
                 !strcasecmp("Gtid_Slave_Pos", fields[i].name)) {
        slavegtid = row[i];
      }
    }
    if (slavehost) {
      slave_count++;
      fprintf(file, "SHOW SLAVE STATUS:");
      if (isms)
        fprintf(file, "\n\tConnection name: %s", connname);
      fprintf(file, "\n\tHost: %s\n\tLog: %s\n\tPos: %s\n\tGTID:%s\n\n",
              slavehost, slavelog, slavepos, slavegtid);
      g_message("Written slave status");
    }
  }
  if (slave_count > 1)
    g_warning("Multisource replication found. Do not trust in the exec_master_log_pos as it might cause data inconsistencies. Search 'Replication and Transaction Inconsistencies' on MySQL Documentation");

  fflush(file);
  if (master)
    mysql_free_result(master);
  if (slave)
    mysql_free_result(slave);
  if (mdb)
    mysql_free_result(mdb);

//    if (g_atomic_int_dec_and_test(&schema_counter)) {
//      g_mutex_unlock(ready_schema_mutex);
//    }
}

gboolean process_job_builder_job(struct thread_data *td, struct job *job){
    switch (job->type) {
    case JOB_DUMP_DATABASE:
      thd_JOB_DUMP_DATABASE(td,job);
      break;
    case JOB_DUMP_ALL_DATABASES:
      thd_JOB_DUMP_ALL_DATABASES(td,job);
      break;
    case JOB_TABLE:
      thd_JOB_TABLE(td, job);
      break;
    case JOB_SHUTDOWN:
      g_free(job);
      return FALSE;
      break;
    default:
      g_error("Something very bad happened!");
  }
  return TRUE;
}

gboolean process_job(struct thread_data *td, struct job *job){
    switch (job->type) {
    case JOB_DUMP:
      thd_JOB_DUMP(td, job);
      break;
    case JOB_DUMP_NON_INNODB:
      thd_JOB_DUMP(td, job);
//      if (g_atomic_int_dec_and_test(&non_innodb_table_counter) &&
//          g_atomic_int_get(&non_innodb_done)) {
//        g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
//      }
      break;
    case JOB_CHECKSUM:
      do_JOB_CHECKSUM(td,job);
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
    case JOB_WRITE_MASTER_STATUS:
      write_snapshot_info(td->thrconn, job->job_data);
      g_free(job);
      break;
    case JOB_SHUTDOWN:
      g_free(job);
      return FALSE;
      break;
    default:
      g_error("Something very bad happened!");
    }
  return TRUE;
}



void process_queue(GAsyncQueue * queue, struct thread_data *td, GMutex *resume_mutex, gboolean (*p)(), void (*f)()){
  struct job *job = NULL;
  for (;;) {
    if (td->conf->pause_resume){
      resume_mutex = (GMutex *)g_async_queue_try_pop(td->conf->pause_resume);
      if (resume_mutex != NULL){
        g_mutex_lock(resume_mutex);
        g_mutex_unlock(resume_mutex);
        resume_mutex=NULL;
        continue;
      }
    }
    if (f!=NULL)
      f();
    job = (struct job *)g_async_queue_pop(queue);
    if (shutdown_triggered && (job->type != JOB_SHUTDOWN)) {
      continue;
    }
    if (!p(td, job)){
      break;
    }
  }
}

void build_lock_tables_statement(struct configuration *conf){
  g_mutex_lock(non_innodb_table_mutex);
  GList *iter = non_innodb_table;
  struct db_table *dbt;
  if ( iter != NULL){
    dbt = (struct db_table *)iter->data;
    conf->lock_tables_statement = g_string_sized_new(30);
    g_string_printf(conf->lock_tables_statement, "LOCK TABLES `%s`.`%s` READ LOCAL",
                      dbt->database->name, dbt->table);
    iter = iter->next;
    for (; iter != NULL; iter = iter->next) {
      dbt = (struct db_table *)iter->data;
      g_string_append_printf(conf->lock_tables_statement, ", `%s`.`%s` READ LOCAL",
                      dbt->database->name, dbt->table);
    }
  }
  g_mutex_unlock(non_innodb_table_mutex);
}


/*void get_next_dbt_and_chunk(struct thread_data *td, struct db_table **dbt,union chunk_step **cs){
  g_mutex_lock(innodb_table_mutex);
  GList *iter=innodb_table;
  union chunk_step *lcs;
  struct db_table *d;
(void)td;
  g_message("get_next_dbt_and_chunk");
  while (iter){
    d=iter->data;
    if (d->chunk_type == NONE){
      *dbt=iter->data;
      innodb_table=g_list_remove(innodb_table,d);
      break;
    }
    lcs=get_next_chunk(d);
    if (lcs!=NULL){
      *cs=lcs;
      *dbt=iter->data;
      break;
    }else{
      iter=iter->next;
      innodb_table=g_list_remove(innodb_table,d);
      continue;
    }   
    iter=iter->next;
  }
  g_mutex_unlock(innodb_table_mutex);  
}
*/



void process_integer_chunk_job(struct thread_data *td, struct table_job *tj){
  g_mutex_lock(tj->chunk_step->integer_step.mutex);
  tj->chunk_step->integer_step.cursor = tj->chunk_step->integer_step.nmin + tj->chunk_step->integer_step.step > tj->chunk_step->integer_step.nmax ? tj->chunk_step->integer_step.nmax : tj->chunk_step->integer_step.nmin + tj->chunk_step->integer_step.step;
  g_mutex_unlock(tj->chunk_step->integer_step.mutex);
  update_where_on_table_job(tj);
  message_dumping_data(td,tj);
  write_table_job_into_file(td->thrconn, tj);
  g_mutex_lock(tj->chunk_step->integer_step.mutex);
  tj->chunk_step->integer_step.nmin=tj->chunk_step->integer_step.cursor;
  g_mutex_unlock(tj->chunk_step->integer_step.mutex);
}



void process_integer_chunk(struct thread_data *td, struct table_job *tj){
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step;
  process_integer_chunk_job(td,tj);
  if (cs->integer_step.prefix)
    g_free(cs->integer_step.prefix);
  cs->integer_step.prefix=NULL;
  while ( cs->integer_step.nmin < cs->integer_step.nmax ){
    process_integer_chunk_job(td,tj);
  }
  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_lock(cs->integer_step.mutex);
  dbt->chunks=g_list_remove(dbt->chunks,cs);
  if (g_list_length(dbt->chunks) == 0){
    g_message("Thread %d: Table %s completed ",td->thread_id,dbt->table);
    dbt->chunks=NULL;
  }
  g_message("Thread %d:Remaining 2 chunks: %d",td->thread_id,g_list_length(dbt->chunks));
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_unlock(cs->integer_step.mutex);
}

void process_char_chunk(struct thread_data *td, struct table_job *tj){
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step;
  while (cs->char_step.list != NULL){
    g_mutex_lock(cs->char_step.mutex);
    cs->char_step.cmin= cs->char_step.list->data;
//    g_free(cmin);
    cs->char_step.list= cs->char_step.list->next;
    cs->char_step.cmax=get_max_char( td->thrconn, dbt, dbt->field, cs->char_step.cmin[0]);
    g_mutex_unlock(cs->char_step.mutex);
    update_where_on_table_job(tj);
    //tj = new_table_job(dbt, NULL, cs->char_step.number, dbt->primary_key, cs);
    message_dumping_data(td,tj);
    write_table_job_into_file(td->thrconn, tj);
    if (cs->char_step.prefix)
      g_free(cs->char_step.prefix);
    cs->char_step.prefix=NULL;
  }
}

void process_partition_chunk(struct thread_data *td, struct table_job *tj){
  union chunk_step *cs = tj->chunk_step;
  gchar *partition=NULL;
  while (cs->partition_step.list != NULL){
    g_mutex_lock(cs->partition_step.mutex);
    partition=g_strdup_printf(" PARTITION (%s) ",(char*)(cs->partition_step.list->data));
    g_message("Partition text: %s", partition);
    cs->partition_step.list= cs->partition_step.list->next;
    g_mutex_unlock(cs->partition_step.mutex);
    tj->partition = partition;
// = new_table_job(dbt, partition ,  cs->partition_step.number, dbt->primary_key, cs);
    message_dumping_data(td,tj);
    write_table_job_into_file(td->thrconn, tj);
    g_free(partition);
  }
}

void *working_thread(struct thread_data *td) {
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

  g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
  // Thread Ready to process jobs
  
  GMutex *resume_mutex=NULL;
  g_message("Thread %d: Creating Jobs", td->thread_id);
  process_queue(td->conf->initial_queue,td, resume_mutex, process_job_builder_job, NULL);

  g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
  process_queue(td->conf->schema_queue,td, resume_mutex, process_job, NULL);

  g_message("Thread %d Schema Done, Starting Non-Innodb", td->thread_id);

  g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1)); 
  g_async_queue_pop(td->conf->ready_non_innodb_queue);
  if (less_locking){
    // Sending LOCK TABLE over all non-innodb tables
    if (mysql_query(td->thrconn, td->conf->lock_tables_statement->str)) {
      g_error("Error locking non-innodb tables %s", mysql_error(td->thrconn));
    }
    // This push will unlock the FTWRL on the Main Connection
    g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
    process_queue(td->conf->non_innodb_queue, td, resume_mutex, process_job, give_me_another_non_innodb_chunk_step);
    if (mysql_query(td->thrconn, UNLOCK_TABLES)) {
      g_error("Error locking non-innodb tables %s", mysql_error(td->thrconn));
    }
  }else{
    process_queue(td->conf->non_innodb_queue, td, resume_mutex, process_job, give_me_another_non_innodb_chunk_step);
    g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
  }

  g_message("Thread %d Non-Innodb Done, Starting Innodb", td->thread_id);
  process_queue(td->conf->innodb_queue, td, resume_mutex, process_job, give_me_another_innodb_chunk_step);
//  start_processing(td, resume_mutex);

  process_queue(td->conf->post_data_queue, td, resume_mutex, process_job, NULL);

  g_message("Thread %d shutting down", td->thread_id);

  if (td->binlog_snapshot_gtid_executed!=NULL)
    g_free(td->binlog_snapshot_gtid_executed);

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
//  g_string_append(field_list, ")");
  mysql_free_result(res);

  return field_list;
}

struct function_pointer pp = {&identity_function,NULL};

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
  GHashTable *ht = g_hash_table_lookup(conf_per_table.all_anonymized_function,k);
  struct function_pointer *fp;
  if (ht){
    while ((row = mysql_fetch_row(res))) {
      fp=(struct function_pointer*)g_hash_table_lookup(ht,row[0]);
      if (fp  != NULL){
        anonymized_function_list=g_list_append(anonymized_function_list,fp);
      }else{
        anonymized_function_list=g_list_append(anonymized_function_list,&pp);
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

gchar *get_character_set_from_collation(MYSQL *conn, gchar *collation){
  g_mutex_lock(character_set_hash_mutex);
  gchar *character_set = g_hash_table_lookup(character_set_hash, collation);
  if (character_set == NULL){
    MYSQL_RES *res = NULL;
    MYSQL_ROW row;
    gchar *query =
      g_strdup_printf("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS "
                      "WHERE collation_name='%s'",
                      collation);
    mysql_query(conn, query);
    g_free(query);
    res = mysql_store_result(conn);
    row = mysql_fetch_row(res);
    g_hash_table_insert(character_set_hash, g_strdup(collation), character_set=g_strdup(row[0]));
    mysql_free_result(res);
  }
  g_mutex_unlock(character_set_hash_mutex);
  return character_set;
}

struct db_table *new_db_table( MYSQL *conn, struct configuration *conf, struct database *database, char *table, char *table_collation, char *datalength){
  struct db_table *dbt = g_new(struct db_table, 1);
  dbt->database = database;
  dbt->table = g_strdup(table);
  dbt->table_filename = get_ref_table(dbt->table);
  dbt->character_set = table_collation==NULL? NULL:get_character_set_from_collation(conn, table_collation);
  dbt->chunk_type = NONE;
  dbt->rows_lock= g_mutex_new();
  dbt->escaped_table = escape_string(conn,dbt->table);
  dbt->anonymized_function=get_anonymized_function_for(conn, dbt->database->name, dbt->table);
  gchar * k = g_strdup_printf("`%s`.`%s`",dbt->database->name,dbt->table);
  dbt->where=g_hash_table_lookup(conf_per_table.all_where_per_table, k);
  dbt->limit=g_hash_table_lookup(conf_per_table.all_limit_per_table, k);
  dbt->num_threads=g_hash_table_lookup(conf_per_table.all_num_threads_per_table, k)?strtoul(g_hash_table_lookup(conf_per_table.all_num_threads_per_table, k), NULL, 10):num_threads;
  dbt->min=NULL;
  dbt->max=NULL;
  dbt->chunks=NULL;
  dbt->chunks_mutex=g_mutex_new();
  dbt->field=get_field_for_dbt(conn,dbt,conf);
  dbt->primary_key = get_primary_key_string(conn, dbt->database->name, dbt->table);
  set_chunk_strategy_for_dbt(conn, dbt);
  g_free(k);
  dbt->complete_insert = complete_insert || detect_generated_fields(conn, dbt->database->escaped, dbt->escaped_table);
  if (dbt->complete_insert) {
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

void free_db_table(struct db_table * dbt){
  g_free(dbt->table);
  g_mutex_free(dbt->rows_lock);
  g_free(dbt->escaped_table);
  g_string_free(dbt->select_fields, TRUE);
  if (dbt->min!=NULL) g_free(dbt->min);
  if (dbt->max!=NULL) g_free(dbt->max);
/*  g_free();
  g_free();
  g_free();*/
  g_free(dbt);
}

void new_table_to_dump(MYSQL *conn, struct configuration *conf, gboolean is_view, struct database * database, char *table, char *collation, char *datalength, gchar *ecol){
    /* Green light! */
  g_mutex_lock(database->ad_mutex);
  if (!database->already_dumped){
    create_job_to_dump_schema(database->name, conf);
    database->already_dumped=TRUE;
  }
  g_mutex_unlock(database->ad_mutex);

  struct db_table *dbt = new_db_table( conn, conf, database, table, collation, datalength);

 // if is a view we care only about schema
  if (!is_view) {
  // with trx_consistency_only we dump all as innodb_table
    if (!no_schemas) {
//      write_table_metadata_into_file(dbt);
      g_mutex_lock(table_schemas_mutex);
      table_schemas=g_list_prepend( table_schemas, dbt) ;
      g_mutex_unlock(table_schemas_mutex);
      create_job_to_dump_table_schema( dbt, conf);
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
          dbt->is_innodb=TRUE;
//          create_job_to_dump_table(dbt,conf);
          g_mutex_lock(innodb_table_mutex);
          innodb_table=g_list_prepend(innodb_table,dbt);
          g_mutex_unlock(innodb_table_mutex);

        } else {
          dbt->is_innodb=FALSE;
//          create_job_to_dump_table(dbt,conf);
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
  guint collcol = -1;
  determine_ecol_ccol(result, &ecol, &ccol, &collcol);
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

    new_table_to_dump(conn, conf, is_view, database, row[0], row[collcol], row[6], row[ecol]);

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
  guint64 rows_count =
      write_table_data_into_file(conn, tj);

  if (!rows_count)
    g_message("Empty table %s.%s", tj->dbt->database->name, tj->dbt->table);
  
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

gboolean real_write_data(FILE *file, float *filesize, GString *data) {
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
  *filesize+=written;
  return TRUE;
}


gboolean write_data(FILE *file, GString *data) {
  float f=0;
  return real_write_data(file, &f, data);
}

void initialize_load_data_statement(GString *statement, gchar * table, gchar *character_set, gchar *basename, MYSQL_FIELD * fields, guint num_fields){
  g_string_append_printf(statement, "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE `%s` ", basename, table);
  if (character_set)
    g_string_append_printf(statement, "CHARACTER SET %s ",character_set);
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
}

void initialize_sql_statement(GString *statement){
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
}

void write_column_into_string( MYSQL *conn, gchar **column, MYSQL_FIELD field, gulong length,GString *escaped, GString *statement_row, struct function_pointer *fun_ptr_i){
  if (load_data){
    if (!*column) {
      g_string_append(statement_row, "\\N");
    }else if (field.type != MYSQL_TYPE_LONG && field.type != MYSQL_TYPE_LONGLONG  && field.type != MYSQL_TYPE_INT24  && field.type != MYSQL_TYPE_SHORT ){
      g_string_append(statement_row,fields_enclosed_by);
      g_string_set_size(escaped, length * 2 + 1);
      escape_function(conn, escaped->str, fun_ptr_i->function(column,fun_ptr_i->memory), length);
      g_string_append(statement_row,escaped->str);
      g_string_append(statement_row,fields_enclosed_by);
    }else
      g_string_append(statement_row, fun_ptr_i->function(column,fun_ptr_i->memory));
  }else{
    /* Don't escape safe formats, saves some time */
    if (!*column) {
      g_string_append(statement_row, "NULL");
    } else if (field.flags & NUM_FLAG) {
      g_string_append(statement_row, fun_ptr_i->function(column,fun_ptr_i->memory));
    } else if ( field.type == MYSQL_TYPE_BLOB ) {
      g_string_set_size(escaped, length * 2 + 1);
      g_string_append(statement_row,"0x");
      mysql_hex_string(escaped->str,fun_ptr_i->function(column,fun_ptr_i->memory),length);
      g_string_append(statement_row,escaped->str);
    } else {
      /* We reuse buffers for string escaping, growing is expensive just at
       * the beginning */
      g_string_set_size(escaped, length * 2 + 1);
      mysql_real_escape_string(conn, escaped->str, fun_ptr_i->function(column,fun_ptr_i->memory), length);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(statement_row, "CONVERT(");
      g_string_append_c(statement_row, '\"');
      g_string_append(statement_row, escaped->str);
      g_string_append_c(statement_row, '\"');
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(statement_row, " USING UTF8MB4)");
    }
  }
}

void write_row_into_string(MYSQL *conn, struct db_table * dbt, MYSQL_ROW row, MYSQL_FIELD *fields, gulong *lengths, guint num_fields, GString *escaped, GString *statement_row){
  guint i = 0;
  g_string_append(statement_row, lines_starting_by);
  GList *f = dbt->anonymized_function;
    struct function_pointer *fun_ptr_i=&pp;
    for (i = 0; i < num_fields; i++) {
      if (f){
        fun_ptr_i=f->data;
        f=f->next;
      }
      write_column_into_string( conn, &(row[i]), fields[i], lengths[i], escaped, statement_row, fun_ptr_i);
      if (i < num_fields - 1) {
        g_string_append(statement_row, fields_terminated_by);
      }
    }
    g_string_append_printf(statement_row,"%s", lines_terminated_by);
}

gboolean write_statement(FILE *load_data_file, float *filessize, GString *statement, struct db_table * dbt){
  if (!real_write_data(load_data_file, filessize, statement)) {
    g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
    return FALSE;
  }
  g_string_set_size(statement, 0);
  return TRUE;
}

void execute_file_per_thread( gchar *sql_fn, gchar *sql_fn3){
  int childpid=fork();
  if(!childpid){
    FILE *sql_file2 = m_open(sql_fn,"r");
    FILE *sql_file3 = m_open(sql_fn3,"w");
    dup2(fileno(sql_file2), STDIN_FILENO);
    dup2(fileno(sql_file3), STDOUT_FILENO);
    execv(exec_per_thread_cmd[0],exec_per_thread_cmd);
    m_close(sql_file2);
    m_close(sql_file3);
  }
}


gboolean write_load_data_statement(struct table_job * tj, MYSQL_FIELD *fields, guint num_fields){
  GString *statement = g_string_sized_new(statement_size);
  char * basename=g_path_get_basename(tj->dat_filename);
  initialize_sql_statement(statement);
  initialize_load_data_statement(statement, tj->dbt->table, tj->dbt->character_set, basename, fields, num_fields);
  if (!write_data(tj->sql_file, statement)) {
    g_critical("Could not write out data for %s.%s", tj->dbt->database->name, tj->dbt->table);
    return FALSE;
  }
  return TRUE;
}

void initialize_fn(gchar ** sql_fn, struct db_table * dbt, guint fn, guint sub_part, const gchar *extension, gchar * f()){
  gchar *stdout_fn=NULL;
  if (use_fifo){
    if (*sql_fn != NULL){
      remove(*sql_fn);
      g_free(*sql_fn);
    }
    *sql_fn = build_fifo_filename(dbt->database->filename, dbt->table_filename, fn, sub_part, extension);
    mkfifo(*sql_fn,0666);
    stdout_fn = build_stdout_filename(dbt->database->filename, dbt->table_filename, fn, sub_part, extension, exec_per_thread_extension);
    execute_file_per_thread(*sql_fn,stdout_fn);
  }else{
    g_free(tj->sql_filename);
    tj->sql_filename = build_data_filename(dbt->database->filename, dbt->table_filename, fn, tj->sub_part);
    tj->sql_file = m_open(tj->sql_filename,"w");
    tj->st_in_file = 0;
    tj->filesize = 0;
  }
}

void initialize_sql_fn(gchar ** sql_fn, struct db_table * dbt, guint fn, guint sub_part){
  initialize_fn(sql_fn,dbt,fn,sub_part,"sql", &build_data_filename);
}

void initialize_load_data_fn(gchar ** sql_fn, struct db_table * dbt, guint fn, guint sub_part){
  initialize_fn(sql_fn,dbt,fn,sub_part,"dat",&build_load_data_filename);
}

guint64 write_row_into_file_in_load_data_mode(MYSQL *conn, MYSQL_RES *result, struct table_job * tj){
  struct db_table * dbt = tj->dbt;
  guint fn = tj->nchunk;
  guint64 num_rows=0;
  GString *escaped = g_string_sized_new(3000);
  guint num_fields = mysql_num_fields(result);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  MYSQL_ROW row;
  GString *statement_row = g_string_sized_new(0);
  GString *statement = g_string_sized_new(statement_size);
  gboolean first_time = FALSE;
  guint sections = tj->where==NULL?1:2;
  if (tj->sql_file == NULL){
     tj->sql_filename = build_data_filename(dbt->database->filename, dbt->table_filename, fn, tj->sub_part);
     tj->sql_file = m_open(tj->sql_filename,"w");
     tj->dat_filename = build_filename(dbt->database->filename, dbt->table_filename, fn, tj->sub_part, "dat");
     tj->dat_file = m_open(tj->dat_filename,"w");
     write_load_data_statement(tj, fields, num_fields); 
     first_time = TRUE;
  }

  while ((row = mysql_fetch_row(result))) {
    gulong *lengths = mysql_fetch_lengths(result);
    num_rows++;
    if ((chunk_filesize &&
        (guint)ceil((float)tj->filesize / 1024 / 1024) >
            chunk_filesize) || first_time) {
      g_message("File size limit reached");
      if (!first_time && !write_statement(tj->dat_file, &(tj->filesize), statement_row, dbt)) {
        return num_rows;
      }


      m_close(tj->sql_file);
      m_close(tj->dat_file);
      tj->sql_file=NULL;
      tj->dat_file=NULL;

      if (stream) {
        g_async_queue_push(stream_queue, g_strdup(tj->sql_filename));
        g_async_queue_push(stream_queue, g_strdup(tj->dat_filename));
  
      }

      char * basename=g_path_get_basename(load_data_fn);

      initialize_sql_statement(statement);
      initialize_load_data_statement(statement, dbt->table, dbt->character_set, basename, fields, num_fields);

      tj->sql_filename = build_data_filename(dbt->database->filename, dbt->table_filename, fn, tj->sub_part);
      tj->sql_file = m_open(tj->sql_filename,"w");
      tj->dat_filename = build_filename(dbt->database->filename, dbt->table_filename, fn, tj->sub_part, "dat");
      tj->dat_file = m_open(tj->dat_filename,"w");
      write_load_data_statement(tj, fields, num_fields);
      first_time=FALSE;
      if (sections == 1){
        fn++;
      }else{
        tj->sub_part++;
      }  
    }
    g_string_set_size(statement_row, 0);
    write_row_into_string(conn, dbt, row, fields, lengths, num_fields, escaped, statement_row);
    tj->filesize+=statement_row->len+1;
    g_string_append(statement, statement_row->str);
    /* INSERT statement is closed before over limit but this is load data, so we only need to flush the data to disk*/
    if (statement->len + statement_row->len + 1 > statement_size) {
      if (!write_statement(tj->dat_file, &(tj->filesize), statement, dbt)) {
        return num_rows;
      }
    }
  }
  if (statement->len > 0)
    if (!real_write_data(tj->dat_file, &(tj->filesize), statement)) {
      g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
      return num_rows;
    }
  if (!compress_output) {
    if (tj->sql_file) { 
      fclose((FILE *)tj->sql_file);
      tj->sql_file=NULL;
      if (stream && tj->sql_filename) g_async_queue_push(stream_queue, g_strdup(tj->sql_filename));
    }
    if (tj->dat_filename){
      fclose((FILE *)tj->dat_file);
      tj->dat_file=NULL;
      if (stream && tj->dat_filename) g_async_queue_push(stream_queue, g_strdup(tj->dat_filename));
    }
  }else{
    if (tj->sql_file) {
      gzclose((gzFile)tj->sql_file);
      tj->sql_file=NULL;
      if (stream && tj->sql_filename) g_async_queue_push(stream_queue, g_strdup(tj->sql_filename));
    }
    if (tj->dat_file){
      gzclose((gzFile)tj->dat_file);
      tj->dat_file=NULL;
      if (stream && tj->dat_filename) g_async_queue_push(stream_queue, g_strdup(tj->dat_filename));
    }
  }
  if (use_fifo && (load_data_fn != NULL)){
    remove(load_data_fn);
    g_free(load_data_fn);
  }
  return num_rows;
}

guint64 write_row_into_file_in_sql_mode(MYSQL *conn, MYSQL_RES *result, struct table_job * tj){
  // There are 2 possible options to chunk the files:
  // - no chunk: this means that will be just 1 data file
  // - chunk_filesize: this function will be spliting the per filesize, this means that multiple files will be created
  // Split by row is before this step
  // It could write multiple INSERT statments in a data file if statement_size is reached
  struct db_table * dbt = tj->dbt;
  guint sections = tj->where==NULL?1:2;
  guint num_fields = mysql_num_fields(result);
  GString *escaped = g_string_sized_new(3000);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  MYSQL_ROW row;
  GString *statement = g_string_sized_new(statement_size);
  GString *statement_row = g_string_sized_new(0);
  gulong *lengths = NULL;
  guint64 num_rows = 0;
  guint64 num_rows_st = 0;  
  guint fn = nchunk;
  if (tj->sql_file == NULL){
    initialize_sql_fn(&sql_fn, dbt, fn, sub_part);

  while ((row = mysql_fetch_row(result))) {
    lengths = mysql_fetch_lengths(result);
    num_rows++;

    if (!statement->len) {
      // if statement->len is 0 we consider that new statement needs to be written
      // A file can be chunked by amount of rows or file size.
      if (!tj->st_in_file) {
        // File Header
        initialize_sql_statement(statement);
        if (!real_write_data(tj->sql_file, &(tj->filesize), statement)) {
          g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
          return num_rows;
        }
      }
      append_insert (dbt->complete_insert, statement, dbt->table, fields, num_fields);
      num_rows_st = 0;
    }

    if (statement_row->len) {
      // previous row needs to be written
      g_string_append(statement, statement_row->str);
      g_string_set_size(statement_row, 0);
      num_rows_st++;
    }

    write_row_into_string(conn, dbt, row, fields, lengths, num_fields, escaped, statement_row);

    if (statement->len + statement_row->len + 1 > statement_size) {
      // We need to flush the statement into disk
      if (num_rows_st == 0) {
        g_string_append(statement, statement_row->str);
        g_string_set_size(statement_row, 0);
        g_warning("Row bigger than statement_size for %s.%s", dbt->database->name,
                  dbt->table);
      }
      g_string_append(statement, statement_terminated_by);

      if (!real_write_data(tj->sql_file, &(tj->filesize), statement)) {
        g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
        return num_rows;
      }
      tj->st_in_file++;
      if (chunk_filesize &&
          (guint)ceil((float)tj->filesize / 1024 / 1024) >
              chunk_filesize) {
        // We reached the file size limit, we need to rotate the file
        if (sections == 1){
          fn++;
        }else{
          tj->sub_part++;
        }
        m_close(tj->sql_file);
        tj->sql_file=NULL;
        if (stream) {
          g_async_queue_push(stream_queue, g_strdup(tj->sql_filename));
        }
        initialize_sql_fn(&sql_fn, dbt, fn, sub_part);
        st_in_file = 0;
      }
      g_string_set_size(statement, 0);
    } else {
      if (num_rows_st)
        g_string_append_c(statement, ',');
      g_string_append(statement, statement_row->str);
      num_rows_st++;
      g_string_set_size(statement_row, 0);
    }
  }
  if (statement_row->len > 0) {
    /* this last row has not been written out */
    if (!statement->len)
      append_insert (dbt->complete_insert, statement, dbt->table, fields, num_fields);
    g_string_append(statement, statement_row->str);
  }

  if (statement->len > 0) {
    g_string_append(statement, statement_terminated_by);
    if (!real_write_data(tj->sql_file, &(tj->filesize), statement)) {
      g_critical(
          "Could not write out closing newline for %s.%s, now this is sad!",
          dbt->database->name, dbt->table);
      return num_rows;
    }
    tj->st_in_file++;
  }
  g_mutex_lock(dbt->rows_lock);
  dbt->rows+=num_rows;
  g_mutex_unlock(dbt->rows_lock);
  g_string_free(statement, TRUE);
  g_string_free(escaped, TRUE);
  g_string_free(statement_row, TRUE);
  return num_rows;
}

/* Do actual data chunk reading/writing magic */
guint64 write_table_data_into_file(MYSQL *conn, struct table_job * tj){
  guint64 num_rows = 0;
//  guint64 num_rows_st = 0;
  MYSQL_RES *result = NULL;
  char *query = NULL;

  /* Ghm, not sure if this should be statement_size - but default isn't too big
   * for now */
  /* Poor man's database code */
  query = g_strdup_printf(
      "SELECT %s %s FROM `%s`.`%s` %s %s %s %s %s %s %s %s %s %s %s",
      (detected_server == SERVER_TYPE_MYSQL) ? "/*!40001 SQL_NO_CACHE */" : "", 
      tj->dbt->select_fields->str, 
      tj->dbt->database->name, tj->dbt->table, tj->partition?tj->partition:"", 
       (tj->where || where_option   || tj->dbt->where) ? "WHERE"  : "" ,      tj->where ?      tj->where : "",  
       (tj->where && where_option )                    ? "AND"    : "" ,   where_option ?   where_option : "", 
      ((tj->where || where_option ) && tj->dbt->where) ? "AND"    : "" , tj->dbt->where ? tj->dbt->where : "", 
      tj->order_by ? "ORDER BY" : "", tj->order_by   ? tj->order_by   : "", 
      tj->dbt->limit ?  "LIMIT" : "", tj->dbt->limit ? tj->dbt->limit : "");
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    // ERROR 1146
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping table (%s.%s) data: %s\nQuery: %s", tj->dbt->database->name, tj->dbt->table,
                mysql_error(conn), query);
    } else {
      g_critical("Error dumping table (%s.%s) data: %s\nQuery: %s ", tj->dbt->database->name, tj->dbt->table,
                 mysql_error(conn), query);
      errors++;
    }
    goto cleanup;
  }

  /* Poor man's data dump code */
  if (load_data)
    num_rows = write_row_into_file_in_load_data_mode(conn, result, tj);
  else
    num_rows=write_row_into_file_in_sql_mode(conn, result, tj);

  if (mysql_errno(conn)) {
    g_critical("Could not read data from %s.%s: %s", tj->dbt->database->name, tj->dbt->table,
               mysql_error(conn));
    errors++;
  }

cleanup:
  g_free(query);

  if (result) {
    mysql_free_result(result);
  }

  return num_rows;
}


