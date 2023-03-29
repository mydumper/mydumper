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
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
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
#include "common.h"
#include "mydumper_start_dump.h"
#include "mydumper_jobs.h"
#include "mydumper_common.h"
#include "mydumper_stream.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_pmm_thread.h"
#include "mydumper_exec_command.h"
#include "mydumper_masquerade.h"
#include "mydumper_chunks.h"
#include "mydumper_write.h"
/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

#include "mydumper_global.h"

/* Program options */
gchar *tidb_snapshot = NULL;
GList *no_updated_tables = NULL;
int longquery = 60;
int longquery_retries = 0;
int longquery_retry_interval = 60;
int need_dummy_read = 0;
int need_dummy_toku_read = 0;
int compress_output = 0;
int killqueries = 0;
int lock_all_tables = 0;
gboolean no_schemas = FALSE;
gboolean no_locks = FALSE;
gboolean it_is_a_consistent_backup = FALSE;
gboolean less_locking = FALSE;
gboolean no_backup_locks = FALSE;
gboolean no_ddl_locks = FALSE;
gboolean dump_tablespaces = FALSE;
GList *all_dbts = NULL;
GList *table_schemas = NULL;
GList *trigger_schemas = NULL;
GList *view_schemas = NULL;
GList *schema_post = NULL;
//gint non_innodb_table_counter = 0;
//gint schema_counter = 0;
gint non_innodb_done = 0;
guint updated_since = 0;
guint trx_consistency_only = 0;
gchar *pmm_resolution = NULL;
gchar *pmm_path = NULL;
gboolean pmm = FALSE;
guint pause_at=0;
guint resume_at=0;
gchar **db_items=NULL;

//GRecMutex *ready_database_dump_mutex = NULL;
GRecMutex *ready_table_dump_mutex = NULL;

struct configuration_per_table conf_per_table = {NULL, NULL, NULL, NULL};
gchar *exec_command=NULL;

void initialize_start_dump(){
  initialize_set_names();
  initialize_working_thread();
  conf_per_table.all_anonymized_function=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_where_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_limit_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_num_threads_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  // until we have an unique option on lock types we need to ensure this
  if (no_locks || trx_consistency_only)
    less_locking = 0;

  // clarify binlog coordinates with trx_consistency_only
  if (trx_consistency_only)
    g_warning("Using trx_consistency_only, binlog coordinates will not be "
              "accurate if you are writing to non transactional tables.");

  if (db){
    db_items=g_strsplit(db,",",0);
  }

  if (pmm_path){
    pmm=TRUE;
    if (!pmm_resolution){
      pmm_resolution=g_strdup("high");
    }
  }else if (pmm_resolution){
    pmm=TRUE;
    pmm_path=g_strdup_printf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution",pmm_resolution);
  }

  if (stream && exec_command != NULL){
    m_critical("Stream and execute a command is not supported");
  }
}

void set_disk_limits(guint p_at, guint r_at){
  pause_at=p_at;
  resume_at=r_at;
}

gboolean is_disk_space_ok(guint val){
  struct statvfs buffer;
  int ret = statvfs(output_directory, &buffer);
  if (!ret) {
    const double available = (double)(buffer.f_bfree * buffer.f_frsize) / 1024 / 1024;
    return available > val;
  }else{
    g_warning("Disk space check failed");
  }
  return TRUE;
}

void *monitor_disk_space_thread (void *queue){
  (void)queue;
  guint i=0;
  GMutex **pause_mutex_per_thread=g_new(GMutex * , num_threads) ;
  for(i=0;i<num_threads;i++){
    pause_mutex_per_thread[i]=g_mutex_new();
  }

  gboolean previous_state = TRUE, current_state = TRUE;

  while (disk_limits != NULL){
    current_state = previous_state ? is_disk_space_ok(pause_at) : is_disk_space_ok(resume_at);
    if (previous_state != current_state){
      if (!current_state){
        g_warning("Pausing backup disk space lower than %dMB. You need to free up to %dMB to resume",pause_at,resume_at);
        for(i=0;i<num_threads;i++){
          g_mutex_lock(pause_mutex_per_thread[i]);
          g_async_queue_push(queue,pause_mutex_per_thread[i]);
        }
      }else{
        g_warning("Resuming backup");
        for(i=0;i<num_threads;i++){
          g_mutex_unlock(pause_mutex_per_thread[i]);
        }
      }
      previous_state = current_state;

    }
    sleep(10);
  }
  return NULL;
}

GMutex **pause_mutex_per_thread=NULL;

gboolean sig_triggered(void * user_data, int signal) {
  if (signal == SIGTERM){
    shutdown_triggered = TRUE;
  }else{

    guint i=0;
    if (pause_mutex_per_thread == NULL){
      pause_mutex_per_thread=g_new(GMutex * , num_threads) ;
      for(i=0;i<num_threads;i++){
        pause_mutex_per_thread[i]=g_mutex_new();
      }
    }
    if (((struct configuration *)user_data)->pause_resume == NULL)
      ((struct configuration *)user_data)->pause_resume = g_async_queue_new();
    GAsyncQueue *queue = ((struct configuration *)user_data)->pause_resume;
    if (!daemon_mode){
      fprintf(stdout, "Ctrl+c detected! Are you sure you want to cancel(Y/N)?");
      for(i=0;i<num_threads;i++){
        g_mutex_lock(pause_mutex_per_thread[i]);
        g_async_queue_push(queue,pause_mutex_per_thread[i]);
      }
      int c=0;
      while (1){
        do{
          c=fgetc(stdin);
        }while (c=='\n');
        if ( c == 'N' || c == 'n'){
          for(i=0;i<num_threads;i++)
            g_mutex_unlock(pause_mutex_per_thread[i]);
          return TRUE;
        }
        if ( c == 'Y' || c == 'y'){
          shutdown_triggered = TRUE;
          for(i=0;i<num_threads;i++)
            g_mutex_unlock(pause_mutex_per_thread[i]);
          goto finish;
        }
      }
    }
  }
finish:
  g_message("Shutting down gracefully");
  return FALSE;
}

gboolean sig_triggered_int(void * user_data) {
  return sig_triggered(user_data,SIGINT);
}
gboolean sig_triggered_term(void * user_data) {
  return sig_triggered(user_data,SIGTERM);
}

void *signal_thread(void *data) {
  g_unix_signal_add(SIGINT, sig_triggered_int, data);
  g_unix_signal_add(SIGTERM, sig_triggered_term, data);
  ((struct configuration *)data)->loop  = g_main_loop_new (NULL, TRUE);
  g_main_loop_run (((struct configuration *)data)->loop);
  g_message("Ending signal thread");
  return NULL;
}


GHashTable * mydumper_initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=initialize_hash_of_session_variables();
  g_hash_table_insert(set_session_hash,g_strdup("information_schema_stats_expiry"),g_strdup("0 /*!80003"));
  return set_session_hash;
}

MYSQL *create_connection() {
  MYSQL *conn;
  conn = mysql_init(NULL);

  char *mydumper=g_strdup("mydumper");
  m_connect(conn, mydumper ,db_items!=NULL?db_items[0]:db);
  g_free(mydumper);

  execute_gstring(conn, set_session);
  return conn;
}


MYSQL *create_main_connection() {
  MYSQL *conn;
  conn = mysql_init(NULL);

  char *mydumper=g_strdup("mydumper");
  m_connect(conn, mydumper ,db_items!=NULL?db_items[0]:db);
  g_free(mydumper);

  set_session = g_string_new(NULL);
  set_global = g_string_new(NULL);
  set_global_back = g_string_new(NULL);
  detected_server = detect_server(conn);
  GHashTable * set_session_hash = mydumper_initialize_hash_of_session_variables();
  GHashTable * set_global_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  if (key_file != NULL ){
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_global_hash,"mydumper_global_variables");
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_session_hash,"mydumper_session_variables");
    load_per_table_info_from_key_file(key_file, &conf_per_table, &init_function_pointer);
  }
  refresh_set_session_from_hash(set_session,set_session_hash);
  refresh_set_global_from_hash(set_global, set_global_back, set_global_hash);
  free_hash_table(set_session_hash);
  g_hash_table_unref(set_session_hash);
  execute_gstring(conn, set_session);
  execute_gstring(conn, set_global);

  switch (detected_server) {
  case SERVER_TYPE_MYSQL:
    g_message("Connected to a MySQL server");
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  case SERVER_TYPE_MARIADB:
    g_message("Connected to a MariaDB server");
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  case SERVER_TYPE_TIDB:
    g_message("Connected to a TiDB server");
    break;
  default:
    m_critical("Cannot detect server type");
    break;
  }

  return conn;
}

void get_not_updated(MYSQL *conn, FILE *file) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  gchar *query =
      g_strdup_printf("SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM "
                      "information_schema.TABLES WHERE TABLE_TYPE = 'BASE "
                      "TABLE' AND UPDATE_TIME < NOW() - INTERVAL %d DAY",
                      updated_since);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  while ((row = mysql_fetch_row(res))) {
    no_updated_tables = g_list_prepend(no_updated_tables, row[0]);
    fprintf(file, "%s\n", row[0]);
  }
  no_updated_tables = g_list_reverse(no_updated_tables);
  fflush(file);
}

void long_query_wait(MYSQL *conn){
  char *p3=NULL;
    while (TRUE) {
      int longquery_count = 0;
      if (mysql_query(conn, "SHOW PROCESSLIST")) {
        g_warning("Could not check PROCESSLIST, no long query guard enabled: %s",
                  mysql_error(conn));
        break;
      } else {
       MYSQL_RES *res = mysql_store_result(conn);
        MYSQL_ROW row;

        /* Just in case PROCESSLIST output column order changes */
        MYSQL_FIELD *fields = mysql_fetch_fields(res);
        guint i;
        int tcol = -1, ccol = -1, icol = -1, ucol = -1;
        for (i = 0; i < mysql_num_fields(res); i++) {
        if (!strcasecmp(fields[i].name, "Command"))
            ccol = i;
          else if (!strcasecmp(fields[i].name, "Time"))
            tcol = i;
          else if (!strcasecmp(fields[i].name, "Id"))
            icol = i;
          else if (!strcasecmp(fields[i].name, "User"))
            ucol = i;
        }
        if ((tcol < 0) || (ccol < 0) || (icol < 0)) {
          m_critical("Error obtaining information from processlist");
        }
        while ((row = mysql_fetch_row(res))) {
          if (row[ccol] && strcmp(row[ccol], "Query"))
            continue;
          if (row[ucol] && !strcmp(row[ucol], "system user"))
            continue;
          if (row[tcol] && atoi(row[tcol]) > longquery) {
            if (killqueries) {
              if (mysql_query(conn,
                              p3 = g_strdup_printf("KILL %lu", atol(row[icol])))) {
                g_warning("Could not KILL slow query: %s", mysql_error(conn));
                longquery_count++;
              } else {
                g_warning("Killed a query that was running for %ss", row[tcol]);
              }
              g_free(p3);
            } else {
              longquery_count++;
            }
          }
        }
        mysql_free_result(res);
        if (longquery_count == 0)
          break;
        else {
          if (longquery_retries == 0) {
            m_critical("There are queries in PROCESSLIST running longer than "
                       "%us, aborting dump,\n\t"
                       "use --long-query-guard to change the guard value, kill "
                       "queries (--kill-long-queries) or use \n\tdifferent "
                       "server for dump",
                       longquery);
          }
          longquery_retries--;
          g_warning("There are queries in PROCESSLIST running longer than "
                         "%us, retrying in %u seconds (%u left).",
                         longquery, longquery_retry_interval, longquery_retries);
          sleep(longquery_retry_interval);
        }
      }
    }
}

void send_mariadb_backup_locks(MYSQL *conn){
  if (mysql_query(conn, "BACKUP STAGE START")) {
    m_critical("Couldn't acquire BACKUP STAGE START: %s",
               mysql_error(conn));
    errors++;
  }

  if (mysql_query(conn, "BACKUP STAGE FLUSH")) {
    m_critical("Couldn't acquire BACKUP STAGE FLUSH: %s",
               mysql_error(conn));
    errors++;
  }
  if (mysql_query(conn, "BACKUP STAGE BLOCK_DDL")) {
    m_critical("Couldn't acquire BACKUP STAGE BLOCK_DDL: %s",
               mysql_error(conn));
    errors++;
  }

  if (mysql_query(conn, "BACKUP STAGE BLOCK_COMMIT")) {
    m_critical("Couldn't acquire BACKUP STAGE BLOCK_COMMIT: %s",
               mysql_error(conn));
    errors++;
  }
}

void send_percona57_backup_locks(MYSQL *conn){
  if (mysql_query(conn, "LOCK TABLES FOR BACKUP")) {
    m_critical("Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will "
               "not be consistent: %s",
               mysql_error(conn));
    errors++;
  }

  if (mysql_query(conn, "LOCK BINLOG FOR BACKUP")) {
    m_critical("Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will "
               "not be consistent: %s",
               mysql_error(conn));
    errors++;
  }
}

void send_lock_instance_backup(MYSQL *conn){
  if (mysql_query(conn, "LOCK INSTANCE FOR BACKUP")) {
    m_critical("Couldn't acquire LOCK INSTANCE FOR BACKUP: %s",
               mysql_error(conn));
    errors++;
  }
} 

void send_unlock_tables(MYSQL *conn){
  mysql_query(conn, "UNLOCK TABLES");
}

void send_unlock_binlogs(MYSQL *conn){
  mysql_query(conn, "UNLOCK BINLOG");
}

void send_unlock_instance_backup(MYSQL *conn){
  mysql_query(conn, "UNLOCK INSTANCE");
}

void send_backup_stage_end(MYSQL *conn){
  mysql_query(conn, "BACKUP STAGE END");
}

void send_flush_table_with_read_lock(MYSQL *conn){
        g_message("Sending Flush Table");
        if (mysql_query(conn, "FLUSH NO_WRITE_TO_BINLOG TABLES")) {
          g_warning("Flush tables failed, we are continuing anyways: %s",
                   mysql_error(conn));
        }
        g_message("Acquiring FTWRL");
       if (mysql_query(conn, "FLUSH TABLES WITH READ LOCK")) {
          g_critical("Couldn't acquire global lock, snapshots will not be "
                   "consistent: %s",
                   mysql_error(conn));
          errors++;
        }
}
void determine_ddl_lock_function(MYSQL ** conn, void(**flush_table)(MYSQL *), void (**acquire_lock_function)(MYSQL *), void (** release_lock_function)(MYSQL *), void (** release_binlog_function)(MYSQL *)) {
  mysql_query(*conn, "SELECT @@version_comment, @@version");
  MYSQL_RES *res2 = mysql_store_result(*conn);
  MYSQL_ROW ver;
  while ((ver = mysql_fetch_row(res2))) {
    if (g_str_has_prefix(ver[0], "Percona")){
      if (g_str_has_prefix(ver[1], "8.")) {
        *acquire_lock_function = &send_lock_instance_backup;
        *release_lock_function = &send_unlock_instance_backup;
        break;
      }
      if (g_str_has_prefix(ver[1], "5.7.")) {
        *acquire_lock_function = &send_percona57_backup_locks;
        *release_binlog_function = &send_unlock_binlogs;
        *release_lock_function = &send_unlock_tables;
        *conn = create_connection();
        break;
      }
    }
    if (g_str_has_prefix(ver[0], "MySQL")){
      if (g_str_has_prefix(ver[1], "8.")) {
        *acquire_lock_function = &send_lock_instance_backup;
        *release_lock_function = &send_unlock_instance_backup;
        break;
      }
    }
    if (g_str_has_prefix(ver[0], "mariadb")){
      if ((g_str_has_prefix(ver[1], "10.5")) || 
          (g_str_has_prefix(ver[1], "10.6"))) {
        *flush_table = NULL;
        *acquire_lock_function = &send_mariadb_backup_locks;
        *release_lock_function = &send_backup_stage_end;
        break;
      }
    }
  }
  mysql_free_result(res2);
}


void send_lock_all_tables(MYSQL *conn){
  // LOCK ALL TABLES
  GString *query = g_string_sized_new(16777216);
  gchar *dbtb = NULL;
  gchar **dt = NULL;
  GList *tables_lock = NULL;
  GList *iter = NULL;
  guint success = 0;
  guint retry = 0;
  guint lock = 1;
  guint i = 0;

  if (tables) {
    for (i = 0; tables[i] != NULL; i++) {
      dt = g_strsplit(tables[i], ".", 0);
      if (tables_skiplist_file && check_skiplist(dt[0], dt[1]))
        continue;
      if (!eval_regex(dt[0], dt[1]))
        continue;
      dbtb = g_strdup_printf("`%s`.`%s`", dt[0], dt[1]);
      tables_lock = g_list_prepend(tables_lock, dbtb);
    }
    tables_lock = g_list_reverse(tables_lock);
  }else{
    if (db) {
      GString *db_quoted_list=NULL;
      db_quoted_list=g_string_sized_new(strlen(db));
      g_string_append_printf(db_quoted_list,"'%s'",db_items[i]);
      i++;
      while (i<g_strv_length(db_items)){
        g_string_append_printf(db_quoted_list,",'%s'",db_items[i]);
        i++;
      }

      g_string_printf(
            query,
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA in (%s) AND TABLE_TYPE ='BASE TABLE' AND NOT "
            "(TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR "
            "TABLE_NAME = 'general_log'))",
            db_quoted_list->str);
    } else {
      g_string_printf(
        query,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_TYPE ='BASE TABLE' AND TABLE_SCHEMA NOT IN "
        "('information_schema', 'performance_schema', 'data_dictionary') "
        "AND NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR "
        "TABLE_NAME = 'general_log'))");
    }
  }

  if (tables_lock == NULL && query->len > 0  ) {
    if (mysql_query(conn, query->str)) {
      g_critical("Couldn't get table list for lock all tables: %s",
                 mysql_error(conn));
      errors++;
    } else {
      MYSQL_RES *res = mysql_store_result(conn);
      MYSQL_ROW row;

      while ((row = mysql_fetch_row(res))) {
        lock = 1;
        if (tables) {
          int table_found = 0;
          for (i = 0; tables[i] != NULL; i++)
            if (g_ascii_strcasecmp(tables[i], row[1]) == 0)
              table_found = 1;
          if (!table_found)
              lock = 0;
        }
        if (lock && tables_skiplist_file && check_skiplist(row[0], row[1]))
          continue;
        if (lock && !eval_regex(row[0], row[1]))
          continue;
        if (lock) {
          dbtb = g_strdup_printf("`%s`.`%s`", row[0], row[1]);
          tables_lock = g_list_prepend(tables_lock, dbtb);
        }
      }
      tables_lock = g_list_reverse(tables_lock);
    }
  }
  if (tables_lock != NULL) {
  // Try three times to get the lock, this is in case of tmp tables
  // disappearing
    while (!success && retry < 4) {
      g_string_set_size(query,0);
      g_string_append(query, "LOCK TABLE");
      for (iter = tables_lock; iter != NULL; iter = iter->next) {
        g_string_append_printf(query, "%s READ,", (char *)iter->data);
      }
      g_strrstr(query->str,",")[0]=' ';

      if (mysql_query(conn, query->str)) {
        gchar *failed_table = NULL;
        gchar **tmp_fail;

        tmp_fail = g_strsplit(mysql_error(conn), "'", 0);
        tmp_fail = g_strsplit(tmp_fail[1], ".", 0);
        failed_table = g_strdup_printf("`%s`.`%s`", tmp_fail[0], tmp_fail[1]);
        for (iter = tables_lock; iter != NULL; iter = iter->next) {
          if (strcmp(iter->data, failed_table) == 0) {
            tables_lock = g_list_remove(tables_lock, iter->data);
          }
        }
        g_free(tmp_fail);
        g_free(failed_table);
      } else {
        success = 1;
      }
      retry += 1;
    }
    if (!success) {
      m_critical("Lock all tables fail: %s", mysql_error(conn));
    }
  }else{
    g_warning("No table found to lock");
//    exit(EXIT_FAILURE);
  }
  g_free(query->str);
  g_list_free(tables_lock);
}

void start_dump() {
  initialize_start_dump();
  initialize_common();
  initialize_connection(key_file!=NULL && g_key_file_has_group(key_file,"mydumper")?defaults_file:NULL);
  initialize_masquerade();

  /* Give ourselves an array of tables to dump */
  if (tables_list)
    tables = get_table_list(tables_list);

  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);

  /* Validate that thread count passed on CLI is a valid count */
  check_num_threads();

  initialize_regex();

  MYSQL *conn = create_main_connection();
  main_connection = conn;
  MYSQL *second_conn = conn;
  struct configuration conf = {1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};
  char *metadata_partial_filename, *metadata_filename;
  char *u;
  detect_server_version(conn);
  void (*flush_table_function)(MYSQL *) = &send_flush_table_with_read_lock;
  void (*acquire_ddl_lock_function)(MYSQL *) = NULL;
  void (*release_ddl_lock_function)(MYSQL *) = NULL;
  void (*release_binlog_function)(MYSQL *) = NULL;
  struct db_table *dbt=NULL;
//  struct schema_post *sp;
  guint n;
  FILE *nufile = NULL;
  GThread *disk_check_thread = NULL;
  if (disk_limits!=NULL){
    conf.pause_resume = g_async_queue_new();
    disk_check_thread = g_thread_create(monitor_disk_space_thread, conf.pause_resume, FALSE, NULL);
  }
  GThread *sthread = NULL;
  if (!daemon_mode){
    GError *serror;
    sthread =
        g_thread_create(signal_thread, &conf, FALSE, &serror);
    if (sthread == NULL) {
      m_critical("Could not create signal thread: %s", serror->message);
      g_error_free(serror);
    }
  }


  GThread *pmmthread = NULL;
  if (pmm){

    g_message("Using PMM resolution %s at %s", pmm_resolution, pmm_path);
    GError *serror;
    pmmthread =
        g_thread_create(pmm_thread, &conf, FALSE, &serror);
    if (pmmthread == NULL) {
      m_critical("Could not create pmm thread: %s", serror->message);
      g_error_free(serror);
    }
  }

  metadata_partial_filename = g_strdup_printf("%s/metadata.partial", dump_directory);
  metadata_filename = g_strndup(metadata_partial_filename, (unsigned)strlen(metadata_partial_filename) - 8);

  FILE *mdfile = g_fopen(metadata_partial_filename, "w");
  if (!mdfile) {
    m_critical("Couldn't write metadata file %s (%d)", metadata_partial_filename, errno);
  }

  if (updated_since > 0) {
    u = g_strdup_printf("%s/not_updated_tables", dump_directory);
    nufile = g_fopen(u, "w");
    if (!nufile) {
      m_critical("Couldn't write not_updated_tables file (%d)", errno);
    }
    get_not_updated(conn, nufile);
  }

  if (!no_locks) {
  // We check SHOW PROCESSLIST, and if there're queries
  // larger than preset value, we terminate the process.
  // This avoids stalling whole server with flush.
		long_query_wait(conn);
  }

  if (detected_server == SERVER_TYPE_TIDB) {
    g_message("Skipping locks because of TiDB");
    if (!tidb_snapshot) {

      // Generate a @@tidb_snapshot to use for the worker threads since
      // the tidb-snapshot argument was not specified when starting mydumper

      if (mysql_query(conn, "SHOW MASTER STATUS")) {
        m_critical("Couldn't generate @@tidb_snapshot: %s", mysql_error(conn));
      } else {

        MYSQL_RES *result = mysql_store_result(conn);
        MYSQL_ROW row = mysql_fetch_row(
            result); /* There should never be more than one row */
        tidb_snapshot = g_strdup(row[1]);
        mysql_free_result(result);
      }
    }

    // Need to set the @@tidb_snapshot for the master thread
    gchar *query =
        g_strdup_printf("SET SESSION tidb_snapshot = '%s'", tidb_snapshot);

    g_message("Set to tidb_snapshot '%s'", tidb_snapshot);

    if (mysql_query(conn, query)) {
      m_critical("Failed to set tidb_snapshot: %s", mysql_error(conn));
    }
    g_free(query);

  }else{

    if (!no_locks) {
      // This backup will lock the database
      if (!no_backup_locks)
        determine_ddl_lock_function(&second_conn,&flush_table_function, &acquire_ddl_lock_function,&release_ddl_lock_function, &release_binlog_function);

      if (lock_all_tables) {
        send_lock_all_tables(conn);
      } else {
        if (flush_table_function != NULL) {
          flush_table_function(conn);
        }
        if (acquire_ddl_lock_function != NULL) {
          g_message("Acquiring DDL lock");
          acquire_ddl_lock_function(second_conn);
        }
      }
    } else {
      g_warning("Executing in no-locks mode, snapshot might not be consistent");
    }
  }


// TODO: this should be deleted on future releases. 
  if (mysql_get_server_version(conn) < 40108) {
    mysql_query(
        conn,
        "CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB");
    need_dummy_read = 1;
  }

  // tokudb do not support consistent snapshot
  mysql_query(conn, "SELECT @@tokudb_version");
  MYSQL_RES *rest = mysql_store_result(conn);
  if (rest != NULL && mysql_num_rows(rest)) {
    mysql_free_result(rest);
    g_message("TokuDB detected, creating dummy table for CS");
    mysql_query(
        conn,
        "CREATE TABLE IF NOT EXISTS mysql.tokudbdummy (a INT) ENGINE=TokuDB");
    need_dummy_toku_read = 1;
  }

  // Do not start a transaction when lock all tables instead of FTWRL,
  // since it can implicitly release read locks we hold
  // TODO: this should be deleted as main connection is not being used for export data
//  if (!lock_all_tables) {
//    g_message("Sending start transaction in main connection");
//    mysql_query(conn, "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */");
//  }

  if (need_dummy_read) {
    mysql_query(conn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy");
    MYSQL_RES *res = mysql_store_result(conn);
    if (res)
      mysql_free_result(res);
  }
  if (need_dummy_toku_read) {
    mysql_query(conn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy");
    MYSQL_RES *res = mysql_store_result(conn);
    if (res)
      mysql_free_result(res);
  }

  GDateTime *datetime = g_date_time_new_now_local();
  char *datetimestr=g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S");
  fprintf(mdfile, "# Started dump at: %s\n", datetimestr);
  g_message("Started dump at: %s", datetimestr);
  g_free(datetimestr);

  if (stream){
    initialize_stream();
  }

  if (exec_command != NULL){
    initialize_exec_command();
    stream=TRUE;
  
  }


  conf.initial_queue = g_async_queue_new();
  conf.schema_queue = g_async_queue_new();
  conf.post_data_queue = g_async_queue_new();
  conf.innodb_queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.non_innodb_queue = g_async_queue_new();
  conf.ready_non_innodb_queue = g_async_queue_new();
  conf.unlock_tables = g_async_queue_new();
  conf.gtid_pos_checked = g_async_queue_new();
  conf.are_all_threads_in_same_pos = g_async_queue_new();
  conf.db_ready = g_async_queue_new();
//  ready_database_dump_mutex = g_rec_mutex_new();
//  g_rec_mutex_lock(ready_database_dump_mutex);
  ready_table_dump_mutex = g_rec_mutex_new();
  g_rec_mutex_lock(ready_table_dump_mutex);

  g_message("conf created");

  if (detected_server == SERVER_TYPE_MYSQL || detected_server == SERVER_TYPE_MARIADB) {
    create_job_to_dump_metadata(&conf, mdfile);
  }

  // Begin Job Creation

  if (dump_tablespaces){
    create_job_to_dump_tablespaces(&conf);
  }
  if (db) {
    guint i=0;
    for (i=0;i<g_strv_length(db_items);i++){
      struct database *this_db=new_database(conn,db_items[i],TRUE);
      create_job_to_dump_database(this_db, &conf);
      if (!no_schemas)
        create_job_to_dump_schema(this_db, &conf);
    }
  }
  if (tables) {
    create_job_to_dump_table_list(tables, &conf);
  }
  if (( db == NULL ) && ( tables == NULL )) {
    create_job_to_dump_all_databases(&conf);
  }
  g_message("End job creation");
  // End Job Creation
  GThread *chunk_builder=NULL;
  if (!no_data){
    chunk_builder=g_thread_create((GThreadFunc)chunk_builder_thread, &conf, TRUE, NULL);
  }

  GThread **threads = g_new(GThread *, num_threads );
  struct thread_data *td =
      g_new(struct thread_data, num_threads * (less_locking + 1));
  g_message("Creating workers");
  for (n = 0; n < num_threads; n++) {
    td[n].conf = &conf;
    td[n].thread_id = n + 1;
    td[n].less_locking_stage = FALSE;
    td[n].binlog_snapshot_gtid_executed = NULL;
    td[n].pause_resume_mutex=NULL;
    td[n].table_name=NULL;
    threads[n] =
        g_thread_create((GThreadFunc)working_thread, &td[n], TRUE, NULL);
 //   g_async_queue_pop(conf.ready);
  }


// are all in the same gtid pos?
  gchar *binlog_snapshot_gtid_executed = NULL; 
  gboolean binlog_snapshot_gtid_executed_status_local=FALSE;
  guint start_transaction_retry=0;
  while (!binlog_snapshot_gtid_executed_status_local && start_transaction_retry < MAX_START_TRANSACTION_RETRIES ){
    binlog_snapshot_gtid_executed_status_local=TRUE;
    for (n = 0; n < num_threads; n++) {
      g_async_queue_pop(conf.gtid_pos_checked);
    }
    binlog_snapshot_gtid_executed=g_strdup(td[0].binlog_snapshot_gtid_executed);
    for (n = 1; n < num_threads; n++) {
      binlog_snapshot_gtid_executed_status_local=binlog_snapshot_gtid_executed_status_local && g_strcmp0(td[n].binlog_snapshot_gtid_executed,binlog_snapshot_gtid_executed)==0;
    }
    for (n = 0; n < num_threads; n++) {
      g_async_queue_push(conf.are_all_threads_in_same_pos,binlog_snapshot_gtid_executed_status_local?GINT_TO_POINTER(1):GINT_TO_POINTER(2));
    }
    start_transaction_retry++;
  }

  for (n = 0; n < num_threads; n++) {
    g_async_queue_pop(conf.ready);
  }

  // IMPORTANT: At this point, all the threads are in sync

  if (trx_consistency_only) {
    g_message("Transactions started, unlocking tables");
    mysql_query(conn, "UNLOCK TABLES /* trx-only */");
    if (release_binlog_function != NULL){
      g_message("Releasing binlog lock");
      release_binlog_function(second_conn);
    }
  }

  
  g_message("Waiting database finish");
//  if (database_counter > 0)
//    g_rec_mutex_lock(ready_database_dump_mutex);
  g_async_queue_pop(conf.db_ready);
  g_list_free(no_updated_tables);

  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.initial_queue, j);
  }

  for (n = 0; n < num_threads; n++) {
    g_async_queue_pop(conf.ready);
  }

  g_message("Shutdown jobs for less locking enqueued");
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.schema_queue, j);
  }

 
  if (less_locking){
    build_lock_tables_statement(&conf);
  }
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf.ready_non_innodb_queue, GINT_TO_POINTER(1));
  }

/*
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.non_innodb_queue, j);
  }
*/

  if (!no_locks && !trx_consistency_only) {
    for (n = 0; n < num_threads; n++) {
      g_async_queue_pop(conf.unlock_tables);
    }
    g_message("Non-InnoDB dump complete, unlocking tables");
    mysql_query(conn, "UNLOCK TABLES /* FTWRL */");
    g_message("Releasing FTWR lock");
    if (release_binlog_function != NULL){
      g_message("Releasing binlog lock");
      release_binlog_function(second_conn);
    }
  }

/*
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.innodb_queue, j);
  }
*/
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.post_data_queue, j);
  }

  if (!no_data){
    g_thread_join(chunk_builder);
  }

  g_message("Waiting threads to complete");
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  finalize_working_thread();
  finalize_write();
  if (release_ddl_lock_function != NULL) {
    g_message("Releasing DDL lock");
    release_ddl_lock_function(second_conn);
  }
  g_message("Queue count: %d %d %d %d %d", g_async_queue_length(conf.initial_queue), g_async_queue_length(conf.schema_queue), g_async_queue_length(conf.non_innodb_queue), g_async_queue_length(conf.innodb_queue), g_async_queue_length(conf.post_data_queue));
  // close main connection
  if (conn != second_conn)
    mysql_close(second_conn);
  execute_gstring(main_connection, set_global_back);
  mysql_close(conn);
  g_message("Main connection closed");  

  GList *iter = NULL;
  // TODO: We need to create jobs for metadata.
  all_dbts = g_list_reverse(all_dbts);
  for (iter = all_dbts; iter != NULL; iter = iter->next) {
    dbt = (struct db_table *)iter->data;
//    write_table_metadata_into_file(dbt);
    fprintf(mdfile,"\n[`%s`.`%s`]\nRows = %"G_GINT64_FORMAT"\n", dbt->database->name, dbt->table_filename, dbt->rows);
    if (dbt->data_checksum)
      fprintf(mdfile,"data_checksum = %s\n", dbt->data_checksum);
    if (dbt->schema_checksum)
      fprintf(mdfile,"schema_checksum = %s\n", dbt->schema_checksum);
    if (dbt->indexes_checksum)
      fprintf(mdfile,"indexes_checksum = %s\n", dbt->indexes_checksum);
    if (dbt->triggers_checksum)
      fprintf(mdfile,"triggers_checksum = %s\n", dbt->triggers_checksum);
    free_db_table(dbt);
  }
  g_list_free(all_dbts);
  write_database_on_disk(mdfile);
  g_list_free(table_schemas);
  table_schemas=NULL;
  if (pmm){
    kill_pmm_thread();
//    g_thread_join(pmmthread);
  }
  g_async_queue_unref(conf.innodb_queue);
  conf.innodb_queue=NULL;
  g_async_queue_unref(conf.non_innodb_queue);
  conf.non_innodb_queue=NULL;
  g_async_queue_unref(conf.unlock_tables);
  conf.unlock_tables=NULL;
  g_async_queue_unref(conf.ready);
  conf.ready=NULL;
  g_async_queue_unref(conf.schema_queue);
  conf.schema_queue=NULL;
  g_async_queue_unref(conf.initial_queue);
  conf.initial_queue=NULL;
  g_async_queue_unref(conf.post_data_queue);
  conf.post_data_queue=NULL;

  g_async_queue_unref(conf.ready_non_innodb_queue);
  conf.ready_non_innodb_queue=NULL;

  g_date_time_unref(datetime);
  datetime = g_date_time_new_now_local();
  datetimestr=g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S");
  g_date_time_unref(datetime);
  fprintf(mdfile, "# Finished dump at: %s\n", datetimestr);
  fclose(mdfile);
  if (updated_since > 0)
    fclose(nufile);
  g_rename(metadata_partial_filename, metadata_filename);
  if (stream) {
    g_async_queue_push(stream_queue, g_strdup(metadata_filename));
  }
  g_free(metadata_partial_filename);
  g_free(metadata_filename);
  g_message("Finished dump at: %s",datetimestr);
  g_free(datetimestr);

  if (stream) {
    if (exec_command!=NULL){
      wait_exec_command_to_finish();
    }else{
      g_async_queue_push(stream_queue, g_strdup(""));
      wait_stream_to_finish();
    }
    if (no_delete == FALSE && output_directory_param == NULL)
      if (g_rmdir(output_directory) != 0)
        g_critical("Backup directory not removed: %s", output_directory);
  }

  g_free(td);
  g_free(threads);
  if (sthread!=NULL)
    g_thread_unref(sthread);
  free_databases();

  if (disk_check_thread!=NULL){
    disk_limits=NULL;
  }

  g_string_free(set_session, TRUE);
  g_string_free(set_global, TRUE);
  g_string_free(set_global_back, TRUE);
  g_strfreev(db_items);

  g_hash_table_unref(conf_per_table.all_anonymized_function);
  g_hash_table_unref(conf_per_table.all_where_per_table);
  g_hash_table_unref(conf_per_table.all_limit_per_table);
  g_hash_table_unref(conf_per_table.all_num_threads_per_table);

  finalize_masquerade();

  g_async_queue_unref(conf.gtid_pos_checked);
  g_async_queue_unref(conf.are_all_threads_in_same_pos);
  g_async_queue_unref(conf.db_ready);
  g_rec_mutex_clear(ready_table_dump_mutex);
//  g_source_remove(SIGINT);
//  g_source_remove(SIGTERM);
//  g_main_loop_quit(conf.loop);
  g_main_loop_unref(conf.loop);

  if (tables_list)
    g_strfreev(tables);

  free_regex();
  free_common();
  free_set_names();
  if (no_locks){
    if (it_is_a_consistent_backup)
      g_message("This is a consistent backup.");
    else
      g_message("This is NOT a consistent backup.");
  }
}

