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
#include "common_options.h"
#include "mydumper_global.h"
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
int killqueries = 0;
int lock_all_tables = 0;
gboolean skip_ddl_locks= FALSE;
gboolean replica_stopped = FALSE;
gboolean no_locks = FALSE;
gboolean it_is_a_consistent_backup = FALSE;
gboolean less_locking = FALSE;
gboolean no_backup_locks = FALSE;
gboolean no_ddl_locks = FALSE;
gboolean dump_tablespaces = FALSE;
GHashTable *all_dbts=NULL;
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
//GThread *wait_pid_thread=NULL;
char * (*identifier_quote_character_protect)(char *r);
//GRecMutex *ready_database_dump_mutex = NULL;
GRecMutex *ready_table_dump_mutex = NULL;

struct configuration_per_table conf_per_table = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
gchar *exec_command=NULL;

void initialize_start_dump(){
  all_dbts=g_hash_table_new(g_str_hash, g_str_equal);
  initialize_set_names();
  initialize_working_thread();
  conf_per_table.all_anonymized_function=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_where_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_limit_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_num_threads_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  conf_per_table.all_columns_on_select_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  conf_per_table.all_columns_on_insert_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  conf_per_table.all_partition_regex_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  conf_per_table.all_rows_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
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

/*
void * wait_pid(void *data){
  (void)data;
  int status=0;
  int child_pid;
  g_message("Waiting pid started");
  for (;;){
    child_pid=wait(&status);
    if (child_pid>0)
      child_process_ended(child_pid);
  }
  return NULL;
}
*/

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
      char *datetimestr=m_date_time_new_now_local();
      fprintf(stdout, "%s: Ctrl+c detected! Are you sure you want to cancel(Y/N)?", datetimestr);
      g_free(datetimestr);
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
          datetimestr=m_date_time_new_now_local();
          fprintf(stdout, "%s: Resuming backup\n", datetimestr);
          g_free(datetimestr);
          for(i=0;i<num_threads;i++)
            g_mutex_unlock(pause_mutex_per_thread[i]);

          return TRUE;
        }
        if ( c == 'Y' || c == 'y'){
          datetimestr=m_date_time_new_now_local();
          fprintf(stdout, "%s: Backup cancelled\n", datetimestr);
          g_free(datetimestr);
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

void initialize_sql_mode(GHashTable * set_session_hash){
  GString *str= g_string_new(sql_mode);
  g_string_replace(str, "ORACLE", "", 0);
  g_string_replace(str, ",,", ",", 0);
  set_session_hash_insert(set_session_hash, "SQL_MODE",
		  g_string_free(str, FALSE));
}


GHashTable * mydumper_initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=initialize_hash_of_session_variables();
  set_session_hash_insert(set_session_hash, "information_schema_stats_expiry", g_strdup("0 /*!80003"));
  return set_session_hash;
}

MYSQL *create_connection() {
  MYSQL *conn;
  conn = mysql_init(NULL);

  m_connect(conn);//, db_items!=NULL?db_items[0]:db);

  execute_gstring(conn, set_session);
  return conn;
}

static
void detect_quote_character(MYSQL *conn)
{
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  const char *query = "SELECT FIND_IN_SET('ANSI', @@SQL_MODE) OR FIND_IN_SET('ANSI_QUOTES', @@SQL_MODE)";

  if (mysql_query(conn, query)){
    g_warning("We were not able to determine ANSI mode: %s", mysql_error(conn));
    return ;
  }

  if (!(res = mysql_store_result(conn))){
    g_warning("We were not able to determine ANSI mode");
    return ;
  }
  row = mysql_fetch_row(res);
  if (!strcmp(row[0], "0")) {
    identifier_quote_character= BACKTICK;
    identifier_quote_character_str= "`";
    fields_enclosed_by= "\"";
    identifier_quote_character_protect = &backtick_protect;
  } else {
    identifier_quote_character= DOUBLE_QUOTE;
    identifier_quote_character_str= "\"";
    fields_enclosed_by= "'";
    identifier_quote_character_protect = &double_quoute_protect;
  }
  mysql_free_result(res);
}

static
void detect_sql_mode(MYSQL *conn){
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  GString *str;

  const char *query= "SELECT @@SQL_MODE";
  if (mysql_query(conn, query)){
    g_critical("Error getting SQL_MODE: %s", mysql_error(conn));
  }

  if (!(res= mysql_store_result(conn))){
    g_critical("Error getting SQL_MODE");
  }
  row= mysql_fetch_row(res);
  str= g_string_new(NULL);
  if (!g_strstr_len(row[0],-1, "NO_AUTO_VALUE_ON_ZERO"))
    g_string_printf(str, "'NO_AUTO_VALUE_ON_ZERO,%s'", row[0]);
  else
    g_string_printf(str, "'%s'", row[0]);
  g_string_replace(str, "NO_BACKSLASH_ESCAPES", "", 0);
  g_string_replace(str, ",,", ",", 0);

  /*
    The below 4 will be returned back if there is ORACLE in SQL_MODE. We can
    not remove ORACLE from dump files because restoring PACKAGE requires it. But we
    may remove ORACLE from mydumper session because SHOW CREATE PACKAGE works
    without ORACLE (see initialize_sql_mode()).
    The dump must retain all table options, so we cut out NO_TABLE_OPTIONS here:
    it doesn't play any role in dump files, but we are interested it doesn't
    appear in mydumpmer session.
  */
  g_string_replace(str, "PIPES_AS_CONCAT", "", 0);
  g_string_replace(str, ",,", ",", 0);
  g_string_replace(str, "NO_KEY_OPTIONS", "", 0);
  g_string_replace(str, ",,", ",", 0);
  g_string_replace(str, "NO_TABLE_OPTIONS", "", 0);
  g_string_replace(str, ",,", ",", 0);
  g_string_replace(str, "NO_FIELD_OPTIONS", "", 0);
  g_string_replace(str, ",,", ",", 0);
  g_string_replace(str, "STRICT_TRANS_TABLES", "", 0);
  g_string_replace(str, ",,", ",", 0);
  sql_mode= g_string_free(str, FALSE);
  g_assert(sql_mode);
  mysql_free_result(res);
}

MYSQL *create_main_connection() {
  MYSQL *conn;
  conn = mysql_init(NULL);

  m_connect(conn); //, db_items!=NULL?db_items[0]:db);

  set_session = g_string_new(NULL);
  set_global = g_string_new(NULL);
  set_global_back = g_string_new(NULL);
  detect_server_version(conn);
  detected_server = get_product(); 
  GHashTable * set_session_hash = mydumper_initialize_hash_of_session_variables();
  GHashTable * set_global_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  if (key_file != NULL ){
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_global_hash,"mydumper_global_variables");
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_session_hash,"mydumper_session_variables");
    load_per_table_info_from_key_file(key_file, &conf_per_table, &init_function_pointer);
  }
  sql_mode=g_strdup(g_hash_table_lookup(set_session_hash,"SQL_MODE"));
  if (!sql_mode){
    detect_sql_mode(conn);
    initialize_sql_mode(set_session_hash);
  }
  refresh_set_session_from_hash(set_session,set_session_hash);
  refresh_set_global_from_hash(set_global, set_global_back, set_global_hash);
  free_hash_table(set_session_hash);
  g_hash_table_unref(set_session_hash);
  execute_gstring(conn, set_session);
  execute_gstring(conn, set_global);
  detect_quote_character(conn);
  initialize_headers();
  initialize_write();

  switch (detected_server) {
  case SERVER_TYPE_MYSQL:
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  case SERVER_TYPE_MARIADB:
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  case SERVER_TYPE_TIDB:
    data_checksums=FALSE;
    break;
  case SERVER_TYPE_PERCONA:
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  case SERVER_TYPE_UNKNOWN:
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  default:
    m_critical("Cannot detect server type");
    break;
  }

  g_message("Connected to %s %d.%d.%d", get_product_name(), get_major(), get_secondary(), get_revision());
  
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
  if (mysql_query(conn, query)){
    g_free(query);
    return;
  }

  g_free(query);

  if (!(res = mysql_store_result(conn)))
    return;

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


int mysql_query_verbose(MYSQL *mysql, const char *q)
{
  int res= mysql_query(mysql, q);
  if (!res)
    g_message("%s: OK", q);
  else
    g_message("%s: %s (%d)", q, mysql_error(mysql), res);
  return res;
}


void send_backup_stage_on_block_commit(MYSQL *conn){
/*
  if (mysql_query(conn, "BACKUP STAGE START")) {
    m_critical("Couldn't acquire BACKUP STAGE START: %s",
               mysql_error(conn));
    errors++;
  }
*/
  if (mysql_query_verbose(conn, "BACKUP STAGE BLOCK_COMMIT")) {
    m_critical("Couldn't acquire BACKUP STAGE BLOCK_COMMIT: %s",
               mysql_error(conn));
    errors++;
  }
}


void send_mariadb_backup_locks(MYSQL *conn){
  if (mysql_query_verbose(conn, "BACKUP STAGE START")) {
    m_critical("Couldn't acquire BACKUP STAGE START: %s",
               mysql_error(conn));
    errors++;
  }
/*
  if (mysql_query(conn, "BACKUP STAGE FLUSH")) {
    m_critical("Couldn't acquire BACKUP STAGE FLUSH: %s",
               mysql_error(conn));
    errors++;
  }
*/
  if (mysql_query_verbose(conn, "BACKUP STAGE BLOCK_DDL")) {
    m_critical("Couldn't acquire BACKUP STAGE BLOCK_DDL: %s",
               mysql_error(conn));
    errors++;
  }
/*
  if (mysql_query(conn, "BACKUP STAGE BLOCK_COMMIT")) {
    m_critical("Couldn't acquire BACKUP STAGE BLOCK_COMMIT: %s",
               mysql_error(conn));
    errors++;
  }
*/
}

void send_percona57_backup_locks(MYSQL *conn){
  if (mysql_query_verbose(conn, "LOCK TABLES FOR BACKUP")) {
    m_critical("Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will "
               "not be consistent: %s",
               mysql_error(conn));
    errors++;
  }

  if (mysql_query_verbose(conn, "LOCK BINLOG FOR BACKUP")) {
    m_critical("Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will "
               "not be consistent: %s",
               mysql_error(conn));
    errors++;
  }
}

void send_ddl_lock_instance_backup(MYSQL *conn){
  if (mysql_query_verbose(conn, "LOCK INSTANCE FOR BACKUP")) {
    m_critical("Couldn't acquire LOCK INSTANCE FOR BACKUP: %s",
               mysql_error(conn));
    errors++;
  }
} 

void send_unlock_tables(MYSQL *conn){
  mysql_query_verbose(conn, "UNLOCK TABLES");
}

void send_unlock_binlogs(MYSQL *conn){
  mysql_query_verbose(conn, "UNLOCK BINLOG");
}

void send_ddl_unlock_instance_backup(MYSQL *conn){
  mysql_query_verbose(conn, "UNLOCK INSTANCE");
}

void send_backup_stage_end(MYSQL *conn){
  mysql_query_verbose(conn, "BACKUP STAGE END");
}

void send_flush_table_with_read_lock(MYSQL *conn){
        if (mysql_query_verbose(conn, "FLUSH NO_WRITE_TO_BINLOG TABLES")) {
          g_warning("Flush tables failed, we are continuing anyways: %s",
                   mysql_error(conn));
        }
       if (mysql_query_verbose(conn, "FLUSH TABLES WITH READ LOCK")) {
          g_critical("Couldn't acquire global lock, snapshots will not be "
                   "consistent: %s",
                   mysql_error(conn));
          errors++;
        }
}

void default_locking(void(**acquire_global_lock_function)(MYSQL *), void (**release_global_lock_function)(MYSQL *), void (**acquire_ddl_lock_function)(MYSQL *), void (** release_ddl_lock_function)(MYSQL *), void (** release_binlog_function)(MYSQL *)) {
  *acquire_ddl_lock_function = NULL;
  *release_ddl_lock_function = NULL;

  *acquire_global_lock_function = &send_flush_table_with_read_lock;
  *release_global_lock_function = &send_unlock_tables;

  *release_binlog_function = NULL;
}

void determine_ddl_lock_function(MYSQL ** conn, void(**acquire_global_lock_function)(MYSQL *), void (**release_global_lock_function)(MYSQL *), void (**acquire_ddl_lock_function)(MYSQL *), void (** release_ddl_lock_function)(MYSQL *), void (** release_binlog_function)(MYSQL *)) {
  switch(get_product()){
    case SERVER_TYPE_PERCONA:
      switch (get_major()) {
        case 8:
          *acquire_ddl_lock_function = &send_ddl_lock_instance_backup;
          *release_ddl_lock_function = &send_ddl_unlock_instance_backup;

          *acquire_global_lock_function = &send_flush_table_with_read_lock;
          *release_global_lock_function = &send_unlock_tables;

	  break;
        case 5:
          if (get_secondary() == 7) {
            if (no_backup_locks){
              *acquire_ddl_lock_function = NULL;
              *release_ddl_lock_function = NULL;
            }else{
              *acquire_ddl_lock_function = &send_percona57_backup_locks;
              *release_ddl_lock_function = &send_unlock_tables;
            }

            *acquire_global_lock_function = &send_flush_table_with_read_lock;
            *release_global_lock_function = &send_unlock_tables;

            *release_binlog_function = &send_unlock_binlogs;

            *conn = create_connection();
          }else{
            default_locking(acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
          }
          break;
        default:
          default_locking(acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
          break;
      }
      break;
    case SERVER_TYPE_MYSQL: 
      switch (get_major()) {
        case 8:
          *acquire_ddl_lock_function = &send_ddl_lock_instance_backup;
          *release_ddl_lock_function = &send_ddl_unlock_instance_backup;

          *acquire_global_lock_function = &send_flush_table_with_read_lock;
          *release_global_lock_function = &send_unlock_tables;
          break;
        default:
          default_locking( acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
          break;
      }
      break;
    case SERVER_TYPE_MARIADB:
      if ((get_major() == 10 && get_secondary() >= 5) || get_major() > 10) {
        *acquire_ddl_lock_function = &send_mariadb_backup_locks;
//            *release_ddl_lock_function = &send_backup_stage_end;
        *release_ddl_lock_function = NULL;

        *acquire_global_lock_function = &send_backup_stage_on_block_commit;
        *release_global_lock_function = &send_backup_stage_end;

//            *conn = create_connection();
      }else{
        default_locking( acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
      }
      break;
    default:
      default_locking( acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
      break;
  }
}


// see write_database_on_disk() for db write to metadata

void print_dbt_on_metadata_gstring(struct db_table *dbt, GString *data){
  char *name= newline_protect(dbt->database->name);
  char *table_filename= newline_protect(dbt->table_filename);
  char *table= newline_protect(dbt->table);
  const char q= identifier_quote_character;
  g_mutex_lock(dbt->chunks_mutex);
  g_string_append_printf(data,"\n[%c%s%c.%c%s%c]\n", q, name, q, q, table_filename, q);
  g_string_append_printf(data, "real_table_name=%s\nrows = %"G_GINT64_FORMAT"\n", table, dbt->rows);
  g_free(name);
  g_free(table_filename);
  g_free(table);
  if (dbt->is_sequence)
    g_string_append_printf(data,"is_sequence = 1\n");
  if (dbt->data_checksum)
    g_string_append_printf(data,"data_checksum = %s\n", dbt->data_checksum);
  if (dbt->schema_checksum)
    g_string_append_printf(data,"schema_checksum = %s\n", dbt->schema_checksum);
  if (dbt->indexes_checksum)
    g_string_append_printf(data,"indexes_checksum = %s\n", dbt->indexes_checksum);
  if (dbt->triggers_checksum)
    g_string_append_printf(data,"triggers_checksum = %s\n", dbt->triggers_checksum);
  g_mutex_unlock(dbt->chunks_mutex);
}

void print_dbt_on_metadata(FILE *mdfile, struct db_table *dbt){
  GString *data = g_string_sized_new(100);
  print_dbt_on_metadata_gstring(dbt, data);
  fprintf(mdfile, "%s", data->str);
  if (check_row_count && (dbt->rows != dbt->rows_total)) {
    m_critical("Row count mismatch found for %s.%s: got %u of %u expected",
               dbt->database->name, dbt->table, dbt->rows, dbt->rows_total);
  }
}


void send_lock_all_tables(MYSQL *conn){
  // LOCK ALL TABLES
  GString *query = g_string_sized_new(16777216);
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  gchar *dbtb = NULL;
  gchar **dt = NULL;
  GList *tables_lock = NULL;
  GList *iter = NULL;
  guint success = 0;
  guint retry = 0;

  if (tables) {
    for (guint i = 0; tables[i] != NULL; i++) {
      dt = g_strsplit(tables[i], ".", 0);
      g_string_printf(query, "SHOW TABLES IN %s LIKE '%s'", dt[0], dt[1]);
      if (mysql_query(conn, query->str)) {
        g_error("Error showing tables in: %s - Could not execute query: %s", dt[0],
                  mysql_error(conn));
        errors++;
        return;
      }else {
        res = mysql_store_result(conn);
        while ((row = mysql_fetch_row(res))) {
          if (tables_skiplist_file && check_skiplist(dt[0], row[0]))
            continue;
          if (is_mysql_special_tables(dt[0], row[0]))
            continue;
          if (!eval_regex(dt[0], row[0]))
            continue;
          dbtb = g_strdup_printf("%s%s%s.%s%s%s", identifier_quote_character_str, dt[0], identifier_quote_character_str, identifier_quote_character_str, row[0], identifier_quote_character_str);
          tables_lock = g_list_prepend(tables_lock, dbtb);
        }
      }
    }
    tables_lock = g_list_reverse(tables_lock);
  } else { 
    if (db) {
      GString *db_quoted_list=NULL;
      guint i=0;
      db_quoted_list=g_string_sized_new(strlen(db));
      g_string_append_printf(db_quoted_list,"'%s'",db_items[i]);
      i++;
      for (; i<g_strv_length(db_items); i++){
        g_string_append_printf(db_quoted_list,",'%s'",db_items[i]);
      }

      g_string_printf(
        query, 
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA in (%s) AND TABLE_TYPE ='BASE TABLE'",
        db_quoted_list->str);
    } else {
      g_string_printf(
        query,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_TYPE ='BASE TABLE' AND TABLE_SCHEMA NOT IN "
        "('information_schema', 'performance_schema', 'data_dictionary')");
    }
    if (mysql_query(conn, query->str)) {
      g_critical("Couldn't get table list for lock all tables: %s",
                 mysql_error(conn));
      errors++;
    } else {
      res = mysql_store_result(conn);
      while ((row = mysql_fetch_row(res))) {
        // no need to check if the tb exists in the tables.
        if (tables_skiplist_file && check_skiplist(row[0], row[1]))
          continue;
        if (is_mysql_special_tables(row[0], row[1]))
          continue;
        if (!eval_regex(row[0], row[1]))
          continue;
        dbtb = g_strdup_printf("%s%s%s.%s%s%s", identifier_quote_character_str, row[0], identifier_quote_character_str, identifier_quote_character_str, row[1], identifier_quote_character_str);
        tables_lock = g_list_prepend(tables_lock, dbtb);
      }
    }
  }
  if (g_list_length(tables_lock) > 0) {
  // Try three times to get the lock, this is in case of tmp tables
  // disappearing
    while (g_list_length(tables_lock) > 0 && !success && retry < 4 ) {
      g_string_set_size(query,0);
      g_string_append(query, "LOCK TABLE ");
      for (iter = tables_lock; iter != NULL; iter = iter->next) {
        g_string_append_printf(query, " %s READ,", (char *)iter->data);
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
        g_message("LOCK TABLES: %s", query->str);
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
  mysql_free_result(res);
  g_free(query->str);
  g_list_free(tables_lock);
}

void write_replica_info(MYSQL *conn, FILE *file) {
  MYSQL_RES *slave = NULL;
  MYSQL_FIELD *fields;
  MYSQL_ROW row;

  char *slavehost = NULL;
  char *slavelog = NULL;
  char *slavepos = NULL;
  char *slavegtid = NULL;

  char *channel_name = NULL;

  const char *gtid_title = NULL;
  guint i;
  guint isms = 0;
  mysql_query(conn, "SELECT @@default_master_connection");
  MYSQL_RES *rest = mysql_store_result(conn);
  if (rest != NULL && mysql_num_rows(rest)) {
    mysql_free_result(rest);
    g_message("Multisource slave detected.");
    isms = 1;
  }

  if (isms){
    if (mysql_query(conn, show_all_replicas_status ))
      g_critical("Error executing %s: %s", show_all_replicas_status, mysql_error(conn));
  }else{
    if (mysql_query(conn, show_replica_status))
      g_critical("Error executing %s: %s", show_replica_status, mysql_error(conn));
  }

  guint slave_count=0;
  slave = mysql_store_result(conn);

  if (!slave || mysql_num_rows(slave) == 0){
    goto cleanup;
  }
  mysql_free_result(slave);
  g_message("Stopping replica");
  replica_stopped=!mysql_query(conn, stop_replica_sql_thread);
  if (!replica_stopped){
    g_warning("Not able to stop replica: %s", mysql_error(conn));
  }
  if (isms)
    mysql_query(conn, show_all_replicas_status);
  else
    mysql_query(conn, show_replica_status);

  slave = mysql_store_result(conn);

  GString *replication_section_str = g_string_sized_new(100);

  while (slave && (row = mysql_fetch_row(slave))) {
    g_string_set_size(replication_section_str,0);
    fields = mysql_fetch_fields(slave);
    slavepos=NULL;
    slavelog=NULL;
    slavehost=NULL;
    slavegtid=NULL;
    channel_name=NULL;
    gtid_title=NULL;
    for (i = 0; i < mysql_num_fields(slave); i++) {
      if (!strcasecmp("exec_master_log_pos", fields[i].name)) {
        slavepos = row[i];
      } else if (!strcasecmp("relay_master_log_file", fields[i].name)) {
        slavelog = row[i];
      } else if (!strcasecmp("master_host", fields[i].name)) {
        slavehost = row[i];
      } else if (!strcasecmp("Executed_Gtid_Set", fields[i].name)){
        gtid_title="Executed_Gtid_Set";
        slavegtid = remove_new_line(row[i]);
      } else if (!strcasecmp("Gtid_Slave_Pos", fields[i].name)) {
        gtid_title="Gtid_Slave_Pos";
        slavegtid = remove_new_line(row[i]);
      } else if ( ( !strcasecmp("connection_name", fields[i].name) || !strcasecmp("Channel_Name", fields[i].name) ) && strlen(row[i]) > 1) {
        channel_name = row[i];
      }
      g_string_append_printf(replication_section_str,"# %s = ", fields[i].name);
      (fields[i].type != MYSQL_TYPE_LONG && fields[i].type != MYSQL_TYPE_LONGLONG  && fields[i].type != MYSQL_TYPE_INT24  && fields[i].type != MYSQL_TYPE_SHORT )  ?
      g_string_append_printf(replication_section_str,"'%s'\n", remove_new_line(row[i])):
      g_string_append_printf(replication_section_str,"%s\n", remove_new_line(row[i]));
    }
    if (slavehost) {
      slave_count++;
      fprintf(file, "[replication%s%s]", channel_name!=NULL?".":"", channel_name!=NULL?channel_name:"");
      fprintf(file, "\n# relay_master_log_file = \'%s\'\n# exec_master_log_pos = %s\n# %s = %s\n",
              slavelog, slavepos, gtid_title, slavegtid);
      fprintf(file,"%s",replication_section_str->str);
      fprintf(file,"# myloader_exec_reset_slave = 0 # 1 means execute the command\n# myloader_exec_change_master = 0 # 1 means execute the command\n# myloader_exec_start_slave = 0 # 1 means execute the command\n");
      g_message("Written slave status");
    }
  }
  g_string_free(replication_section_str,TRUE);
  if (slave_count > 1)
    g_warning("Multisource replication found. Do not trust in the exec_master_log_pos as it might cause data inconsistencies. Search 'Replication and Transaction Inconsistencies' on MySQL Documentation");

  fflush(file);
cleanup:
  if (slave)
    mysql_free_result(slave);
}

void start_dump() {
  if (clear_dumpdir)
    clear_dump_directory(dump_directory);
  else if (!dirty_dumpdir && !is_empty_dir(dump_directory)) {
    g_error("Directory is not empty (use --clear or --dirty): %s\n", dump_directory);
  }
  check_num_threads();
  g_message("Using %u dumper threads", num_threads);
  initialize_start_dump();
  initialize_common();

  initialize_connection(MYDUMPER);
  initialize_masquerade();

  /* Give ourselves an array of tables to dump */
  if (tables_list)
    tables = get_table_list(tables_list);

  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);

  initialize_regex(partition_regex);
//  detect_server_version(conn);
  MYSQL *conn = create_main_connection();
  main_connection = conn;
  MYSQL *second_conn = conn;
  struct configuration conf;
  memset(&conf, 0, sizeof(conf));
  conf.use_any_index= 1;
  char *metadata_partial_filename, *metadata_filename;
  char *u;
//  detect_server_version(conn);
  void (*acquire_global_lock_function)(MYSQL *) = NULL;
  void (*release_global_lock_function)(MYSQL *) = NULL;
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

  if (stream)
    metadata_partial_filename= g_strdup_printf("%s/metadata.header", dump_directory);
  else
    metadata_partial_filename= g_strdup_printf("%s/metadata.partial", dump_directory);
  metadata_filename = g_strdup_printf("%s/metadata", dump_directory);

  FILE *mdfile = g_fopen(metadata_partial_filename, "w");
  if (!mdfile) {
    m_critical("Couldn't create metadata file %s (%s)", metadata_partial_filename, strerror(errno));
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

  GDateTime *datetime = g_date_time_new_now_local();
  char *datetimestr=g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S");
  fprintf(mdfile, "# Started dump at: %s\n", datetimestr);
  g_message("Started dump at: %s", datetimestr);
  g_free(datetimestr);

  /* Write dump config into beginning of metadata, stream this first */
  {
    g_assert(identifier_quote_character == BACKTICK || identifier_quote_character == DOUBLE_QUOTE);
    const char *qc= identifier_quote_character == BACKTICK ? "BACKTICK" : "DOUBLE_QUOTE";
    fprintf(mdfile, "[config]\nquote_character = %s\n", qc);
    fprintf(mdfile, "\n[myloader_session_variables]");
    fprintf(mdfile, "\nSQL_MODE=%s /*!40101\n\n", sql_mode);
    fflush(mdfile);
  }

  if (stream){
    initialize_stream();
    stream_queue_push(NULL, g_strdup(metadata_partial_filename));
    fclose(mdfile);
    metadata_partial_filename= g_strdup_printf("%s/metadata.partial", dump_directory);
    mdfile= g_fopen(metadata_partial_filename, "w");
    if (!mdfile) {
      m_critical("Couldn't create metadata file %s (%s)", metadata_partial_filename, strerror(errno));
    }
  }

  if (detected_server != SERVER_TYPE_TIDB) {
    write_replica_info(conn, mdfile);
  }


  if (detected_server == SERVER_TYPE_TIDB) {
    g_message("Skipping locks because of TiDB");
    if (!tidb_snapshot) {

      // Generate a @@tidb_snapshot to use for the worker threads since
      // the tidb-snapshot argument was not specified when starting mydumper

      if (mysql_query(conn, show_binary_log_status)) {
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
    set_tidb_snapshot(conn);
    g_message("Set to tidb_snapshot '%s'", tidb_snapshot);

  }else{

    if (!no_locks) {
      // This backup will lock the database
      determine_ddl_lock_function(&second_conn, &acquire_global_lock_function,&release_global_lock_function, &acquire_ddl_lock_function, &release_ddl_lock_function, &release_binlog_function);
      if (skip_ddl_locks){
        acquire_ddl_lock_function=NULL;
        release_ddl_lock_function=NULL;
      }

      if (lock_all_tables) {
        send_lock_all_tables(conn);
      } else {

        if (acquire_ddl_lock_function != NULL) {
          g_message("Acquiring DDL lock");
          acquire_ddl_lock_function(second_conn);
        }

        if (acquire_global_lock_function != NULL) {
          g_message("Acquiring Global lock");
          acquire_global_lock_function(conn);
        }
      }
    } else {
      g_warning("Executing in no-locks mode, snapshot might not be consistent");
    }
  }

// TODO: this should be deleted on future releases. 
  server_version= mysql_get_server_version(conn);
  if (server_version < 40108) {
    mysql_query(
        conn,
        "CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB");
    need_dummy_read = 1;
  }
  /* TODO: MySQL also supports PACKAGE (Percona?) */
  if (get_product() != SERVER_TYPE_MARIADB || server_version < 100300)
    nroutines= 2;

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
/*
  GDateTime *datetime = g_date_time_new_now_local();
  char *datetimestr=g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S");
  fprintf(mdfile, "# Started dump at: %s\n", datetimestr);
  g_message("Started dump at: %s", datetimestr);
  g_free(datetimestr);

  // Write dump config into beginning of metadata, stream this first 
  {
    g_assert(identifier_quote_character == BACKTICK || identifier_quote_character == DOUBLE_QUOTE);
    const char *qc= identifier_quote_character == BACKTICK ? "BACKTICK" : "DOUBLE_QUOTE";
    fprintf(mdfile, "[config]\nquote_character = %s\n", qc);
    fprintf(mdfile, "\n[myloader_session_variables]");
*/
//    fprintf(mdfile, "\nSQL_MODE=%s /*!40101\n\n", sql_mode);
/*    fflush(mdfile);
  }

  if (stream){
    initialize_stream();
    stream_queue_push(NULL, g_strdup(metadata_partial_filename));
    fclose(mdfile);
    metadata_partial_filename= g_strdup_printf("%s/metadata.partial", dump_directory);
    mdfile= g_fopen(metadata_partial_filename, "w");
    if (!mdfile) {
      m_critical("Couldn't create metadata file %s (%s)", metadata_partial_filename, strerror(errno));
    }
  }

  write_replica_info(conn, mdfile);
*/

  if (exec_command != NULL){
    initialize_exec_command();
    stream=TRUE;
  
  }


//  wait_pid_thread = g_thread_create((GThreadFunc)wait_pid, NULL, FALSE, NULL);


  conf.initial_queue = g_async_queue_new();
  conf.schema_queue = g_async_queue_new();
  conf.post_data_queue = g_async_queue_new();
  conf.innodb.queue= g_async_queue_new();
  conf.innodb.defer= g_async_queue_new();
  // These are initialized in the guts of initialize_start_dump() above
  g_assert(give_me_another_innodb_chunk_step_queue &&
           give_me_another_non_innodb_chunk_step_queue &&
           innodb_table &&
           non_innodb_table);
  conf.innodb.request_chunk= give_me_another_innodb_chunk_step_queue;
  conf.innodb.table_list= innodb_table;
  conf.innodb.descr= "InnoDB";
  conf.ready = g_async_queue_new();
  conf.non_innodb.queue= g_async_queue_new();
  conf.non_innodb.defer= g_async_queue_new();
  conf.non_innodb.request_chunk= give_me_another_non_innodb_chunk_step_queue;
  conf.non_innodb.table_list= non_innodb_table;
  conf.non_innodb.descr= "Non-InnoDB";
  conf.ready_non_innodb_queue = g_async_queue_new();
  conf.unlock_tables = g_async_queue_new();
  conf.gtid_pos_checked = g_async_queue_new();
  conf.are_all_threads_in_same_pos = g_async_queue_new();
  conf.db_ready = g_async_queue_new();
  conf.binlog_ready = g_async_queue_new();
//  ready_database_dump_mutex = g_rec_mutex_new();
//  g_rec_mutex_lock(ready_database_dump_mutex);
  ready_table_dump_mutex = g_rec_mutex_new();
  g_rec_mutex_lock(ready_table_dump_mutex);

  g_message("conf created");

  if (is_mysql_like()) {
    create_job_to_dump_metadata(&conf, mdfile);
  }

  // Begin Job Creation

  if (dump_tablespaces){
    create_job_to_dump_tablespaces(&conf);
  }

  // if tables and db both exists , should not call dump_database_thread
  if (tables && g_strv_length(tables) > 0) {
    create_job_to_dump_table_list(tables, &conf);
  } else if (db_items && g_strv_length(db_items) > 0) {
    guint i=0;
    for (i=0;i<g_strv_length(db_items);i++){
      struct database *this_db=new_database(conn,db_items[i],TRUE);
//      if (this_db->dump_triggers)
//        create_job_to_dump_schema_triggers(this_db, &conf);
      create_job_to_dump_database(this_db, &conf);
      if (!no_schemas)
        create_job_to_dump_schema(this_db, &conf);
    }
  } else {
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
    if (release_global_lock_function)
      release_global_lock_function(conn);
    if (release_binlog_function != NULL){
      g_async_queue_pop(conf.binlog_ready);
      g_message("Releasing binlog lock");
      release_binlog_function(second_conn);
    }
  }

  if (replica_stopped){
    g_message("Starting replica");
    if (mysql_query(conn, start_replica_sql_thread)){
      g_warning("Not able to start replica: %s", mysql_error(conn));
    }
  }

  g_message("Waiting database finish");

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

  if (!no_locks && !trx_consistency_only) {
    for (n = 0; n < num_threads; n++) {
      g_async_queue_pop(conf.unlock_tables);
    }
    g_message("Non-InnoDB dump complete, releasing global locks");
    if (release_global_lock_function)
      release_global_lock_function(conn);
//    mysql_query(conn, "UNLOCK TABLES /* FTWRL */");
    g_message("Global locks released");
    if (release_binlog_function != NULL){
      g_async_queue_pop(conf.binlog_ready);
      g_message("Releasing binlog lock");
      release_binlog_function(second_conn);
    }
  }

  g_async_queue_unref(conf.binlog_ready);

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
  g_message("Queue count: %d %d %d %d %d", g_async_queue_length(conf.initial_queue),
            g_async_queue_length(conf.schema_queue),
            g_async_queue_length(conf.non_innodb.queue) + g_async_queue_length(conf.non_innodb.defer),
            g_async_queue_length(conf.innodb.queue) + g_async_queue_length(conf.innodb.defer),
            g_async_queue_length(conf.post_data_queue));
  // close main connection
  if (conn != second_conn)
    mysql_close(second_conn);
  execute_gstring(main_connection, set_global_back);
  mysql_close(conn);
  g_message("Main connection closed");  


  wait_close_files();

  GList *keys= g_hash_table_get_keys(all_dbts);
  keys= g_list_sort(keys, key_strcmp);
  for (GList *it= keys; it; it= g_list_next(it)) {
    dbt= (struct db_table *) g_hash_table_lookup(all_dbts, it->data);
    g_assert(dbt);
    print_dbt_on_metadata(mdfile, dbt);
  }
  write_database_on_disk(mdfile);
  g_list_free(table_schemas);
  table_schemas=NULL;
  if (pmm){
    kill_pmm_thread();
  }
  g_async_queue_unref(conf.innodb.defer);
  conf.innodb.defer= NULL;
  g_async_queue_unref(conf.innodb.queue);
  conf.innodb.queue= NULL;
  g_async_queue_unref(conf.non_innodb.defer);
  conf.non_innodb.defer= NULL;
  g_async_queue_unref(conf.non_innodb.queue);
  conf.non_innodb.queue= NULL;
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
    stream_queue_push(NULL, g_strdup(metadata_filename));
    if (exec_command!=NULL){
      wait_exec_command_to_finish();
    }else{
      stream_queue_push(NULL, g_strdup(""));
      wait_stream_to_finish();
    }
    if (no_delete == FALSE && output_directory_param == NULL)
      if (g_rmdir(output_directory) != 0)
        g_critical("Backup directory not removed: %s", output_directory);
  }

  for (GList *it= keys; it; it= g_list_next(it)) {
    dbt= (struct db_table *) g_hash_table_lookup(all_dbts, it->data);
    free_db_table(dbt);
  }
  g_list_free(keys);
  g_hash_table_unref(all_dbts);
  g_free(metadata_partial_filename);
  g_free(metadata_filename);
  g_message("Finished dump at: %s", datetimestr);
  g_free(datetimestr);

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

