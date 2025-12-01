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

#include <mysql.h>

#include <glib-unix.h>
#include <sys/statvfs.h>

#include "mydumper_start_dump.h"
#include "mydumper_jobs.h"
#include "mydumper_common.h"
#include "mydumper_stream.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_pmm.h"
#include "mydumper_exec_command.h"
#include "mydumper_masquerade.h"
#include "mydumper_chunks.h"
#include "mydumper_write.h"
#include "mydumper_global.h"
#include "mydumper_create_jobs.h"
#include "mydumper_file_handler.h"

/* Program options */
gchar *tidb_snapshot = NULL;
int longquery = 60;
int longquery_retries = 0;
int longquery_retry_interval = 60;
int killqueries = 0;
gboolean skip_ddl_locks= FALSE;
gboolean no_backup_locks = FALSE;
gboolean dump_tablespaces = FALSE;
guint updated_since = 0;
gchar *exec_command=NULL;

// Shared variables
GList *no_updated_tables = NULL;
int need_dummy_read = 0;
int need_dummy_toku_read = 0;
GList *table_schemas = NULL;
GList *trigger_schemas = NULL;
GList *view_schemas = NULL;
GList *schema_post = NULL;
gboolean it_is_a_consistent_backup = FALSE;
GHashTable *all_dbts=NULL;
char * (*identifier_quote_character_protect)(char *r);
struct configuration_per_table conf_per_table = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
gboolean replica_stopped = FALSE;
gboolean merge_dumpdir= FALSE;
gboolean clear_dumpdir= FALSE;
gboolean dirty_dumpdir= FALSE;
gchar *initial_source_log = NULL;
gchar *initial_source_pos = NULL;
gchar *initial_source_gtid = NULL;

// Program options used only on this file 
extern guint ftwrl_max_wait_time;
extern guint ftwrl_timeout_retries;

// static variables
static GMutex **pause_mutex_per_thread=NULL;
static guint pause_at=0;
static guint resume_at=0;
static gchar **db_items=NULL;
static GRecMutex *ready_table_dump_mutex = NULL;
static gboolean ftwrl_completed=FALSE;
static
void initialize_start_dump(){
  all_dbts=g_hash_table_new(g_str_hash, g_str_equal);
  initialize_table();
  initialize_working_thread();
	initialize_conf_per_table(&conf_per_table);

  // until we have an unique option on lock types we need to ensure this
  if (sync_thread_lock_mode==NO_LOCK || sync_thread_lock_mode==SAFE_NO_LOCK)
    trx_tables=TRUE;

  // clarify binlog coordinates with --trx-tables
  if (trx_tables)
    g_warning("Using --trx-tables options, binlog coordinates will not be "
              "accurate if you are writing to non transactional tables.");

  if (source_db){
    db_items=g_strsplit(source_db,",",0);
  }
}

void set_disk_limits(guint p_at, guint r_at){
  pause_at=p_at;
  resume_at=r_at;
}

static
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

static
void *monitor_disk_space_thread (void *queue){
  (void)queue;
  guint i=0;
  GMutex **_pause_mutex_per_thread=g_new(GMutex * , num_threads) ;
  for(i=0;i<num_threads;i++){
    _pause_mutex_per_thread[i]=g_mutex_new();
  }

  gboolean previous_state = TRUE, current_state = TRUE;

  while (disk_limits != NULL){
    current_state = previous_state ? is_disk_space_ok(pause_at) : is_disk_space_ok(resume_at);
    if (previous_state != current_state){
      if (!current_state){
        g_warning("Pausing backup disk space lower than %dMB. You need to free up to %dMB to resume",pause_at,resume_at);
        for(i=0;i<num_threads;i++){
          g_mutex_lock(_pause_mutex_per_thread[i]);
          g_async_queue_push(queue,_pause_mutex_per_thread[i]);
        }
      }else{
        g_warning("Resuming backup");
        for(i=0;i<num_threads;i++){
          g_mutex_unlock(_pause_mutex_per_thread[i]);
        }
      }
      previous_state = current_state;

    }
    sleep(10);
  }
  return NULL;
}

// | Id  | User            | Host             | db   | Command | Time   | State                  | Info                  | Time_ms   | Rows_sent | Rows_examined |
static
void determine_columns_on_show_processlist( MYSQL_FIELD *fields, guint num_fields, int *id_col, int *user_col, int *command_col, int *time_col, int *info_col ){
  /* Just in case PROCESSLIST output column order changes */
  guint i;
  for (i = 0; i < num_fields; i++) {
    if (id_col && !strcasecmp(fields[i].name, "Id"))
      *id_col = i;
    else if (user_col && !strcasecmp(fields[i].name, "User"))
      *user_col = i;
    else if (command_col && !strcasecmp(fields[i].name, "Command"))
      *command_col = i;
    else if (time_col && !strcasecmp(fields[i].name, "Time"))
      *time_col = i;
    else if (info_col && !strcasecmp(fields[i].name, "Info"))
      *info_col = i;
  }
  if ((     id_col && *id_col < 0) ||
      (command_col && *command_col < 0) ||
      (   time_col && *time_col < 0)){
    m_critical("Error obtaining information from processlist");
  }
}

void *monitor_ftwrl_thread (void *thread_id){
  MYSQL *conn;
  MYSQL_RES *res = NULL;
  gboolean ftwrl_found_in_processlist = FALSE;
  conn = mysql_init(NULL);
  m_connect(conn);
  gchar *query=NULL;
  while (!ftwrl_completed){
    if (ftwrl_found_in_processlist == TRUE) {
      sleep(ftwrl_max_wait_time);
    } else {
      sleep(1);
    }
    res = m_store_result(conn,"SHOW PROCESSLIST", m_warning, "Could not check PROCESSLIST");
    if (!res){
       break;
    } else {
      MYSQL_ROW row;

      /* Just in case PROCESSLIST output column order changes */
      int id_col = -1, info_col=-1; 
      determine_columns_on_show_processlist(mysql_fetch_fields(res), mysql_num_fields(res), &id_col, NULL, NULL, NULL, &info_col);
      while ((row = mysql_fetch_row(res))) {
        if ((atol(row[id_col]) == *((guint *)(thread_id)))){
           if (!strcasecmp(FLUSH_TABLES_WITH_READ_LOCK, row[info_col]) || !strcasecmp(FLUSH_NO_WRITE_TO_BINLOG_TABLES, row[info_col])){
             if (ftwrl_found_in_processlist == TRUE){
               m_query_warning(conn, query = g_strdup_printf("KILL QUERY %lu", atol(row[id_col])), "Could not KILL slow query", NULL);
               g_free(query);
               ftwrl_found_in_processlist = FALSE;
             } else {
               ftwrl_found_in_processlist = TRUE;
             }
           }
//            g_message("%s found. KILL %d",row[info_col], id_col);
        }
      }
    }
    mysql_free_result(res);
  }
  mysql_close(conn);
  return NULL;
}

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

static 
void *signal_thread(void *data) {
  g_unix_signal_add(SIGINT, sig_triggered_int, data);
  g_unix_signal_add(SIGTERM, sig_triggered_term, data);
  ((struct configuration *)data)->loop  = g_main_loop_new (NULL, TRUE);
  g_main_loop_run (((struct configuration *)data)->loop);
  g_message("Ending signal thread");
  return NULL;
}

static
void initialize_sql_mode(GHashTable * set_session_hash){
  GString *str= g_string_new(sql_mode);
  g_string_replace(str, "ORACLE", "", 0);
  g_string_replace(str, ",,", ",", 0);
  set_session_hash_insert(set_session_hash, "SQL_MODE",
		  g_string_free(str, FALSE));
}

static
GHashTable * mydumper_initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=initialize_hash_of_session_variables();
  set_session_hash_insert(set_session_hash, "information_schema_stats_expiry", g_strdup("0 /*!80003"));
  return set_session_hash;
}

static
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
  MYSQL_RES *res = m_store_result(conn, "SELECT FIND_IN_SET('ANSI', @@SQL_MODE) OR FIND_IN_SET('ANSI_QUOTES', @@SQL_MODE)", m_warning, "We were not able to determine ANSI mode",NULL);
  if (!res){
    identifier_quote_character= BACKTICK;
    identifier_quote_character_str= "`";
    fields_enclosed_by= "\"";
    identifier_quote_character_protect = &backtick_protect;
		return ;
  }

  MYSQL_ROW row = mysql_fetch_row(res);
  if (row && !strcmp(row[0], "0")) {
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
  struct M_ROW *mr = m_store_result_single_row(conn, "SELECT @@SQL_MODE", "Error getting SQL_MODE",NULL);

  if (!mr->res || !mr->row){
    m_store_result_row_free(mr);
    return;
  }

  GString *str= g_string_new(NULL);

  if (!g_strstr_len(mr->row[0],-1, "NO_AUTO_VALUE_ON_ZERO"))
    g_string_printf(str, "'NO_AUTO_VALUE_ON_ZERO,%s'", mr->row[0]);
  else
    g_string_printf(str, "'%s'", mr->row[0]);
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
  m_store_result_row_free(mr);
}

static
MYSQL *create_main_connection() {
  MYSQL *conn;
  conn = mysql_init(NULL);

  m_connect(conn); //, db_items!=NULL?db_items[0]:db);

  set_session = g_string_new(NULL);
  set_global = g_string_new(NULL);
  set_global_back = g_string_new(NULL);
  server_detect(conn);
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

  switch (get_product()) {
  case SERVER_TYPE_MYSQL:
  case SERVER_TYPE_MARIADB:
  case SERVER_TYPE_PERCONA:
  case SERVER_TYPE_DOLT:
  case SERVER_TYPE_RDS:
  case SERVER_TYPE_UNKNOWN:
    set_transaction_isolation_level_repeatable_read(conn);
    break;
  case SERVER_TYPE_TIDB:
    data_checksums=FALSE;
    break;
  }

  g_message("Connected to %s %d.%d.%d", get_product_name(), get_major(), get_secondary(), get_revision());
  
  return conn;
}

static
void get_not_updated(MYSQL *conn, FILE *file) {
  MYSQL_ROW row;

  gchar *query =
      g_strdup_printf("SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM "
                      "information_schema.TABLES WHERE TABLE_TYPE = 'BASE "
                      "TABLE' AND UPDATE_TIME < NOW() - INTERVAL %d DAY",
                      updated_since);
  MYSQL_RES *res = m_store_result(conn, query, m_warning, "Updated since query failed", NULL);
  g_free(query);

  if (!res)
    return;

  while ((row = mysql_fetch_row(res))) {
    no_updated_tables = g_list_prepend(no_updated_tables, row[0]);
    fprintf(file, "%s\n", row[0]);
  }
  mysql_free_result(res);
  no_updated_tables = g_list_reverse(no_updated_tables);
  fflush(file);
}

static
void long_query_wait(MYSQL *conn){
  char *p3=NULL;
  while (TRUE) {
    int longquery_count = 0;
    MYSQL_RES *res = m_store_result(conn,"SHOW PROCESSLIST", m_warning, "Could not check PROCESSLIST, no long query guard enabled");
    if (!res){
       break;
    } else {
      MYSQL_ROW row;

      /* Just in case PROCESSLIST output column order changes */
      int tcol = -1, ccol = -1, icol = -1, ucol = -1;
      determine_columns_on_show_processlist(mysql_fetch_fields(res), mysql_num_fields(res), &icol, &ucol, &ccol, &tcol, NULL);
      while ((row = mysql_fetch_row(res))) {
        if (row[ccol] && strcmp(row[ccol], "Query"))
          continue;
        if (row[ucol] && !strcmp(row[ucol], "system user"))
          continue;
        if (row[tcol] && atoi(row[tcol]) > longquery) {
          if (killqueries) {
            if (m_query_warning(conn, p3 = g_strdup_printf("KILL %lu", atol(row[icol])), "Could not KILL slow query", NULL)){
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

static
void send_backup_stage_on_block_commit(MYSQL *conn){
  m_query_verbose(conn, "BACKUP STAGE BLOCK_COMMIT", m_critical, "Couldn't acquire BACKUP STAGE BLOCK_COMMIT", NULL);
}

static
void send_mariadb_backup_locks(MYSQL *conn){
  m_query_verbose(conn, "BACKUP STAGE START", m_critical, "Couldn't acquire BACKUP STAGE START", NULL);
  m_query_verbose(conn, "BACKUP STAGE BLOCK_DDL", m_critical, "Couldn't acquire BACKUP STAGE BLOCK_DDL", NULL);
}

static
void send_percona57_backup_locks(MYSQL *conn){
  m_query_verbose(conn, "LOCK TABLES FOR BACKUP", m_critical, "Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will not be consistent", NULL);
  m_query_verbose(conn, "LOCK BINLOG FOR BACKUP", m_critical, "Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will not be consistent", NULL);
}

static
void send_ddl_lock_instance_backup(MYSQL *conn){
  m_query_verbose(conn, "LOCK INSTANCE FOR BACKUP", m_critical, "Couldn't acquire LOCK INSTANCE FOR BACKUP", NULL);
} 

static
void send_unlock_tables(MYSQL *conn){
  m_query_verbose(conn, "UNLOCK TABLES", m_warning, "Failed to UNLOCK TABLES", NULL);
}

static
void send_unlock_binlogs(MYSQL *conn){
  m_query_verbose(conn, "UNLOCK BINLOG", m_warning, "Failed to UNLOCK BINLOG", NULL);
}

static
void send_ddl_unlock_instance_backup(MYSQL *conn){
  m_query_verbose(conn, "UNLOCK INSTANCE", m_warning, "Failed to UNLOCK INSTANCE", NULL);
}

static
void send_backup_stage_end(MYSQL *conn){
  m_query_verbose(conn, "BACKUP STAGE END", m_warning, "Failed to BACKUP STAGE END", NULL);
}

static
void send_flush_table_with_read_lock(MYSQL *conn){
  guint id=mysql_thread_id(conn);
  m_thread_new("mon_ftwrl", monitor_ftwrl_thread, &id, "FTWRL monitor thread could not be created");

  guint i=0;
try_FLUSH_NO_WRITE_TO_BINLOG_TABLES:
  i++;
  if (( m_query_verbose(conn, FLUSH_NO_WRITE_TO_BINLOG_TABLES, m_warning, "Flush tables failed, we are continuing anyways")) && 
      ( ftwrl_timeout_retries == 0 || (i < ftwrl_timeout_retries )))
      goto try_FLUSH_NO_WRITE_TO_BINLOG_TABLES;

try_FLUSH_TABLES_WITH_READ_LOCK:
  if (( m_query_verbose(conn, FLUSH_TABLES_WITH_READ_LOCK, m_critical, "Couldn't acquire global lock, snapshots will not be consistent")) &&
      ( ftwrl_timeout_retries == 0 || i < ftwrl_timeout_retries ))
      goto try_FLUSH_TABLES_WITH_READ_LOCK;

  ftwrl_completed=TRUE;
}

static
void initialize_tidb_snapshot(MYSQL *conn){
  if (!tidb_snapshot){
    // Generate a @@tidb_snapshot to use for the worker threads since
    // the tidb-snapshot argument was not specified when starting mydumper
    struct M_ROW *mr = m_store_result_row(conn, show_binary_log_status, m_critical, m_warning, "Couldn't generate @@tidb_snapshot");
    tidb_snapshot = g_strdup(mr->row[1]);
    m_store_result_row_free(mr);
  }
  // Need to set the @@tidb_snapshot for the master thread
  set_tidb_snapshot(conn);
  g_message("Set to tidb_snapshot '%s'", tidb_snapshot);
}

static
void default_locking(void(**acquire_global_lock_function)(MYSQL *), void (**release_global_lock_function)(MYSQL *), void (**acquire_ddl_lock_function)(MYSQL *), void (** release_ddl_lock_function)(MYSQL *), void (** release_binlog_function)(MYSQL *)) {
  *acquire_ddl_lock_function = NULL;
  *release_ddl_lock_function = NULL;

  *acquire_global_lock_function = &send_flush_table_with_read_lock;
  *release_global_lock_function = &send_unlock_tables;

  *release_binlog_function = NULL;
}

static
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
    case SERVER_TYPE_RDS:
      m_critical("We support LOCK_ALL and SAFE_NO_LOCK modes for RDS/Aurora. Select one of them to configure --sync-thread-lock-mode");
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
      }else{
        default_locking( acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
      }
      break;
    case SERVER_TYPE_TIDB:
      *acquire_global_lock_function=&initialize_tidb_snapshot;
      break;
    default:
      default_locking( acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function);
      break;
  }
}


// see write_database_on_disk() for db write to metadata

void print_dbt_on_metadata_gstring(struct db_table *dbt, GString *data){
  char *name= newline_protect(dbt->database->source_database);
  char *table= newline_protect(dbt->table);
  g_mutex_lock(dbt->chunks_mutex);
  gchar *lkey=build_dbt_key(dbt->database->database_name_in_filename, dbt->table_filename);
  g_string_append_printf(data,"\n[%s]\n", lkey);
  g_string_append_printf(data, "real_table_name=%s\nrows = %"G_GINT64_FORMAT"\n", table, dbt->rows);
  g_free(name);
  g_free(lkey);
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

static
void print_dbt_on_metadata(FILE *mdfile, struct db_table *dbt){
  GString *data = g_string_sized_new(100);
  print_dbt_on_metadata_gstring(dbt, data);
  fprintf(mdfile, "%s", data->str);
  if (check_row_count && !dbt->object_to_export.no_data && (dbt->rows != dbt->rows_total)) {
    m_critical("Row count mismatch found for %s.%s: got %u of %u expected",
               dbt->database->source_database, dbt->table, dbt->rows, dbt->rows_total);
  }
}

static
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
      res=m_store_result_critical(conn, query->str, "Error showing tables in: %s - Could not execute query", dt[0]);
      if (res){
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
    if (source_db) {
      GString *db_quoted_list=NULL;
      guint i=0;
      db_quoted_list=g_string_sized_new(strlen(source_db));
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
    res = m_store_result_critical(conn, query->str, "Couldn't get table list for lock all tables", NULL);
    if (res){
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
    g_message("Initialing Lock All tables");
    while (g_list_length(tables_lock) > 0 && !success && retry < 4 ) {
      g_string_set_size(query,0);
      g_string_append(query, "LOCK TABLE ");
      for (iter = tables_lock; iter != NULL; iter = iter->next) {
        g_string_append_printf(query, " %s READ,", (char *)iter->data);
      }
      g_strrstr(query->str,",")[0]=' ';

      if (m_query_warning(conn, query->str, "Lock Table failed", NULL)) {
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
//        g_message("LOCK TABLES: %s", query->str);
        success = 1;
      }
      retry += 1;
    }
    if (!success) {
      m_critical("Lock all tables fail: %s", mysql_error(conn));
    }
    g_message("Lock All tables completed");
  }else{
    g_warning("No table found to lock");
//    exit(EXIT_FAILURE);
  }
  mysql_free_result(res);
  g_free(query->str);
  g_list_free(tables_lock);
}

guint isms = 0;
static
void m_stop_replica(MYSQL *conn) {
  MYSQL_RES *slave = NULL;
  MYSQL_RES *rest=NULL;
  if (get_product() == SERVER_TYPE_MARIADB ){
    rest=m_store_result(conn, "SELECT @@default_master_connection", m_warning, "Variable @@default_master_connection not found", NULL);
    if (rest != NULL && mysql_num_rows(rest)) {
      mysql_free_result(rest);
      g_message("Multisource slave detected.");
      isms = 1;
    }
  }

  if (isms)
    m_query_critical(conn, show_all_replicas_status, "Error executing %s", show_all_replicas_status);
  else
    m_query_critical(conn, show_replica_status, "Error executing %s", show_replica_status);

  slave = mysql_store_result(conn);

  if (!slave || mysql_num_rows(slave) == 0){
    goto cleanup;
  }
  g_message("Stopping replica");
  replica_stopped=!m_query_warning(conn, stop_replica_sql_thread, "Not able to stop replica",NULL);
  if (source_control_command==AWS){
    discard_mysql_output(conn);
  }

cleanup:
  if (slave)
    mysql_free_result(slave);
}

// Here is where the backup process start

void start_dump(struct configuration *conf) {
  memset(conf, 0, sizeof(struct configuration));

  MYSQL *conn = NULL, *second_conn = NULL;
  char *metadata_partial_filename, *metadata_filename;
  char *u;
  void (*acquire_global_lock_function)(MYSQL *) = NULL;
  void (*release_global_lock_function)(MYSQL *) = NULL;
  void (*acquire_ddl_lock_function)(MYSQL *) = NULL;
  void (*release_ddl_lock_function)(MYSQL *) = NULL;
  void (*release_binlog_function)(MYSQL *) = NULL;
  struct db_table *dbt=NULL;
  guint n;
  FILE *nufile = NULL;
  GThread *disk_check_thread = NULL;
  GThread *sthread = NULL;
  FILE *mdfile=NULL;

  // Initializing process
  if (clear_dumpdir)
    clear_dump_directory(dump_directory);
  else if (!(dirty_dumpdir || merge_dumpdir) && !is_empty_dir(dump_directory)) {
    g_error("Directory is not empty (use --clear, --dirty or --merge): %s\n", dump_directory);
  }

  check_num_threads();
  g_message("Using %u dumper threads", num_threads);
  initialize_start_dump();
  initialize_common();
  initialize_create_jobs(conf);
  initialize_connection(MYDUMPER);
  initialize_masquerade();

  /* Give ourselves an array of tables to dump */
  if (tables_list)
    tables = get_table_list(tables_list);

  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);

  initialize_regex(partition_regex);

  // Connecting to the database
  conn = create_main_connection();
  main_connection = conn;
  second_conn = conn;
  conf->use_any_index= 1;

  if (disk_limits!=NULL){
    conf->pause_resume = g_async_queue_new();
    disk_check_thread = m_thread_new("mon_disk",monitor_disk_space_thread, conf->pause_resume, "Monitor thread could not be created");
  }

//  GThread *throttling_thread = 
  if (throttle_variable)
    m_thread_new("mon_thro",monitor_throttling_thread, NULL, "Monitor throttling thread could not be created");

  // signal_thread is disable if daemon mode
  if (!daemon_mode)
    sthread = m_thread_new("signal", signal_thread, conf, "Signal thread could not be created");

  // Initilizing METADATA file
  if (stream)
    metadata_partial_filename= g_strdup_printf("%s/metadata.header", dump_directory);
  else
    metadata_partial_filename= g_strdup_printf("%s/metadata.partial", dump_directory);
  metadata_filename = g_strdup_printf("%s/metadata", dump_directory);

  if (merge_dumpdir)
   if (g_rename(metadata_filename, metadata_partial_filename))
     m_critical("We were not able to rename metadata (%s) file to %s",metadata_filename, metadata_partial_filename);

  mdfile = g_fopen(metadata_partial_filename, "a");
  if (!mdfile) {
    m_critical("Couldn't create metadata file %s (%s)", metadata_partial_filename, strerror(errno));
  }

  // Initilizing NOT UPDATED TABLES feature 
  if (updated_since > 0) {
    u = g_strdup_printf("%s/not_updated_tables", dump_directory);
    nufile = g_fopen(u, "w");
    if (!nufile) {
      m_critical("Couldn't write not_updated_tables file (%d)", errno);
    }
    get_not_updated(conn, nufile);
  }

  // If we are locking, we need to be sure there is no long running queries
  if (sync_thread_lock_mode!=NO_LOCK && sync_thread_lock_mode!=SAFE_NO_LOCK && is_mysql_like()) {
  // We check SHOW PROCESSLIST, and if there're queries
  // larger than preset value, we terminate the process.
  // This avoids stalling whole server with flush.
		long_query_wait(conn);
  }

  // Recoring that backup has started
  GDateTime *datetime = g_date_time_new_now_local();
  char *datetimestr=g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S");
  g_date_time_unref(datetime);
  fprintf(mdfile, "# Started dump at: %s\n", datetimestr);
  g_message("Started dump at: %s", datetimestr);
  g_free(datetimestr);

  /* Write dump config into beginning of metadata, stream this first */
  g_assert(identifier_quote_character == BACKTICK || identifier_quote_character == DOUBLE_QUOTE);
  const char *qc= identifier_quote_character == BACKTICK ? "BACKTICK" : "DOUBLE_QUOTE";
  fprintf(mdfile, "[config]\nquote-character = %s\n", qc);
  if (load_data || csv )
    fprintf(mdfile, "local-infile = 1\n");
  fprintf(mdfile, "\n[myloader_session_variables]");
  fprintf(mdfile, "\nSQL_MODE=%s /*!40101\n", sql_mode);
  fflush(mdfile);

  // Initilizing stream backup
  if (stream){
    if (exec_command)
      g_error("--exec and --stream are not comptabile, use --exec-per-thread instead as file extension is needed to stream the out file");
    initialize_stream();
    fclose(mdfile);
    stream_queue_push(NULL, g_strdup(metadata_partial_filename));
    metadata_partial_filename= g_strdup_printf("%s/metadata.partial", dump_directory);
    mdfile= g_fopen(metadata_partial_filename, "w");
    if (!mdfile) {
      m_critical("Couldn't create metadata file %s (%s)", metadata_partial_filename, strerror(errno));
    }
  }

  // Initilizing exec_command
  if (exec_command != NULL)
    initialize_exec_command();

  // Write replica information
  if (get_product() != SERVER_TYPE_TIDB) {
    if (replica_data.enabled)
      m_stop_replica(conn);
  }

  // Determine the locking mechanisim that is going to be used
  // and send locks to database if needed
  switch (sync_thread_lock_mode){
    case NO_LOCK:
      g_message("Executing in NO_LOCK mode, we are not able to ensure that backup will be consistent");
      break;
    case SAFE_NO_LOCK:
      g_message("Executing in SAFE_NO_LOCK mode. This backup will fail if all threads are not in the same point in time, which ensures consistency");
      break;
    case LOCK_ALL:
      send_lock_all_tables(conn);
      break;
    case AUTO:
      determine_ddl_lock_function(&second_conn, &acquire_global_lock_function,&release_global_lock_function, &acquire_ddl_lock_function, &release_ddl_lock_function, &release_binlog_function);
      break;
    case FTWRL:
      determine_ddl_lock_function(&second_conn, &acquire_global_lock_function,&release_global_lock_function, &acquire_ddl_lock_function, &release_ddl_lock_function, &release_binlog_function);
      acquire_global_lock_function = &send_flush_table_with_read_lock;
      release_global_lock_function = &send_unlock_tables;
      break;
    case GTID:
      g_message("Using binlog_snapshot_gtid_executed which doesn't lock the database but uses best effort to sync the threads");
      break;
  }
  if (skip_ddl_locks){
    acquire_ddl_lock_function=NULL;
    release_ddl_lock_function=NULL;
  }

  if (acquire_ddl_lock_function != NULL) {
    g_message("Acquiring DDL lock");
    acquire_ddl_lock_function(second_conn);
  }

  if (acquire_global_lock_function != NULL) {
    g_message("Acquiring Global lock");
    acquire_global_lock_function(conn);
  }

  // TODO: this should be deleted on future releases. 
  server_version= mysql_get_server_version(conn);
  if (server_version < 40108) {
    m_query_warning(conn, "CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB", "Not able to create dummy table for InnoDB", NULL);
    need_dummy_read = 1;
  }
  // TODO: MySQL also supports PACKAGE (Percona?)
  if (get_product() != SERVER_TYPE_MARIADB || server_version < 100300)
    nroutines= 2;

  // tokudb do not support consistent snapshot
  MYSQL_RES *rest = m_store_result(conn, "SELECT @@tokudb_version", m_message, "@@tokudb_version not found", NULL);
  if (rest){
    if (mysql_num_rows(rest)) {
      mysql_free_result(rest);
      g_message("TokuDB detected, creating dummy table for CS");
      m_query_warning(conn, "CREATE TABLE IF NOT EXISTS mysql.tokudbdummy (a INT) ENGINE=TokuDB", "Not able to create dummy table for TokuDB", NULL);
      need_dummy_toku_read = 1;
    }
    mysql_free_result(rest);
  }

  if (need_dummy_read) {
    rest = m_store_result(conn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy", m_warning, "Select on mysql.mydumperdummy has failed", NULL);
    if (rest)
      mysql_free_result(rest);
  }
  if (need_dummy_toku_read) {
    rest = m_store_result(conn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy", m_warning, "Select on mysql.tokudbdummy has failed", NULL);
    if (rest)
      mysql_free_result(rest);
  }
  trace("Initilizing the configuration");
  // Initilizing the configuration

  conf->initial_queue = g_async_queue_new();
  conf->initial_completed_queue = g_async_queue_new();

  conf->schema_queue = g_async_queue_new();
  conf->post_data_queue = g_async_queue_new();
  conf->transactional.queue= g_async_queue_new();
  conf->transactional.defer= g_async_queue_new();
  // These are initialized in the guts of initialize_start_dump() above
  g_assert(give_me_another_transactional_chunk_step_queue &&
           give_me_another_non_transactional_chunk_step_queue &&
           transactional_table &&
           non_transactional_table);
  conf->transactional.request_chunk= give_me_another_transactional_chunk_step_queue;
  conf->transactional.table_list= transactional_table;
  conf->transactional.descr= "transactional";
  conf->ready = g_async_queue_new();
  conf->non_transactional.queue= g_async_queue_new();
  conf->non_transactional.defer= g_async_queue_new();
  conf->non_transactional.request_chunk= give_me_another_non_transactional_chunk_step_queue;
  conf->non_transactional.table_list= non_transactional_table;
  conf->non_transactional.descr= "non-transactional";
  conf->ready_non_transactional_queue = g_async_queue_new();
  conf->unlock_tables = g_async_queue_new();
  conf->gtid_pos_checked = g_async_queue_new();
  conf->are_all_threads_in_same_pos = g_async_queue_new();
  conf->db_ready = g_async_queue_new();
  conf->source_and_replica_status_queue = g_async_queue_new();
  ready_table_dump_mutex = g_rec_mutex_new();
  g_rec_mutex_lock(ready_table_dump_mutex);

  trace("Begin Job Creation");
  // Begin Job Creation

  // Create metadata job
  if (is_mysql_like())
    create_job_to_write_source_and_replica_status(mdfile);
  else
    g_async_queue_push(conf->source_and_replica_status_queue,GINT_TO_POINTER(1));
  
  trace("Create tablespace jobs");
  // Create tablespace jobs
  if (dump_tablespaces){
    create_job_to_dump_tablespaces();
  }

  // There are 3 ways to dump tables based on the filters
  // - Specific tables
  // - Specific databases
  // - All databases
  //
  // if tables and db both exists , should not call dump_database_thread
  if (tables && g_strv_length(tables) > 0) {
    trace("Specific tables");
    create_job_to_dump_table_list(tables);
  } else if (db_items && g_strv_length(db_items) > 0) {
    trace("Specific databases");
    guint i=0;
    for (i=0;i<g_strv_length(db_items);i++){
      struct database *this_db=get_database(conn,db_items[i],!no_schemas);
      create_job_to_dump_database(this_db);
    }
  } else {
    trace("All databases");
    create_job_to_dump_all_databases();
  }

  trace("End Job Creation");
  // End Job Creation

  // Starting the chunk builder
  start_chunk_builder(conf);

  // Starting the workers threads
  start_working_thread(conf);
  g_async_queue_pop(conf->source_and_replica_status_queue);
  g_async_queue_unref(conf->source_and_replica_status_queue);

  gchar *source_log = NULL;
  gchar *source_pos = NULL;
  gchar *source_gtid = NULL;
  get_binlog_position(conn, &source_log, &source_pos, &source_gtid);
  
  if (g_strcmp0(source_log, initial_source_log) ||
      g_strcmp0(source_pos, initial_source_pos) ||
      g_strcmp0(source_gtid,initial_source_gtid)){
    if (sync_thread_lock_mode == NO_LOCK){
      g_warning("There are differences in the binlog position at the beginning of the backup and after syncing threads, so we cannot guarantee the backup to be consistent due to the use of NO_LOCK. Continues anyway, use SAFE_NO_LOCK otherwise.");
      trace("Backup will be inconsistent %s %s %d || %s %s %d || %s %s %d", source_log, initial_source_log, g_strcmp0(source_log, initial_source_log), source_pos, initial_source_pos,g_strcmp0(source_pos, initial_source_pos), source_gtid,initial_source_gtid, g_strcmp0(source_gtid,initial_source_gtid));
    } else if (sync_thread_lock_mode == SAFE_NO_LOCK){
      trace("Backup will be inconsistent %s %s %d || %s %s %d || %s %s %d", source_log, initial_source_log, g_strcmp0(source_log, initial_source_log), source_pos, initial_source_pos,g_strcmp0(source_pos, initial_source_pos), source_gtid,initial_source_gtid, g_strcmp0(source_gtid,initial_source_gtid));
      m_error("There are differences in the binlog position at the beginning of the backup and after syncing threads, so we cannot guarantee the backup to be consistent. Stopping backup due to the use of SAFE_NO_LOCK.");
    }
  }else{
    g_message("Backup will be consistent");
  }

  // IMPORTANT: At this point, all the threads are in sync

  if (trx_tables) {
    // Releasing locks as user instructed that all tables are transactional
    g_message("Transactions started, unlocking tables");
    if (release_binlog_function != NULL){
      g_message("Releasing binlog lock");
      release_binlog_function(second_conn);
    }
    if (release_global_lock_function)
      release_global_lock_function(conn);
    if (is_mysql_like() && replica_stopped){
      g_message("Starting replica");
      m_query_warning(conn, start_replica_sql_thread, "Not able to start replica", NULL);

      if (source_control_command==AWS){
        discard_mysql_output(conn);
      }
      replica_stopped=FALSE;
    }
  }

  // Every time a schema job is created a counter increases
  // Every time that a schema jobs is completed, the counter decreases
  // When the counter reaches to 0, it releases conf->db_ready 
  g_message("Waiting database finish");
  g_async_queue_pop(conf->db_ready);

  // At this point all schema jobs are completed

  g_list_free(no_updated_tables);

  // We let working threads know that initial_queue has been completed
  // sending them a JOB_SHUTDOWN job.
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf->initial_queue, j);
  }

  for (n = 0; n < num_threads; n++) {
    g_async_queue_pop(conf->initial_completed_queue);
  }
  // at this point initial jobs has been completed
  // which means that all schema jobs has been created 
  // we are able to send the JOB_SHUTDOWN to schema_queue
  g_message("Shutdown schema jobs");
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf->schema_queue, j);
  }
  // In case that we are NOT exporting transactional table, we need to 
  // build the lock table statement, at this stage, before
  // let workers to start dumping data
  if (!trx_tables){
    build_lock_tables_statement(conf);
  }
  // Allowing workers to start dumping Non-Transactional tables
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->ready_non_transactional_queue, GINT_TO_POINTER(1));
  }

  // Releasing locks if possible
  if (sync_thread_lock_mode!=NO_LOCK && sync_thread_lock_mode!=SAFE_NO_LOCK && !trx_tables) {
    for (n = 0; n < num_threads; n++) {
      g_async_queue_pop(conf->unlock_tables);
    }
    if (release_binlog_function != NULL){
      g_message("Releasing binlog lock");
      release_binlog_function(second_conn);
    }
    g_message("Non-InnoDB dump complete, releasing global locks");
    if (release_global_lock_function)
      release_global_lock_function(conn);
    g_message("Global locks released");
  }

  // At this point, we can start the replica if it was stopped
  if (is_mysql_like() && replica_stopped){
    g_message("Starting replica");
    m_query_warning(conn, start_replica_sql_thread, "Not able to start replica", NULL);

    if (source_control_command==AWS){
      discard_mysql_output(conn);
    }
  }

  // All the jobs related to post data has been created and enquequed
  // so, we can send the JOB_SHUTDOWN
  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf->post_data_queue, j);
  }
  // At this point the main process, needs to wait the working threads to finish 
  wait_working_thread_to_finish();

  // Backup is done
  // Starting to finalize it
  finalize_working_thread();
  finalize_chunk();
  finalize_write();

  // Releasing DDL lock if possible
  if (release_ddl_lock_function != NULL) {
    g_message("Releasing DDL lock");
    release_ddl_lock_function(second_conn);
  }

  g_message("Queue count: %d %d %d %d %d", g_async_queue_length(conf->initial_queue),
            g_async_queue_length(conf->schema_queue),
            g_async_queue_length(conf->non_transactional.queue) + g_async_queue_length(conf->non_transactional.defer),
            g_async_queue_length(conf->transactional.queue) + g_async_queue_length(conf->transactional.defer),
            g_async_queue_length(conf->post_data_queue));

  // Closing main connection
  if (conn != second_conn)
    mysql_close(second_conn);
  execute_gstring(main_connection, set_global_back);
  mysql_close(conn);
  g_message("Main connection closed");  

  // There are scenarios where we need to wait files to flush to disk  
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
  g_async_queue_unref(conf->transactional.defer);
  conf->transactional.defer= NULL;
  g_async_queue_unref(conf->transactional.queue);
  conf->transactional.queue= NULL;
  g_async_queue_unref(conf->non_transactional.defer);
  conf->non_transactional.defer= NULL;
  g_async_queue_unref(conf->non_transactional.queue);
  conf->non_transactional.queue= NULL;
  g_async_queue_unref(conf->unlock_tables);
  conf->unlock_tables=NULL;
  g_async_queue_unref(conf->ready);
  conf->ready=NULL;
  g_async_queue_unref(conf->schema_queue);
  conf->schema_queue=NULL;
  g_async_queue_unref(conf->initial_queue);
  conf->initial_queue=NULL;
  g_async_queue_unref(conf->post_data_queue);
  conf->post_data_queue=NULL;

  g_async_queue_unref(conf->ready_non_transactional_queue);
  conf->ready_non_transactional_queue=NULL;

  fprintf(mdfile, "[config]\nmax-statement-size = %" G_GUINT64_FORMAT "\n", max_statement_size);
  fprintf(mdfile, "num-sequences = %d\n", num_sequences);

  datetime = g_date_time_new_now_local();
  datetimestr=g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S");
  g_date_time_unref(datetime);
  fprintf(mdfile, "# Finished dump at: %s\n", datetimestr);
  fclose(mdfile);
  if (updated_since > 0)
    fclose(nufile);

  if (g_rename(metadata_partial_filename, metadata_filename))
    m_critical("We were not able to rename metadata file");

  if (stream) {
    stream_queue_push(NULL, g_strdup(metadata_filename));
    if (exec_command!=NULL){
      wait_exec_command_to_finish();
    }else{
      stream_queue_push(NULL, g_strdup(""));
      wait_stream_to_finish();
    }
    if (no_delete == FALSE && output_directory_str == NULL)
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

  g_async_queue_unref(conf->gtid_pos_checked);
  g_async_queue_unref(conf->are_all_threads_in_same_pos);
  g_async_queue_unref(conf->db_ready);
  g_rec_mutex_clear(ready_table_dump_mutex);
//  g_source_remove(SIGINT);
//  g_source_remove(SIGTERM);
//  g_main_loop_quit(conf->loop);
  if (!daemon_mode && conf->loop)
    g_main_loop_unref(conf->loop);

  if (tables_list)
    g_strfreev(tables);

  free_regex();
  free_common();
}

