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

#include "mydumper_start_dump.h"
#include "mydumper_jobs.h"
#include "mydumper_common.h"
#include "mydumper_stream.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_masquerade.h"
#include "mydumper_jobs.h"
#include "mydumper_chunks.h"
#include "mydumper_write.h"
#include "mydumper_global.h"
#include "mydumper_arguments.h"

/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif



extern int (*m_write)(int file, const char * buff, int len);

GMutex *init_mutex = NULL;
/* Program options */
gboolean use_savepoints = FALSE;
gint database_counter = 0;
//gint table_counter = 0;
gchar *ignore_engines = NULL;
gchar *binlog_snapshot_gtid_executed = NULL;
gboolean binlog_snapshot_gtid_executed_status = FALSE;
guint binlog_snapshot_gtid_executed_count = 0;
char **ignore = NULL;
int skip_tz = 0;
int sync_wait = -1;
gboolean dump_events = FALSE;
gboolean dump_routines = FALSE;
gboolean no_dump_views = FALSE;
gboolean views_as_tables=FALSE;
gboolean no_dump_sequences = FALSE;
gboolean success_on_1146 = FALSE;

GList  *innodb_table = NULL;
GMutex *innodb_table_mutex = NULL;
GList  *non_innodb_table = NULL;
GMutex *non_innodb_table_mutex = NULL;
GMutex *table_schemas_mutex = NULL;
GMutex *all_dbts_mutex=NULL;
GMutex *trigger_schemas_mutex = NULL;
GMutex *view_schemas_mutex = NULL;
guint less_locking_threads = 0;
GHashTable *character_set_hash=NULL;
GMutex *character_set_hash_mutex = NULL;

gboolean dump_checksums = FALSE;
gboolean data_checksums = FALSE;
gboolean schema_checksums = FALSE;
gboolean routine_checksums = FALSE;
gboolean exit_if_broken_table_found = FALSE;
// For daemon mode
int build_empty_files = 0;
gchar *where_option=NULL;
gchar *rows_per_chunk=NULL;

void dump_database_thread(MYSQL *, struct configuration*, struct database *);

guint64 min_chunk_step_size = 0;
guint64 rows_per_file = 0;
guint64 max_chunk_step_size = 0;

void parse_rows_per_chunk(gchar *rows_p_chunk, guint64 *min, guint64 *start, guint64 *max){
  gchar **split=g_strsplit(rows_p_chunk, ":", 0);
  guint len = g_strv_length(split);
  switch (len){
   case 0:
     g_critical("This should not happend");
     break;
   case 1:
     *start= strtol(split[0],NULL, 10);
     *min  = *start;
     *max  = *start;
     break;
   case 2:
     *min  = strtol(split[0],NULL, 10);
     *start= strtol(split[1],NULL, 10);
     *max  = *start;
     break;
   default:
     *min  = strtol(split[0],NULL, 10);
     *start= strtol(split[1],NULL, 10);
     *max  = strtol(split[2],NULL, 10);
     break;
  }
  g_strfreev(split);
}

int m_fopen(char **filename, const char *type ){
	(void) type;
  return open(*filename, O_CREAT|O_WRONLY|O_TRUNC, 0660 );
}


int new_m_fopen(char **filename, int type ){
  return open(*filename, type);
}

void initialize_working_thread(){
  database_counter = 0;
  if (rows_per_chunk)
    parse_rows_per_chunk(rows_per_chunk, &min_chunk_step_size, &rows_per_file, &max_chunk_step_size);

  if (max_chunk_step_size > G_MAXUINT64 / num_threads){
    max_chunk_step_size= G_MAXUINT64 / num_threads;
    m_error("This should not happen");
  }

  character_set_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free);
  character_set_hash_mutex = g_mutex_new();
  non_innodb_table_mutex = g_mutex_new();
  innodb_table_mutex = g_mutex_new();
  view_schemas_mutex = g_mutex_new();
  table_schemas_mutex = g_mutex_new();
  trigger_schemas_mutex = g_mutex_new();
  all_dbts_mutex = g_mutex_new();
  init_mutex = g_mutex_new();
  binlog_snapshot_gtid_executed = NULL;
  if (less_locking)
    less_locking_threads = num_threads;


  /* savepoints workaround to avoid metadata locking issues
     doesnt work for chuncks 

     UPDATE: this is not true anymore
   */
/*  if (rows_per_file && use_savepoints) {
    use_savepoints = FALSE;
    g_warning("--use-savepoints disabled by --rows");
  }
*/

  /* Give ourselves an array of engines to ignore */
  if (ignore_engines)
    ignore = g_strsplit(ignore_engines, ",", 0);


// TODO: We need to cleanup this

  m_close=(void *) &m_close_pipe;
  m_write=(void *)&write;

  if (compress_method==NULL && exec_per_thread==NULL && exec_per_thread_extension == NULL) {
    m_open=&m_fopen;
    m_close=(void *) &m_close_file;
    m_write=(void *)&write;
    exec_per_thread_extension=EMPTY_STRING;
  } else {
    if (compress_method!=NULL && (exec_per_thread!=NULL || exec_per_thread_extension!=NULL)){
      m_critical("--compression and --exec-per-thread are not comptatible");
    }
    gchar *cmd=NULL;
    initialize_gzip_cmd();
    if ( g_strcmp0(compress_method,GZIP)==0){
      if ((cmd=get_gzip_cmd()) == NULL){
        g_error("gzip command not found on any static location, use --exec-per-thread for non default locations");
      }
      exec_per_thread=g_strdup_printf("%s -c", cmd);
      exec_per_thread_command=gzip_cmd;
      exec_per_thread_extension=GZIP_EXTENSION;
    }else{ 
      initialize_zstd_cmd();
      if ( g_strcmp0(compress_method,ZSTD)==0){
      if ( (cmd=get_zstd_cmd()) == NULL ){
        g_error("zstd command not found on any static location, use --exec-per-thread for non default locations");
      }
      exec_per_thread=g_strdup_printf("%s -c", cmd);
      exec_per_thread_command=zstd_cmd;
      exec_per_thread_extension=ZSTD_EXTENSION;
    }
    }
    m_open=&m_open_pipe;
    m_close=&m_close_pipe;
  }

  initialize_jobs();
  initialize_chunk();
  initialize_write();

  if (dump_checksums){
    data_checksums = TRUE;
    schema_checksums = TRUE;
    routine_checksums = TRUE;
  }

}


void finalize_working_thread(){
  g_hash_table_destroy(character_set_hash);
  g_mutex_free(character_set_hash_mutex);
  g_mutex_free(innodb_table_mutex);
  g_mutex_free(non_innodb_table_mutex);
  g_mutex_free(view_schemas_mutex);
  g_mutex_free(table_schemas_mutex);
  g_mutex_free(trigger_schemas_mutex);
  g_mutex_free(all_dbts_mutex);
  g_mutex_free(init_mutex);
  if (binlog_snapshot_gtid_executed!=NULL)
    g_free(binlog_snapshot_gtid_executed);

  finalize_chunk();
}

void thd_JOB_DUMP_ALL_DATABASES( struct thread_data *td, struct job *job){
  // TODO: This should be in a job as needs to be done by a thread.
  MYSQL_RES *databases;
  MYSQL_ROW row;
  if (mysql_query(td->thrconn, "SHOW DATABASES") ||
      !(databases = mysql_store_result(td->thrconn))) {
    m_critical("Unable to list databases: %s", mysql_error(td->thrconn));
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
        create_job_to_dump_schema(db_tmp, td->conf);
        db_tmp->already_dumped=TRUE;
      }
      g_mutex_unlock(db_tmp->ad_mutex);
    }
    create_job_to_dump_database(db_tmp, td->conf);
  }
  if (g_atomic_int_dec_and_test(&database_counter)) {
//    g_rec_mutex_unlock(ready_database_dump_mutex);
    g_async_queue_push(td->conf->db_ready,GINT_TO_POINTER(1));
  }
  mysql_free_result(databases);
  g_free(job);
}

void thd_JOB_DUMP_DATABASE(struct thread_data *td, struct job *job){
  struct dump_database_job * ddj = (struct dump_database_job *)job->job_data;
  g_message("Thread %d: dumping db information for `%s`", td->thread_id,
            ddj->database->name);
  dump_database_thread(td->thrconn, td->conf, ddj->database);
  g_free(ddj);
  g_free(job);
  if (g_atomic_int_dec_and_test(&database_counter)) {
    g_async_queue_push(td->conf->db_ready,GINT_TO_POINTER(1));
//    g_rec_mutex_unlock(ready_database_dump_mutex);
  }
}

void get_table_info_to_process_from_list(MYSQL *conn, struct configuration *conf, gchar ** table_list) {

  gchar **dt = NULL;
  char *query = NULL;
  guint x;

  for (x = 0; table_list[x] != NULL; x++) {
    dt = g_strsplit(table_list[x], ".", 0);

  // Need 7 columns with DATA_LENGTH as the last one for this to work
    if (detected_server == SERVER_TYPE_MARIADB){
      query =
          g_strdup_printf("SELECT TABLE_NAME, ENGINE, TABLE_TYPE as COMMENT, TABLE_COLLATION as COLLATION, AVG_ROW_LENGTH, DATA_LENGTH FROM "
                          "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'",
                          dt[0], dt[1]);
    }else{
      query =
          g_strdup_printf("SHOW TABLE STATUS FROM %s LIKE '%s'", dt[0], dt[1]);
    }
    if (mysql_query(conn, (query))) {
      g_critical("Error showing table status on: %s - Could not execute query: %s", dt[0],
                 mysql_error(conn));
      errors++;
      return;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    guint ecol = -1, ccol = -1, collcol = -1, rowscol = 0;
    determine_show_table_status_columns(result, &ecol, &ccol, &collcol, &rowscol);
    struct database * database=NULL;
    if (get_database(conn, dt[0], &database)){
      if (!database->already_dumped){
        g_mutex_lock(database->ad_mutex);
        if (!database->already_dumped){
          create_job_to_dump_schema(database, conf);
          database->already_dumped=TRUE;
        }
        g_mutex_unlock(database->ad_mutex);
//        g_async_queue_push(conf->ready_database_dump, GINT_TO_POINTER(1));
      }
    }

    if (!result) {
      g_critical("Could not list tables for %s: %s", database->name, mysql_error(conn));
      errors++;
      return;
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {

      int is_view = 0;
      int is_sequence = 0;

      if ((detected_server == SERVER_TYPE_MYSQL ||
           detected_server == SERVER_TYPE_MARIADB) &&
          (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
        is_view = 1;

      if ((detected_server == SERVER_TYPE_MARIADB) &&
          (row[ccol] == NULL || !strcmp(row[ccol], "SEQUENCE")))
        is_sequence = 1;

      /* Checks skip list on 'database.table' string */
      if (tables_skiplist_file && check_skiplist(database->name, row[0]))
        continue;

      /* Checks PCRE expressions on 'database.table' string */
      if (!eval_regex(database->name, row[0]))
        continue;

      new_table_to_dump(conn, conf, is_view, is_sequence, database, row[0], row[collcol], row[6], row[ecol], rowscol>0?g_ascii_strtoull(row[rowscol], NULL, 10):0);
    }
    mysql_free_result(result);
    g_strfreev(dt);
  }

  g_free(query);
  if (g_atomic_int_dec_and_test(&database_counter)) {
    g_async_queue_push(conf->db_ready,GINT_TO_POINTER(1));
//    g_rec_mutex_unlock(ready_database_dump_mutex);
  }

}



void thd_JOB_DUMP_TABLE_LIST(struct thread_data *td, struct job *job){
  struct dump_table_list_job * dtlj = (struct dump_table_list_job *)job->job_data;
  get_table_info_to_process_from_list(td->thrconn, td->conf, dtlj->table_list);
  g_free(dtlj);
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


void thd_JOB_DUMP(struct thread_data *td, struct job *job){
  struct table_job *tj = (struct table_job *)job->job_data;

  if (use_savepoints){
    if (td->table_name!=NULL){
      if (tj->dbt->table != td->table_name){
        if ( mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
          g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
        }
        if (mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
          g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
        }
        td->table_name = tj->dbt->table;
      }
    }else{
      if (mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
        g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
      }
      td->table_name = tj->dbt->table;
    }
  }
  tj->td=td;

//  g_debug("chunk_type: %d %p", tj->dbt->chunk_type, tj->dbt->chunk_functions.process);
  tj->chunk_step_item->chunk_functions.process(tj, tj->chunk_step_item);

/*  if (use_savepoints &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }*/
  free_table_job(tj);
  tj->td=NULL;
  g_free(job);
}

void initialize_thread(struct thread_data *td){
  m_connect(td->thrconn);
  g_message("Thread %d: connected using MySQL connection ID %lu",
            td->thread_id, mysql_thread_id(td->thrconn));
}

void initialize_consistent_snapshot(struct thread_data *td){
  if ( sync_wait != -1 && mysql_query(td->thrconn, g_strdup_printf("SET SESSION WSREP_SYNC_WAIT = %d",sync_wait))){
    m_critical("Failed to set wsrep_sync_wait for the thread: %s",
               mysql_error(td->thrconn));
  }
  set_transaction_isolation_level_repeatable_read(td->thrconn);
  guint start_transaction_retry=0;
  gboolean cont = FALSE; 
  while ( !cont && (start_transaction_retry < MAX_START_TRANSACTION_RETRIES )){
//  Uncommenting the sleep will cause inconsitent scenarios always, which is useful for debugging 
//    sleep(td->thread_id);
    g_debug("Thread %d: Start trasaction #%d", td->thread_id, start_transaction_retry);
    if (mysql_query(td->thrconn,
                  "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")) {
      m_critical("Failed to start consistent snapshot: %s", mysql_error(td->thrconn));
    }
    if (mysql_query(td->thrconn,
                  "SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'")) {
      g_warning("Failed to get binlog_snapshot_gtid_executed: %s", mysql_error(td->thrconn));
    }else{
      MYSQL_RES *res = mysql_store_result(td->thrconn);
      if (res){
        MYSQL_ROW row = mysql_fetch_row(res);
        if (row!=NULL)
          td->binlog_snapshot_gtid_executed=g_strdup(row[1]);
        else
          td->binlog_snapshot_gtid_executed=g_strdup("");
        mysql_free_result(res);
      }else{
        g_warning("Failed to get content of binlog_snapshot_gtid_executed: %s", mysql_error(td->thrconn));
        td->binlog_snapshot_gtid_executed=g_strdup("");
      }
    }
    start_transaction_retry++;
    g_async_queue_push(td->conf->gtid_pos_checked, GINT_TO_POINTER(1));
    cont=GPOINTER_TO_INT(g_async_queue_pop(td->conf->are_all_threads_in_same_pos)) == 1;
  } 

  if (g_strcmp0(td->binlog_snapshot_gtid_executed,"")==0){
    if (no_locks){
      g_warning("We are not able to determine if the backup will be consistent.");
    }
  }else{
    if (cont){
        g_message("All threads in the same position. This will be a consistent backup.");
        it_is_a_consistent_backup=TRUE;
    }else{
      if (no_locks){ 
        g_warning("Backup will not be consistent, but we are continuing because you use --no-locks.");
      }else{
        m_critical("Backup will not be consistent. Threads are in different points in time. Use --no-locks if you expect inconsistent backups.");
      }
    }
  }
}

void check_connection_status(struct thread_data *td){
  if (detected_server == SERVER_TYPE_TIDB) {
    // Worker threads must set their tidb_snapshot in order to be safe
    // Because no locking has been used.
    set_tidb_snapshot(td->thrconn);
    g_message("Thread %d: set to tidb_snapshot '%s'", td->thread_id,
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

  char *slavehost = NULL;
  char *slavelog = NULL;
  char *slavepos = NULL;
  char *slavegtid = NULL;
  char *channel_name = NULL;
  const char *gtid_title = NULL;
  guint isms;
  guint i;

  
  if (mysql_query(conn, "SHOW MASTER STATUS"))
    m_critical("Couldn't get master position: %s", mysql_error(conn));
    
  master = mysql_store_result(conn);
  if (master && (row = mysql_fetch_row(master))) {
    masterlog = row[0];
    masterpos = row[1];
    /* Oracle/Percona GTID */
    if (mysql_num_fields(master) == 5) {
      mastergtid = remove_new_line(row[4]);
    } else {
      /* Let's try with MariaDB 10.x */
      /* Use gtid_binlog_pos due to issue with gtid_current_pos with galera
 *        * cluster, gtid_binlog_pos works as well with normal mariadb server
 *               * https://jira.mariadb.org/browse/MDEV-10279 */
      mysql_query(conn, "SELECT @@gtid_binlog_pos");
      mdb = mysql_store_result(conn);
      if (mdb && (row = mysql_fetch_row(mdb))) {
        mastergtid = remove_new_line(row[0]);
      }
    }
  }

  if (masterlog) {
    fprintf(file, "[master]\n# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\nFile = %s\nPosition = %s\nExecuted_Gtid_Set = %s\n\n",
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
    case JOB_DUMP_TABLE_LIST:
      thd_JOB_DUMP_TABLE_LIST(td,job);
      break;
    case JOB_DUMP_DATABASE:
      thd_JOB_DUMP_DATABASE(td,job);
      break;
    case JOB_DUMP_ALL_DATABASES:
      thd_JOB_DUMP_ALL_DATABASES(td,job);
      break;
//    case JOB_TABLE:
//      thd_JOB_TABLE(td, job);
//      break;
    case JOB_WRITE_MASTER_STATUS:
      write_snapshot_info(td->thrconn, job->job_data);
      g_async_queue_push(td->conf->binlog_ready,GINT_TO_POINTER(1));
      g_free(job);
      break;
    case JOB_SHUTDOWN:
      g_free(job);
      return FALSE;
      break;
    default:
      m_error("Something very bad happened!");
  }
  return TRUE;
}

gboolean process_job(struct thread_data *td, struct job *job){
    switch (job->type) {
    case JOB_DETERMINE_CHUNK_TYPE:
      set_chunk_strategy_for_dbt(td->thrconn, (struct db_table *)(job->job_data));
      g_free(job);
      break;
    case JOB_DUMP:
      thd_JOB_DUMP(td, job);
      break;
    case JOB_DUMP_NON_INNODB:
      thd_JOB_DUMP(td, job);
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
    case JOB_SEQUENCE:
      do_JOB_SEQUENCE(td,job);
      break;
    case JOB_TRIGGERS:
      do_JOB_TRIGGERS(td,job);
      break;
    case JOB_SCHEMA_TRIGGERS:
      do_JOB_SCHEMA_TRIGGERS(td,job);
      break;
    case JOB_SCHEMA_POST:
      do_JOB_SCHEMA_POST(td,job);
      break;
    case JOB_WRITE_MASTER_STATUS:
      write_snapshot_info(td->thrconn, job->job_data);
      g_async_queue_push(td->conf->binlog_ready,GINT_TO_POINTER(1));
      g_free(job);
      break;
    case JOB_SHUTDOWN:
      g_free(job);
      return FALSE;
      break;
    default:
      m_error("Something very bad happened!");
    }
  return TRUE;
}

void check_pause_resume( struct thread_data *td ){
  if (td->conf->pause_resume){
    td->pause_resume_mutex = (GMutex *)g_async_queue_try_pop(td->conf->pause_resume);
    if (td->pause_resume_mutex != NULL){
      g_message("Thread %d: Pausing thread",td->thread_id);
      g_mutex_lock(td->pause_resume_mutex);
      g_mutex_unlock(td->pause_resume_mutex);
      td->pause_resume_mutex=NULL;
    }
  }
}

void process_queue(GAsyncQueue * queue, struct thread_data *td, gboolean (*p)(), void (*f)()){
  struct job *job = NULL;
  for (;;) {
    check_pause_resume(td);
    if (f!=NULL)
      f();
    job = (struct job *)g_async_queue_pop(queue);
    if (shutdown_triggered && (job->type != JOB_SHUTDOWN)) {
      g_message("Thread %d: Process has been cacelled",td->thread_id);
      return;
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

void update_estimated_remaining_chunks_on_dbt(struct db_table *dbt){
  GList *l=dbt->chunks;
  guint64 total=0;
  while (l!=NULL){
    switch (((struct chunk_step_item *)l)->chunk_type){
      case INTEGER:
        total+=((union chunk_step *)(l->data))->integer_step.estimated_remaining_steps;
        break;
      case CHAR:
        total+=((union chunk_step *)(l->data))->char_step.estimated_remaining_steps;
        break;
      default:
        total++;
        break; 
    }
    l=l->next;
  }
  dbt->estimated_remaining_steps=total;
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
      m_critical("Failed to disable binlog for the thread: %s",
                 mysql_error(td->thrconn));
    }
    initialize_consistent_snapshot(td);
    check_connection_status(td);
  }
/*  if (set_names_statement){
    mysql_query(td->thrconn, set_names_statement);
  }*/

  g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
  // Thread Ready to process jobs
  
  g_message("Thread %d: Creating Jobs", td->thread_id);
  process_queue(td->conf->initial_queue,td, process_job_builder_job, NULL);

  g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
  g_message("Thread %d: Schema queue", td->thread_id);
  process_queue(td->conf->schema_queue,td, process_job, NULL);

  if (stream) send_initial_metadata();

  if (!no_data){
    g_message("Thread %d: Schema Done, Starting Non-Innodb", td->thread_id);

    g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1)); 
    g_async_queue_pop(td->conf->ready_non_innodb_queue);
    if (less_locking){
      // Sending LOCK TABLE over all non-innodb tables
      if (td->conf->lock_tables_statement!=NULL && mysql_query(td->thrconn, td->conf->lock_tables_statement->str)) {
        m_error("Error locking non-innodb tables %s", mysql_error(td->thrconn));
      }
      // This push will unlock the FTWRL on the Main Connection
      g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
      process_queue(td->conf->non_innodb_queue, td, process_job, give_me_another_non_innodb_chunk_step);
      if (mysql_query(td->thrconn, UNLOCK_TABLES)) {
        m_error("Error locking non-innodb tables %s", mysql_error(td->thrconn));
      }
    }else{
      process_queue(td->conf->non_innodb_queue, td, process_job, give_me_another_non_innodb_chunk_step);
      g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
    }

    g_message("Thread %d: Non-Innodb Done, Starting Innodb", td->thread_id);
    process_queue(td->conf->innodb_queue, td, process_job, give_me_another_innodb_chunk_step);
  //  start_processing(td, resume_mutex);
  }else{
    g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
  }

  if (use_savepoints && td->table_name != NULL &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }

  process_queue(td->conf->post_data_queue, td, process_job, NULL);

  g_message("Thread %d: shutting down", td->thread_id);

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


gboolean has_json_fields(MYSQL *conn, char *database, char *table) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and "
                      "COLUMN_TYPE ='json'",
                      database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  if (res == NULL){
    return FALSE;
  }
  row = mysql_fetch_row(res);
  if (row != NULL){
    mysql_free_result(res);
    return TRUE;
  }
//  g_string_append(field_list, ")");
  mysql_free_result(res);

  return FALSE;
}


struct function_pointer ** get_anonymized_function_for(MYSQL *conn, gchar *database, gchar *table){
  // TODO #364: this is the place where we need to link the column between file loaded and dbt.
  // Currently, we are using identity_function, which return the same data.
  // Key: `database`.`table`.`column`
  gchar * k = g_strdup_printf("`%s`.`%s`",database,table);
  GHashTable *ht = g_hash_table_lookup(conf_per_table.all_anonymized_function,k);
  struct function_pointer ** anonymized_function_list=NULL;
  if (ht){

    MYSQL_RES *res = NULL;
    MYSQL_ROW row;

    gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;",
                      database, table);
    mysql_query(conn, query);
    g_free(query);

    struct function_pointer *fp;
    res = mysql_store_result(conn);
    guint i=0;
    anonymized_function_list = g_new0(struct function_pointer *, mysql_num_rows(res));
    g_message("Using masquerade function on `%s`.`%s`", database, table);
    while ((row = mysql_fetch_row(res))) {
      fp=(struct function_pointer*)g_hash_table_lookup(ht,row[0]);
      if (fp != NULL){
        g_message("Masquerade function found on `%s`.`%s`.`%s`", database, table, row[0]);
        anonymized_function_list[i]=fp;
      }else{
        anonymized_function_list[i]=&pp;
      }
      i++;
    }
    mysql_free_result(res);
  }
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

void get_primary_key_separated_by_comma(struct db_table * dbt) {
  GString *field_list = g_string_new(""); 
  GList *list=dbt->primary_key;
  gboolean first = TRUE;
  while (list){
    if (first) {
      first = FALSE;
    }else{
      g_string_append(field_list, ",");
    }
    g_string_append(field_list, (char*)list->data);
    list=list->next;
  }
  dbt->primary_key_separated_by_comma = g_string_free(field_list, FALSE); 
}

/*
void get_primary_key_string_old(MYSQL *conn, struct db_table * dbt) {
  dbt->primary_key_separated_by_comma = NULL;
  dbt->multicolumn = FALSE;
  if (!order_by_primary_key) return;

  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
          g_strdup_printf("SELECT k.COLUMN_NAME, ORDINAL_POSITION "
                          "FROM information_schema.table_constraints t "
                          "LEFT JOIN information_schema.key_column_usage k "
                          "USING(constraint_name,table_schema,table_name) "
                          "WHERE t.constraint_type IN ('PRIMARY KEY', 'UNIQUE') "
                          "AND t.table_schema='%s' "
                          "AND t.table_name='%s' "
                          "ORDER BY t.constraint_type, ORDINAL_POSITION; ",
                          dbt->database->name, dbt->table);

  if (mysql_query(conn, query)){
    return;
  }
  g_free(query);

  if (!(res = mysql_store_result(conn)))
    return;
  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else if (atoi(row[1]) > 1) {
      if (atoi(row[1]) == 2) {
//        second_field=
      }
      dbt->multicolumn = TRUE;
      g_string_append(field_list, ",");
    } else {
      break;
    }

    gchar *tb = g_strdup_printf("`%s`", row[0]);
    g_string_append(field_list, tb);
    g_free(tb);
  }
  mysql_free_result(res);
  // Return NULL if we never found a PRIMARY or UNIQUE key
  if (first) {
    g_string_free(field_list, TRUE);
    return;
  } else {
    dbt->primary_key_separated_by_comma = g_string_free(field_list, FALSE);
  }
}
*/

struct db_table *new_db_table( MYSQL *conn, struct configuration *conf, struct database *database, char *table, char *table_collation, char *datalength, guint64 rows_in_sts){
  struct db_table *dbt = g_new(struct db_table, 1);
  dbt->status = UNDEFINED;
  dbt->database = database;
  dbt->table = g_strdup(table);
  dbt->table_filename = get_ref_table(dbt->table);
  dbt->rows_in_sts = rows_in_sts;
  dbt->character_set = table_collation==NULL? NULL:get_character_set_from_collation(conn, table_collation);
  dbt->has_json_fields = has_json_fields(conn, dbt->database->name, dbt->table);
  dbt->rows_lock= g_mutex_new();
  dbt->escaped_table = escape_string(conn,dbt->table);
  dbt->anonymized_function=get_anonymized_function_for(conn, dbt->database->name, dbt->table);
  gchar * k = g_strdup_printf("`%s`.`%s`",dbt->database->name,dbt->table);
  dbt->where=g_hash_table_lookup(conf_per_table.all_where_per_table, k);
  dbt->limit=g_hash_table_lookup(conf_per_table.all_limit_per_table, k);
  dbt->columns_on_select=g_hash_table_lookup(conf_per_table.all_columns_on_select_per_table, k);
  dbt->columns_on_insert=g_hash_table_lookup(conf_per_table.all_columns_on_insert_per_table, k);
  dbt->partition_regex=g_hash_table_lookup(conf_per_table.all_partition_regex_per_table, k);
  gchar *rows_p_chunk=g_hash_table_lookup(conf_per_table.all_rows_per_table, k);
  if (rows_p_chunk )
    parse_rows_per_chunk(rows_p_chunk, &(dbt->min_chunk_step_size), &(dbt->starting_chunk_step_size), &(dbt->max_chunk_step_size));
  else{
    dbt->min_chunk_step_size=min_chunk_step_size;
    dbt->starting_chunk_step_size=rows_per_file;
    dbt->max_chunk_step_size=max_chunk_step_size;
  }
  if (dbt->min_chunk_step_size == 1 && dbt->min_chunk_step_size == dbt->starting_chunk_step_size && dbt->starting_chunk_step_size != dbt->max_chunk_step_size ){
    dbt->min_chunk_step_size = 2;
    dbt->starting_chunk_step_size = 2;
    g_warning("Setting min and start rows per file to 2 on %s", k);
  }
  dbt->num_threads=g_hash_table_lookup(conf_per_table.all_num_threads_per_table, k)?strtoul(g_hash_table_lookup(conf_per_table.all_num_threads_per_table, k), NULL, 10):num_threads;
  dbt->estimated_remaining_steps=1;
  dbt->min=NULL;
  dbt->max=NULL;
//  dbt->chunk_type_item.chunk_type = UNDEFINED;
//  dbt->chunk_type_item.chunk_step = NULL;
  dbt->chunks=NULL;
//  dbt->initial_chunk_step=NULL;
  dbt->insert_statement=NULL;
  dbt->chunks_mutex=g_mutex_new();
//  g_mutex_lock(dbt->chunks_mutex);
  dbt->chunks_queue=g_async_queue_new();
  dbt->chunks_completed=g_new(int,1);
  *(dbt->chunks_completed)=0;
  get_primary_key(conn,dbt,conf);
  dbt->primary_key_separated_by_comma = NULL;
  if (order_by_primary_key)
    get_primary_key_separated_by_comma(dbt);
  dbt->multicolumn = g_list_length(dbt->primary_key) > 1;

//  dbt->primary_key = get_primary_key_string(conn, dbt->database->name, dbt->table);
  dbt->chunk_filesize=chunk_filesize;
//  create_job_to_determine_chunk_type(dbt, g_async_queue_push, );

  g_free(k);
  dbt->complete_insert = complete_insert || detect_generated_fields(conn, dbt->database->escaped, dbt->escaped_table);
  if (dbt->complete_insert) {
    dbt->select_fields = get_insertable_fields(conn, dbt->database->escaped, dbt->escaped_table);
  } else {
    dbt->select_fields = g_string_new("*");
  }
  dbt->indexes_checksum=NULL;
  dbt->data_checksum=NULL;
  dbt->schema_checksum=NULL;
  dbt->triggers_checksum=NULL;
  dbt->rows=0;
  if (!datalength)
    dbt->datalength = 0;
  else
    dbt->datalength = g_ascii_strtoull(datalength, NULL, 10);

 // dbt->chunk_functions.process=NULL;
  return dbt; 
}

void free_db_table(struct db_table * dbt){
//  g_debug("Freeing dbt: `%s`.`%s`", dbt->database->name, dbt->table);
  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_free(dbt->rows_lock);
  g_free(dbt->escaped_table);
  if (dbt->insert_statement)
    g_string_free(dbt->insert_statement,TRUE);
  g_string_free(dbt->select_fields, TRUE);
  if (dbt->min!=NULL) g_free(dbt->min);
  if (dbt->max!=NULL) g_free(dbt->max);
/*  g_free();
  g_free();
  g_free();*/
  g_free(dbt->data_checksum);
  dbt->data_checksum=NULL;
  g_free(dbt->chunks_completed);
//  g_free(dbt->field);

/*

  union chunk_step * cs = NULL;
  switch (dbt->chunk_type) {
    case INTEGER:  
      cs = (union chunk_step *)g_async_queue_try_pop(dbt->chunks_queue);
      while (cs != NULL ){
        if (cs->integer_step.status==COMPLETED)
          free_integer_step(cs);
        else
          g_error("Trying to free uncompleted integer step `%s`.`%s`", dbt->database->name, dbt->table);
        cs = (union chunk_step *)g_async_queue_try_pop(dbt->chunks_queue);
      }
      g_async_queue_unref(dbt->chunks_queue);
    default:
      break;
  }
*/

  g_free(dbt->table);
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_free(dbt->chunks_mutex);
  g_free(dbt);
}

void new_table_to_dump(MYSQL *conn, struct configuration *conf, gboolean is_view, gboolean is_sequence, struct database * database, char *table, char *collation, char *datalength, gchar *ecol, guint64 rows_in_sts){
    /* Green light! */
  g_mutex_lock(database->ad_mutex);
  if (!database->already_dumped){
    create_job_to_dump_schema(database, conf);
    database->already_dumped=TRUE;
  }
  g_mutex_unlock(database->ad_mutex);

  struct db_table *dbt = new_db_table( conn, conf, database, table, collation, datalength, rows_in_sts);
  g_mutex_lock(all_dbts_mutex);
  all_dbts=g_list_prepend( all_dbts, dbt) ;
  g_mutex_unlock(all_dbts_mutex);

  // if a view or sequence we care only about schema
  if ((!is_view || views_as_tables ) && !is_sequence) {
  // with trx_consistency_only we dump all as innodb_table
    if (!no_schemas) {
//      write_table_metadata_into_file(dbt);
      g_mutex_lock(table_schemas_mutex);
      table_schemas=g_list_prepend( table_schemas, dbt) ;
      g_mutex_unlock(table_schemas_mutex);
      create_job_to_dump_table_schema( dbt, conf);
    }
    if (dump_triggers && !database->dump_triggers) {
      create_job_to_dump_triggers(conn, dbt, conf);
    }
    if (!no_data) {
      if (ecol != NULL && g_ascii_strcasecmp("MRG_MYISAM",ecol)) {
        if (data_checksums && !( get_major() == 5 && get_secondary() == 7 && dbt->has_json_fields ) ){
          create_job_to_dump_checksum(dbt, conf);
        }
        if (trx_consistency_only ||
          (ecol != NULL && (!g_ascii_strcasecmp("InnoDB", ecol) || !g_ascii_strcasecmp("TokuDB", ecol)))) {
          dbt->is_innodb=TRUE;
          g_mutex_lock(innodb_table_mutex);
          innodb_table=g_list_prepend(innodb_table,dbt);
          g_mutex_unlock(innodb_table_mutex);

        } else {
          dbt->is_innodb=FALSE;
          g_mutex_lock(non_innodb_table_mutex);
          non_innodb_table = g_list_prepend(non_innodb_table, dbt);
          g_mutex_unlock(non_innodb_table_mutex);
        }
      }else{
        if (is_view){
          dbt->is_innodb=FALSE;
          g_mutex_lock(non_innodb_table_mutex);
          non_innodb_table = g_list_prepend(non_innodb_table, dbt);
          g_mutex_unlock(non_innodb_table_mutex);
        }
      }
    }
  } else if (is_view) {
    if (!no_schemas) {
      create_job_to_dump_view(dbt, conf);
    }
  } else { // is_sequence
    if (!no_schemas) {
      create_job_to_dump_sequence(dbt, conf);
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
    g_free(query);
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
    mysql_free_result(result);

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
      g_free(query);
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
    g_free(query);
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
  if (detected_server == SERVER_TYPE_MYSQL || detected_server == SERVER_TYPE_PERCONA || detected_server == SERVER_TYPE_UNKNOWN ||
      detected_server == SERVER_TYPE_TIDB)
    query = g_strdup("SHOW TABLE STATUS");
  else if (detected_server == SERVER_TYPE_MARIADB)
    query =
        g_strdup_printf("SELECT TABLE_NAME, ENGINE, TABLE_TYPE as COMMENT, TABLE_COLLATION as COLLATION, AVG_ROW_LENGTH, DATA_LENGTH FROM "
                        "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s'",
                        database->escaped);
  else
      return;

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
  guint rowscol = 0;
  determine_show_table_status_columns(result, &ecol, &ccol, &collcol, &rowscol);
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
    int is_sequence = 0;

    /* We now do care about views!
            num_fields>1 kicks in only in case of 5.0 SHOW FULL TABLES or SHOW
       TABLE STATUS row[1] == NULL if it is a view in 5.0 'SHOW TABLE STATUS'
            row[1] == "VIEW" if it is a view in 5.0 'SHOW FULL TABLES'
    */
    if ((is_mysql_like() || 
         detected_server == SERVER_TYPE_TIDB ) &&
        (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
      is_view = 1;

    if ((detected_server == SERVER_TYPE_MARIADB) &&
        !strcmp(row[ccol], "SEQUENCE"))
      is_sequence = 1;

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
    if (dump && ignore && !is_view && !is_sequence) {
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

    if (is_sequence && no_dump_sequences)
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
    if (no_updated_tables && !is_view && !is_sequence) {
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
    
    new_table_to_dump(conn, conf, is_view, is_sequence, database, row[0], row[collcol], row[6], row[ecol], rowscol>0 && row[rowscol] !=NULL ?g_ascii_strtoull(row[rowscol], NULL, 10):0);

  }

  mysql_free_result(result);

  if (determine_if_schema_is_elected_to_dump_post(conn,database)) {
    create_job_to_dump_post(database, conf);
//    struct schema_post *sp = g_new(struct schema_post, 1);
//    sp->database = database;
//    schema_post = g_list_prepend(schema_post, sp);
  }


  if (database->dump_triggers)
    create_job_to_dump_schema_triggers(database, conf);

  g_free(query);

  return;
}
