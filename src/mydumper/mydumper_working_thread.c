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
#include <glib/gstdio.h>

#include "mydumper.h"
#include "mydumper_start_dump.h"
#include "mydumper_common.h"
#include "mydumper_stream.h"
#include "mydumper_database.h"
#include "mydumper_jobs.h"
#include "mydumper_chunks.h"
#include "mydumper_global.h"
#include "mydumper_arguments.h"
#include "mydumper_file_handler.h"
#include "mydumper_create_jobs.h"
#include "mydumper_working_thread.h"

/* Program options */
gboolean order_by_primary_key = FALSE;
gboolean use_savepoints = FALSE;
gboolean clear_dumpdir= FALSE;
gboolean dirty_dumpdir= FALSE;
gchar *ignore_engines_str = NULL;
int skip_tz = 0;
gboolean dump_events = FALSE;
gboolean dump_routines = FALSE;
gboolean no_dump_views = FALSE;
gboolean views_as_tables=FALSE;
gboolean no_dump_sequences = FALSE;
gboolean dump_checksums = FALSE;
gboolean data_checksums = FALSE;
gboolean schema_checksums = FALSE;
gboolean routine_checksums = FALSE;
gboolean exit_if_broken_table_found = FALSE;
int build_empty_files = 0;

// Extern
extern gboolean use_single_column;
extern guint64 min_integer_chunk_step_size;
extern guint64 max_integer_chunk_step_size;

// Shared variables
gint database_counter = 0;
struct MList  *transactional_table = NULL;
struct MList  *non_transactional_table = NULL;
guint64 min_chunk_step_size = 0;
guint64 starting_chunk_step_size = 0;
guint64 max_chunk_step_size = 0;

// Static
static GMutex *init_mutex = NULL;
static gchar *binlog_snapshot_gtid_executed = NULL; 
static char **ignore_engines = NULL;
static int sync_wait = -1;
static GMutex *table_schemas_mutex = NULL;
static GMutex *all_dbts_mutex=NULL;
static GMutex *trigger_schemas_mutex = NULL;
static GMutex *view_schemas_mutex = NULL;
static GHashTable *character_set_hash=NULL;
static GMutex *character_set_hash_mutex = NULL;
static const guint tablecol= 0;
static GThread **threads=NULL;
static struct thread_data *thread_data;

static
void dump_database_thread(MYSQL *, struct configuration*, struct database *);
static
void *working_thread(struct thread_data *td);

void initialize_working_thread(){
  database_counter = 0;

  if (max_chunk_step_size > G_MAXUINT64 / num_threads){
    max_chunk_step_size= G_MAXUINT64 / num_threads;
    m_error("This should not happen");
  }

  character_set_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free);
  character_set_hash_mutex = g_mutex_new();
  transactional_table=g_new(struct MList, 1);
  non_transactional_table=g_new(struct MList, 1);
  transactional_table->list=NULL;
  non_transactional_table->list=NULL;
  non_transactional_table->mutex = g_mutex_new();
  transactional_table->mutex = g_mutex_new();

  view_schemas_mutex = g_mutex_new();
  table_schemas_mutex = g_mutex_new();
  trigger_schemas_mutex = g_mutex_new();
  all_dbts_mutex = g_mutex_new();
  init_mutex = g_mutex_new();
  binlog_snapshot_gtid_executed = NULL;


  /* savepoints workaround to avoid metadata locking issues
     doesnt work for chuncks 

     UPDATE: this is not true anymore
   */
/*  if (starting_chunk_step_size && use_savepoints) {
    use_savepoints = FALSE;
    g_warning("--use-savepoints disabled by --rows");
  }
*/

  /* Give ourselves an array of engines to ignore */
  if (ignore_engines_str)
    ignore_engines = g_strsplit(ignore_engines_str, ",", 0);


// TODO: We need to cleanup this

  if (compress_method==NULL && exec_per_thread==NULL && exec_per_thread_extension == NULL) {
    exec_per_thread_extension=EMPTY_STRING;
    initialize_file_handler(FALSE);
  }else{
    if (compress_method!=NULL && (exec_per_thread!=NULL || exec_per_thread_extension!=NULL)){
      m_critical("--compression and --exec-per-thread are not comptatible");
    }
    gchar *cmd=NULL;
    if ( g_strcmp0(compress_method,GZIP)==0){
      if ((cmd=get_gzip_cmd()) == NULL){
        g_error("gzip command not found on any static location, use --exec-per-thread for non default locations");
      }
      exec_per_thread=g_strdup_printf("%s -c", cmd);
      exec_per_thread_extension=GZIP_EXTENSION;
    }else 
    if ( g_strcmp0(compress_method,ZSTD)==0){
      if ( (cmd=get_zstd_cmd()) == NULL ){
        g_error("zstd command not found on any static location, use --exec-per-thread for non default locations");
      }
      exec_per_thread=g_strdup_printf("%s -c", cmd);
      exec_per_thread_extension=ZSTD_EXTENSION;
    }
    initialize_file_handler(TRUE);
  }

  initialize_jobs();
  initialize_chunk();

  if (dump_checksums){
    data_checksums = TRUE;
    schema_checksums = TRUE;
    routine_checksums = TRUE;
  }
}

void start_working_thread(struct configuration *conf ){
  guint n;
  threads = g_new(GThread *, num_threads );
  thread_data =
      g_new(struct thread_data, num_threads);
  g_message("Creating workers");
  for (n = 0; n < num_threads; n++) {
    thread_data[n].conf = conf;
    thread_data[n].thread_id = n + 1;
    thread_data[n].binlog_snapshot_gtid_executed = NULL;
    thread_data[n].pause_resume_mutex=NULL;
    thread_data[n].table_name=NULL;
    thread_data[n].thread_data_buffers.statement = g_string_sized_new(2*statement_size);
    thread_data[n].thread_data_buffers.row = g_string_sized_new(statement_size);
    thread_data[n].thread_data_buffers.column = g_string_sized_new(statement_size);
    thread_data[n].thread_data_buffers.escaped = g_string_sized_new(statement_size);
    threads[n] =
        m_thread_new("data", (GThreadFunc)working_thread, &thread_data[n], "Data thread could not be created");
  }

  if (sync_thread_lock_mode==GTID){
    gchar *_binlog_snapshot_gtid_executed = NULL;
    gboolean binlog_snapshot_gtid_executed_status_local=FALSE;
    guint start_transaction_retry=0;
    while (!binlog_snapshot_gtid_executed_status_local && start_transaction_retry < MAX_START_TRANSACTION_RETRIES ){
      binlog_snapshot_gtid_executed_status_local=TRUE;
      for (n = 0; n < num_threads; n++) {
        g_async_queue_pop(conf->gtid_pos_checked);
      }
      _binlog_snapshot_gtid_executed=g_strdup(thread_data[0].binlog_snapshot_gtid_executed);
      for (n = 1; n < num_threads; n++) {
        binlog_snapshot_gtid_executed_status_local=binlog_snapshot_gtid_executed_status_local && g_strcmp0(thread_data[n].binlog_snapshot_gtid_executed,_binlog_snapshot_gtid_executed)==0;
      }
      for (n = 0; n < num_threads; n++) {
        g_async_queue_push(conf->are_all_threads_in_same_pos,binlog_snapshot_gtid_executed_status_local?GINT_TO_POINTER(1):GINT_TO_POINTER(2));
      }
      start_transaction_retry++;
    }
  }

  for (n = 0; n < num_threads; n++) {
    g_async_queue_pop(conf->ready);
  }
}

void wait_working_thread_to_finish(){
  guint n;
  g_message("Waiting threads to complete");
  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
}

void finalize_working_thread(){
  g_hash_table_destroy(character_set_hash);
  g_mutex_free(character_set_hash_mutex);
  g_mutex_free(view_schemas_mutex);
  g_mutex_free(table_schemas_mutex);
  g_mutex_free(trigger_schemas_mutex);
  g_mutex_free(all_dbts_mutex);
  g_mutex_free(init_mutex);
  if (binlog_snapshot_gtid_executed!=NULL)
    g_free(binlog_snapshot_gtid_executed);

  finalize_chunk();
  g_free(thread_data);
  g_free(threads);
}

static
void get_primary_key(MYSQL *conn, struct db_table * dbt, struct configuration *conf){
  MYSQL_RES *indexes = NULL;
  MYSQL_ROW row;
  dbt->primary_key=NULL;
  // first have to pick index, in future should be able to preset in
  //    * configuration too 
  gchar *query = g_strdup_printf("SHOW INDEX FROM %s%s%s.%s%s%s",
                        identifier_quote_character_str, dbt->database->name, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str);
  indexes = m_store_result(conn, query, m_warning, "Failed to execute SHOW INDEX over %s", dbt->database->name);
  g_free(query);

  if (indexes){
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[2], "PRIMARY") ) {
        // Pick first column in PK, cardinality doesn't matter
        dbt->primary_key=g_list_append(dbt->primary_key,g_strdup(row[4]));
      }
    }
    if (dbt->primary_key)
      goto cleanup;

    // If no PK found, try using first UNIQUE index
    mysql_data_seek(indexes, 0);
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[1], "0")) {
        // Again, first column of any unique index 
        dbt->primary_key=g_list_append(dbt->primary_key,g_strdup(row[4]));
      }
    }

    if (dbt->primary_key)
      goto cleanup;

    // Still unlucky? Pick any high-cardinality index 
    if (!dbt->primary_key && conf->use_any_index) {
      guint64 max_cardinality = 0;
      guint64 cardinality = 0;
      gchar *field=NULL;
      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[3], "1")) {
          if (row[6])
            cardinality = strtoul(row[6], NULL, 10);
          if (cardinality > max_cardinality) {
            field = g_strdup(row[4]);
            max_cardinality = cardinality;
          }
        }
      }
      if (field)
        dbt->primary_key=g_list_append(dbt->primary_key,field);
    }
  }

cleanup:
  if (indexes)
    mysql_free_result(indexes);
}

void thd_JOB_DUMP_ALL_DATABASES( struct thread_data *td, struct job *job){
  // TODO: This should be in a job as needs to be done by a thread.
  MYSQL_RES *databases;
  MYSQL_ROW row;
  databases=m_store_result(td->thrconn, "SHOW DATABASES", m_critical, "Unable to list databases", NULL);

  while ((row = mysql_fetch_row(databases))) {
    if (!strcasecmp(row[0], "information_schema") ||
        !strcasecmp(row[0], "performance_schema") ||
        !strcasecmp(row[0], "data_dictionary") ||
        (tables_skiplist_file && check_skiplist(row[0], NULL)))
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

    query= g_strdup_printf("SHOW TABLE STATUS FROM %s%s%s LIKE '%s'", identifier_quote_character_str, dt[0], identifier_quote_character_str, dt[1]);
    MYSQL_RES *result = m_store_result(conn, query, m_critical, "Error showing table status on: %s - Could not execute query", dt[0]);
    g_free(query);

    if (!result) {
      return;
    }

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

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {

      int is_view = 0;
      int is_sequence = 0;

      if ((get_product() == SERVER_TYPE_MYSQL ||
           get_product() == SERVER_TYPE_MARIADB ||
           get_product() == SERVER_TYPE_DOLT) &&
          (row[ecol] == NULL) &&
          (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
        is_view = 1;

      if ((get_product() == SERVER_TYPE_MARIADB) &&
          (row[ccol] == NULL || !strcmp(row[ccol], "SEQUENCE")))
        is_sequence = 1;

      /* Checks skip list on 'database.table' string */
      if (tables_skiplist_file && check_skiplist(database->name, row[0]))
        continue;

      /* Special tables */
      if (is_mysql_special_tables(database->name, row[0])) {
        continue;
      }

      /* Checks PCRE expressions on 'database.table' string */
      if (!eval_regex(database->name, row[0]))
        continue;

      create_job_to_dump_table(conf, is_view, is_sequence, database, g_strdup(row[tablecol]), g_strdup(row[collcol]), g_strdup(row[ecol]));
    }
    mysql_free_result(result);
    g_strfreev(dt);
  }

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
        m_query_critical(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper", "Rollback to savepoint failed", NULL);
        m_query_critical(td->thrconn, "SAVEPOINT mydumper","Savepoint failed", NULL);
        td->table_name = tj->dbt->table;
      }
    }else{
      m_query_critical(td->thrconn, "SAVEPOINT mydumper", "Savepoint failed", NULL);
      td->table_name = tj->dbt->table;
    }
  }
  tj->td=td;

  tj->chunk_step_item->chunk_functions.process(tj, tj->chunk_step_item);
  g_mutex_lock(tj->dbt->chunks_mutex);
  tj->dbt->current_threads_running--;
  g_mutex_unlock(tj->dbt->chunks_mutex);

  free_table_job(tj);
  g_free(job);
}

void initialize_thread(struct thread_data *td){
  m_connect(td->thrconn);
  g_message("Thread %d: connected using MySQL connection ID %lu",
            td->thread_id, mysql_thread_id(td->thrconn));
}

void initialize_consistent_snapshot(struct thread_data *td){
  gchar *query=NULL;
  if ( sync_wait != -1)
    m_query_critical(td->thrconn, query=g_strdup_printf("SET SESSION WSREP_SYNC_WAIT = %d",sync_wait), "Failed to set wsrep_sync_wait for the thread",NULL);
  g_free(query);
  set_transaction_isolation_level_repeatable_read(td->thrconn);
  guint start_transaction_retry=0;
  gboolean cont = FALSE;

  if (sync_thread_lock_mode==GTID){ 
    while ( !cont && (start_transaction_retry < MAX_START_TRANSACTION_RETRIES )){
//    Uncommenting the sleep will cause inconsitent scenarios always, which is useful for debugging 
//      sleep(td->thread_id);
      g_debug("Thread %d: Start transaction #%d", td->thread_id, start_transaction_retry);
      m_query_critical(td->thrconn,"START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */", "Failed to start consistent snapshot", NULL);
      MYSQL_RES *res = m_store_result_critical (td->thrconn, "SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'", "Failed to get binlog_snapshot_gtid_executed", NULL);
      if (res){
        MYSQL_ROW row = mysql_fetch_row(res);
        if (row!=NULL)
          td->binlog_snapshot_gtid_executed=g_strdup(row[1]);
        else
          m_critical("Failed to get content of binlog_snapshot_gtid_executed");
        mysql_free_result(res);
      }else
        m_critical("Failed to get content of binlog_snapshot_gtid_executed: %s", mysql_error(td->thrconn));
      start_transaction_retry++;
      g_async_queue_push(td->conf->gtid_pos_checked, GINT_TO_POINTER(1));
      cont=GPOINTER_TO_INT(g_async_queue_pop(td->conf->are_all_threads_in_same_pos)) == 1;
    }

    if (cont){
      g_message("All threads in the same position. This will be a consistent backup.");
      it_is_a_consistent_backup=TRUE;
    }else{
      m_critical("We were not able to sync all threads. We unsuccessfully tried %d times. Reducing the amount of threads might help.", MAX_START_TRANSACTION_RETRIES);
    }
  }else
    m_query_critical(td->thrconn,"START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */", "Failed to start consistent snapshot", NULL);
}

static
void check_connection_status(struct thread_data *td){
  if (get_product() == SERVER_TYPE_TIDB) {
    // Worker threads must set their tidb_snapshot in order to be safe
    // Because no locking has been used.
    set_tidb_snapshot(td->thrconn);
    g_message("Thread %d: set to tidb_snapshot '%s'", td->thread_id,
              tidb_snapshot);
  }

  /* Unfortunately version before 4.1.8 did not support consistent snapshot
   * transaction starts, so we cheat */
  if (need_dummy_read) {
    MYSQL_RES *res = m_store_result(td->thrconn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy", m_warning, "Failed to select on mysql.mydumperdummy", NULL);
    if (res)
      mysql_free_result(res);
  }
  if (need_dummy_toku_read) {
    MYSQL_RES *res = m_store_result(td->thrconn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy", m_warning, "Failed to select on mysql.tokudbdummy", NULL);
    if (res)
      mysql_free_result(res);
  }
}


/* Write some stuff we know about snapshot, before it changes */
void write_snapshot_info(MYSQL *conn, FILE *file) {
  char *masterlog = NULL;
  char *masterpos = NULL;
  char *mastergtid = NULL;

  
  struct M_ROW *mr = m_store_result_row(conn, show_binary_log_status, m_warning, m_message, "Couldn't get master position", NULL);
  if ( mr->row ) {
    masterlog = mr->row[0];
    masterpos = mr->row[1];
    /* Oracle/Percona GTID */
    if (mysql_num_fields(mr->res) == 5) {
      mastergtid = remove_new_line(mr->row[4]);
    } else {
      /* Let's try with MariaDB 10.x */
      /* Use gtid_binlog_pos due to issue with gtid_current_pos with galera
 *        * cluster, gtid_binlog_pos works as well with normal mariadb server
 *               * https://jira.mariadb.org/browse/MDEV-10279 */
      m_store_result_row_free(mr);
      mr = m_store_result_row(conn, "SELECT @@gtid_binlog_pos", NULL, NULL, "Failed to get @@gtid_binlog_pos", NULL);
      if (mr->row){
        mastergtid = remove_new_line(mr->row[0]);
      }
    }
  }
  m_store_result_row_free(mr);

  if (masterlog) {
    fprintf(file, "[source]\n# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n");
    if (source_data > 0){
      fprintf(file, "#SOURCE_HOST = \"%s\"\n#SOURCE_PORT = \n#SOURCE_USER = \"\"\n#SOURCE_PASSWORD = \"\"\n", hostname);
      if (((source_data) & (1<<(3)))>0)
        fprintf(file, "SOURCE_SSL = 1\n");
      else
        fprintf(file, "#SOURCE_SSL = {0|1}\n");
      fprintf(file, "executed_gtid_set = \"%s\"\n", mastergtid);
      if (((source_data) & (1<<(4)))>0){
        fprintf(file, "SOURCE_AUTO_POSITION = 1\n");
        fprintf(file, "#SOURCE_LOG_FILE = \"%s\"\n#SOURCE_LOG_POS = %s\n", masterlog, masterpos);
      }else{
        fprintf(file, "SOURCE_LOG_FILE = \"%s\"\nSOURCE_LOG_POS = %s\n", masterlog, masterpos);
        fprintf(file, "#SOURCE_AUTO_POSITION = {0|1}\n");
      }
      fprintf(file, "myloader_exec_reset_replica = %d\nmyloader_exec_change_source = %d\nmyloader_exec_start_replica = %d\n",
          ((source_data) & (1<<(0))) , ((source_data) & (1<<(1)))>0?1:0, ((source_data) & (1<<(2)))>0?1:0);
    }else{
      fprintf(file, "File = %s\nPosition = %s\nExecuted_Gtid_Set = %s\n",
					masterlog, masterpos, mastergtid);
		}
    g_message("Written master status");
  }

  fflush(file);

}

static void thd_JOB_TABLE(struct thread_data *td, struct job *job);
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
    case JOB_TABLE:
      thd_JOB_TABLE(td, job);
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
    case JOB_DEFER:
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

void process_queue(GAsyncQueue * queue, struct thread_data *td, gboolean do_builder, GAsyncQueue *chunk_step_queue){
  struct job *job = NULL;
  for (;;) {
    check_pause_resume(td);
    if (chunk_step_queue) {
      g_async_queue_push(chunk_step_queue, GINT_TO_POINTER(1));
    }
    job = (struct job *)g_async_queue_pop(queue);
    if (shutdown_triggered && (job->type != JOB_SHUTDOWN)) {
      g_message("Thread %d: Process has been cacelled",td->thread_id);
      return;
    }
    if (do_builder) {
      if (!process_job_builder_job(td, job)) {
        break;
      }
    } else {
      if (!process_job(td, job)) {
        break;
      }
    }
  }
}

void build_lock_tables_statement(struct configuration *conf){
  g_mutex_lock(non_transactional_table->mutex);
  GList *iter = non_transactional_table->list;
  struct db_table *dbt;
  if ( iter != NULL){
    dbt = (struct db_table *)iter->data;
    conf->lock_tables_statement = g_string_sized_new(30);
    g_string_printf(conf->lock_tables_statement, "LOCK TABLES %s%s%s.%s%s%s READ LOCAL",
                    identifier_quote_character_str, dbt->database->name, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str);
    iter = iter->next;
    for (; iter != NULL; iter = iter->next) {
      dbt = (struct db_table *)iter->data;
      g_string_append_printf(conf->lock_tables_statement, ", %s%s%s.%s%s%s READ LOCAL",
                             identifier_quote_character_str, dbt->database->name, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str);
    }
  }
  g_mutex_unlock(non_transactional_table->mutex);
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

static
void *working_thread(struct thread_data *td) {
  // mysql_init is not thread safe, especially in Connector/C
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);

  initialize_thread(td);
  execute_gstring(td->thrconn, set_session);

  // Initialize connection 
  if (!skip_tz)       m_query_critical(td->thrconn, "/*!40103 SET TIME_ZONE='+00:00' */", "Failed to set time zone", NULL);
  if (use_savepoints) m_query_critical(td->thrconn, "SET SQL_LOG_BIN = 0", "Failed to disable binlog for the thread", NULL);

  initialize_consistent_snapshot(td);
  check_connection_status(td);

  g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
  // Thread Ready to process jobs
  
  g_message("Thread %d: Creating Jobs", td->thread_id);
  process_queue(td->conf->initial_queue,td, TRUE, NULL);
  g_async_queue_push(td->conf->initial_completed_queue, GINT_TO_POINTER(1));

  g_message("Thread %d: Processing Schema jobs", td->thread_id);
  process_queue(td->conf->schema_queue,td, FALSE, NULL);

  if (stream) send_initial_metadata();

  if (!no_data){
    g_message("Thread %d: Schema jobs are done, Starting exporting data for Non-Transactional tables", td->thread_id);

    g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1)); 
    g_async_queue_pop(td->conf->ready_non_transactional_queue);

    if (trx_tables){
      // Processing non-transactional tables
      // This queue should be empty, but we are processing just in case.
      process_queue(td->conf->non_transactional.queue, td, FALSE, td->conf->non_transactional.request_chunk);
      process_queue(td->conf->non_transactional.defer, td, FALSE, NULL);

      // This push will unlock the FTWRL on the Main Connection
      g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
    }else{
      // Sending LOCK TABLE over all non-transactional tables
      if (td->conf->lock_tables_statement!=NULL) m_query_critical(td->thrconn, td->conf->lock_tables_statement->str, "Error locking non-transactional tables", NULL);

      // This push will unlock the FTWRL on the Main Connection
      g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));

      // Processing non-transactional tables
      process_queue(td->conf->non_transactional.queue, td, FALSE, td->conf->non_transactional.request_chunk);
      process_queue(td->conf->non_transactional.defer, td, FALSE, NULL);

      // At this point, this thread is able to unlock the non-transactional tables
      m_query_critical(td->thrconn, UNLOCK_TABLES, "Error locking non-transactional tables", NULL);
    }

    // Processing Transactional tables
    g_message("Thread %d: Non-Transactional tables are done, Starting exporting data for Transactional tables", td->thread_id);
    process_queue(td->conf->transactional.queue, td, FALSE, td->conf->transactional.request_chunk);
    process_queue(td->conf->transactional.defer, td, FALSE, NULL);
  }else{
    g_async_queue_push(td->conf->unlock_tables, GINT_TO_POINTER(1));
  }

  if (use_savepoints && td->table_name != NULL) m_query_critical(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper", "Rollback to savepoint failed", NULL);

  g_message("Thread %d: Processing remaining objects jobs", td->thread_id);
  process_queue(td->conf->post_data_queue, td, FALSE, NULL);

  g_message("Thread %d: Shutting down", td->thread_id);

  if (td->binlog_snapshot_gtid_executed!=NULL)
    g_free(td->binlog_snapshot_gtid_executed);

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  return NULL;
}

static
GString *get_selectable_fields(MYSQL *conn, char *database, char *table) {
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra "
                      "not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%' ORDER BY ORDINAL_POSITION ASC",
                      database, table);
  MYSQL_RES *res=m_store_result_critical(conn, query, "Failed to get Selectable Fields", NULL);
  g_free(query);

  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else {
      g_string_append(field_list, ",");
    }

    char *field_name= identifier_quote_character_protect(row[0]);
    gchar *tb = g_strdup_printf("%s%s%s", identifier_quote_character_str, field_name, identifier_quote_character_str);
    g_free(field_name);
    g_string_append(field_list, tb);
    g_free(tb);
  }

  mysql_free_result(res);

  return field_list;
}

static
gboolean has_json_fields(MYSQL *conn, char *database, char *table) {
  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and "
                      "COLUMN_TYPE ='json'",
                      database, table);
  struct M_ROW *mr = m_store_result_row(conn, query, m_critical, m_warning, "Failed to get JSON fields on %s.%s: %s", database, table, query);
  g_free(query);
  if (mr->row){
    m_store_result_row_free(mr);
    return TRUE;
  }
  m_store_result_row_free(mr);
  return FALSE;
}

static
gboolean detect_generated_fields(MYSQL *conn, gchar *database, gchar* table) {
  gboolean result = FALSE;
  if (ignore_generated_fields)
    return FALSE;

  gchar *query = g_strdup_printf(
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
      "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND extra LIKE '%%GENERATED%%' AND extra NOT LIKE '%%DEFAULT_GENERATED%%' "
      "LIMIT 1",
      database, table);

  struct M_ROW *mr = m_store_result_row(conn, query, m_warning, m_message, "Failed to detect generated fields", NULL);
  g_free(query);
  result=mr->row!=NULL;
  m_store_result_row_free(mr);
  return result;
}

static
gchar *get_character_set_from_collation(MYSQL *conn, gchar *collation){
  g_mutex_lock(character_set_hash_mutex);
  gchar *character_set = g_hash_table_lookup(character_set_hash, collation);
  if (character_set == NULL){
    gchar *query =
      g_strdup_printf("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS "
                      "WHERE collation_name='%s'",
                      collation);
    struct M_ROW *mr = m_store_result_row(conn, query, m_critical, m_warning, "Failed to get CHARACTER_SET from collation %s", collation);
    g_free(query);
    if (mr->row)
      g_hash_table_insert(character_set_hash, g_strdup(collation), character_set=g_strdup(mr->row[0]));
    m_store_result_row_free(mr);
  }
  g_mutex_unlock(character_set_hash_mutex);
  return character_set;
}

static
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
    char *field_name= identifier_quote_character_protect((char*) list->data);
    gchar *tb = g_strdup_printf("%s%s%s", identifier_quote_character_str, field_name, identifier_quote_character_str);
    g_free(field_name);
    g_string_append(field_list, tb);
    list=list->next;
  }
  if (field_list->len>0)
    dbt->primary_key_separated_by_comma = g_string_free(field_list, FALSE); 
}

static
gboolean new_db_table(struct db_table **d, MYSQL *conn, struct configuration *conf,
                      struct database *database, char *table, char *table_collation,
                      gboolean is_sequence)
{
  gchar * lkey = build_dbt_key(database->name,table);
	g_mutex_lock(all_dbts_mutex);
  struct db_table *dbt = g_hash_table_lookup(all_dbts, lkey);
  gboolean b;
  if (dbt){
    g_free(lkey);
    b=FALSE;
    g_mutex_unlock(all_dbts_mutex);
  }else{
    dbt = g_new(struct db_table, 1);
		dbt->key=lkey;
    dbt->status = UNDEFINED;
    g_hash_table_insert(all_dbts, lkey, dbt);
    g_mutex_unlock(all_dbts_mutex);
    dbt->database = database;
    dbt->table = identifier_quote_character_protect(table);
    dbt->table_filename = get_ref_table(dbt->table);
    dbt->is_sequence= is_sequence;
    if (table_collation==NULL)
      dbt->character_set = NULL;
    else{
      dbt->character_set=get_character_set_from_collation(conn, table_collation);
      if ( dbt->character_set == NULL)
        g_warning("Collation '%s' not found on INFORMATION_SCHEMA.COLLATIONS used by `%s`.`%s`",table_collation,database->name,table);
    }
    dbt->has_json_fields = has_json_fields(conn, dbt->database->name, dbt->table);
    dbt->rows_lock= g_mutex_new();
    dbt->escaped_table = escape_string(conn,dbt->table);
    dbt->where=g_hash_table_lookup(conf_per_table.all_where_per_table, lkey);
    dbt->limit=g_hash_table_lookup(conf_per_table.all_limit_per_table, lkey);
    parse_object_to_export(&(dbt->object_to_export),g_hash_table_lookup(conf_per_table.all_object_to_export, lkey));

    dbt->partition_regex=g_hash_table_lookup(conf_per_table.all_partition_regex_per_table, lkey);
    dbt->max_threads_per_table=max_threads_per_table;
    dbt->current_threads_running=0;

    // Load chunk step size values
    gchar *rows_p_chunk=g_hash_table_lookup(conf_per_table.all_rows_per_table, lkey);
    if (rows_p_chunk )
      dbt->split_integer_tables=parse_rows_per_chunk(rows_p_chunk, &(dbt->min_chunk_step_size), &(dbt->starting_chunk_step_size), &(dbt->max_chunk_step_size),"Invalid option on rows in configuration file");
    else{
      dbt->split_integer_tables=split_integer_tables;
      dbt->min_chunk_step_size=min_chunk_step_size;
      dbt->starting_chunk_step_size=starting_chunk_step_size;
      dbt->max_chunk_step_size=max_chunk_step_size;
    }
    if (dbt->min_chunk_step_size == 1 && dbt->min_chunk_step_size == dbt->starting_chunk_step_size && dbt->starting_chunk_step_size != dbt->max_chunk_step_size ){
      dbt->min_chunk_step_size = 2;
      dbt->starting_chunk_step_size = 2;
      g_warning("Setting min and start rows per file to 2 on %s", lkey);
    }
    dbt->is_fixed_length=dbt->min_chunk_step_size != 0 && dbt->min_chunk_step_size == dbt->starting_chunk_step_size && dbt->starting_chunk_step_size == dbt->max_chunk_step_size;

    if ( dbt->min_chunk_step_size==0)
      dbt->min_chunk_step_size=MIN_CHUNK_STEP_SIZE;

    // honor --rows-hard
    if ( max_integer_chunk_step_size != 0 && (dbt->max_chunk_step_size > max_integer_chunk_step_size || dbt->max_chunk_step_size == 0 ))
      dbt->max_chunk_step_size=max_integer_chunk_step_size;
    if ( min_integer_chunk_step_size != 0 && (dbt->min_chunk_step_size < min_integer_chunk_step_size ))
      dbt->min_chunk_step_size=min_integer_chunk_step_size;


    dbt->num_threads=g_hash_table_lookup(conf_per_table.all_num_threads_per_table, lkey)?strtoul(g_hash_table_lookup(conf_per_table.all_num_threads_per_table, lkey), NULL, 10):num_threads;
    dbt->estimated_remaining_steps=1;
    dbt->min=NULL;
    dbt->max=NULL;
//  dbt->chunk_type_item.chunk_type = UNDEFINED;
//  dbt->chunk_type_item.chunk_step = NULL;
    dbt->chunks=NULL;
//  dbt->initial_chunk_step=NULL;
    dbt->load_data_header=NULL;
    dbt->load_data_suffix=NULL;
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
    dbt->multicolumn = !use_single_column && g_list_length(dbt->primary_key) > 1;

//  dbt->primary_key = get_primary_key_string(conn, dbt->database->name, dbt->table);
    dbt->chunk_filesize=chunk_filesize;
//  create_job_to_determine_chunk_type(dbt, g_async_queue_push, );

    gchar *columns_on_select=g_hash_table_lookup(conf_per_table.all_columns_on_select_per_table, lkey);

    dbt->columns_on_insert=g_hash_table_lookup(conf_per_table.all_columns_on_insert_per_table, lkey);

    dbt->select_fields=NULL;

    if (columns_on_select){
      dbt->select_fields=g_string_new(columns_on_select);
    }else if (!dbt->columns_on_insert){
      dbt->complete_insert = complete_insert || detect_generated_fields(conn, dbt->database->escaped, dbt->escaped_table);
      if (dbt->complete_insert) {
        dbt->select_fields = get_selectable_fields(conn, dbt->database->escaped, dbt->escaped_table);
      }
    }

//    dbt->anonymized_function=get_anonymized_function_for(conn, dbt);
    dbt->anonymized_function=NULL;
    dbt->indexes_checksum=NULL;
    dbt->data_checksum=NULL;
    dbt->schema_checksum=NULL;
    dbt->triggers_checksum=NULL;
    dbt->rows=0;
 // dbt->chunk_functions.process=NULL;
    b=TRUE;
  }
  *d=dbt;
  return b; 
}

void free_db_table(struct db_table * dbt){
  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_free(dbt->rows_lock);
  g_free(dbt->escaped_table);
  if (dbt->insert_statement)
    g_string_free(dbt->insert_statement,TRUE);
  if (dbt->select_fields)
    g_string_free(dbt->select_fields, TRUE);
  if (dbt->min!=NULL) g_free(dbt->min);
  if (dbt->max!=NULL) g_free(dbt->max);
  g_free(dbt->data_checksum);
  dbt->data_checksum=NULL;
  g_free(dbt->chunks_completed);

  g_free(dbt->table);
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_free(dbt->chunks_mutex);
  g_free(dbt);
}

static
void new_table_to_dump(MYSQL *conn, struct configuration *conf, gboolean is_view,
                       gboolean is_sequence, struct database * database, char *table,
                       char *collation, gchar *ecol)
{
    /* Green light! */
  g_mutex_lock(database->ad_mutex);
  if (!database->already_dumped){
    create_job_to_dump_schema(database, conf);
    database->already_dumped=TRUE;
  }
  g_mutex_unlock(database->ad_mutex);

  struct db_table *dbt=NULL;
  gboolean b= new_db_table(&dbt, conn, conf, database, table, collation, is_sequence);
  if (b){
  // if a view or sequence we care only about schema
  if ((!is_view || views_as_tables ) && !is_sequence) {
  // with --trx-tables we dump all as transactional tables
    if (!no_schemas && !dbt->object_to_export.no_schema) {
//      write_table_metadata_into_file(dbt);
      g_mutex_lock(table_schemas_mutex);
      table_schemas=g_list_prepend( table_schemas, dbt) ;
      g_mutex_unlock(table_schemas_mutex);
      create_job_to_dump_table_schema( dbt, conf);
    }
    if (dump_triggers && !database->dump_triggers && !dbt->object_to_export.no_trigger) {
      create_job_to_dump_triggers(conn, dbt, conf);
    }
    if (!no_data && !dbt->object_to_export.no_data) {
      if (ecol != NULL && g_ascii_strcasecmp("MRG_MYISAM",ecol)) {
        if (data_checksums && !( get_major() == 5 && get_secondary() == 7 && dbt->has_json_fields ) ){
          create_job_to_dump_checksum(dbt, conf);
        }
        if (trx_tables ||
          (ecol != NULL && (!g_ascii_strcasecmp("InnoDB", ecol) || !g_ascii_strcasecmp("TokuDB", ecol)))) {
          dbt->is_transactional=TRUE;
          g_mutex_lock(transactional_table->mutex);
          transactional_table->list=g_list_prepend(transactional_table->list,dbt);
          g_mutex_unlock(transactional_table->mutex);

        } else {
          dbt->is_transactional=FALSE;
          g_mutex_lock(non_transactional_table->mutex);
          non_transactional_table->list = g_list_prepend(non_transactional_table->list, dbt);
          g_mutex_unlock(non_transactional_table->mutex);
        }
      }else{
        if (is_view){
          dbt->is_transactional=FALSE;
          g_mutex_lock(non_transactional_table->mutex);
          non_transactional_table->list = g_list_prepend(non_transactional_table->list, dbt);
          g_mutex_unlock(non_transactional_table->mutex);
        }
      }
    }
  } else if (is_view) {
    if (!no_schemas && !dbt->object_to_export.no_schema) {
      create_job_to_dump_view(dbt, conf);
    }
  } else { // is_sequence
    if (!no_schemas && !dbt->object_to_export.no_schema) {
      create_job_to_dump_sequence(dbt, conf);
    }
  }
  }
}

static
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

  if (dump_routines) {
    g_assert(nroutines > 0);
    for (guint r= 0; r < nroutines; r++) {
      query= g_strdup_printf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", routine_type[r], database->escaped);
      result=m_store_result(conn, query, m_critical, "Error showing procedure on: %s - Could not execute query", database->name);
      g_free(query);
      if (!result)
        return FALSE;
      while ((row= mysql_fetch_row(result))) {
        /* Checks skip list on 'database.sp' string */
        if (tables_skiplist_file && check_skiplist(database->name, row[1]))
          continue;

        /* Checks PCRE expressions on 'database.sp' string */
        if (!eval_regex(database->name, row[1]))
          continue;

        mysql_free_result(result);
        return TRUE;
      }
      mysql_free_result(result);
    } // for (i= 0; i < upper_bound; i++)
  } // if (dump_routines)

  if (dump_events) {
    // EVENTS
    query = g_strdup_printf("SHOW EVENTS FROM %s%s%s", identifier_quote_character_str, database->name, identifier_quote_character_str);
    result=m_store_result(conn, query, m_critical, "Error showing events on: %s - Could not execute query", database->name);
    g_free(query);
    if (!result)
      return FALSE;
    while ((row = mysql_fetch_row(result))) {
      /* Checks skip list on 'database.sp' string */
      if (tables_skiplist_file && check_skiplist(database->name, row[1]))
        continue;
      /* Checks PCRE expressions on 'database.sp' string */
      if ( !eval_regex(database->name, row[1]))
        continue;

      mysql_free_result(result);
      return TRUE;
    }
    mysql_free_result(result);
  }
  return FALSE;
}

static
void dump_database_thread(MYSQL *conn, struct configuration *conf, struct database *database) {

  if (mysql_select_db(conn, database->name)) {
    g_critical("Could not select database: %s (%s)", database->name, mysql_error(conn));
    errors++;
    return;
  }

  const char *query= "SHOW TABLE STATUS";
  MYSQL_RES *result = m_store_result(conn, query,m_critical, "Error showing tables on: %s - Could not execute query", database->name);
  if (!result)
    return;

  guint ecol= -1, ccol= -1, collcol= -1, rowscol= 0;
  determine_show_table_status_columns(result, &ecol, &ccol, &collcol, &rowscol);

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
         get_product() == SERVER_TYPE_TIDB ) &&
        (row[ecol] == NULL ) &&
        (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
      is_view = 1;

    if ((get_product() == SERVER_TYPE_MARIADB) &&
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
    if (dump && ignore_engines && !is_view && !is_sequence) {
      for (i = 0; ignore_engines[i] != NULL; i++) {
        if (g_ascii_strcasecmp(ignore_engines[i], row[ecol]) == 0) {
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
    /* if tables and db both exists , should not call dump_database_thread */
    if (tables && !is_table_in_list(database->name, row[0], tables))
      continue;

    /* Special tables */
    if (is_mysql_special_tables(database->name, row[0])) {
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
    create_job_to_dump_table(conf, is_view, is_sequence, database, g_strdup(row[tablecol]), g_strdup(row[collcol]), g_strdup(row[ecol]));
  }

  mysql_free_result(result);

  if (determine_if_schema_is_elected_to_dump_post(conn,database)) {
    create_job_to_dump_post(database, conf);
//    struct schema_post *sp = g_new(struct schema_post, 1);
//    sp->database = database;
//    schema_post = g_list_prepend(schema_post, sp);
  }


  if (dump_triggers && database->dump_triggers)
    create_job_to_dump_schema_triggers(database, conf);

  return;
}

static
void thd_JOB_TABLE(struct thread_data *td, struct job *job){
  struct dump_table_job *dtj=(struct dump_table_job *)job->job_data;
  new_table_to_dump(td->thrconn, td->conf, dtj->is_view, dtj->is_sequence, dtj->database, dtj->table,
                      dtj->collation, dtj->engine);
  free(dtj->collation);
  free(dtj->engine);
  free(dtj);
}
