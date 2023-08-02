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

    Authors:        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
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
#include <glib/gstdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include "config.h"
#include "common.h"
#include "myloader_stream.h"

#include "connection.h"
//#include "getPassword.h"
#include "logging.h"
#include "set_verbose.h"
#include "locale.h"
#include "server_detect.h"
#include "tables_skiplist.h"
#include "regex.h"
#include "myloader_process.h"
#include "myloader_common.h"
#include "common_options.h"
//#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_pmm_thread.h"
#include "myloader_restore_job.h"
#include "myloader_intermediate_queue.h"
#include "myloader_arguments.h"
#include "myloader_global.h"
#include "myloader_worker_index.h"
#include "myloader_worker_schema.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_post.h"

guint commit_count = 1000;
gchar *input_directory = NULL;
gchar *directory = NULL;
gchar *pwd=NULL;
gboolean overwrite_tables = FALSE;

gboolean innodb_optimize_keys = FALSE;
gboolean innodb_optimize_keys_per_table = FALSE;
gboolean innodb_optimize_keys_all_tables = FALSE;

gboolean enable_binlog = FALSE;
gboolean disable_redo_log = FALSE;
gboolean skip_triggers = FALSE;
gboolean skip_post = FALSE;
gboolean serial_tbl_creation = FALSE;
gboolean resume = FALSE;
guint rows = 0;
gchar *source_db = NULL;
gchar *purge_mode_str=NULL;
guint errors = 0;
struct restore_errors detailed_errors = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
guint max_threads_per_table=4;
guint max_threads_per_table_hard=4;
guint max_threads_for_schema_creation=4;
guint max_threads_for_index_creation=4;
guint max_threads_for_post_creation=4;
gboolean stream = FALSE;
gboolean no_delete = FALSE;

GMutex *load_data_list_mutex=NULL;
GHashTable * load_data_list = NULL;
//unsigned long long int total_data_sql_files = 0;
//unsigned long long int progress = 0;
//GHashTable *db_hash=NULL;
extern GHashTable *db_hash;
extern gboolean shutdown_triggered;
extern gboolean skip_definer;
const char DIRECTORY[] = "import";

gchar *pmm_resolution = NULL;
gchar *pmm_path = NULL;
gboolean pmm = FALSE;

GHashTable * myloader_initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=initialize_hash_of_session_variables();
  if (!enable_binlog)
    g_hash_table_insert(set_session_hash,g_strdup("SQL_LOG_BIN"),g_strdup("0"));
  if (commit_count > 1)
    g_hash_table_insert(set_session_hash,g_strdup("AUTOCOMMIT"),g_strdup("0"));

  return set_session_hash;
}


gchar * print_time(GTimeSpan timespan){
  GTimeSpan days   = timespan/G_TIME_SPAN_DAY;
  GTimeSpan hours  =(timespan-(days*G_TIME_SPAN_DAY))/G_TIME_SPAN_HOUR;
  GTimeSpan minutes=(timespan-(days*G_TIME_SPAN_DAY)-(hours*G_TIME_SPAN_HOUR))/G_TIME_SPAN_MINUTE;
  GTimeSpan seconds=(timespan-(days*G_TIME_SPAN_DAY)-(hours*G_TIME_SPAN_HOUR)-(minutes*G_TIME_SPAN_MINUTE))/G_TIME_SPAN_SECOND;
  return g_strdup_printf("%" G_GINT64_FORMAT " %02" G_GINT64_FORMAT ":%02" G_GINT64_FORMAT ":%02" G_GINT64_FORMAT "",days,hours,minutes,seconds);
}


gint compare_by_time(gconstpointer a, gconstpointer b){
  return
    g_date_time_difference(((struct db_table *)a)->finish_time,((struct db_table *)a)->start_data_time) >
    g_date_time_difference(((struct db_table *)b)->finish_time,((struct db_table *)b)->start_data_time);
}


void show_dbt(void* key, void* dbt, void *total){
  (void) key;
  (void) dbt;
  (void) total;
  g_message("Table %s", (char*) key);
//  *((guint *)total)= 100;
//  //    if (((struct db_table*)dbt)->schema_state >= CREATED)
//  //        *((guint *)total)= *((guint *)total) + 1;
 //        //*((guint *)total) + 2+ ((struct db_table*)dbt)->schema_state >= CREATED /*ALL_DONE*/ ? 1 : 0;
  }

void create_database(struct thread_data *td, gchar *database) {
  gchar *query = NULL;

  const gchar *filename =
      g_strdup_printf("%s-schema-create.sql", database);
  const gchar *filenamegz =
      g_strdup_printf("%s-schema-create.sql%s", database, exec_per_thread_extension);
  const gchar *filepath = g_strdup_printf("%s/%s-schema-create.sql",
                                          directory, database);
  const gchar *filepathgz = g_strdup_printf("%s/%s-schema-create.sql%s",
                                            directory, database, exec_per_thread_extension);

  if (g_file_test(filepath, G_FILE_TEST_EXISTS)) {
    g_atomic_int_add(&(detailed_errors.schema_errors), restore_data_from_file(td, database, NULL, filename, TRUE));
  } else if (g_file_test(filepathgz, G_FILE_TEST_EXISTS)) {
    g_atomic_int_add(&(detailed_errors.schema_errors), restore_data_from_file(td, database, NULL, filenamegz, TRUE));
  } else {
    query = g_strdup_printf("CREATE DATABASE IF NOT EXISTS `%s`", database);
    if (!m_query(td->thrconn, query, m_warning, "Fail to create database: %s", database))
      g_atomic_int_inc(&(detailed_errors.schema_errors));
//    if (mysql_query(td->thrconn, query)){
//      g_warning("Fail to create database: %s", database);
//    }
  }

  g_free(query);
  return;
}


void print_errors(){
  g_message(
    "Errors found:\n"
    "- Tablespace:\t%d\n"
    "- Schema:    \t%d\n"
    "- Data:      \t%d\n"
    "- View:      \t%d\n"
    "- Sequence:  \t%d\n"
    "- Index:     \t%d\n"
    "- Trigger:   \t%d\n"
    "- Constraint:\t%d\n"
    "- Post:      \t%d\n"
    "Retries:\t%d",
    detailed_errors.tablespace_errors,
    detailed_errors.schema_errors,
    detailed_errors.data_errors,
    detailed_errors.view_errors,
    detailed_errors.sequence_errors,
    detailed_errors.index_errors,
    detailed_errors.trigger_errors,
    detailed_errors.constraints_errors,
    detailed_errors.post_errors,
    detailed_errors.retries);


}

int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};

  GError *error = NULL;
  GOptionContext *context;

  setlocale(LC_ALL, "");
  g_thread_init(NULL);

  signal(SIGCHLD, SIG_IGN);

  if (db == NULL && source_db != NULL) {
    db = g_strdup(source_db);
  }

  context = load_contex_entries();

  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    m_critical("option parsing failed: %s, try --help\n", error->message);
  }
  g_strfreev(tmpargv);

  if (help){
    printf("%s", g_option_context_get_help (context, FALSE, NULL));
    exit(0);
  }

  if (debug) {
    set_debug();
    set_verbose(3);
  } else {
    set_verbose(verbose);
  }

  initialize_common_options(context, "myloader");
  g_option_context_free(context);

  hide_password(argc, argv);
  ask_password();

  if (program_version) {
    print_version("myloader");
    exit(EXIT_SUCCESS);
  }

  initialize_set_names();


  load_data_list_mutex=g_mutex_new();
  load_data_list = g_hash_table_new ( g_str_hash, g_str_equal );

  if (pmm_path){
    pmm=TRUE;
    if (!pmm_resolution){
      pmm_resolution=g_strdup("high");
    }
  }else if (pmm_resolution){
    pmm=TRUE;
    pmm_path=g_strdup_printf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution",pmm_resolution);
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
//  initialize_job(purge_mode_str);
  initialize_restore_job(purge_mode_str);
  char *current_dir=g_get_current_dir();
  if (!input_directory) {
    if (stream){
      GDateTime * datetime = g_date_time_new_now_local();
      char *datetimestr;
      datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");

      directory = g_strdup_printf("%s/%s-%s",current_dir, DIRECTORY, datetimestr);
      create_backup_dir(directory,fifo_directory);
      g_date_time_unref(datetime);
      g_free(datetimestr); 
    }else{
      m_critical("a directory needs to be specified, see --help\n");
    }
  } else {
    directory=g_str_has_prefix(input_directory,"/")?input_directory:g_strdup_printf("%s/%s", current_dir, input_directory);
    if (!g_file_test(input_directory,G_FILE_TEST_IS_DIR)){
      if (stream){
        create_backup_dir(directory,fifo_directory);
      }else{
        m_critical("the specified directory doesn't exists\n");
      }
    }
    if (!stream){
      char *p = g_strdup_printf("%s/metadata", directory);
      if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
        m_critical("the specified directory %s is not a mydumper backup",directory);
      }
//      initialize_directory();
    }
  }
  if (fifo_directory){
    if (fifo_directory[0] != '/' ){
      gchar *tmp_fifo_directory=fifo_directory;
      fifo_directory=g_strdup_printf("%s/%s", current_dir, tmp_fifo_directory);
    }
    create_fifo_dir(fifo_directory);
  }

  g_free(current_dir);
  g_chdir(directory);
  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);
  initialize_process(&conf);
  initialize_common();
  initialize_connection(MYLOADER);
  initialize_regex(NULL);
  GError *serror;
  GThread *sthread =
      g_thread_create(signal_thread, &conf, FALSE, &serror);
  if (sthread == NULL) {
    m_critical("Could not create signal thread: %s", serror->message);
    g_error_free(serror);
  }

  MYSQL *conn;
  conn = mysql_init(NULL);
  m_connect(conn);

  set_session = g_string_new(NULL);
  set_global = g_string_new(NULL);
  set_global_back = g_string_new(NULL);
  detect_server_version(conn);
  detected_server = get_product();
  GHashTable * set_session_hash = myloader_initialize_hash_of_session_variables();
  GHashTable * set_global_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  if (key_file != NULL ){
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_global_hash,"myloader_global_variables");
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_session_hash,"myloader_session_variables");
  }
  refresh_set_session_from_hash(set_session,set_session_hash);
  refresh_set_global_from_hash(set_global,set_global_back, set_global_hash);
  execute_gstring(conn, set_session);
  execute_gstring(conn, set_global);

  identifier_quote_character_str=g_strdup_printf("%c",identifier_quote_character);
  // TODO: we need to set the variables in the initilize session varibles, not from:
//  if (mysql_query(conn, "SET SESSION wait_timeout = 2147483")) {
//    g_warning("Failed to increase wait_timeout: %s", mysql_error(conn));
//  }

  if (disable_redo_log){
    if ((get_major() == 8) && (get_secondary() == 0) && (get_revision() > 21)){
      g_message("Disabling redologs");
      m_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", m_critical, "DISABLE INNODB REDO LOG failed");
//      mysql_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG");
    }else{
      m_error("Disabling redologs is not supported for version %d.%d.%d", get_major(), get_secondary(), get_revision());
    }
  }
//  mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
  // To here.
  conf.database_queue = g_async_queue_new();
  conf.table_queue = g_async_queue_new();
  conf.data_queue = g_async_queue_new();
  conf.post_table_queue = g_async_queue_new();
  conf.post_queue = g_async_queue_new();
  conf.index_queue = g_async_queue_new();
  conf.view_queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.pause_resume = g_async_queue_new();
  conf.table_list_mutex = g_mutex_new();
  conf.stream_queue = g_async_queue_new();
  conf.table_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  conf.table_hash_mutex=g_mutex_new();
  db_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );

  if (g_file_test("resume",G_FILE_TEST_EXISTS)){
    if (!resume){
      m_critical("Resume file found but --resume has not been provided");
    }
  }else{
    if (resume){
      m_critical("Resume file not found");
    }
  }

  struct thread_data t;
  t.thread_id = 0;
  t.conf = &conf;
  t.thrconn = conn;
  t.current_database=NULL;
  t.status=WAITING;

  if (tables_list)
    tables = get_table_list(tables_list);

  // Create database before the thread, to allow connection
  if (db){
    struct database * d=get_db_hash(g_strdup(db), g_strdup(db));
    create_database(&t, db);
    d->schema_state=CREATED;
  }

  if (serial_tbl_creation)
    max_threads_for_schema_creation=1;

  initialize_worker_schema(&conf);
  initialize_worker_index(&conf);
  initialize_intermediate_queue(&conf);

  if (stream){
    if (resume){
      m_critical("We don't expect to find resume files in a stream scenario");
    }
    initialize_stream(&conf);

    wait_until_first_metadata();
  }

  initialize_loader_threads(&conf);

  if (stream){
    wait_stream_to_finish();
  }else{
    process_directory(&conf);
  }
  wait_schema_worker_to_finish();
  wait_loader_threads_to_finish();
  create_index_shutdown_job(&conf);
  wait_index_worker_to_finish();
  initialize_post_loding_threads(&conf);
  create_post_shutdown_job(&conf);
  wait_post_worker_to_finish();
  g_async_queue_unref(conf.ready);
  conf.ready=NULL;

  if (disable_redo_log)
    m_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG", m_critical, "ENABLE INNODB REDO LOG failed");

  g_async_queue_unref(conf.data_queue);
  conf.data_queue=NULL;

  GList * tl=conf.table_list;
  while (tl != NULL){
    checksum_dbt(tl->data, conn);
    tl=tl->next;
  }


  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, db_hash);
  struct database *d=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &d ) ) {
    if (d->schema_checksum != NULL)
      checksum_database_template(d->name, d->schema_checksum,  conn, "Schema create checksum", checksum_database_defaults);
    if (d->post_checksum != NULL)
      checksum_database_template(d->name, d->post_checksum,  conn, "Post checksum", checksum_process_structure);
    if (d->triggers_checksum != NULL)
      checksum_database_template(d->name, d->triggers_checksum,  conn, "Triggers checksum", checksum_trigger_structure_from_database);
  }


  if (stream && no_delete == FALSE && input_directory == NULL){
    m_remove(directory,"metadata");
    if (g_rmdir(directory) != 0)
        g_critical("Restore directory not removed: %s", directory);
  }

  if (change_master_statement != NULL ){
    int i=0;
    gchar** line=g_strsplit(change_master_statement->str, ";\n", -1);
    for (i=0; i < (int)g_strv_length(line);i++){
       if (strlen(line[i])>2){
         GString *str=g_string_new(line[i]);
         g_string_append_c(str,';');
         m_query(conn, str->str, m_warning, "Sending CHANGE MASTER: %s", str->str);
         g_string_free(str,TRUE);
       }
    }
  }


  g_async_queue_unref(conf.database_queue);
  g_async_queue_unref(conf.table_queue);
  g_async_queue_unref(conf.pause_resume);
  g_async_queue_unref(conf.post_table_queue);
  g_async_queue_unref(conf.post_queue);
  free_hash(set_session_hash);
  g_hash_table_remove_all(set_session_hash);
  g_hash_table_unref(set_session_hash);
  execute_gstring(conn, set_global_back);
  mysql_close(conn);
  mysql_thread_end();
  mysql_library_end();
  g_free(directory);
  free_loader_threads();

  if (pmm){
    kill_pmm_thread();
//    g_thread_join(pmmthread);
  }

//  g_hash_table_foreach(conf.table_hash,&show_dbt, NULL);
  free_table_hash(conf.table_hash);
  g_hash_table_remove_all(conf.table_hash);
  g_hash_table_unref(conf.table_hash);
  g_list_free_full(conf.checksum_list,g_free);

  free_set_names();
  print_errors();

  stop_signal_thread();

  if (logoutfile) {
    fclose(logoutfile);
  }

/*
  GList * tl=g_list_sort(conf.table_list, compare_by_time);
  g_message("Import timings:");
  g_message("Data      \t| Index    \t| Total   \t| Table");
  while (tl != NULL){
    struct db_table * dbt=tl->data;
    GTimeSpan diff1=g_date_time_difference(dbt->start_index_time,dbt->start_time);
    GTimeSpan diff2=g_date_time_difference(dbt->finish_time,dbt->start_index_time);
    g_message("%s\t| %s\t| %s\t| `%s`.`%s`",print_time(diff1),print_time(diff2),print_time(diff1+diff2),dbt->real_database,dbt->real_table);
    tl=tl->next;
  }
*/


  return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}

