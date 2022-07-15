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

    Authors:        Andrew Hutchings, SkySQL (andrew at skysql dot com)
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
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
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
#include "myloader_jobs_manager.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_pmm_thread.h"
#include "myloader_restore_job.h"
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
gchar *set_names_str=NULL;
guint errors = 0;
guint max_threads_per_table=4;
gboolean append_if_not_exist=FALSE;
//unsigned long long int total_data_sql_files = 0;
//unsigned long long int progress = 0;
//GHashTable *db_hash=NULL;
extern GHashTable *db_hash;
extern gboolean shutdown_triggered;

const char DIRECTORY[] = "import";

gchar *pmm_resolution = NULL;
gchar *pmm_path = NULL;
gboolean pmm = FALSE;
gboolean arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (g_strstr_len(option_name,22,"--innodb-optimize-keys")){
    innodb_optimize_keys = TRUE;
    if (value==NULL){
      innodb_optimize_keys_per_table = TRUE;
      innodb_optimize_keys_all_tables = FALSE;
      return TRUE;
    }
    if (g_strstr_len(value,22,"AFTER_IMPORT_PER_TABLE")){
      innodb_optimize_keys_per_table = TRUE;
      innodb_optimize_keys_all_tables = FALSE;
      return TRUE;
    }
    if (g_strstr_len(value,23,"AFTER_IMPORT_ALL_TABLES")){
      innodb_optimize_keys_all_tables = TRUE;
      innodb_optimize_keys_per_table = FALSE;
      return TRUE;
    }
  }
  return FALSE;
}

static GOptionEntry entries[] = {
    {"directory", 'd', 0, G_OPTION_ARG_STRING, &input_directory,
     "Directory of the dump to import", NULL},
    {"queries-per-transaction", 'q', 0, G_OPTION_ARG_INT, &commit_count,
     "Number of queries per transaction, default 1000", NULL},
    {"overwrite-tables", 'o', 0, G_OPTION_ARG_NONE, &overwrite_tables,
     "Drop tables if they already exist", NULL},
    {"append-if-not-exist", 0, 0, G_OPTION_ARG_NONE,&append_if_not_exist,
      "Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented", NULL},
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db,
     "An alternative database to restore into", NULL},
    {"source-db", 's', 0, G_OPTION_ARG_STRING, &source_db,
     "Database to restore", NULL},
    {"enable-binlog", 'e', 0, G_OPTION_ARG_NONE, &enable_binlog,
     "Enable binary logging of the restore data", NULL},
    {"innodb-optimize-keys", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "Creates the table without the indexes and it adds them at the end", NULL},
    { "set-names",0, 0, G_OPTION_ARG_STRING, &set_names_str, 
      "Sets the names, use it at your own risk, default binary", NULL },
    {"logfile", 'L', 0, G_OPTION_ARG_FILENAME, &logfile,
     "Log file name to use, by default stdout is used", NULL},
    { "purge-mode", 0, 0, G_OPTION_ARG_STRING, &purge_mode_str, 
      "This specify the truncate mode which can be: NONE, DROP, TRUNCATE and DELETE", NULL },
    { "disable-redo-log", 0, 0, G_OPTION_ARG_NONE, &disable_redo_log,
      "Disables the REDO_LOG and enables it after, doesn't check initial status", NULL },
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows,
     "Split the INSERT statement into this many rows.", NULL},
    {"max-threads-per-table", 0, 0, G_OPTION_ARG_INT, &max_threads_per_table,
     "Maximum number of threads per table to use, default 4", NULL},
    {"skip-triggers", 0, 0, G_OPTION_ARG_NONE, &skip_triggers, "Do not import triggers. By default, it imports triggers",
     NULL},
    {"skip-post", 0, 0, G_OPTION_ARG_NONE, &skip_post,
     "Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions", NULL},
    {"no-data", 0, 0, G_OPTION_ARG_NONE, &no_data, "Do not dump or import table data",
     NULL},
    {"serialized-table-creation",0, 0, G_OPTION_ARG_NONE, &serial_tbl_creation, 
      "Table recreation will be executed in serie, one thread at a time",NULL},
    {"resume",0, 0, G_OPTION_ARG_NONE, &resume,
      "Expect to find resume file in backup dir and will only process those files",NULL},
    { "pmm-path", 0, 0, G_OPTION_ARG_STRING, &pmm_path,
      "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution", NULL },
    { "pmm-resolution", 0, 0, G_OPTION_ARG_STRING, &pmm_resolution,
      "which default will be high", NULL },    
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

GHashTable * myloader_initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=initialize_hash_of_session_variables();
  if (!enable_binlog)
    g_hash_table_insert(set_session_hash,g_strdup("SQL_LOG_BIN"),g_strdup("0"));
  if (commit_count > 1)
    g_hash_table_insert(set_session_hash,g_strdup("AUTOCOMMIT"),g_strdup("0"));

  return set_session_hash;
}

void create_database(struct thread_data *td, gchar *database) {
  gchar *query = NULL;

  const gchar *filename =
      g_strdup_printf("%s-schema-create.sql", database);
  const gchar *filenamegz =
      g_strdup_printf("%s-schema-create.sql%s", database, compress_extension);
  const gchar *filepath = g_strdup_printf("%s/%s-schema-create.sql",
                                          directory, database);
  const gchar *filepathgz = g_strdup_printf("%s/%s-schema-create.sql%s",
                                            directory, database, compress_extension);

  if (g_file_test(filepath, G_FILE_TEST_EXISTS)) {
    restore_data_from_file(td, database, NULL, filename, TRUE);
  } else if (g_file_test(filepathgz, G_FILE_TEST_EXISTS)) {
    restore_data_from_file(td, database, NULL, filenamegz, TRUE);
  } else {
    query = g_strdup_printf("CREATE DATABASE IF NOT EXISTS `%s`", database);
    if (mysql_query(td->thrconn, query)){
      g_warning("Fail to create database: %s", database);
    }
  }

  g_free(query);
  return;
}

int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};

  GError *error = NULL;
  GOptionContext *context;

  setlocale(LC_ALL, "");
  g_thread_init(NULL);

  if (db == NULL && source_db != NULL) {
    db = g_strdup(source_db);
  }

  context = g_option_context_new("multi-threaded MySQL loader");
  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);
//  load_common_entries(main_group);
  load_connection_entries(main_group);
  load_regex_entries(main_group);
  load_restore_entries(main_group);
  g_option_context_set_main_group(context, main_group);
  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }
  g_strfreev(tmpargv);

  set_verbose(verbose);
  initialize_common_options(context,"myloader");
  g_option_context_free(context);

  hide_password(argc, argv);
  ask_password();

  if (program_version) {
    g_print("myloader %s, built against MySQL %s\n", VERSION,
            MYSQL_VERSION_STR);
    exit(EXIT_SUCCESS);
  }

  set_verbose(verbose);

#ifdef ZWRAP_USE_ZSTD
  compress_extension = g_strdup(".zst");
#else
  compress_extension = g_strdup(".gz");
#endif
  if (set_names_str){
    gchar *tmp_str=g_strdup_printf("/*!40101 SET NAMES %s*/",set_names_str);
    set_names_str=tmp_str;
  } else 
    set_names_str=g_strdup("/*!40101 SET NAMES binary*/");

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
      g_critical("Could not create pmm thread: %s", serror->message);
      g_error_free(serror);
      exit(EXIT_FAILURE);
    }
  }
  initialize_job(purge_mode_str);
  char *current_dir=g_get_current_dir();
  if (!input_directory) {
    if (stream){
      GDateTime * datetime = g_date_time_new_now_local();
      char *datetimestr;
      datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");

      directory = g_strdup_printf("%s/%s-%s",current_dir, DIRECTORY, datetimestr);
      create_backup_dir(directory);
      g_date_time_unref(datetime);
      g_free(datetimestr); 
    }else{
      g_critical("a directory needs to be specified, see --help\n");
      exit(EXIT_FAILURE);
    }
  } else {
    directory=g_strdup_printf("%s/%s", g_str_has_prefix(input_directory,"/")?"":current_dir, input_directory);
    if (!g_file_test(input_directory,G_FILE_TEST_IS_DIR)){
      if (stream){
        create_backup_dir(directory);
      }else{
        g_critical("the specified directory doesn't exists\n");
        exit(EXIT_FAILURE);
      }
    }
    if (!stream){
      char *p = g_strdup_printf("%s/metadata", directory);
      if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
        g_critical("the specified directory %s is not a mydumper backup",directory);
        exit(EXIT_FAILURE);
      }
      initialize_directory();
    }
  }
  g_free(current_dir);
  g_chdir(directory);
  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);
  initialize_process(&conf);
  initialize_common();
  initialize_regex();
  GError *serror;
  GThread *sthread =
      g_thread_create(signal_thread, &conf, FALSE, &serror);
  if (sthread == NULL) {
    g_critical("Could not create signal thread: %s", serror->message);
    g_error_free(serror);
    exit(EXIT_FAILURE);
  }

  MYSQL *conn;
  conn = mysql_init(NULL);
  m_connect(conn,"myloader",NULL);

  set_session = g_string_new(NULL);
  detected_server = detect_server(conn);
  GHashTable * set_session_hash = myloader_initialize_hash_of_session_variables();
  if (defaults_file)
    load_session_hash_from_key_file(key_file,set_session_hash,"myloader_variables");
  refresh_set_session_from_hash(set_session,set_session_hash);
  execute_gstring(conn, set_session);

  // TODO: we need to set the variables in the initilize session varibles, not from:
//  if (mysql_query(conn, "SET SESSION wait_timeout = 2147483")) {
//    g_warning("Failed to increase wait_timeout: %s", mysql_error(conn));
//  }

//  if (!enable_binlog)
//    mysql_query(conn, "SET SQL_LOG_BIN=0");
  if (disable_redo_log){
    g_message("Disabling redologs");
    mysql_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG");
  }
  mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
  // To here.
  conf.database_queue = g_async_queue_new();
  conf.table_queue = g_async_queue_new();
  conf.data_queue = g_async_queue_new();
  conf.post_table_queue = g_async_queue_new();
  conf.post_queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.pause_resume = g_async_queue_new();
  db_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );

  if (resume && !g_file_test("resume",G_FILE_TEST_EXISTS)){
    g_critical("Resume file not found");
    exit(EXIT_FAILURE);
  }

  struct thread_data t;
  t.thread_id = 0;
  t.conf = &conf;
  t.thrconn = conn;
  t.current_database=NULL;

  if (tables_list)
    tables = g_strsplit(tables_list, ",", 0);

  // Create database before the thread, to allow connection
  if (db){
    db_hash_insert(g_strdup(db), g_strdup(db));
    create_database(&t, db);
  }

  if (stream){
    initialize_stream(&conf);
  }

  initialize_loader_threads(&conf);
  
  if (stream){
    wait_stream_to_finish();
  }else{
    restore_from_directory(&conf);
  }

  wait_loader_threads_to_finish();

  g_async_queue_unref(conf.ready);
  conf.ready=NULL;

  if (disable_redo_log)
    mysql_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG");

  g_async_queue_unref(conf.data_queue);
  conf.data_queue=NULL;
  checksum_databases(&t);

  if (stream && no_delete == FALSE && input_directory == NULL){
    // remove metadata files
    GList *e=conf.metadata_list;
    while (e) {
      m_remove(directory, e->data);
      e=e->next;
    }
    m_remove(directory,"metadata");
    if (g_rmdir(directory) != 0)
        g_critical("Restore directory not removed: %s", directory);
  }

  g_async_queue_unref(conf.database_queue);
  g_async_queue_unref(conf.table_queue);
  g_async_queue_unref(conf.pause_resume);
  g_async_queue_unref(conf.post_table_queue);
  g_async_queue_unref(conf.post_queue);
  free_hash(set_session_hash);
  g_hash_table_remove_all(set_session_hash);
  g_hash_table_unref(set_session_hash);
  mysql_close(conn);
  mysql_thread_end();
  mysql_library_end();
  g_free(directory);
  free_loader_threads();

  if (pmm){
    kill_pmm_thread();
//    g_thread_join(pmmthread);
  }
  free_table_hash(conf.table_hash);
  g_hash_table_remove_all(conf.table_hash);
  g_hash_table_unref(conf.table_hash);
  g_list_free_full(conf.checksum_list,g_free);
  if (logoutfile) {
    fclose(logoutfile);
  }

  stop_signal_thread();
  return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}

