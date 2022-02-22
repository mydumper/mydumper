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
#include "zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include <glib-unix.h>
#include "config.h"
#include "src/common.h"
#include "src/myloader_stream.h"

#include "connection.h"
#include "getPassword.h"
#include "logging.h"
#include "set_verbose.h"
#include "locale.h"
#include "src/server_detect.h"
#include "src/tables_skiplist.h"
#include "src/regex.h"
#include "src/myloader_process.h"
#include "src/myloader_common.h"
#include "src/common_options.h"
#include "src/myloader_job.h"
#include "src/myloader_directory.h"

guint commit_count = 1000;
gchar *input_directory = NULL;
gchar *directory = NULL;
gchar *pwd=NULL;
gboolean overwrite_tables = FALSE;
gboolean innodb_optimize_keys = FALSE;
gboolean enable_binlog = FALSE;
gboolean disable_redo_log = FALSE;
gboolean skip_triggers = FALSE;
gboolean skip_post = FALSE;
gboolean skip_definer = FALSE;
gboolean serial_tbl_creation = FALSE;
gboolean resume = FALSE;
guint rows = 0;
gchar *source_db = NULL;
gchar *purge_mode_str=NULL;
gchar *set_names_str=NULL;
static GMutex *init_mutex = NULL;
guint errors = 0;
guint max_threads_per_table=4;
//unsigned long long int total_data_sql_files = 0;
//unsigned long long int progress = 0;
//GHashTable *db_hash=NULL;
extern GHashTable *tbl_hash;
extern GHashTable *db_hash;
extern gboolean shutdown_triggered;
const char DIRECTORY[] = "import";

void *process_queue(struct thread_data *td);
void checksum_table_filename(const gchar *filename, MYSQL *conn);
void checksum_databases(struct thread_data *td);
void create_database(struct thread_data *td, gchar *database);

static GOptionEntry entries[] = {
    {"directory", 'd', 0, G_OPTION_ARG_STRING, &input_directory,
     "Directory of the dump to import", NULL},
    {"queries-per-transaction", 'q', 0, G_OPTION_ARG_INT, &commit_count,
     "Number of queries per transaction, default 1000", NULL},
    {"overwrite-tables", 'o', 0, G_OPTION_ARG_NONE, &overwrite_tables,
     "Drop tables if they already exist", NULL},
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db,
     "An alternative database to restore into", NULL},
    {"source-db", 's', 0, G_OPTION_ARG_STRING, &source_db,
     "Database to restore", NULL},
    {"enable-binlog", 'e', 0, G_OPTION_ARG_NONE, &enable_binlog,
     "Enable binary logging of the restore data", NULL},
    {"innodb-optimize-keys", 0, 0, G_OPTION_ARG_NONE, &innodb_optimize_keys,
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
    {"skip-definer", 0, 0, G_OPTION_ARG_NONE, &skip_definer,
     "Removes DEFINER from the CREATE statement. By default, statements are not modified", NULL},
    {"no-data", 0, 0, G_OPTION_ARG_NONE, &no_data, "Do not dump or import table data",
     NULL},
    {"serialized-table-creation",0, 0, G_OPTION_ARG_NONE, &serial_tbl_creation, 
      "Table recreation will be executed in serie, one thread at a time",NULL},
    {"resume",0, 0, G_OPTION_ARG_NONE, &resume,
      "Expect to find resume file in backup dir and will only process those files",NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

GHashTable * initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  if (detected_server == SERVER_TYPE_MYSQL){
    g_hash_table_insert(set_session_hash,g_strdup("WAIT_TIMEOUT"),g_strdup("2147483"));
    g_hash_table_insert(set_session_hash,g_strdup("NET_WRITE_TIMEOUT"),g_strdup("2147483"));
  }
  if (!enable_binlog)
    g_hash_table_insert(set_session_hash,g_strdup("SQL_LOG_BIN"),g_strdup("0"));
  if (commit_count > 1)
    g_hash_table_insert(set_session_hash,g_strdup("AUTOCOMMIT"),g_strdup("0"));

  return set_session_hash;
}


GMutex **pause_mutex_per_thread=NULL;

gboolean sig_triggered(void * user_data, int signal) {
  guint i=0;
  GAsyncQueue *queue=NULL;
  if (signal == SIGTERM){
    shutdown_triggered = TRUE;
  }else{
    if (pause_mutex_per_thread == NULL){
      pause_mutex_per_thread=g_new(GMutex * , num_threads) ;
      for(i=0;i<num_threads;i++){
        pause_mutex_per_thread[i]=g_mutex_new();
      }
    }
    if (((struct configuration *)user_data)->pause_resume == NULL)
      ((struct configuration *)user_data)->pause_resume = g_async_queue_new();
    queue = ((struct configuration *)user_data)->pause_resume;
    for(i=0;i<num_threads;i++){
      g_mutex_lock(pause_mutex_per_thread[i]);
      g_async_queue_push(queue,pause_mutex_per_thread[i]);
    }
    g_critical("Ctrl+c detected! Are you sure you want to cancel(Y/N)?");
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
        break;
      }
    }
  }

  g_message("Writing resume.partial file");
  queue = ((struct configuration *)user_data)->file_list_to_do;
  gchar *filename;
  gchar *p=g_strdup("resume.partial"),*p2=g_strdup("resume");

  void *outfile = g_fopen(p, "w");
  filename=g_async_queue_pop(queue);
  while(g_strcmp0(filename,"NO_MORE_FILES")!=0){
    fprintf(outfile, "%s\n", filename);
    filename=g_async_queue_pop(queue);
  }
  fclose(outfile);
  if (g_rename(p, p2) != 0){
    g_critical("Error renaming resume.partial to resume");
  }
  g_free(p);
  g_free(p2);
  g_message("Shutting down gracefully completed.");
  return FALSE;
}

gboolean sig_triggered_int(void * user_data) {
  return sig_triggered(user_data,SIGINT);
}
gboolean sig_triggered_term(void * user_data) {
  return sig_triggered(user_data,SIGTERM);
}

void *signal_thread(void *data) {
  GMainLoop * loop=NULL;
  g_unix_signal_add(SIGINT, sig_triggered_int, data);
  g_unix_signal_add(SIGTERM, sig_triggered_term, data);
  loop = g_main_loop_new (NULL, TRUE);
  g_main_loop_run (loop);
  return NULL;
}


void m_connect(MYSQL *conn){
  configure_connection(conn, "myloader");
  if (!mysql_real_connect(conn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Error connection to database: %s", mysql_error(conn));
    exit(EXIT_FAILURE);
  }
}

int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};

  GError *error = NULL;
  GOptionContext *context;

  setlocale(LC_ALL, "");
  g_thread_init(NULL);

  init_mutex = g_mutex_new();
  if (db == NULL && source_db != NULL) {
    db = g_strdup(source_db);
  }

  context = g_option_context_new("multi-threaded MySQL loader");
  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);
  g_option_context_set_main_group(context, main_group);
  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }

  if (defaults_file != NULL){
    load_config_file(defaults_file, context, "myloader");
  }
  g_option_context_free(context);

  if (password != NULL){
    int i=1;
    for(i=1; i < argc; i++){
      gchar * p= g_strstr_len(argv[i],-1,password);
      if (p != NULL){
        strncpy(p, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", strlen(password));
      }
    }
  }
  // prompt for password if it's NULL
  if (sizeof(password) == 0 || (password == NULL && askPassword)) {
    password = passwordPrompt();
  }

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
  initialize_job(purge_mode_str);

  pwd=g_str_has_prefix(input_directory,"/")?g_strdup(""):g_get_current_dir();
  if (!input_directory) {
    if (stream){
      GDateTime * datetime = g_date_time_new_now_local();
      char *datetimestr;
      datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");
      directory = g_strdup_printf("%s/%s-%s",pwd, DIRECTORY, datetimestr);
      create_backup_dir(directory);
      g_free(datetimestr); 
    }else{
      g_critical("a directory needs to be specified, see --help\n");
      exit(EXIT_FAILURE);
    }
  } else {
    if (!g_file_test(input_directory,G_FILE_TEST_IS_DIR)){
      g_critical("the specified directory doesn't exists\n");
      exit(EXIT_FAILURE);
    }
    directory=g_strdup_printf("%s/%s", pwd, input_directory);
    if (!stream){
      char *p = g_strdup_printf("%s/metadata", directory);
      if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
        g_critical("the specified directory %s is not a mydumper backup",directory);
        exit(EXIT_FAILURE);
      }
    }
  }
  g_chdir(directory);
  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);

  initialize_process(&conf);
  initiliaze_common();
  initialize_regex(regexstring);

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
  m_connect(conn);

  set_session = g_string_new(NULL);
  detected_server = detect_server(conn);
  GHashTable * set_session_hash = initialize_hash_of_session_variables();
  if (defaults_file)
    load_hash_from_key_file(set_session_hash, NULL, defaults_file, "myloader_variables");
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
  conf.file_list_to_do = g_async_queue_new();
  db_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  tbl_hash=g_hash_table_new ( g_str_hash, g_str_equal );

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
    db_hash_insert(db, db);
    create_database(&t, db);
  }

  if (stream){
    initialize_stream(&conf);
  }

  guint n;
  GThread **threads = g_new(GThread *, num_threads);
  struct thread_data *td = g_new(struct thread_data, num_threads);
  for (n = 0; n < num_threads; n++) {
    td[n].conf = &conf;
    td[n].thread_id = n + 1;
    threads[n] =
        g_thread_create((GThreadFunc)process_queue, &td[n], TRUE, NULL);
    // Here, the ready queue is being used to serialize the connection to the database.
    // We don't want all the threads try to connect at the same time
    g_async_queue_pop(conf.ready);
  }
  
  if (stream){
    wait_stream_to_finish();
  }else{
    restore_from_directory(&conf);
  }

  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }


  if (shutdown_triggered)
    g_async_queue_push(conf.file_list_to_do, g_strdup("NO_MORE_FILES"));
  g_async_queue_unref(conf.ready);
  if (disable_redo_log)
    mysql_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG");

  g_async_queue_unref(conf.data_queue);

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
  mysql_close(conn);
  mysql_thread_end();
  mysql_library_end();
  g_free(directory);
  g_free(td);
  g_free(threads);

  if (logoutfile) {
    fclose(logoutfile);
  }

  return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}

// this can be moved to the table structure and executed before index creation.
void checksum_databases(struct thread_data *td) {
  g_message("Starting table checksum verification");

  const gchar *filename = NULL;
  GList *e = td->conf->checksum_list;
  while (e){
    filename=e->data;
    checksum_table_filename(filename, td->thrconn);
    e=e->next;
  }
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

void checksum_table_filename(const gchar *filename, MYSQL *conn) {
  gchar *database = NULL, *table = NULL;
  get_database_table_from_file(filename,"-checksum",&database,&table);
  gchar *real_database=db_hash_lookup(database);
  gchar *real_table=g_hash_table_lookup(tbl_hash,table);
  void *infile;
  char checksum[256];
  int errn=0;
  char * row=checksum_table(conn, db ? db : real_database, real_table, &errn);
  gboolean is_compressed = FALSE; 
  gchar *path = g_build_filename(directory, filename, NULL);

  if (!g_str_has_suffix(path, compress_extension)) {
    infile = g_fopen(path, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(path, "r");
    is_compressed=TRUE;
  }

  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return;
  }
  
  char * cs= !is_compressed ? fgets(checksum, 256, infile) :gzgets((gzFile)infile, checksum, 256);
  if (cs != NULL) {
    if(strcmp(checksum, row) != 0) {
      g_warning("Checksum mismatch found for `%s`.`%s`. Got '%s', expecting '%s'", db ? db : real_database, real_table, row, checksum);
      errors++;
    }
    else {
      g_message("Checksum confirmed for `%s`.`%s`", db ? db : real_database, real_table);
    }
  } else {
    g_critical("error reading file %s (%d)", filename, errno);
    errors++;
    return;
  }
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
}


void *process_queue(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);
  td->current_database=NULL;

  m_connect(td->thrconn);

  mysql_query(td->thrconn, set_names_str);
  mysql_query(td->thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
  mysql_query(td->thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
  mysql_query(td->thrconn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=db;
    execute_use(td, "Initializing thread");
  }
  g_debug("Thread %d: Starting import", td->thread_id);
  if (stream){
    process_stream_queue(td);
  }else{
    process_directory_queue(td);
  }
  struct job *job = NULL;
  gboolean cont=TRUE;

//  g_message("Thread %d: Starting post import task over table", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_table_queue);
//    g_message("%s",((struct restore_job *)job->job_data)->object);
    execute_use_if_needs_to(td, job->use_database, "Restoring post table");
    cont=process_job(td, job);
  }
//  g_message("Thread %d: Starting post import task: triggers, procedures and triggers", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_queue);
    execute_use_if_needs_to(td, job->use_database, "Restoring post tasks");
    cont=process_job(td, job);
  }

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  g_debug("Thread %d ending", td->thread_id);
  return NULL;
}
