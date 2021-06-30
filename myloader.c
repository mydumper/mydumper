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
#include <zlib.h>
#include "config.h"
#include "common.h"
#include "myloader.h"
#include "connection.h"
#include "getPassword.h"
#include "logging.h"
#include "set_verbose.h"

guint commit_count = 1000;
gchar *directory = NULL;
gboolean overwrite_tables = FALSE;
gboolean innodb_optimize_keys = FALSE;
gboolean enable_binlog = FALSE;
gboolean sync_before_add_index = FALSE;
gboolean disable_redo_log = FALSE;
gchar *source_db = NULL;
gchar *purge_mode_str=NULL;
gchar *set_names_str=NULL;
enum purge_mode purge_mode;
static GMutex *init_mutex = NULL;
static GMutex *progress_mutex = NULL;
guint errors = 0;

unsigned long long int total_data_sql_files = 0;
unsigned long long int progress = 0;
gboolean read_data(FILE *file, gboolean is_compressed, GString *data,
                   gboolean *eof);
void restore_data_from_file(MYSQL *conn, char *database, char *table,
                  const char *filename, gboolean is_schema, gboolean need_use, 
		  gboolean is_create_table, GAsyncQueue *fast_index_creation_queue);
void restore_data_in_gstring_from_file(MYSQL *conn, char *database, char *table, 
		  GString *data, const char *filename, gboolean is_schema, 
		  guint *query_counter);
void restore_data_in_gstring(MYSQL *conn, char *database, char *table, GString *data, const char *filename, gboolean is_schema, guint *query_counter);
void *process_queue(struct thread_data *td);
void add_table(const gchar *filename, struct configuration *conf);
void checksum_table_filename(const gchar *filename, MYSQL *conn);
void add_schema(const gchar *filename, GAsyncQueue *fast_index_creation_queue, MYSQL *conn);
void restore_databases(struct configuration *conf, MYSQL *conn);
void checksum_databases(MYSQL *conn);
void restore_schema_view(MYSQL *conn);
void restore_schema_triggers(MYSQL *conn);
void restore_schema_post(MYSQL *conn);
void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data);
void create_database(MYSQL *conn, gchar *database);

static GOptionEntry entries[] = {
    {"directory", 'd', 0, G_OPTION_ARG_STRING, &directory,
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
    { "sync-before-add-index", 0, 0, G_OPTION_ARG_NONE, &sync_before_add_index,
      "If --innodb-optimize-keys is used, this option will force all the data threads to complete before starting the create index phase", NULL },
    { "disable-redo-log", 0, 0, G_OPTION_ARG_NONE, &disable_redo_log,
      "Disables the REDO_LOG and enables it after, doesn't check initial status", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, 0};

  GError *error = NULL;
  GOptionContext *context;

  g_thread_init(NULL);

  init_mutex = g_mutex_new();
  progress_mutex = g_mutex_new();

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
  if (config_file != NULL){
    set_session = g_string_new(NULL);
    load_config_file(config_file, context, "myloader", set_session);
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

  if (set_names_str){
    gchar *tmp_str=g_strdup_printf("/*!40101 SET NAMES %s*/",set_names_str);
    set_names_str=tmp_str;
  } else 
    set_names_str=g_strdup("/*!40101 SET NAMES binary*/");

  if (purge_mode_str){
    if (!strcmp(purge_mode_str,"TRUNCATE")){
      purge_mode=TRUNCATE;
    } else if (!strcmp(purge_mode_str,"DROP")){
      purge_mode=DROP;
    } else if (!strcmp(purge_mode_str,"DELETE")){
      purge_mode=DELETE;
    } else if (!strcmp(purge_mode_str,"NONE")){
      purge_mode=NONE;
    } else {
      g_error("Purge mode unknown");
    }
  } else if (overwrite_tables) 
    purge_mode=DROP; // Default mode is DROP when overwrite_tables is especified
  else purge_mode=NONE;
  
  if (!directory) {
    g_critical("a directory needs to be specified, see --help\n");
    exit(EXIT_FAILURE);
  } else {
    if (!g_file_test(directory,G_FILE_TEST_IS_DIR)){
      g_critical("the specified directory doesn't exists\n");
      exit(EXIT_FAILURE);
    }
    char *p = g_strdup_printf("%s/metadata", directory);
    if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
      g_critical("the specified directory is not a mydumper backup\n");
      exit(EXIT_FAILURE);
    }
  }
  MYSQL *conn;
  conn = mysql_init(NULL);

  configure_connection(conn, "myloader");
  if (!mysql_real_connect(conn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Error connection to database: %s", mysql_error(conn));
    exit(EXIT_FAILURE);
  }

  if (mysql_query(conn, "SET SESSION wait_timeout = 2147483")) {
    g_warning("Failed to increase wait_timeout: %s", mysql_error(conn));
  }

  if (!enable_binlog)
    mysql_query(conn, "SET SQL_LOG_BIN=0");

  if (disable_redo_log){
    g_message("Disabling redologs");
    mysql_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG");
  }
  mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
  conf.queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.fast_index_creation_queue = g_async_queue_new();

  guint n;
  GThread **threads = g_new(GThread *, num_threads);
  struct thread_data *td = g_new(struct thread_data, num_threads);
  for (n = 0; n < num_threads; n++) {
    td[n].conf = &conf;
    td[n].thread_id = n + 1;
    threads[n] =
        g_thread_create((GThreadFunc)process_queue, &td[n], TRUE, NULL);
    g_async_queue_pop(conf.ready);
  }

  g_message("%d threads created", num_threads);

  restore_databases(&conf, conn);

  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_WAIT;
    g_async_queue_push(conf.queue, j);
  }

  for (n = 0; n < num_threads; n++) 
    g_async_queue_pop(conf.ready);

  g_async_queue_unref(conf.ready);

  // move from a queue to the done query
  struct job *job =(struct job *)g_async_queue_try_pop(conf.fast_index_creation_queue);
  while (job != NULL){
    g_async_queue_push(conf.queue, job);
    job =(struct job *)g_async_queue_try_pop(conf.fast_index_creation_queue);
  }


  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.queue, j);
  }

  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }

  restore_schema_post(conn);

  restore_schema_view(conn);

  restore_schema_triggers(conn);

  if (disable_redo_log)
    mysql_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG");

  g_async_queue_unref(conf.queue);

  checksum_databases(conn);

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

void restore_databases(struct configuration *conf, MYSQL *conn) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (g_strrstr(filename, "-schema.sql")) {
        add_schema(filename, conf->fast_index_creation_queue, conn);
      }
    }
  }
  g_dir_rewind(dir);
  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (!g_strrstr(filename, "-schema.sql") &&
          !g_strrstr(filename, "-schema-view.sql") &&
          !g_strrstr(filename, "-schema-triggers.sql") &&
          !g_strrstr(filename, "-schema-post.sql") &&
          !g_strrstr(filename, "-schema-create.sql") &&
          g_strrstr(filename, ".sql")) {
        total_data_sql_files++;
      }
    }
  }
  g_dir_rewind(dir);
  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (!g_strrstr(filename, "-schema.sql") &&
          !g_strrstr(filename, "-schema-view.sql") &&
          !g_strrstr(filename, "-schema-triggers.sql") &&
          !g_strrstr(filename, "-schema-post.sql") &&
          !g_strrstr(filename, "-schema-create.sql") &&
          g_strrstr(filename, ".sql")) {
        add_table(filename, conf);
      }
    }
  }

  g_dir_close(dir);
}

void checksum_databases(MYSQL *conn) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  g_message("Starting table checksum verification");

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (g_strrstr(filename, ".checksum")) {
        checksum_table_filename(filename, conn);
      }
    }
  }
  g_dir_close(dir);
}

void restore_schema_view(MYSQL *conn) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (g_strrstr(filename, "-schema-view.sql")) {
        add_schema(filename, NULL, conn);
      }
    }
  }

  g_dir_close(dir);
}

void restore_schema_triggers(MYSQL *conn) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);
  gchar **split_file = NULL;
  gchar *database = NULL;
  gchar **split_table = NULL;
  gchar *table = NULL;

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (g_strrstr(filename, "-schema-triggers.sql")) {
        split_file = g_strsplit(filename, ".", 0);
        database = split_file[0];
        split_table = g_strsplit(split_file[1], "-schema", 0);
        table = split_table[0];
        g_message("Restoring triggers for `%s`.`%s`", db ? db : database,
                  table);
        restore_data_from_file(conn, database, table, filename, TRUE, TRUE, FALSE,NULL);
      }
    }
  }

  g_strfreev(split_table);
  g_strfreev(split_file);
  g_dir_close(dir);
}

void restore_schema_post(MYSQL *conn) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);
  gchar **split_file = NULL;
  gchar *database = NULL;
  // gchar* table=NULL;

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    if (!source_db ||
        g_str_has_prefix(filename, g_strdup_printf("%s-schema-post", source_db))) {
      if (g_strrstr(filename, "-schema-post.sql")) {
        split_file = g_strsplit(filename, "-schema-post.sql", 0);
        database = split_file[0];
        // table= split_file[0]; //NULL
        g_message("Restoring routines and events for `%s`", db ? db : database);
        restore_data_from_file(conn, database, NULL, filename, TRUE, TRUE, FALSE,NULL);
      }
    }
  }

  g_strfreev(split_file);
  g_dir_close(dir);
}

void create_database(MYSQL *conn, gchar *database) {

  gchar *query = NULL;

  const gchar *filename =
      g_strdup_printf("%s-schema-create.sql", source_db ? source_db : database);
  const gchar *filenamegz =
      g_strdup_printf("%s-schema-create.sql.gz", source_db ? source_db : database);
  const gchar *filepath = g_strdup_printf("%s/%s-schema-create.sql",
                                          directory, source_db ? source_db : database);
  const gchar *filepathgz = g_strdup_printf("%s/%s-schema-create.sql.gz",
                                            directory, source_db ? source_db : database);

  if (g_file_test(filepath, G_FILE_TEST_EXISTS)) {
    restore_data_from_file(conn, database, NULL, filename, TRUE, FALSE, FALSE,NULL);
  } else if (g_file_test(filepathgz, G_FILE_TEST_EXISTS)) {
    restore_data_from_file(conn, database, NULL, filenamegz, TRUE, FALSE, FALSE,NULL);
  } else {
    query = g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
    mysql_query(conn, query);
  }

  g_free(query);
  return;
}

void add_schema(const gchar *filename, GAsyncQueue *fast_index_creation_queue, MYSQL *conn) {
  // 0 is database, 1 is table with -schema on the end
  gchar **split_file = g_strsplit(filename, ".", 0);
  gchar *database = split_file[0];
  // Remove the -schema from the table name
  gchar **split_table = g_strsplit(split_file[1], "-schema", 0);
  gchar *table = split_table[0];

  gchar *query =
      g_strdup_printf("SHOW CREATE DATABASE `%s`", db ? db : database);
  if (mysql_query(conn, query)) {
    g_message("Creating database `%s`", db ? db : database);
    create_database(conn, database);
  } else {
    MYSQL_RES *result = mysql_store_result(conn);
    // In drizzle the query succeeds with no rows
    my_ulonglong row_count = mysql_num_rows(result);
    mysql_free_result(result);
    if (row_count == 0) {
      create_database(conn, database);
    }
  }
  int truncate_or_delete_failed=0;
  if (overwrite_tables) {
    if (purge_mode == DROP) {	  
      g_message("Dropping table or view (if exists) `%s`.`%s`",
                db ? db : database, table);
      query = g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`",
                              db ? db : database, table);
      mysql_query(conn, query);
      query = g_strdup_printf("DROP VIEW IF EXISTS `%s`.`%s`", db ? db : database,
                              table);
      mysql_query(conn, query);
    } else if (purge_mode == TRUNCATE) {
      g_message("Truncating table `%s`.`%s`", db ? db : database, table);
      query= g_strdup_printf("TRUNCATE TABLE `%s`.`%s`", db ? db : database, table);
      truncate_or_delete_failed= mysql_query(conn, query);
      if (truncate_or_delete_failed)
        g_warning("Truncate failed, we are going to try to create table or view");
    } else if (purge_mode == DELETE) {
      g_message("Deleting content of table `%s`.`%s`", db ? db : database, table);
      query= g_strdup_printf("DELETE FROM `%s`.`%s`", db ? db : database, table);
      truncate_or_delete_failed= mysql_query(conn, query);
      if (truncate_or_delete_failed)
        g_warning("Delete failed, we are going to try to create table or view"); 
    }
  }

  g_free(query);

  if ((purge_mode == TRUNCATE || purge_mode == DELETE) && !truncate_or_delete_failed){
      g_message("Skipping table creation `%s`.`%s`", db ? db : database, table);
  }else{
    g_message("Creating table `%s`.`%s`", db ? db : database, table);
    restore_data_from_file(conn, database, table, filename, TRUE, TRUE, TRUE, fast_index_creation_queue);
  }
  g_strfreev(split_table);
  g_strfreev(split_file);
  return;
}

void add_table(const gchar *filename, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct restore_job *rj = g_new(struct restore_job, 1);
  j->job_data = (void *)rj;
  rj->filename = g_strdup(filename);
  j->type = JOB_RESTORE_FILENAME;
  gchar **split_file = g_strsplit(filename, ".", 0);
  rj->database = g_strdup(split_file[0]);
  rj->table = g_strdup(split_file[1]);
  rj->part = g_ascii_strtoull(split_file[2], NULL, 10);

  g_async_queue_push(conf->queue, j);
  return;
}

void checksum_table_filename(const gchar *filename, MYSQL *conn) {
  // 0 is database, 1 is table
  gchar **split_file = g_strsplit(filename, ".", 0);
  gchar *database = split_file[0];
  gchar *table = split_file[1];
  void *infile;
  char checksum[256];
  int errn=0;
  char * row=checksum_table(conn, db ? db : database, table, &errn);
  gboolean is_compressed = FALSE; 
  gchar *path = g_build_filename(directory, filename, NULL);

  if (!g_str_has_suffix(path, ".gz")) {
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
      g_warning("Checksum mismatch found for `%s`.`%s`. Got '%s', expecting '%s'", db ? db : database, table, row, checksum);
      errors++;
    }
    else {
      g_message("Checksum confirmed for `%s`.`%s`", db ? db : database, table);
    }
  } else {
    g_critical("error reading file %s (%d)", filename, errno);
    errors++;
    return;
  }
}

void *process_queue(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_mutex);
  MYSQL *thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);

  configure_connection(thrconn, "myloader");

  if (!mysql_real_connect(thrconn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Failed to connect to MySQL server: %s", mysql_error(thrconn));
    exit(EXIT_FAILURE);
  }

  if (mysql_query(thrconn, "SET SESSION wait_timeout = 2147483")) {
    g_warning("Failed to increase wait_timeout: %s", mysql_error(thrconn));
  }

  if (!enable_binlog)
    mysql_query(thrconn, "SET SQL_LOG_BIN=0");

  mysql_query(thrconn, set_names_str);
  mysql_query(thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
  mysql_query(thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
  mysql_query(thrconn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
  if (commit_count > 1)
    mysql_query(thrconn, "SET autocommit=0");

  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  struct job *job = NULL;
  struct restore_job *rj = NULL;
  for (;;) {
    job = (struct job *)g_async_queue_pop(conf->queue);

    switch (job->type) {
    case JOB_RESTORE_STRING:
      rj = (struct restore_job *)job->job_data;
      g_message("Thread %d restoring indexes `%s`.`%s`", td->thread_id,
                rj->database, rj->table);
      guint query_counter=0;
      //restore_data_in_gstring(thrconn, rj->database, rj->table, rj->prestatement, NULL, FALSE, &query_counter);
      restore_data_in_gstring(thrconn, rj->database, rj->table, rj->statement, NULL, FALSE, &query_counter);
      if (rj->database)
        g_free(rj->database);
      if (rj->table)
        g_free(rj->table);
      if (rj->statement)
        g_string_free(rj->statement,TRUE);
      g_free(rj);
      g_free(job);
      break;
    case JOB_RESTORE_FILENAME:
      rj = (struct restore_job *)job->job_data;
      g_mutex_lock(progress_mutex);
      progress++;
      g_message("Thread %d restoring `%s`.`%s` part %d. Progress %llu of %llu .", td->thread_id,
                rj->database, rj->table, rj->part, progress,total_data_sql_files);
      g_mutex_unlock(progress_mutex);
      restore_data_from_file(thrconn, rj->database, rj->table, rj->filename, FALSE, TRUE, FALSE,NULL);
      if (rj->database)
        g_free(rj->database);
      if (rj->table)
        g_free(rj->table);
      if (rj->filename)
        g_free(rj->filename);
      g_free(rj);
      g_free(job);
      break;
    case JOB_WAIT:
      g_async_queue_push(conf->ready, GINT_TO_POINTER(1));
      break;
    case JOB_SHUTDOWN:
      g_message("Thread %d shutting down", td->thread_id);
      if (thrconn)
        mysql_close(thrconn);
      g_free(job);
      mysql_thread_end();
      return NULL;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
  }
  if (thrconn)
    mysql_close(thrconn);
  mysql_thread_end();
  return NULL;
}


void restore_data_in_gstring_from_file(MYSQL *conn, char *database, char *table, GString *data, const char *filename, gboolean is_schema, guint *query_counter)
{
                if (mysql_real_query(conn, data->str, data->len)) {

                        g_critical("Error restoring %s.%s from file %s: %s \n%s", db ? db : database, table, filename, mysql_error(conn),data->str);
                        errors++;
                        return;
                }
                *query_counter=*query_counter+1;
                if (!is_schema && (commit_count > 1) &&(*query_counter == commit_count)) {
                        *query_counter= 0;
                        if (mysql_query(conn, "COMMIT")) {
                                g_critical("Error committing data for %s.%s: %s", db ? db : database, table, mysql_error(conn));
                                errors++;
                                return;
                        }
                        mysql_query(conn, "START TRANSACTION");
                }
                g_string_set_size(data, 0);
}

void restore_data_in_gstring(MYSQL *conn, char *database, char *table, GString *data, const char *filename, gboolean is_schema, guint *query_counter)
{
  gchar** line=g_strsplit(data->str, ";\n", -1);
  int i=0;	
  for (i=0; i < (int)g_strv_length(line);i++){
     if (strlen(line[i])>2){
       GString *str=g_string_new(line[i]);
       g_string_append_c(str,';');
       restore_data_in_gstring_from_file(conn, database, table, str, filename, is_schema, query_counter);
     }
  }
}


void append_alter_table(GString * alter_table_statement, char *database, char *table){
  g_string_append(alter_table_statement,"ALTER TABLE `");
  g_string_append(alter_table_statement,db ? db : database);
  g_string_append(alter_table_statement,"`.`");
  g_string_append(alter_table_statement,table);
  g_string_append(alter_table_statement,"` ");
}

void finish_alter_table(GString * alter_table_statement){
  gchar * str=g_strrstr_len(alter_table_statement->str,alter_table_statement->len,",");
  if ((str - alter_table_statement->str) > (long int)(alter_table_statement->len - 5)){
    *str=';';
    g_string_append_c(alter_table_statement,'\n');
  }else
    g_string_append(alter_table_statement,";\n");
}


void restore_data_from_file(MYSQL *conn, char *database, char *table,
                  const char *filename, gboolean is_schema, gboolean need_use, 
		  gboolean is_create_table, GAsyncQueue *fast_index_creation_queue) {
  void *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  guint query_counter = 0;
  GString *data = g_string_sized_new(512);

  gchar *path = g_build_filename(directory, filename, NULL);

  if (!g_str_has_suffix(path, ".gz")) {
    infile = g_fopen(path, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(path, "r");
    is_compressed = TRUE;
  }

  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return;
  }

  if (need_use) {
    gchar *query = g_strdup_printf("USE `%s`", db ? db : database);

    if (mysql_query(conn, query)) {
      g_critical("Error switching to database %s whilst restoring table %s from file %s",
                 db ? db : database, table, filename);
      g_free(query);
      errors++;
      return;
    }

    g_free(query);
  }

  if (!is_schema && (commit_count > 1) )
    mysql_query(conn, "START TRANSACTION");
  GString *alter_table_statement=g_string_sized_new(512);
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (is_create_table){
          if (innodb_optimize_keys){

            // Check if it is a /*!40  SET 
	    if (g_strrstr(data->str,"/*!40")){
              g_string_append(alter_table_statement,data->str);
	      restore_data_in_gstring_from_file(conn, database, table, data, filename, is_schema, &query_counter);
	    }else{
              // Processing CREATE TABLE statement
              gboolean is_innodb_table=FALSE;
              gchar** split_file= g_strsplit(data->str, "\n", -1);
              gchar *autoinc_column=NULL;
              GString *table_without_indexes=g_string_sized_new(512);
              append_alter_table(alter_table_statement,db ? db : database,table);
              int fulltext_counter=0;
              int i=0;
              for (i=0; i < (int)g_strv_length(split_file);i++){
                if ( g_strstr_len(split_file[i],5,"  KEY")
                  || g_strstr_len(split_file[i],8,"  UNIQUE")
                  || g_strstr_len(split_file[i],9,"  SPATIAL")
                  || g_strstr_len(split_file[i],10,"  FULLTEXT")
                  || g_strstr_len(split_file[i],7,"  INDEX")
                  || g_strstr_len(split_file[i],12,"  CONSTRAINT")
                 ){
                // Ignore if the first column of the index is the AUTO_INCREMENT column
                  if (!g_strstr_len(split_file[i],12,"  CONSTRAINT") && (autoinc_column != NULL) && (g_strrstr(split_file[i],autoinc_column))){
                    g_string_append(table_without_indexes, split_file[i]);
                    g_string_append_c(table_without_indexes,'\n');
                  }else{
                    if (g_strrstr(split_file[i],"  FULLTEXT")){
                      fulltext_counter++;
                    }
                    if (fulltext_counter>1){
                      fulltext_counter=1;
                      finish_alter_table(alter_table_statement);
                      append_alter_table(alter_table_statement,db ? db : database,table);
                    }
                    g_string_append(alter_table_statement,"\n ADD");
                    g_string_append(alter_table_statement, split_file[i]);
                  }
                }else{
                  if (g_strrstr(split_file[i],"AUTO_INCREMENT")){
                    gchar** autoinc_split=g_strsplit(split_file[i],"`",3);
                    autoinc_column=g_strdup_printf("(`%s`", autoinc_split[1]);
                  }
                  g_string_append(table_without_indexes, split_file[i]);
                  g_string_append_c(table_without_indexes,'\n');
                }
                if (g_strrstr(split_file[i],"ENGINE=InnoDB")){
                  is_innodb_table=TRUE;
	        }
              }
              finish_alter_table(alter_table_statement);
              if (is_innodb_table){
                g_message("Fast index creation will be use for table: %s.%s",db ? db : database,table);
                restore_data_in_gstring_from_file(conn, database, table, g_string_new(g_strjoinv("\n)",g_strsplit(table_without_indexes->str,",\n)",-1))) , filename, is_schema, &query_counter);
                struct restore_job *rj = g_new(struct restore_job, 1);
                rj->statement = alter_table_statement;
                rj->database = g_strdup(database);
                rj->table = g_strdup(table);
                struct job *j = g_new0(struct job, 1);
                j->job_data = (void *)rj;
                j->type = JOB_RESTORE_STRING;
                g_async_queue_push(fast_index_creation_queue, j);
                g_string_set_size(data, 0);
              }else{
                restore_data_in_gstring_from_file(conn, database, table, data, filename, is_schema, &query_counter);
              }
            }
	  }else{
            restore_data_in_gstring_from_file(conn, database, table, data, filename, is_schema, &query_counter);
	  }
        }else{
          restore_data_in_gstring_from_file(conn, database, table, data, filename, is_schema, &query_counter);
        }
      }
    } else {
      g_critical("error reading file %s (%d)", filename, errno);
      errors++;
      return;
    }
  }
  if (!is_schema && (commit_count > 1) && mysql_query(conn, "COMMIT")) {
    g_critical("Error committing data for %s.%s from file %s: %s",
               db ? db : database, table, filename, mysql_error(conn));
    errors++;
  }
  g_string_free(data, TRUE);
  g_free(path);
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
  return;
}

gboolean read_data(FILE *file, gboolean is_compressed, GString *data,
                   gboolean *eof) {
  char buffer[256];

  do {
    if (!is_compressed) {
      if (fgets(buffer, 256, file) == NULL) {
        if (feof(file)) {
          *eof = TRUE;
          buffer[0] = '\0';
        } else {
          return FALSE;
        }
      }
    } else {
      if (!gzgets((gzFile)file, buffer, 256)) {
        if (gzeof((gzFile)file)) {
          *eof = TRUE;
          buffer[0] = '\0';
        } else {
          return FALSE;
        }
      }
    }
    g_string_append(data, buffer);
  } while ((buffer[strlen(buffer)] != '\0') && *eof == FALSE);

  return TRUE;
}
