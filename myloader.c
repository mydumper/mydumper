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
#include "config.h"
#include "common.h"
#include "myloader.h"
#include "connection.h"
#include "getPassword.h"
#include "logging.h"
#include "set_verbose.h"
#include "locale.h"
#include "server_detect.h"

guint commit_count = 1000;
gchar *input_directory = NULL;
gchar *directory = NULL;
gboolean overwrite_tables = FALSE;
gboolean innodb_optimize_keys = FALSE;
gboolean enable_binlog = FALSE;
gboolean disable_redo_log = FALSE;
guint rows = 0;
gchar *source_db = NULL;
gchar *purge_mode_str=NULL;
gchar *set_names_str=NULL;
enum purge_mode purge_mode;
static GMutex *init_mutex = NULL;
static GMutex *progress_mutex = NULL;
guint errors = 0;
guint max_threads_per_table=4;
unsigned long long int total_data_sql_files = 0;
unsigned long long int progress = 0;
GHashTable *db_hash=NULL;
static GMutex *db_hash_mutex = NULL;
GHashTable *tbl_hash=NULL;

const char DIRECTORY[] = "import";

gboolean read_data(FILE *file, gboolean is_compressed, GString *data,
                   gboolean *eof);
int restore_data_from_file(struct thread_data *td, char *database, char *table,
                  const char *filename, gboolean is_schema);
int restore_data_in_gstring_by_statement(struct thread_data *td, 
		  GString *data, gboolean is_schema, 
		  guint *query_counter);
int restore_data_in_gstring(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter);
void *process_queue(struct thread_data *td);
void checksum_table_filename(const gchar *filename, MYSQL *conn);
void load_directory_information(struct configuration *conf);
void checksum_databases(struct thread_data *td);
void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data);
void create_database(struct thread_data *td, gchar *database);
gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash);
gint compare_filename_part (gconstpointer a, gconstpointer b);
void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table);
void append_alter_table(GString * alter_table_statement, char *database, char *table);
void finish_alter_table(GString * alter_table_statement);
guint execute_use(struct thread_data *td, const gchar *msg);
int overwrite_table(MYSQL *conn,gchar * database, gchar * table);
gchar * get_database_name_from_content(const gchar *filename);
void process_restore_job(struct thread_data *td, struct restore_job *rj, int count);
gboolean process_job(struct thread_data *td, struct job *job, int count);
void *process_stream(struct configuration *conf);
GAsyncQueue *get_queue_for_type(struct configuration *conf, enum file_type current_ft);
void execute_use_if_needs_to(struct thread_data *td, gchar *database, const gchar * msg);

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
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


int split_and_restore_data_in_gstring_by_statement(struct thread_data *td,
                  GString *data, gboolean is_schema, guint *query_counter)
{
  char *next_line=g_strstr_len(data->str,-1,"\n");
  char *insert_statement=g_strndup(data->str, next_line - data->str);
  int r=0;
  gchar *current_line=next_line+1;
  next_line=g_strstr_len(current_line, -1, "\n");
  GString * new_insert=g_string_sized_new(strlen(insert_statement));
  do {
    guint current_rows=0;
    new_insert=g_string_append(new_insert,insert_statement);
    do {
      char *line=g_strndup(current_line, next_line - current_line);
      g_string_append(new_insert, line);
      g_free(line);
      current_rows++;
      current_line=next_line+1;
      next_line=g_strstr_len(current_line, -1, "\n");
    } while (current_rows < rows && next_line != NULL);
    new_insert->str[new_insert->len - 1]=';';
    r+=restore_data_in_gstring_by_statement(td, new_insert, is_schema, query_counter);
  } while (next_line != NULL);
  g_string_free(new_insert,TRUE);
  g_string_set_size(data, 0);
  return r;
}

struct job * new_job (enum job_type type, void *job_data, char *use_database) {
  struct job *j = g_new0(struct job, 1);
  j->type = type;
  j->use_database=use_database;
  switch (type){
    case JOB_WAIT:
      j->job_data = (GAsyncQueue *)job_data;
    case JOB_SHUTDOWN:
      break;
    default:
      j->job_data = (struct restore_job *)job_data;
  }
  return j;
}
char * db_hash_lookup(gchar *database){
  char *r=NULL;
  g_mutex_lock(db_hash_mutex);
  r=db?db:g_hash_table_lookup(db_hash,database);
  g_mutex_unlock(db_hash_mutex);
  return r;
}
void db_hash_insert(gchar *k, gchar *v){
  g_mutex_lock(db_hash_mutex);
  g_hash_table_insert(db_hash, k, v);
  g_mutex_unlock(db_hash_mutex);
}

struct db_table* append_new_db_table(char * filename, gchar * database, gchar *table, guint64 number_rows, GHashTable *table_hash, GString *alter_table_statement){
  if ( database == NULL || table == NULL){
    g_critical("It was not possible to process file: %s",filename);
    exit(EXIT_FAILURE);
  }
  char *real_database=db_hash_lookup(database);
  if (real_database == NULL){
    g_critical("It was not possible to process file: %s",filename);
    exit(EXIT_FAILURE);
  }
  gchar *lkey=g_strdup_printf("%s_%s",database, table);
  struct db_table * dbt=g_hash_table_lookup(table_hash,lkey);
  g_free(lkey);
  if (dbt == NULL){
    dbt=g_new(struct db_table,1);
    dbt->filename=filename;
    dbt->database=database;
    // This should be the only place where we should use `db ? db : `
    dbt->real_database = db ? db : real_database;
    dbt->table=g_strdup(table);
    dbt->real_table=dbt->table;
    dbt->rows=number_rows;
    dbt->restore_job_list = NULL;
    dbt->queue=g_async_queue_new();
    dbt->current_threads=0;
    dbt->max_threads=max_threads_per_table;
    dbt->mutex=g_mutex_new();
    dbt->indexes=alter_table_statement;
    dbt->start_time=NULL;
    dbt->start_index_time=NULL;
    dbt->finish_time=NULL;
    dbt->schema_created=FALSE;
    g_hash_table_insert(table_hash, g_strdup_printf("%s_%s",dbt->database,dbt->table),dbt);
  }else{
    if (number_rows>0) dbt->rows=number_rows;
    if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
//    if (real_table != NULL) dbt->real_table=g_strdup(real_table);
  }
  return dbt;
}

struct restore_job * new_restore_job( char * filename, char * database, struct db_table * dbt, GString * statement, guint part, enum restore_job_type type, const char *object){
  struct restore_job *rj = g_new(struct restore_job, 1);
  rj->type=type;
  rj->filename= filename;
  rj->database= database;
  rj->statement = statement;
  rj->dbt = dbt;
  rj->part = part;
  rj->object = object;
  return rj;
}

void free_restore_job(struct restore_job * rj){
  // We consider that
  if (rj->filename != NULL ) g_free(rj->filename);
  if (rj->statement != NULL ) g_string_free(rj->statement,TRUE);
  if (rj != NULL ) g_free(rj);
}

void sync_threads_on_queue(GAsyncQueue *ready_queue,GAsyncQueue *comm_queue,const gchar *msg){
  guint n;
  GAsyncQueue * queue = g_async_queue_new();
  for (n = 0; n < num_threads; n++){
    g_async_queue_push(comm_queue, new_job(JOB_WAIT, queue, NULL));
  }
  for (n = 0; n < num_threads; n++)
    g_async_queue_pop(ready_queue);
  g_debug("%s",msg);
  for (n = 0; n < num_threads; n++)
    g_async_queue_push(queue, GINT_TO_POINTER(1));
}

void sync_threads(struct configuration * conf){
  sync_threads_on_queue(conf->ready, conf->data_queue,"Syncing");
}

gchar * print_time(GTimeSpan timespan){
  GTimeSpan days=timespan/G_TIME_SPAN_DAY;
  GTimeSpan hours=(timespan-(days*G_TIME_SPAN_DAY))/G_TIME_SPAN_HOUR;
  GTimeSpan minutes=(timespan-(days*G_TIME_SPAN_HOUR)-(hours*G_TIME_SPAN_HOUR))/G_TIME_SPAN_MINUTE;
  GTimeSpan seconds=(timespan-(days*G_TIME_SPAN_MINUTE)-(hours*G_TIME_SPAN_HOUR)-(minutes*G_TIME_SPAN_MINUTE))/G_TIME_SPAN_SECOND;
  return g_strdup_printf("%ld %02ld:%02ld:%02ld",days,hours,minutes,seconds);
}

void restore_from_directory(struct configuration *conf){
  // Leaving just on thread to execute the add constraints as it might cause deadlocks
  guint n=0;
  for (n = 0; n < num_threads-1; n++) {
    g_async_queue_push(conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  load_directory_information(conf);
  sync_threads_on_queue(conf->ready,conf->database_queue,"Step 1 completed, Databases created");
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->database_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  sync_threads_on_queue(conf->ready,conf->table_queue,"Step 2 completed, Tables created");
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  // We need to sync all the threads before continue
  sync_threads_on_queue(conf->ready,conf->data_queue,"Step 3 completed, load data finished");
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->data_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  g_async_queue_push(conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  g_debug("Step 4 completed");

  GList * t=conf->table_list;
  g_message("Import timings:");
  g_message("Data      \t| Index    \t| Total   \t| Table");
  while (t != NULL){
    struct db_table * dbt=t->data;
    GTimeSpan diff1=g_date_time_difference(dbt->start_index_time,dbt->start_time);
    GTimeSpan diff2=g_date_time_difference(dbt->finish_time,dbt->start_index_time);
    g_message("%s\t| %s\t| %s\t| `%s`.`%s`",print_time(diff1),print_time(diff2),print_time(diff1+diff2),dbt->real_database,dbt->real_table);
    t=t->next;
  }
  innodb_optimize_keys=FALSE;

  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf->post_table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  // Step 5: Create remaining objects. 
  // TODO: is it possible to do it in parallel? Actually, why aren't we queuing this files?
  g_debug("Step 5 started");

}


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


int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};

  GError *error = NULL;
  GOptionContext *context;

  setlocale(LC_ALL, "");
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
  
  if (!input_directory) {
    if (stream){
      GDateTime * datetime = g_date_time_new_now_local();
      char *datetimestr;
      datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");
      directory = g_strdup_printf("%s-%s", DIRECTORY, datetimestr);
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
    directory=input_directory;
    if (!stream){
      char *p = g_strdup_printf("%s/metadata", directory);
      if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
        g_critical("the specified directory is not a mydumper backup\n");
        exit(EXIT_FAILURE);
      }
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

  set_session = g_string_new(NULL);
  detected_server = detect_server(conn);
  GHashTable * set_session_hash = initialize_hash_of_session_variables();
  if (defaults_file)
    load_hash_from_key_file(set_session_hash, defaults_file, "myloader_variables");
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
  conf.stream_queue = g_async_queue_new();
  db_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  db_hash_mutex=g_mutex_new();
  tbl_hash=g_hash_table_new ( g_str_hash, g_str_equal );

  struct thread_data t;
  t.thread_id = 0;
  t.conf = &conf;
  t.thrconn = conn;
  t.current_database=NULL;

  // Create database before the thread, to allow connection
  if (db){
    db_hash_insert(db, db);
    create_database(&t, db);
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
    GThread *stream_thread = g_thread_create((GThreadFunc)process_stream, &conf, TRUE, NULL);
    g_thread_join(stream_thread);
  }else{
    restore_from_directory(&conf);
  }

  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  g_async_queue_unref(conf.ready);
  if (disable_redo_log)
    mysql_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG");

  g_async_queue_unref(conf.data_queue);

  checksum_databases(&t);

  if (stream && no_delete == FALSE && input_directory == NULL){
    // remove metadata files
    GList *e=conf.metadata_list;
    gchar *path = NULL;
    while (e) {
      path = g_build_filename(directory, e->data, NULL);
      remove(path);
      g_free(path);
      e=e->next;
    }
    path = g_build_filename(directory, "metadata", NULL);
    remove(path);
    g_free(path);
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

int process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, struct db_table *dbt){
  int flag=0;
  gchar** split_file= g_strsplit(statement, "\n", -1);
  gchar *autoinc_column=NULL;
  append_alter_table(alter_table_statement, dbt->real_database,dbt->real_table);
  append_alter_table(alter_table_constraint_statement, dbt->real_database,dbt->real_table);
  int fulltext_counter=0;
  int i=0;
  for (i=0; i < (int)g_strv_length(split_file);i++){
    if ( g_strstr_len(split_file[i],5,"  KEY")
      || g_strstr_len(split_file[i],8,"  UNIQUE")
      || g_strstr_len(split_file[i],9,"  SPATIAL")
      || g_strstr_len(split_file[i],10,"  FULLTEXT")
      || g_strstr_len(split_file[i],7,"  INDEX")
      ){
      // Ignore if the first column of the index is the AUTO_INCREMENT column
      if ((autoinc_column != NULL) && (g_strrstr(split_file[i],autoinc_column))){
        g_string_append(create_table_statement, split_file[i]);
        g_string_append_c(create_table_statement,'\n');
      }else{
        flag+=IS_ALTER_TABLE_PRESENT;
        if (g_strrstr(split_file[i],"  FULLTEXT")) fulltext_counter++;
        if (fulltext_counter>1){
          fulltext_counter=1;
          finish_alter_table(alter_table_statement);
          append_alter_table(alter_table_statement,dbt->real_database,dbt->real_table);
        }
        g_string_append(alter_table_statement,"\n ADD");
        g_string_append(alter_table_statement, split_file[i]);
      }
    }else{
      if (g_strstr_len(split_file[i],12,"  CONSTRAINT")){
        flag+=INCLUDE_CONSTRAINT;
        g_string_append(alter_table_constraint_statement,"\n ADD");
        g_string_append(alter_table_constraint_statement, split_file[i]);
      }else{
        if (g_strrstr(split_file[i],"AUTO_INCREMENT")){
          gchar** autoinc_split=g_strsplit(split_file[i],"`",3);
          autoinc_column=g_strdup_printf("(`%s`", autoinc_split[1]);
        }
        g_string_append(create_table_statement, split_file[i]);
        g_string_append_c(create_table_statement,'\n');
      }
    }
    if (g_strrstr(split_file[i],"ENGINE=InnoDB")) flag+=IS_INNODB_TABLE;
  }
  return flag;
}



void load_schema(struct configuration *conf, struct db_table *dbt, const gchar *filename){
//  g_message("Loading schema for file: %s", filename);
  void *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  GString *create_table_statement=g_string_sized_new(512);
  GString *alter_table_statement=g_string_sized_new(512);
  GString *alter_table_constraint_statement=g_string_sized_new(512);
  if (!g_str_has_suffix(filename, compress_extension)) {
    infile = g_fopen(filename, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(filename, "r");
    is_compressed = TRUE;
  }
  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return;
  }
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_strrstr(data->str,"CREATE ")){
          gchar** create_table= g_strsplit(data->str, "`", 3);
	        dbt->real_table=g_strdup(create_table[1]);
          if ( g_str_has_prefix(dbt->table,"mydumper_")){
            g_hash_table_insert(tbl_hash, dbt->table, dbt->real_table);
          }else{
            g_hash_table_insert(tbl_hash, dbt->real_table, dbt->real_table);
          }
          g_strfreev(create_table);
        }
        if (innodb_optimize_keys){
          // Check if it is a /*!40  SET
          if (g_strrstr(data->str,"/*!40")){
            g_string_append(alter_table_statement,data->str);
            g_string_append(create_table_statement,data->str);
	        }else{
            // Processing CREATE TABLE statement
            GString *new_create_table_statement=g_string_sized_new(512);
            int flag = process_create_table_statement(data->str, new_create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt);
            if (flag & IS_INNODB_TABLE){
              if (flag & IS_ALTER_TABLE_PRESENT){
                finish_alter_table(alter_table_statement);
                g_message("Fast index creation will be use for table: %s.%s",dbt->real_database,dbt->real_table);
              }else{
                g_string_free(alter_table_statement,TRUE);
                alter_table_statement=NULL;
              }
              g_string_append(create_table_statement,g_strjoinv("\n)",g_strsplit(new_create_table_statement->str,",\n)",-1)));
              dbt->indexes=alter_table_statement;
              if (stream){
                struct restore_job *rj = new_restore_job(dbt->filename, dbt->real_database, dbt, dbt->indexes, 0, JOB_RESTORE_STRING, "indexes");
                g_async_queue_push(conf->post_table_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
              }
              if (flag & INCLUDE_CONSTRAINT){
                struct restore_job *rj = new_restore_job(g_strdup(filename), dbt->real_database, dbt, alter_table_constraint_statement, 0, JOB_RESTORE_STRING, "constraint");
                g_async_queue_push(conf->post_table_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
                dbt->constraints=alter_table_constraint_statement;
              }else{
                 g_string_free(alter_table_constraint_statement,TRUE);
              }
              g_string_set_size(data, 0);
            }else{
              g_string_free(alter_table_statement,TRUE);
              g_string_free(alter_table_constraint_statement,TRUE);
      	      g_string_append(create_table_statement,data->str);
            }
          }
    	  }else{
          g_string_append(create_table_statement,data->str);
      	}
      	g_string_set_size(data, 0);
      }
    }
  }
  struct restore_job * rj = new_restore_job(g_strdup(filename), dbt->real_database, dbt, create_table_statement, 0,JOB_RESTORE_SCHEMA_STRING, "");
  g_async_queue_push(conf->table_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
  if (stream && no_delete == FALSE){
    g_message("Removing file: %s", filename);
    remove(filename);
  }
}

gchar * get_database_name_from_filename(const gchar *filename){
  gchar **split_file = g_strsplit(filename, "-schema-create.sql", 2);
  gchar *db_name=g_strdup(split_file[0]);
  g_strfreev(split_file);
  return db_name;
}

void get_database_table_part_name_from_filename(const gchar *filename, gchar **database, gchar **table, guint *part){
  guint l = strlen(filename)-4;
  if (g_str_has_suffix(filename, compress_extension)){
    l-=strlen(compress_extension);
  }
  gchar *f=g_strndup(filename,l);
  gchar **split_db_tbl = g_strsplit(f, ".", -1);
  g_free(f);
  if (g_strv_length(split_db_tbl)>=3){
    *database=g_strdup(split_db_tbl[0]);
    *table=g_strdup(split_db_tbl[1]);
    *part=g_ascii_strtoull(split_db_tbl[2], NULL, 10);
  }else{
    *database=NULL;
    *table=NULL;
    *part=0;
  }
  g_strfreev(split_db_tbl);
}

void get_database_table_name_from_filename(const gchar *filename, const gchar * suffix, gchar **database, gchar **table){
  gchar **split_file = g_strsplit(filename, suffix, 2);
  gchar **split_db_tbl = g_strsplit(split_file[0], ".", -1);
  g_strfreev(split_file);
  if (g_strv_length(split_db_tbl)==2){
    *database=g_strdup(split_db_tbl[0]);
    *table=g_strdup(split_db_tbl[1]);
  }else{
    *database=NULL;
    *table=NULL;
  }
  g_strfreev(split_db_tbl);
}

void process_database_filename(struct configuration *conf, char * filename, const char *object) {
  gchar *db_kname,*db_vname;
  db_vname=db_kname=get_database_name_from_filename(filename);

  if (db_kname!=NULL && g_str_has_prefix(db_kname,"mydumper_")){
    db_vname=get_database_name_from_content(g_build_filename(directory,filename,NULL));
  }
  g_debug("Adding database: %s -> %s", db_kname, db ? db : db_vname);
  db_hash_insert(db_kname, db ? db : db_vname);
  if (!db){
    struct restore_job *rj = new_restore_job(g_strdup(filename), db_vname, NULL, NULL, 0 , JOB_RESTORE_SCHEMA_FILENAME, object);
    g_async_queue_push(conf->database_queue, new_job(JOB_RESTORE,rj,NULL));
  }
}

void process_table_filename(struct configuration *conf, GHashTable *table_hash, char * filename){
  gchar *db_name, *table_name;
  struct db_table *dbt=NULL;
  get_database_table_name_from_filename(filename,"-schema.sql",&db_name,&table_name);
  if (db_name == NULL || table_name == NULL){
      g_critical("It was not possible to process file: %s (1)",filename);
      exit(EXIT_FAILURE);
  }
  char *real_db_name=db_hash_lookup(db_name);
  if (real_db_name==NULL){
    g_critical("It was not possible to process file: %s (2) because real_db_name isn't found",filename);
    exit(EXIT_FAILURE);
  }
  dbt=append_new_db_table(filename, db_name, table_name,0,table_hash,NULL);
  load_schema(conf, dbt,g_build_filename(directory,filename,NULL));
}

void process_data_filename(struct configuration *conf, GHashTable *table_hash, char * filename){
  gchar *db_name, *table_name;
  total_data_sql_files++;
  // TODO: check if it is a data file
  // TODO: we need to count sections of the data file to determine if it is ok.
  guint part;
  get_database_table_part_name_from_filename(filename,&db_name,&table_name,&part);
  if (db_name == NULL || table_name == NULL){
    g_critical("It was not possible to process file: %s (3)",filename);
    exit(EXIT_FAILURE);
  }
  char *real_db_name=db_hash_lookup(db_name);
  struct db_table *dbt=append_new_db_table(real_db_name,db_name, table_name,0,table_hash,NULL);
  //struct db_table *dbt=append_new_db_table(filename,db_name, table_name,0,table_hash,NULL);
  struct restore_job *rj = new_restore_job(g_strdup(filename), dbt->real_database, dbt, NULL, part , JOB_RESTORE_FILENAME, "");
  // in stream mode, there is no need to sort. We can enqueue directly, where? queue maybe?.
  if (stream){
    g_async_queue_push(conf->data_queue, new_job(JOB_RESTORE ,rj,dbt->real_database));
  }else{
    dbt->restore_job_list=g_list_insert_sorted(dbt->restore_job_list,rj,&compare_filename_part);
  }
}

void process_schema_filename(struct configuration *conf, const gchar *filename, const char * object) {
    gchar *database=NULL, *table=NULL, *real_db_name=NULL;
    get_database_table_from_file(filename,"-schema",&database,&table);
    if (database == NULL){
      g_critical("Database is null on: %s",filename);
    }
    real_db_name=db_hash_lookup(database);
    struct restore_job *rj = new_restore_job(g_strdup(filename), real_db_name , NULL , NULL, 0 , JOB_RESTORE_SCHEMA_FILENAME, object);
    g_async_queue_push(conf->post_queue, new_job(JOB_RESTORE,rj,real_db_name));
}

gboolean m_filename_has_suffix(gchar const *str, gchar const *suffix){
  char *str2=g_strdup(str);
  if (g_str_has_suffix(str2, compress_extension)){
    str2[strlen(str)-strlen(compress_extension)]='\0';
  }
  gboolean b=g_str_has_suffix(str2,suffix);
  g_free(str2);
  return b;
}

enum file_type get_file_type (const char * filename){
  if (m_filename_has_suffix(filename, "-schema.sql")) {
    return SCHEMA_TABLE;
  } else if (m_filename_has_suffix(filename, "-metadata")) {
    return METADATA_TABLE;
  } else if ( strcmp(filename, "metadata") == 0 ){
    return METADATA_GLOBAL;
  } else if (m_filename_has_suffix(filename, "-checksum")) {
    return CHECKSUM;
  } else if (m_filename_has_suffix(filename, "-schema-view.sql") ){
    return SCHEMA_VIEW;
  } else if (m_filename_has_suffix(filename, "-schema-triggers.sql") ){
    return SCHEMA_TRIGGER;
  } else if (m_filename_has_suffix(filename, "-schema-post.sql") ){
    return SCHEMA_POST;
  } else if (m_filename_has_suffix(filename, "-schema-create.sql") ){
    return SCHEMA_CREATE;
  } else if (m_filename_has_suffix(filename, ".sql") ){
    return DATA;
  }else if (g_str_has_suffix(filename, ".dat")) 
    return LOAD_DATA;
  return IGNORED;
}


enum file_type process_filename(struct configuration *conf,GHashTable *table_hash, char *filename){
  enum file_type ft= get_file_type(filename);
  if (!source_db ||
    g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
    switch (ft){
      case INIT:
      case SCHEMA_CREATE:
        if (db){
          g_warning("Skipping database creation on file: %s",filename);
          if (stream && no_delete == FALSE){
            gchar *path = g_build_filename(directory, filename, NULL);
            g_message("Removing file: %s", path);
            remove(path);
            g_free(path);
          }
        }else{
          process_database_filename(conf, filename, "create database");
        }
        break;
      case SCHEMA_TABLE:
        process_table_filename(conf,table_hash,filename);
        break;
      case SCHEMA_VIEW:
        process_schema_filename(conf, filename,"view");
        break;
      case SCHEMA_TRIGGER:
        process_schema_filename(conf, filename,"trigger");
        break;
      case SCHEMA_POST:
        // can be enqueued in any order
        process_schema_filename(conf, filename,"post");
        break;
      case CHECKSUM:
        conf->checksum_list=g_list_insert(conf->checksum_list,g_strdup(filename),-1);
        break;
      case METADATA_GLOBAL:
        break;
      case METADATA_TABLE:
        // TODO: we need to process this info
        conf->metadata_list=g_list_insert(conf->metadata_list,g_strdup(filename),-1);
        break;
      case DATA:
        process_data_filename(conf,table_hash,filename);
        break;
      case IGNORED:
        g_warning("Filename %s has been ignored", filename);
        break;
      case LOAD_DATA:
        g_message("Load data file found: %s", filename);
        break;
    }
  }
  return ft;
}

void load_directory_information(struct configuration *conf) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;
  GList *create_table_list=NULL,
        *metadata_list= NULL,
        *data_files_list=NULL,
        *schema_create_list=NULL,
        *view_list=NULL,
        *trigger_list=NULL,
        *post_list=NULL;

  while ((filename = g_dir_read_name(dir))) {
    enum file_type ft= get_file_type(filename);
    if (ft == SCHEMA_POST){
          post_list=g_list_insert(post_list,g_strdup(filename),-1);
    } else if (ft ==  SCHEMA_CREATE ){
          schema_create_list=g_list_insert(schema_create_list,g_strdup(filename),-1);
          conf->schema_create_list=g_list_insert(conf->schema_create_list,g_strdup(filename),-1);
    } else if (!source_db ||
      g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))|| 
      g_str_has_prefix(filename, "mydumper_")) { 
        switch (ft){
          case INIT:
            break;
          case SCHEMA_TABLE:
            create_table_list=g_list_append(create_table_list,g_strdup(filename));
            break;
          case SCHEMA_VIEW:
            view_list=g_list_append(view_list,g_strdup(filename));
            break;
          case SCHEMA_TRIGGER:
            trigger_list=g_list_append(trigger_list,g_strdup(filename));
            break;
          case CHECKSUM:
            conf->checksum_list=g_list_append(conf->checksum_list,g_strdup(filename));
            break;
          case METADATA_GLOBAL:
            break;
          case METADATA_TABLE:
            // TODO: we need to process this info
            metadata_list=g_list_append(metadata_list,g_strdup(filename));
            break;
          case DATA:
            data_files_list=g_list_append(data_files_list,g_strdup(filename));
            break;
          case LOAD_DATA:
            g_message("Load data file found: %s", filename);
            break;            
          default:
            g_warning("File ignored: %s", filename);
            break;
        }
      }
    }
  g_dir_close(dir);

  gchar *f = NULL;
  // CREATE DATABASE
  while (schema_create_list){
    f = schema_create_list->data;
    process_database_filename(conf, f, "create database");
    schema_create_list=schema_create_list->next;
  }

  // CREATE TABLE
  GHashTable *table_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  while (create_table_list != NULL){
    f = create_table_list->data;
    process_table_filename(conf,table_hash,f);
    create_table_list=create_table_list->next;
  }

  // DATA FILES
  while (data_files_list != NULL){
    f = data_files_list->data;
    process_data_filename(conf,table_hash,f);

    data_files_list=data_files_list->next;
  }


  while (view_list != NULL){
    f = view_list->data;
    process_schema_filename(conf, f,"view");
    view_list=view_list->next;
  }

  while (trigger_list != NULL){
    f = trigger_list->data;
    process_schema_filename(conf, f, "trigger");
    trigger_list=trigger_list->next;
  }

  while (post_list != NULL){
    f = post_list->data;
    process_schema_filename(conf, f,"post");
    post_list=post_list->next;
  }
  // SORT DATA FILES TO ENQUEUE
  // iterates over the dbt to create the jobs in the dbt->queue
  // and sorts the dbt for the conf->table_list
  // in stream mode, it is not possible to sort the tables as 
  // we don't know the amount the rows, .metadata are sent at the end.
  GList * table_list=NULL;
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, table_hash );
    struct db_table *dbt=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
    table_list=g_list_insert_sorted_with_data (table_list,dbt,&compare_dbt,table_hash);
    GList *i=dbt->restore_job_list; 
    while (i) {
      g_async_queue_push(dbt->queue, new_job(JOB_RESTORE ,i->data,dbt->real_database));
      i=i->next;
    }
    dbt->count=g_async_queue_length(dbt->queue);
  }
  g_hash_table_destroy(table_hash);
  conf->table_list=table_list;
  // conf->table needs to be set.
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

void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table){
  gchar **split_filename = g_strsplit(filename, sufix, 0);
  gchar **split = g_strsplit(split_filename[0],".",0);
  guint count=g_strv_length(split);
  if (count > 2){
    g_warning("We need to get the db and table name from the create table statement");
    return;
  }
  *table=g_strdup(split[1]);
  *database=g_strdup(split[0]);
}


int overwrite_table(MYSQL *conn,gchar * database, gchar * table){
  int truncate_or_delete_failed=0;
  gchar *query=NULL;
  if (purge_mode == DROP) {
    g_message("Dropping table or view (if exists) `%s`.`%s`",
              database, table);
    query = g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`",
                            database, table);
    mysql_query(conn, query);
    query = g_strdup_printf("DROP VIEW IF EXISTS `%s`.`%s`", database,
                            table);
    mysql_query(conn, query);
  } else if (purge_mode == TRUNCATE) {
    g_message("Truncating table `%s`.`%s`", database, table);
    query= g_strdup_printf("TRUNCATE TABLE `%s`.`%s`", database, table);
    truncate_or_delete_failed= mysql_query(conn, query);
    if (truncate_or_delete_failed)
      g_warning("Truncate failed, we are going to try to create table or view");
  } else if (purge_mode == DELETE) {
    g_message("Deleting content of table `%s`.`%s`", database, table);
    query= g_strdup_printf("DELETE FROM `%s`.`%s`", database, table);
    truncate_or_delete_failed= mysql_query(conn, query);
    if (truncate_or_delete_failed)
      g_warning("Delete failed, we are going to try to create table or view");
  }
  g_free(query);
  return truncate_or_delete_failed;
}

gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash){
  gchar *a_key=g_strdup_printf("%s_%s",((struct db_table *)a)->database,((struct db_table *)a)->table);
  gchar *b_key=g_strdup_printf("%s_%s",((struct db_table *)b)->database,((struct db_table *)b)->table);
  struct db_table * a_val=g_hash_table_lookup(table_hash,a_key);
  struct db_table * b_val=g_hash_table_lookup(table_hash,b_key);
  g_free(a_key);
  g_free(b_key);
  return a_val->rows < b_val->rows;
}
gint compare_filename_part (gconstpointer a, gconstpointer b){
  return ((struct restore_job *)a)->part > ((struct restore_job *)b)->part;
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

gboolean process_job(struct thread_data *td, struct job *job, int count){
  switch (job->type) {
    case JOB_RESTORE:
      process_restore_job(td,job->job_data,count);
      break;
    case JOB_WAIT:
      g_async_queue_push(td->conf->ready, GINT_TO_POINTER(1));
      GAsyncQueue *queue=job->job_data;
      g_async_queue_pop(queue);
      break;
    case JOB_SHUTDOWN:
//      g_message("Thread %d shutting down", thread_id);
      g_free(job);
      return FALSE;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
  }
  return TRUE;
}


void process_restore_job(struct thread_data *td, struct restore_job *rj, int count){
  struct db_table *dbt=rj->dbt;
  dbt=rj->dbt;

  switch (rj->type) {
    case JOB_RESTORE_STRING:
      g_message("Thread %d restoring %s `%s`.`%s` from %s", td->thread_id, rj->object,
                dbt->real_database, dbt->real_table, rj->filename);
      guint query_counter=0;
      restore_data_in_gstring(td, rj->statement, FALSE, &query_counter);
      break;
    case JOB_RESTORE_SCHEMA_STRING:
      g_message("Thread %d restoring table `%s`.`%s` from %s", td->thread_id, 
                dbt->real_database, dbt->real_table, rj->filename);
      int truncate_or_delete_failed=0;
      if (overwrite_tables)
        truncate_or_delete_failed=overwrite_table(td->thrconn,dbt->real_database, dbt->real_table);
      if ((purge_mode == TRUNCATE || purge_mode == DELETE) && !truncate_or_delete_failed){
        g_message("Skipping table creation `%s`.`%s` from %s", dbt->real_database, dbt->real_table, rj->filename);
      }else{
        g_message("Creating table `%s`.`%s` from %s", dbt->real_database, dbt->real_table, rj->filename);
        if (restore_data_in_gstring(td, rj->statement, FALSE, &query_counter)){
          g_critical("Thread %d issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
        }
      }
      dbt->schema_created=TRUE;
      break;
    case JOB_RESTORE_FILENAME:
      g_mutex_lock(progress_mutex);
      progress++;
      g_message("Thread %d restoring `%s`.`%s` part %d of %d from %s. Progress %llu of %llu .", td->thread_id,
                dbt->real_database, dbt->real_table, rj->part, count, rj->filename, progress,total_data_sql_files);
      g_mutex_unlock(progress_mutex);
      if (stream && !dbt->schema_created){
        // In a stream scenario we might need to wait until table is created to start executing inserts.
        int i=0;
        while (!dbt->schema_created && i<100){
          usleep(1000);
          i++;
        }
        if (!dbt->schema_created){
          g_critical("Table has not been created in more than 10 seconds");
          exit(EXIT_FAILURE);
        }
      }
      if (restore_data_from_file(td, dbt->real_database, dbt->real_table, rj->filename, FALSE) > 0){
        g_critical("Thread %d issue restoring %s: %s",td->thread_id,rj->filename, mysql_error(td->thrconn));
      }
      break;
    case JOB_RESTORE_SCHEMA_FILENAME:
      g_message("Thread %d restoring %s on `%s` from %s", td->thread_id, rj->object,
                rj->database, rj->filename);
      restore_data_from_file(td, rj->database, NULL, rj->filename, TRUE );
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
  free_restore_job(rj);
}

void *process_stream_queue(struct thread_data * td) {
  struct job *job = NULL;
  int count =0;
  gboolean cont=TRUE;

  enum file_type ft;
  while (cont){

    ft = (enum file_type )g_async_queue_pop(td->conf->stream_queue);
//    g_message("Thread %d: Processing file type: %d",td->thread_id,ft);
    GAsyncQueue *q= get_queue_for_type(td->conf,ft);
    if (q != NULL){
    job = (struct job *)g_async_queue_pop(q);

    execute_use_if_needs_to(td, job->use_database, "Restoring from stream");
    cont=process_job(td, job, count);
    }
  }
  return NULL;
}


void *process_directory_queue(struct thread_data * td) {
  struct db_table *dbt=NULL;
  struct job *job = NULL;
  gboolean cont=TRUE;
  int count =0;

  // Step 1: creating databases
  while (cont){
    job = (struct job *)g_async_queue_pop(td->conf->database_queue);
    cont=process_job(td, job, count);
  }
  // Step 2: Create tables
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(td->conf->table_queue);
    execute_use_if_needs_to(td, job->use_database, "Restoring tables");
    cont=process_job(td, job, count);
  }

  // Is this correct in a streaming scenario ?
  GList *table_list=td->conf->table_list;
  if (table_list == NULL ) {
    dbt=NULL;
  }else{
    dbt=table_list->data;
    g_mutex_lock(dbt->mutex);
    dbt->current_threads++;
    if (dbt->start_time==NULL)
      dbt->start_time=g_date_time_new_now_local();
    g_mutex_unlock(dbt->mutex);
  }


  // Step 3: Load data
  cont=TRUE;
  while (cont){
    if (dbt != NULL){
      g_mutex_lock(dbt->mutex);
      if (dbt->current_threads > dbt->max_threads){
        dbt->current_threads--;
        g_mutex_unlock(dbt->mutex);
        table_list=table_list->next;
        if (table_list == NULL ){
          dbt=NULL;
          continue;
        }
        dbt=table_list->data;
        g_mutex_lock(dbt->mutex);
        count=dbt->count;
        if (dbt->start_time==NULL) dbt->start_time=g_date_time_new_now_local();
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
      g_mutex_unlock(dbt->mutex);

      job = (struct job *)g_async_queue_try_pop(dbt->queue);

      if (job == NULL){
        g_mutex_lock(dbt->mutex);
        dbt->current_threads--;
        if (dbt->current_threads == 0){
          dbt->current_threads--;
          dbt->start_index_time=g_date_time_new_now_local();
          g_mutex_unlock(dbt->mutex);
          if (dbt->indexes != NULL) {
            g_message("Thread %d restoring indexes `%s`.`%s`", td->thread_id,
                  dbt->real_database, dbt->real_table);
            guint query_counter=0;
            restore_data_in_gstring(td, dbt->indexes, FALSE, &query_counter);
          }
          dbt->finish_time=g_date_time_new_now_local();
        }else{
          g_mutex_unlock(dbt->mutex);
        }
        guint max=dbt->max_threads;
        table_list=table_list->next;
        if (table_list == NULL ){
          dbt=NULL;
          continue;
        }
        dbt=table_list->data;
        g_mutex_lock(dbt->mutex);
        if (dbt->start_time==NULL) dbt->start_time=g_date_time_new_now_local();
        dbt->max_threads = max;
        dbt->current_threads++;
        g_mutex_unlock(dbt->mutex);
        continue;
      }
    }else{
     job = (struct job *)g_async_queue_pop(td->conf->data_queue);
    }
    execute_use_if_needs_to(td, job->use_database, "Restoring data");
    cont=process_job(td, job, count);
  }
  return NULL;
}


void *process_queue(struct thread_data *td) {
  struct configuration *conf = td->conf;
  g_mutex_lock(init_mutex);
  td->thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);
  td->current_database=NULL;

  configure_connection(td->thrconn, "myloader");

  if (!mysql_real_connect(td->thrconn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Failed to connect to MySQL server: %s", mysql_error(td->thrconn));
    exit(EXIT_FAILURE);
  }

//  if (mysql_query(td->thrconn, "SET SESSION wait_timeout = 2147483")) {
//    g_warning("Failed to increase wait_timeout: %s", mysql_error(td->thrconn));
//  }

//  if (!enable_binlog)
//    mysql_query(td->thrconn, "SET SQL_LOG_BIN=0");

  mysql_query(td->thrconn, set_names_str);
  mysql_query(td->thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
  mysql_query(td->thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
  mysql_query(td->thrconn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
//  if (commit_count > 1)
//    mysql_query(td->thrconn, "SET autocommit=0");

  execute_gstring(td->thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  if (db){
    td->current_database=db;
    execute_use(td, "Initializing thread");
    if (stream)
      g_async_queue_push(conf->database_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  g_debug("Thread %d: Starting import", td->thread_id);
  if (stream){
    process_stream_queue(td);
  }else{
    process_directory_queue(td);
  }
  struct job *job = NULL;
  gboolean cont=TRUE;
  int count =0;

//  g_message("Thread %d: Starting post import task over table", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_table_queue);
//    g_message("%s",((struct restore_job *)job->job_data)->object);
    execute_use_if_needs_to(td, job->use_database, "Restoring post table");
    cont=process_job(td, job, count);
  }
//  g_message("Thread %d: Starting post import task: triggers, procedures and triggers", td->thread_id);
  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_queue);
    execute_use_if_needs_to(td, job->use_database, "Restoring post tasks");
    cont=process_job(td, job, count);
  }

  if (td->thrconn)
    mysql_close(td->thrconn);
  mysql_thread_end();
  g_debug("Thread %d ending", td->thread_id);
  return NULL;
}


int restore_data_in_gstring_by_statement(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter)
{
  if (mysql_real_query(td->thrconn, data->str, data->len)) {
	  //g_critical("Error restoring: %s %s", data->str, mysql_error(conn));
    errors++;
    return 1;
  }
  *query_counter=*query_counter+1;
  if (!is_schema && (commit_count > 1) &&(*query_counter == commit_count)) {
    *query_counter= 0;
    if (mysql_query(td->thrconn, "COMMIT")) {
      errors++;
      return 2;
    }
    mysql_query(td->thrconn, "START TRANSACTION");
  }
  g_string_set_size(data, 0);
  return 0;
}

int restore_data_in_gstring(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter)
{
  int i=0;
  int r=0;
  if (data != NULL && data->len > 4){
    gchar** line=g_strsplit(data->str, ";\n", -1);
    for (i=0; i < (int)g_strv_length(line);i++){
       if (strlen(line[i])>2){
         GString *str=g_string_new(line[i]);
         g_string_append_c(str,';');
         r+=restore_data_in_gstring_by_statement(td, str, is_schema, query_counter);
       }
    }
  }
  return r;
}


void append_alter_table(GString * alter_table_statement, char *database, char *table){
  g_string_append(alter_table_statement,"ALTER TABLE `");
  g_string_append(alter_table_statement, database);
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

void execute_use_if_needs_to(struct thread_data *td, gchar *database, const gchar * msg){
  if ( database != NULL && db == NULL ){
    if (td->current_database==NULL || g_strcmp0(database, td->current_database) != 0){
      td->current_database=database;
      if (execute_use(td, msg)){
        exit(EXIT_FAILURE);
      }
    }
  }
}

guint execute_use(struct thread_data *td, const gchar * msg){
  gchar *query = g_strdup_printf("USE `%s`", td->current_database);
  if (mysql_query(td->thrconn, query)) {
    g_critical("Error switching to database `%s` %s", td->current_database, msg);
    g_free(query);
    return 1;
  }
  g_free(query);
  return 0;
}

gchar * get_table_name_from_content(const gchar *filename){
  void *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  if (!g_str_has_suffix(filename, compress_extension)) {
    infile = g_fopen(filename, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(filename, "r");
    is_compressed = TRUE;
  }
  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return NULL;
  }
  gchar *real_database=NULL;
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof)) {
      if (g_str_has_prefix(data->str,"INSERT ")){
        gchar** create= g_strsplit(data->str, "`", 3);
        real_database=g_strdup(create[1]);
        g_strfreev(create);
        break;
      }
      g_string_set_size(data, 0);
    }
  }

  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
  return real_database;
}

gchar * get_database_name_from_content(const gchar *filename){
  void *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  if (!g_str_has_suffix(filename, compress_extension)) {
    infile = g_fopen(filename, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(filename, "r");
    is_compressed = TRUE;
  }
  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return NULL;
  }
  gchar *real_database=NULL;
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_str_has_prefix(data->str,"CREATE ")){
          gchar** create= g_strsplit(data->str, "`", 3);
          real_database=g_strdup(create[1]);
          g_strfreev(create);
          break;
        }
      }
    }
  }

  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
  return real_database;
}

int restore_data_from_file(struct thread_data *td, char *database, char *table,
                  const char *filename, gboolean is_schema){
  void *infile;
  int r=0;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  guint query_counter = 0;
  GString *data = g_string_sized_new(512);
  guint line=0,preline=0;
  gchar *path = g_build_filename(directory, filename, NULL);

  if (!g_str_has_suffix(path, compress_extension)) {
    infile = g_fopen(path, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(path, "r");
    is_compressed = TRUE;
  }

  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return 1;
  }
  if (!is_schema && (commit_count > 1) )
    mysql_query(td->thrconn, "START TRANSACTION");
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        preline=line;
        line+=strcount(data->str);
        if (rows > 0 && g_strrstr_len(data->str,6,"INSERT"))
          split_and_restore_data_in_gstring_by_statement(td,
            data, is_schema, &query_counter);
        else{
          guint tr=restore_data_in_gstring_by_statement(td, data, is_schema, &query_counter);
          r+=tr;
          if (tr > 0){
            g_critical("Error occours between lines: %d and %d on file %s: %s",preline,line,filename,mysql_error(td->thrconn));
          }
        }
        g_string_set_size(data, 0);
      }
    } else {
      g_critical("error reading file %s (%d)", filename, errno);
      errors++;
      return r;
    }
  }
  if (!is_schema && (commit_count > 1) && mysql_query(td->thrconn, "COMMIT")) {
    g_critical("Error committing data for %s.%s from file %s: %s",
               db ? db : database, table, filename, mysql_error(td->thrconn));
    errors++;
  }
  g_string_free(data, TRUE);
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }

  if (stream && no_delete == FALSE){
    g_message("Removing file: %s", path);
    remove(path);
  }
  g_free(path);
  return r;
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

GAsyncQueue *get_queue_for_type(struct configuration *conf, enum file_type current_ft){
  switch (current_ft){
    case INIT:
    case SCHEMA_CREATE:
      return conf->database_queue;
    case SCHEMA_TABLE:
      return conf->table_queue;
    case SCHEMA_VIEW:
    case SCHEMA_TRIGGER:
    case SCHEMA_POST:
    case CHECKSUM:
      return conf->post_queue;
    case METADATA_GLOBAL:
      return NULL;
    case METADATA_TABLE:
      return NULL;
      return conf->post_table_queue;
    case DATA:
      return conf->data_queue;
      break;
    case IGNORED:
    case LOAD_DATA:
      break;
  }
  return NULL;
}

void send_shutdown_jobs(GAsyncQueue * queue){
      guint n=0;
      for (n = 0; n < num_threads; n++) {
        g_async_queue_push(queue, new_job(JOB_SHUTDOWN,NULL,NULL));
      }
}

void process_stream_filename(struct configuration *conf,GHashTable *table_hash, gchar * filename){
  enum file_type current_ft=process_filename(conf,table_hash,filename);
  if (current_ft != SCHEMA_VIEW &&
      current_ft != SCHEMA_TRIGGER &&
      current_ft != SCHEMA_POST &&
      current_ft != CHECKSUM )
  g_async_queue_push(conf->stream_queue, GINT_TO_POINTER(current_ft));
}

void ml_write( int fd, gchar * buffer){
  long unsigned int len=0;
  len=write(fd, buffer, strlen(buffer));
  if (len != strlen(buffer)) {
    g_critical("Data written is not the same than buffer");
    exit(EXIT_FAILURE);
  }
}

gboolean has_mydumper_suffix(gchar *line){
  return g_str_has_suffix(line,".sql") || g_str_has_suffix(line,".sql.gz") || g_str_has_suffix(line,"metadata") || g_str_has_suffix(line,".checksum") || g_str_has_suffix(line,".checksum.gz") ;
}

void *process_stream(struct configuration *conf){
  char * filename=NULL,*real_filename=NULL;
  char buffer[1000000]; 
  FILE *file=NULL;
  gboolean eof=FALSE;
  GHashTable *table_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  do {
    if(fgets(buffer, 1000000, stdin) == NULL){
      if (file && feof(file)){
        eof = TRUE;
        buffer[0] = '\0';
        fclose(file);
      }else{
        break;
      }
    }else{
      if (g_str_has_prefix(buffer,"-- ")){
        gchar lastchar = buffer[strlen(buffer)-1];
        buffer[strlen(buffer)-1]='\0';
        if (has_mydumper_suffix(buffer)){
          if (file){
            fclose(file);
            process_stream_filename(conf,table_hash,filename);
          }
          real_filename = g_build_filename(directory,&(buffer[3]),NULL);
          filename = g_build_filename(&(buffer[3]),NULL);
          file = g_fopen(real_filename, "w");
        }else{
          buffer[strlen(buffer)-1]=lastchar;
          if (file) ml_write(fileno(file),buffer);
        }
      }else{
        if (file) ml_write(fileno(file),buffer);
      }
    }
  } while (eof == FALSE);
  fclose(file);
  process_stream_filename(conf,table_hash,filename);
  guint n=0;
  for (n = 0; n < num_threads *2 ; n++) {
    g_async_queue_push(conf->data_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->post_table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->post_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->stream_queue, GINT_TO_POINTER(DATA));
  }
  
  return NULL;
}
