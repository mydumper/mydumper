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
GHashTable *tbl_hash=NULL;

gboolean read_data(FILE *file, gboolean is_compressed, GString *data,
                   gboolean *eof);
int restore_data_from_file(MYSQL *conn, char *database, char *table,
                  const char *filename, gboolean is_schema, gboolean need_use); 
int restore_data_in_gstring_from_file(MYSQL *conn, 
		  GString *data, gboolean is_schema, 
		  guint *query_counter);
int restore_data_in_gstring(MYSQL *conn, GString *data, gboolean is_schema, guint *query_counter);
void *process_queue(struct thread_data *td);
void checksum_table_filename(const gchar *filename, MYSQL *conn);
void load_directory_information(struct configuration *conf, MYSQL *conn);
void checksum_databases(MYSQL *conn,struct configuration *conf);
void restore_schema_view(MYSQL *conn,struct configuration *conf);
void restore_schema_triggers(MYSQL *conn,struct configuration *conf);
void restore_schema_post(MYSQL *conn,struct configuration *conf);
void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data);
void create_database(MYSQL *conn, gchar *database);
gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash);
gint compare_filename_part (gconstpointer a, gconstpointer b);
void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table);
void append_alter_table(GString * alter_table_statement, char *database, char *table);
void finish_alter_table(GString * alter_table_statement);
guint execute_use(MYSQL *conn, gchar *database, const gchar *msg);
int overwrite_table(MYSQL *conn,gchar * database, gchar * table);
gchar * get_database_name_from_content(const gchar *filename);
void process_restore_job(MYSQL *thrconn,struct restore_job *rj, int thread_id, int count, gchar ** current_database);
gboolean process_job(struct configuration *conf, MYSQL *thrconn,struct job *job, int thread_id, int count, gchar ** current_database);

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
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows,
     "Split the INSERT statement into this many rows.", NULL},
    {"max-threads-per-table", 0, 0, G_OPTION_ARG_INT, &max_threads_per_table,
     "Maximum number of threads per table to use, default 4", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


int split_and_restore_data_in_gstring_from_file(MYSQL *conn,
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
    r+=restore_data_in_gstring_from_file(conn, new_insert, is_schema, query_counter);
  } while (next_line != NULL);
  g_string_free(new_insert,TRUE);
  g_string_set_size(data, 0);
  return r;
}

struct job * new_job (enum job_type type, void *job_data) {
  struct job *j = g_new0(struct job, 1);
  j->type = type;
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

struct db_table* append_new_db_table(gchar * real_database, gchar * database, gchar *table, guint64 number_rows, GHashTable *table_hash, GString *alter_table_statement){
  gchar *lkey=g_strdup_printf("%s_%s",database, table);
  struct db_table * dbt=g_hash_table_lookup(table_hash,lkey);
  g_free(lkey);
  if (dbt == NULL){
    dbt=g_new(struct db_table,1);
    dbt->database=database;
    dbt->real_database=db?db:real_database;
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
    g_hash_table_insert(table_hash, g_strdup_printf("%s_%s",dbt->database,dbt->table),dbt);
  }else{
    if (number_rows>0) dbt->rows=number_rows;
    if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
//    if (real_table != NULL) dbt->real_table=g_strdup(real_table);
  }
  return dbt;
}

struct restore_job * new_restore_job(char * filename, struct db_table * dbt, GString * statement, guint part, enum restore_job_type type){
  struct restore_job *rj = g_new(struct restore_job, 1);
  rj->type=type;
  rj->filename= filename;
  rj->statement = statement;
  rj->dbt = dbt;
  rj->part = part;
  return rj;
}


void sync_threads_on_queue(GAsyncQueue *ready_queue,GAsyncQueue *comm_queue,const gchar *msg){
  guint n;
  GAsyncQueue * queue = g_async_queue_new();
  for (n = 0; n < num_threads; n++){
    g_async_queue_push(comm_queue, new_job(JOB_WAIT, queue));
  }
  for (n = 0; n < num_threads; n++)
    g_async_queue_pop(ready_queue);
  g_message("%s",msg);
  for (n = 0; n < num_threads; n++)
    g_async_queue_push(queue, GINT_TO_POINTER(1));
}

void sync_threads(struct configuration * conf){
  sync_threads_on_queue(conf->ready, conf->queue,"Syncing");
}

gchar * print_time(GTimeSpan timespan){
  GTimeSpan days=timespan/G_TIME_SPAN_DAY;
  GTimeSpan hours=(timespan-(days*G_TIME_SPAN_DAY))/G_TIME_SPAN_HOUR;
  GTimeSpan minutes=(timespan-(days*G_TIME_SPAN_HOUR)-(hours*G_TIME_SPAN_HOUR))/G_TIME_SPAN_MINUTE;
  GTimeSpan seconds=(timespan-(days*G_TIME_SPAN_MINUTE)-(hours*G_TIME_SPAN_HOUR)-(minutes*G_TIME_SPAN_MINUTE))/G_TIME_SPAN_SECOND;
  return g_strdup_printf("%ld %02ld:%02ld:%02ld",days,hours,minutes,seconds);
}


int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};

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
  set_session = g_string_new(NULL);
  if (config_file != NULL){
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
  conf.pre_queue = g_async_queue_new();
  conf.queue = g_async_queue_new();
  conf.post_queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.constraints_queue = g_async_queue_new();
  db_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  tbl_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  guint n;
  // Leaving just on thread to execute the add constraints as it might cause deadlocks
  for (n = 0; n < num_threads-1; n++) {
    g_async_queue_push(conf.post_queue, new_job(JOB_SHUTDOWN,NULL));
  }

  // Step 1: Create databases | single threaded
  load_directory_information(&conf, conn);
  g_message("Step 1 completed, Databases created");
  // Step 2: Create tables | multithreaded
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

  // We need to sync all the threads before continue
  sync_threads_on_queue(conf.ready,conf.pre_queue,"Step 2 completed, tables created");

  // Contiue with next stage
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf.pre_queue, new_job(JOB_SHUTDOWN,NULL));
  }

  // Step 3: Load data | multithreaded
 
  // We need to sync all the threads before continue
  sync_threads_on_queue(conf.ready,conf.queue,"Step 3 completed, load data finished");

  // Contiue with next stage
  for (n = 0; n < num_threads; n++) {
    g_async_queue_push(conf.queue, new_job(JOB_SHUTDOWN,NULL));
  }

  // Step 4: Create constraints

  // Shutdown the latest thread
  g_async_queue_push(conf.post_queue, new_job(JOB_SHUTDOWN,NULL));

  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  g_message("Step 4 completed");

  GList * t=conf.table_list;
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

  // Step 5: Create remaining objects. TODO: is it possible to do it in parallel?
  g_message("Step 5 started");
  g_async_queue_unref(conf.ready);

  restore_schema_post(conn,&conf);

  restore_schema_view(conn,&conf);

  restore_schema_triggers(conn,&conf);

  if (disable_redo_log)
    mysql_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG");

  g_async_queue_unref(conf.queue);

  checksum_databases(conn,&conf);

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


void load_schema(struct configuration *conf, struct db_table *dbt, const gchar *filename){
  void *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  GString *create_table_statement=g_string_sized_new(512);
  GString *alter_table_statement=g_string_sized_new(512);
  GString *alter_table_constraint_statement=g_string_sized_new(512);
  if (!g_str_has_suffix(filename, ".gz")) {
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
            gboolean is_innodb_table=FALSE;
            gchar** split_file= g_strsplit(data->str, "\n", -1);
            gchar *autoinc_column=NULL;
            GString *table_without_indexes=g_string_sized_new(512);
            gboolean alter_table_present=FALSE;
            append_alter_table(alter_table_statement,db ? db : dbt->database,dbt->real_table);
            append_alter_table(alter_table_constraint_statement,db ? db : dbt->database,dbt->real_table);
            int fulltext_counter=0;
            int i=0;
            gboolean include_constraint=FALSE;
            for (i=0; i < (int)g_strv_length(split_file);i++){
              if ( g_strstr_len(split_file[i],5,"  KEY")
                || g_strstr_len(split_file[i],8,"  UNIQUE")
                || g_strstr_len(split_file[i],9,"  SPATIAL")
                || g_strstr_len(split_file[i],10,"  FULLTEXT")
                || g_strstr_len(split_file[i],7,"  INDEX")
              ){
              // Ignore if the first column of the index is the AUTO_INCREMENT column
                if ((autoinc_column != NULL) && (g_strrstr(split_file[i],autoinc_column))){
                  g_string_append(table_without_indexes, split_file[i]);
                  g_string_append_c(table_without_indexes,'\n');
                }else{
                  alter_table_present=TRUE;
                  if (g_strrstr(split_file[i],"  FULLTEXT")){
                    fulltext_counter++;
                  }
                  if (fulltext_counter>1){
                    fulltext_counter=1;
                    finish_alter_table(alter_table_statement);
                    append_alter_table(alter_table_statement,db ? db : dbt->database,dbt->real_table);
                  }
                    g_string_append(alter_table_statement,"\n ADD");
                    g_string_append(alter_table_statement, split_file[i]);
                }
              }else{
                if (g_strstr_len(split_file[i],12,"  CONSTRAINT")){
                  include_constraint=TRUE;
                  g_string_append(alter_table_constraint_statement,"\n ADD");
                  g_string_append(alter_table_constraint_statement, split_file[i]);
                }else{
                  if (g_strrstr(split_file[i],"AUTO_INCREMENT")){
                    gchar** autoinc_split=g_strsplit(split_file[i],"`",3);
                    autoinc_column=g_strdup_printf("(`%s`", autoinc_split[1]);
                  }
                  g_string_append(table_without_indexes, split_file[i]);
                  g_string_append_c(table_without_indexes,'\n');
                }
              }
              if (g_strrstr(split_file[i],"ENGINE=InnoDB")){
                is_innodb_table=TRUE;
	      }
            }
            if (is_innodb_table){
              if (alter_table_present){
                finish_alter_table(alter_table_statement);
                g_message("Fast index creation will be use for table: %s.%s",db ? db : dbt->database,dbt->real_table);
              }else{
                g_string_free(alter_table_statement,TRUE);
                alter_table_statement=NULL;
              }
              g_string_append(create_table_statement,g_strjoinv("\n)",g_strsplit(table_without_indexes->str,",\n)",-1)));
              dbt->indexes=alter_table_statement;
              if (include_constraint){
                finish_alter_table(alter_table_constraint_statement);
                struct restore_job *rj = new_restore_job(g_strdup(filename), dbt, alter_table_constraint_statement, 0, JOB_RESTORE_STRING);
                g_async_queue_push(conf->post_queue, new_job(JOB_RESTORE,rj));
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
  struct restore_job * rj = new_restore_job(g_strdup(filename), dbt, create_table_statement, 0,JOB_RESTORE_SCHEMA_STRING);
  g_async_queue_push(conf->pre_queue, new_job(JOB_RESTORE,rj));
}

gchar * get_database_name_from_filename(const gchar *filename){
  gchar **split_file = g_strsplit(filename, "-schema-create.sql", 2);
  gchar *db_name=g_strdup(split_file[0]);
  g_strfreev(split_file);
  return db_name;
}

void get_database_table_part_name_from_filename(const gchar *filename, const gchar * suffix, gchar **database, gchar **table, guint *part){
  gchar **split_file = g_strsplit(filename, suffix, 3);
  gchar **split_db_tbl = g_strsplit(split_file[0], ".", -1);
  g_strfreev(split_file);
  if (g_strv_length(split_db_tbl)==3){
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

void load_directory_information(struct configuration *conf, MYSQL *conn) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory, error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;
  GHashTable *table_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  GList *create_table_list=NULL,*metadata_list= NULL,*data_files_list=NULL;
  struct db_table *dbt=NULL;
  while ((filename = g_dir_read_name(dir))) {
    // TODO: read the whole dir, without !source_db and then filter out the objects.
    if (!source_db ||
      g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))) {
      if (g_strrstr(filename, "-schema.sql")) {
        create_table_list=g_list_insert(create_table_list,g_strdup(filename),-1);
      } else if (g_str_has_suffix(filename, ".metadata")) {
        metadata_list=g_list_insert(metadata_list,g_strdup(filename),-1);
      } else if ( strcmp(filename, "metadata") == 0 ){
      } else if (g_str_has_suffix(filename, ".checksum") || g_str_has_suffix(filename, ".checksum.gz")) {
        conf->checksum_list=g_list_insert(conf->checksum_list,g_strdup(filename),-1);
      } else if ( g_strrstr(filename, "-schema-view.sql") ){
        conf->schema_view_list=g_list_insert(conf->schema_view_list,g_strdup(filename),-1);
      } else if ( g_strrstr(filename, "-schema-triggers.sql") ){
        conf->schema_triggers_list=g_list_insert(conf->schema_triggers_list,g_strdup(filename),-1);
      } else if ( g_strrstr(filename, "-schema-post.sql") ){
        conf->schema_post_list=g_list_insert(conf->schema_post_list,g_strdup(filename),-1);
      } else if ( g_strrstr(filename, "-schema-create.sql") ){
        conf->schema_create_list=g_list_insert(conf->schema_create_list,g_strdup(filename),-1);
      } else {
        data_files_list=g_list_insert(data_files_list,g_strdup(filename),-1);
      } 

    }
  }
  g_dir_close(dir);


  // CREATE DATABASe
  GList *schema_create_list=conf->schema_create_list;
  gchar *db_kname,*db_vname;
  while (schema_create_list){
    filename=schema_create_list->data;
    db_vname=db_kname=get_database_name_from_filename(filename);
    if (db_kname!=NULL && g_str_has_prefix(db_kname,"mydumper_")){
      db_vname=get_database_name_from_content(g_build_filename(directory,filename,NULL));
      // Append to hash ref table...
    }
    g_hash_table_insert(db_hash, db_kname, db_vname);
    if (db)
      create_database(conn, db);
    else
      restore_data_from_file(conn, db_vname, NULL, filename, TRUE, FALSE);
    // Append to db
    schema_create_list=schema_create_list->next;
  }


  // CREWATE TABLE
  gchar *db_name, *table_name, *real_db_name;
  while (create_table_list != NULL){
    filename=create_table_list->data;
    get_database_table_name_from_filename(filename,"-schema.sql",&db_name,&table_name);
    if (db_name == NULL || table_name == NULL){
      g_critical("It was not possible to process file: %s",filename);
      exit(EXIT_FAILURE);
    }
    real_db_name=g_hash_table_lookup(db_hash,db_name);
    if (real_db_name==NULL){
      g_critical("It was not possible to process file: %s",filename);
      exit(EXIT_FAILURE);
    }
    dbt=append_new_db_table(real_db_name, db_name, table_name,0,table_hash,NULL);
    load_schema(conf, dbt,g_build_filename(directory,filename,NULL));
    create_table_list=create_table_list->next;
  }

  // DATA FILES
  while (data_files_list != NULL){
    filename=data_files_list->data;
    total_data_sql_files++;
  // TODO: check if it is a data file
  // TODO: we need to count sections of the data file to determine if it is ok.
    guint part;
    get_database_table_part_name_from_filename(filename,".sql",&db_name,&table_name,&part);
    if (db_name == NULL || table_name == NULL){
      g_critical("It was not possible to process file: %s",filename);
      exit(EXIT_FAILURE);
    }
    real_db_name=g_hash_table_lookup(db_hash,db_name);
    dbt=append_new_db_table(real_db_name,db_name, table_name,0,table_hash,NULL);
    struct restore_job *rj = new_restore_job(g_strdup(filename), dbt, NULL, part , JOB_RESTORE_FILENAME);
    dbt->restore_job_list=g_list_insert_sorted(dbt->restore_job_list,rj,&compare_filename_part);
    data_files_list=data_files_list->next;
  }

  GList * table_list=NULL;
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, table_hash );
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
    table_list=g_list_insert_sorted_with_data (table_list,dbt,&compare_dbt,table_hash);
    GList *i=dbt->restore_job_list; 
    while (i) {
      g_async_queue_push(dbt->queue, new_job(JOB_RESTORE ,i->data));
      i=i->next;
    }
    dbt->count=g_async_queue_length(dbt->queue);
  }
  g_hash_table_destroy(table_hash);
  conf->table_list=table_list;
}


// this can be moved to the table structure and executed before index creation.
void checksum_databases(MYSQL *conn,struct configuration *conf) {
  g_message("Starting table checksum verification");

  const gchar *filename = NULL;
  GList *e = conf->checksum_list;
  while (e){
    filename=e->data;
    checksum_table_filename(filename, conn);
    e=e->next;
  }
}
void restore_schema_list(MYSQL *conn,GList * schema_list, const gchar *object, const gboolean overwrite_fun) {
  const gchar *filename = NULL;
  GList *e = schema_list;
  gchar *database=NULL, *table=NULL, *current_database=NULL;
  gchar *execute_msg=g_strdup_printf("on %s restoration",object);
  if (db){
    execute_use(conn, db, execute_msg);
    current_database=db;
  }
  gchar *real_db_name;
  while ( e ) {
    filename=e->data;
    get_database_table_from_file(filename,"-schema",&database,&table);
    if (database == NULL){
      g_critical("Database is null on: %s",filename);
    }
    real_db_name=g_hash_table_lookup(db_hash,database);
    if (!db){
      if (current_database==NULL || g_strcmp0(real_db_name, current_database) != 0){
        execute_use(conn, real_db_name, execute_msg);
        current_database=real_db_name;
      }
    }
    if (table==NULL)
      g_message("Restoring %s on `%s` file %s",object,current_database,filename);
    else
      g_message("Restoring %s on `%s`.`%s` from file %s",object,current_database,table,filename);
    if (overwrite_fun)
      overwrite_table(conn, current_database, table);
    restore_data_from_file(conn, current_database, table, filename, TRUE, TRUE);
    e=e->next;
  }
}

void restore_schema_view(MYSQL *conn,struct configuration *conf) {
  purge_mode = DROP;
  restore_schema_list(conn,conf->schema_view_list,"View", TRUE);
}

void restore_schema_triggers(MYSQL *conn,struct configuration *conf) {
  restore_schema_list(conn,conf->schema_triggers_list,"Triggers", FALSE);
}

void restore_schema_post(MYSQL *conn,struct configuration *conf) {
  restore_schema_list(conn,conf->schema_post_list,"Routines and Events", FALSE);
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
    restore_data_from_file(conn, database, NULL, filename, TRUE, FALSE);
  } else if (g_file_test(filepathgz, G_FILE_TEST_EXISTS)) {
    restore_data_from_file(conn, database, NULL, filenamegz, TRUE, FALSE);
  } else {
    query = g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
    mysql_query(conn, query);
  }

  g_free(query);
  return;
}
void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table){
  gchar **split_filename = g_strsplit(filename, sufix, 0);
  gchar **split = g_strsplit(split_filename[0],".",0);
  guint count=g_strv_length(split);
  if (count > 2){
    g_message("we need to get the db and table name from the create table statement");
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
  get_database_table_from_file(filename,".checksum",&database,&table);
  gchar *real_database=g_hash_table_lookup(db_hash,database);
  gchar *real_table=g_hash_table_lookup(tbl_hash,table);
  void *infile;
  char checksum[256];
  int errn=0;
  char * row=checksum_table(conn, db ? db : real_database, real_table, &errn);
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
}

gboolean process_job(struct configuration *conf, MYSQL *thrconn,struct job *job, int thread_id, int count, gchar **current_database){
  switch (job->type) {
    case JOB_RESTORE:
      process_restore_job(thrconn,job->job_data,thread_id,count,current_database);
      break;
    case JOB_WAIT:
      g_async_queue_push(conf->ready, GINT_TO_POINTER(1));
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


void process_restore_job(MYSQL *thrconn,struct restore_job *rj, int thread_id, int count,gchar **current_database){
  struct db_table *dbt=rj->dbt;
  dbt=rj->dbt;
  if (!db && dbt != NULL){
    if (*current_database==NULL || g_strcmp0(dbt->real_database, *current_database) != 0){
      if (execute_use(thrconn, dbt->real_database, "Restoring Job")){
        exit(EXIT_FAILURE);
      }
      *current_database=dbt->real_database;
    }
  }

  switch (rj->type) {
    case JOB_RESTORE_STRING:
      g_message("Thread %d restoring contraints `%s`.`%s` from %s", thread_id,
                dbt->real_database, dbt->real_table, rj->filename);
      guint query_counter=0;
      restore_data_in_gstring(thrconn, rj->statement, FALSE, &query_counter);
      if (rj->statement)
        g_string_free(rj->statement,TRUE);
      g_free(rj);
      break;
    case JOB_RESTORE_SCHEMA_STRING:
      g_message("Thread %d restoring table `%s`.`%s` from %s", thread_id,
                dbt->real_database, dbt->real_table, rj->filename);
      int truncate_or_delete_failed=0;
      if (overwrite_tables)
        truncate_or_delete_failed=overwrite_table(thrconn,dbt->real_database, dbt->real_table);
      if ((purge_mode == TRUNCATE || purge_mode == DELETE) && !truncate_or_delete_failed){
        g_message("Skipping table creation `%s`.`%s` from %s", dbt->real_database, dbt->real_table, rj->filename);
      }else{
        g_message("Creating table `%s`.`%s` from %s", db ? db : dbt->real_database, dbt->real_table, rj->filename);
        if (restore_data_in_gstring(thrconn, rj->statement, FALSE, &query_counter)){
          g_critical("Thread %d issue restoring %s: %s",thread_id,rj->filename, mysql_error(thrconn));
        }
      }
      break;
    case JOB_RESTORE_FILENAME:
      g_mutex_lock(progress_mutex);
      progress++;
      g_message("Thread %d restoring `%s`.`%s` part %d of %d from %s. Progress %llu of %llu .", thread_id,
                dbt->real_database, dbt->real_table, rj->part, count, rj->filename, progress,total_data_sql_files);
      g_mutex_unlock(progress_mutex);
      if (restore_data_from_file(thrconn, dbt->real_database, dbt->real_table, rj->filename, FALSE, TRUE) > 0){
        g_critical("Thread %d issue restoring %s: %s",thread_id,rj->filename, mysql_error(thrconn));
      }
      if (rj->filename)
        g_free(rj->filename);
      g_free(rj);
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
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

  execute_gstring(thrconn, set_session);
  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  struct db_table *dbt=NULL;
  struct job *job = NULL;
  gboolean cont=TRUE;
  int count =0;
  // Step 2: Create tables
  gchar *current_database=NULL;
  if (db){
    execute_use(thrconn, db,"Creating tables");
    current_database=db;
  }
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->pre_queue);
    cont=process_job(conf, thrconn, job, td->thread_id, count, &current_database);
  }

  GList *table_list=conf->table_list;
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
            restore_data_in_gstring(thrconn, dbt->indexes, FALSE, &query_counter);
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
      // All table jobs done
     job = (struct job *)g_async_queue_pop(conf->queue);
//     break;
    }
    cont=process_job(conf, thrconn, job, td->thread_id, count, &current_database);
  }

  cont=TRUE;
  while (cont){
    job = (struct job *)g_async_queue_pop(conf->post_queue);
    cont=process_job(conf, thrconn, job, td->thread_id, count, &current_database);
  }
  if (thrconn)
    mysql_close(thrconn);
  mysql_thread_end();
  g_message("Thread %d ending", td->thread_id);
  return NULL;
}


int restore_data_in_gstring_from_file(MYSQL *conn, GString *data, gboolean is_schema, guint *query_counter)
{
  if (mysql_real_query(conn, data->str, data->len)) {
g_critical("Error restoring: %s",data->str);
	  //    g_critical("Error restoring %s.%s from file %s: %s \n", db ? db : database, table, filename, mysql_error(conn));
    errors++;
    return 1;
  }
  *query_counter=*query_counter+1;
  if (!is_schema && (commit_count > 1) &&(*query_counter == commit_count)) {
    *query_counter= 0;
    if (mysql_query(conn, "COMMIT")) {
//      g_critical("Error committing data for %s.%s: %s", db ? db : database, table, mysql_error(conn));
      errors++;
      return 2;
    }
    mysql_query(conn, "START TRANSACTION");
  }
  g_string_set_size(data, 0);
  return 0;
}

int restore_data_in_gstring(MYSQL *conn, GString *data, gboolean is_schema, guint *query_counter)
{
  gchar** line=g_strsplit(data->str, ";\n", -1);
  int i=0;
  int r=0;
  for (i=0; i < (int)g_strv_length(line);i++){
     if (strlen(line[i])>2){
       GString *str=g_string_new(line[i]);
       g_string_append_c(str,';');
       r+=restore_data_in_gstring_from_file(conn, str, is_schema, query_counter);
     }
  }
  return r;
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

guint execute_use(MYSQL *conn, gchar *database, const gchar * msg){
  gchar *query = g_strdup_printf("USE `%s`", database);
  if (mysql_query(conn, query)) {
    g_critical("Error switching to database `%s` %s", database,msg);
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
  if (!g_str_has_suffix(filename, ".gz")) {
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
  if (!g_str_has_suffix(filename, ".gz")) {
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

int restore_data_from_file(MYSQL *conn, char *database, char *table,
                  const char *filename, gboolean is_schema, gboolean need_use) {
  void *infile;
  int r=0;
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
    return 1;
  }

  if (need_use) {
    execute_use(conn, database,g_strdup_printf("whilst restoring table %s from file %s",table, filename));
  }

  if (!is_schema && (commit_count > 1) )
    mysql_query(conn, "START TRANSACTION");
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (rows > 0 && g_strrstr_len(data->str,6,"INSERT"))
          split_and_restore_data_in_gstring_from_file(conn,
            data, is_schema, &query_counter);
        else{ 
          r+=restore_data_in_gstring_from_file(conn, data, is_schema, &query_counter);
	}
	g_string_set_size(data, 0);
      }
    } else {
      g_critical("error reading file %s (%d)", filename, errno);
      errors++;
      return r;
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
