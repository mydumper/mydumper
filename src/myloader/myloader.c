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


#if defined MARIADB_CLIENT_VERSION_STR && !defined MYSQL_SERVER_VERSION
#define MYSQL_SERVER_VERSION MARIADB_CLIENT_VERSION_STR
#endif

#include <glib/gstdio.h>
#include <locale.h>
#include <errno.h>

#include "myloader_stream.h"
#include "myloader_process.h"
#include "myloader_common.h"
#include "myloader_directory.h"
#include "myloader_restore.h"
#include "myloader_pmm.h"
#include "myloader_restore_job.h"
#include "myloader_intermediate_queue.h"
#include "myloader_arguments.h"
#include "myloader_global.h"
#include "myloader_worker_index.h"
#include "myloader_worker_schema.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_post.h"
#include "myloader_control_job.h"

guint commit_count = 1000;
gchar *input_directory = NULL;
gchar *directory = NULL;
gchar *pwd=NULL;
gboolean overwrite_tables = FALSE;
gboolean overwrite_unsafe = FALSE;

gboolean optimize_keys = TRUE;
gboolean optimize_keys_per_table = TRUE;
gboolean optimize_keys_all_tables = FALSE;
gboolean kill_at_once = FALSE;
gboolean enable_binlog = FALSE;
gboolean disable_redo_log = FALSE;
enum checksum_modes checksum_mode= CHECKSUM_FAIL;
gboolean skip_triggers = FALSE;
gboolean skip_constraints = FALSE;
gboolean skip_indexes = FALSE;
gboolean skip_post = FALSE;
gboolean serial_tbl_creation = FALSE;
gboolean resume = FALSE;
guint rows = 0;
guint sequences = 0;
guint sequences_processed = 0;
GMutex sequences_mutex;
gchar *source_db = NULL;
extern guint errors;
guint max_errors= 0;
struct restore_errors detailed_errors = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
guint max_threads_for_schema_creation=4;
guint max_threads_for_index_creation=4;
guint max_threads_for_post_creation= 1;
guint retry_count= 10;
gboolean stream = FALSE;
gboolean no_delete = FALSE;

extern GHashTable *db_hash;
extern gboolean shutdown_triggered;
extern gboolean skip_definer;
extern gboolean local_infile;
extern guint64 max_transaction_size;

const char DIRECTORY[] = "import";

struct configuration_per_table conf_per_table = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
GHashTable * set_session_hash=NULL;

GHashTable * myloader_initialize_hash_of_session_variables(){
  GHashTable * _set_session_hash=initialize_hash_of_session_variables();
  if (commit_count > 1)
    set_session_hash_insert(_set_session_hash,"AUTOCOMMIT",g_strdup("0"));
  if (!enable_binlog)
    set_session_hash_insert(_set_session_hash,"SQL_LOG_BIN",g_strdup("0"));
  return _set_session_hash;
}

static
void detect_group_replication_transaction_size_limit(MYSQL * conn) {
  guint64 _max_transaction_size=0;
  struct M_ROW *mr = m_store_result_row(conn, "SELECT @@group_replication_transaction_size_limit / 1024 / 1024",m_message, m_message, "Using default transaction limit", NULL);
  if (mr->row)
    _max_transaction_size=strtoll(mr->row[0], NULL, 10);
  max_transaction_size=_max_transaction_size > max_transaction_size ? _max_transaction_size : max_transaction_size;
  m_store_result_row_free(mr);
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


void initialize_directories(){
  char *current_dir=g_get_current_dir();
  if (!input_directory) {
    if (stream){
      GDateTime * datetime = g_date_time_new_now_local();
      char *datetimestr;
      datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");

      directory = g_strdup_printf("%s/%s-%s",current_dir, DIRECTORY, datetimestr);
      g_date_time_unref(datetime);
      g_free(datetimestr);
    }else{
      if(!help)
        m_critical("a directory needs to be specified, see --help\n");
    }
  } else {
    directory=g_str_has_prefix(input_directory,"/")?input_directory:g_strdup_printf("%s/%s", current_dir, input_directory);
    if (stream){
      if (g_file_test(input_directory,G_FILE_TEST_IS_DIR) && !no_stream)
          m_critical("Backup directory (-d) must not exist when --stream / --stream=TRADITIONAL");
    }else{
      if (!g_file_test(input_directory,G_FILE_TEST_IS_DIR))
        m_critical("the specified directory doesn't exists\n");
      char *p = g_strdup_printf("%s/metadata", directory);
      if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
        m_critical("the specified directory %s is not a mydumper backup as metadata file was not found in it",directory);
      }
    }
  }

  if (fifo_directory){
    if (fifo_directory[0] != '/' ){
      gchar *tmp_fifo_directory=fifo_directory;
      fifo_directory=g_strdup_printf("%s/%s", current_dir, tmp_fifo_directory);
    }
  }else{
    // Set fifo temporary director
    fifo_directory=build_tmp_dir_name();
  }
  g_free(current_dir);
}

void show_dbt(void* _key, void* dbt, void *total){
  (void) dbt;
  (void) total;
  g_message("Table %s", (char*) _key);
//  *((guint *)total)= 100;
//  //    if (((struct db_table*)dbt)->schema_state >= CREATED)
//  //        *((guint *)total)= *((guint *)total) + 1;
 //        //*((guint *)total) + 2+ ((struct db_table*)dbt)->schema_state >= CREATED /*ALL_DONE*/ ? 1 : 0;
  }

void create_database(struct thread_data *td, gchar *database) {

  const gchar *filename =
      g_strdup_printf("%s-schema-create.sql%s", database, exec_per_thread_extension);
  const gchar *filepath = g_strdup_printf("%s/%s-schema-create.sql%s",
                                            directory, database, exec_per_thread_extension);

  if (drop_database)
    execute_drop_database(td, database);
  if (g_file_test(filepath, G_FILE_TEST_EXISTS)) {
    g_atomic_int_add(&(detailed_errors.schema_errors), restore_data_from_mydumper_file(td, filename, TRUE, NULL));
  } else {
    GString *data = g_string_new("CREATE DATABASE IF NOT EXISTS ");
    g_string_append_printf(data,"`%s`", database);
    trace("Creating schema %s", database);
    if (restore_data_in_gstring_extended(td, data , TRUE, NULL, m_critical, "Failed to create database: %s", database) )
      g_atomic_int_inc(&(detailed_errors.schema_errors));
    g_string_free(data, TRUE);
  }

  return;
}

void print_help(){
    print_string("host", hostname);
    print_string("user", username);
    print_string("password", password);
    print_bool("ask-password",askPassword);
    print_int("port",port);
    print_string("socket",socket_path);
    print_string("protocol", protocol_str);
    print_bool("compress-protocol",compress_protocol);
#ifdef WITH_SSL
    print_bool("ssl",ssl);
    print_string("ssl-mode",ssl_mode);
    print_string("key",key);
    print_string("cert", cert);
    print_string("ca",ca);
    print_string("capath",capath);
    print_string("cipher",cipher);
    print_string("tls-version",tls_version);
#endif
    print_list("regex",regex_list);
    print_string("source-db",source_db);

    print_bool("skip-triggers",skip_triggers);
    print_bool("skip-constraints",skip_constraints);
    print_bool("skip-indexes",skip_indexes);
    print_bool("skip-post",skip_post);
    print_bool("no-data",no_data);

    print_string("omit-from-file",tables_skiplist_file);
    print_string("tables-list",tables_list);
    print_string("pmm-path",pmm_path);
    print_string("pmm-resolution",pmm_resolution);

    if (enable_binlog)
      print_bool("enable-binlog",enable_binlog);
    if (!optimize_keys){
      print_string("optimize-keys",SKIP);
    }else if(optimize_keys_per_table){
      print_string("optimize-keys",AFTER_IMPORT_PER_TABLE);
    }else if(optimize_keys_all_tables){
      print_string("optimize-keys",AFTER_IMPORT_ALL_TABLES);
    }else
      print_string("optimize-keys",NULL);

    print_bool("no-schemas",no_schemas);

    print_bool("local-infile", local_infile);
    print_bool("disable-redo-log",disable_redo_log);
    print_string("checksum",checksum_str);
    print_bool("overwrite-tables",overwrite_tables);
    print_bool("overwrite-unsafe",overwrite_unsafe);
    print_int("retry-count",retry_count);
    print_bool("serialized-table-creation",serial_tbl_creation);
    print_bool("stream",stream);

    print_int("max-threads-per-table",max_threads_per_table);
    print_int("max-threads-for-index-creation",max_threads_for_index_creation);
    print_int("max-threads-for-post-actions",max_threads_for_post_creation);
    print_int("max-threads-for-schema-creation",max_threads_for_schema_creation);
    print_string("exec-per-thread",exec_per_thread);
    print_string("exec-per-thread-extension",exec_per_thread_extension);

    print_int("rows",rows);
    print_int("queries-per-transaction",commit_count);
    print_bool("append-if-not-exist",append_if_not_exist);
    print_string("set-names",set_names_in_conn_by_default);

    print_bool("skip-definer",skip_definer);
    print_bool("help",help);

    print_string("directory",input_directory);
    print_string("logfile",logfile);

    print_string("database",db);
    print_string("quote-character",identifier_quote_character_str);
    print_bool("resume",resume);
    print_int("threads",num_threads);
    print_bool("version",program_version);
    print_bool("verbose",verbose);
    print_bool("debug",debug);
    print_string("defaults-file",defaults_file);
    print_string("defaults-extra-file",defaults_extra_file);
    print_string("fifodir",fifo_directory);
    exit(EXIT_SUCCESS);
}


void print_errors(){
  if (detailed_errors.tablespace_errors ||
    detailed_errors.schema_errors ||
    detailed_errors.data_errors ||
    detailed_errors.data_warnings ||
    detailed_errors.view_errors ||
    detailed_errors.sequence_errors ||
    detailed_errors.index_errors ||
    detailed_errors.trigger_errors ||
    detailed_errors.constraints_errors ||
    detailed_errors.post_errors ||
    detailed_errors.skip_errors ||
    detailed_errors.retries)
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
    "Warnings found:\n"
    "- Data:\t%d\n"
    "- Skip Schema:\t%d\n"
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
    detailed_errors.data_warnings,
    detailed_errors.skip_errors,
    detailed_errors.retries);


}

int main(int argc, char *argv[]) {
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL};

  GError *error = NULL;
  GOptionContext *context;

  setlocale(LC_ALL, "");
  g_thread_init(NULL);
  set_thread_name("MNT");

  signal(SIGCHLD, SIG_IGN);

  context = load_contex_entries();

  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    m_critical("option parsing failed: %s, try --help\n", error->message);
  }

  // Loading the defaults file:
  initialize_common_options(context, "myloader");














  g_strfreev(tmpargv);

  if (db == NULL && source_db != NULL) {
    db = g_strdup(source_db);
  }

  if (overwrite_unsafe)
    overwrite_tables= TRUE;

  check_num_threads();

  if (num_threads > max_threads_per_table)
    g_message("Using %u loader threads (%u per table)", num_threads, max_threads_per_table);
  else
    g_message("Using %u loader threads", num_threads);

  initialize_set_names();

  if (debug) {
    set_debug();
    verbose=3;
  }

  if (help){
    printf("%s", g_option_context_get_help (context, FALSE, NULL));
  }

  if (program_version) {
    print_version("myloader");
    if (!help)
      exit(EXIT_SUCCESS);
    printf("\n");
  }

  if (help)
    print_help();

  set_verbose(verbose);

  g_message("MyDumper restore version: %s", VERSION);

  // Starts modifying file in disk, creating objects and restore

  hide_password(argc, argv);
  ask_password();

  initialize_pmm();

  initialize_restore_job();
  initialize_directories();

  conf.context=context;

  initialize_restore();

  if(stream && !no_stream)
    create_dir(directory);
  create_dir(fifo_directory);

  g_message("Using %s as FIFO directory, please remove it if restoration fails", fifo_directory);

  start_pmm_thread((void *)&conf);

  g_chdir(directory);
  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);
  initialize_process(&conf);
  initialize_common();
  initialize_connection(MYLOADER);
  initialize_regex(NULL);
  if (!kill_at_once){
    m_thread_new("myloader_signal",signal_thread, &conf, "Signal thread could not be created");
  }

  MYSQL *conn;
  conn = mysql_init(NULL);
  m_connect(conn);

  set_session = g_string_new(NULL);
  set_global = g_string_new(NULL);
  set_global_back = g_string_new(NULL);
  server_detect(conn);
  set_session_hash = myloader_initialize_hash_of_session_variables();
  GHashTable * set_global_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  if (key_file != NULL ){
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_global_hash,"myloader_global_variables");
    load_hash_of_all_variables_perproduct_from_key_file(key_file,set_session_hash,"myloader_session_variables");
  }
	initialize_conf_per_table(&conf_per_table);
  load_per_table_info_from_key_file(key_file, &conf_per_table, NULL );
  if (max_transaction_size == DEFAULT_MAX_TRANSACTION_SIZE)
    detect_group_replication_transaction_size_limit(conn);

  // To here.
  conf.database_queue = g_async_queue_new();
  conf.table_queue = g_async_queue_new();
  conf.retry_queue = g_async_queue_new();
  conf.data_queue = g_async_queue_new();
  conf.post_table_queue = g_async_queue_new();
  conf.post_queue = g_async_queue_new();
  conf.index_queue = g_async_queue_new();
  conf.view_queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.pause_resume = g_async_queue_new();
  conf.table_list_mutex = g_mutex_new();
//  conf.stream_queue = g_async_queue_new();
  conf.table_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  conf.table_hash_mutex=g_mutex_new();

  if (g_file_test("resume",G_FILE_TEST_EXISTS)){
    if (!resume){
      m_critical("Resume file found but --resume has not been provided");
    }
  }else{
    if (resume){
      m_critical("Resume file not found");
    }
  }

  initialize_connection_pool();
  struct thread_data *t=g_new(struct thread_data,1);
  initialize_thread_data(t, &conf, WAITING, 0, NULL);

  if (tables_list)
    tables = get_table_list(tables_list);

  if (serial_tbl_creation)
    max_threads_for_schema_creation=1;

  /* TODO: if conf is singleton it must be accessed as global variable */
  initialize_worker_schema(&conf);
  initialize_worker_index(&conf);
  initialize_intermediate_queue(&conf);

  if (stream){
    if (resume){
      m_critical("We don't expect to find resume files in a stream scenario");
    }
    initialize_stream(&conf);
  }else{
    initialize_directory();
    m_thread_new("myloader_directory",(GThreadFunc)process_directory, &conf, "Directory thread could not be created");
  }

  if (!stream){
    wait_directory_to_process_metadata();
  }else{
    wait_stream_to_process_metadata_header();
  }

  remove_ignore_set_session_from_hash();
  refresh_set_session_from_hash(set_session,set_session_hash);
  refresh_set_global_from_hash(set_global,set_global_back, set_global_hash);
  execute_gstring(conn, set_session);
  execute_gstring(conn, set_global);

  if (replication_statements->start_replica_until){
    g_message("Sending start replica until");
    execute_replication_commands(conn,replication_statements->start_replica_until->str);
  }

  start_connection_pool();
  if (disable_redo_log){
    if ((get_major() == 8) && (get_secondary() == 0) && (get_revision() > 21)){
      g_message("Disabling redologs");
      m_query_critical(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", "DISABLE INNODB REDO LOG failed");
    }else{
      m_error("Disabling redologs is not supported for version %d.%d.%d", get_major(), get_secondary(), get_revision());
    }
  }

  if (database_db){
    if (!no_schemas)
      create_database(t, database_db->real_database);
    database_db->schema_state=CREATED;
  }
  start_worker_schema();
  initialize_loader_threads(&conf);

  if (throttle_variable)
    m_thread_new("mon_thro",monitor_throttling_thread, NULL, "Monitor throttling thread could not be created");

  if (stream){
    wait_stream_to_finish();
  }


  GList * tl=conf.table_list;
  while (tl != NULL){
    if(((struct db_table *)(tl->data))->max_connections_per_job==1){
      ((struct db_table *)(tl->data))->max_connections_per_job=0;
    }
    tl=tl->next;
  }

  wait_schema_worker_to_finish();
  wait_loader_threads_to_finish();
  wait_control_job();
  create_index_shutdown_job(&conf);
  wait_index_worker_to_finish();
  initialize_post_loding_threads(&conf);
  create_post_shutdown_job(&conf);
  wait_post_worker_to_finish();
//  wait_control_job();
  g_async_queue_unref(conf.ready);
  conf.ready=NULL;
  g_async_queue_unref(conf.data_queue);
  conf.data_queue=NULL;

  if (disable_redo_log)
    m_query_critical(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG", "ENABLE INNODB REDO LOG failed");

  gboolean checksum_ok=TRUE;
  tl=conf.table_list;
  while (tl != NULL){
    checksum_ok&=checksum_dbt(tl->data, conn);
    tl=tl->next;
  }

  if (checksum_mode != CHECKSUM_SKIP) {
    GHashTableIter iter;
    gchar *lkey;
    g_hash_table_iter_init(&iter, db_hash);
    struct database *d= NULL;
    while (g_hash_table_iter_next(&iter, (gpointer *) &lkey, (gpointer *) &d)) {
      if (d->schema_checksum != NULL && !no_schemas)
        checksum_ok&=checksum_database_template(d->real_database, d->schema_checksum, conn,
                                  "Schema create checksum", checksum_database_defaults);
      if (d->post_checksum != NULL && !skip_post)
        checksum_ok&=checksum_database_template(d->real_database, d->post_checksum, conn,
                                  "Post checksum", checksum_process_structure);
      if (d->triggers_checksum != NULL && !skip_triggers)
        checksum_ok&=checksum_database_template(d->real_database, d->triggers_checksum, conn,
                                  "Triggers checksum", checksum_trigger_structure_from_database);
    }
  }
  wait_restore_threads_to_close();

  if (!checksum_ok){
    if (checksum_mode==CHECKSUM_WARN)
      g_warning("Checksum failed");
    else
      g_error("Checksum failed");
  }

  if (stream && no_delete == FALSE ){ //&& input_directory == NULL){
//    m_remove(directory,"metadata");
//    m_remove(directory, "metadata.header");
    if (g_rmdir(directory) < 0)
        g_warning("Restore directory not removed: %s (%s)", directory, strerror(errno));
  }


  if (replication_statements->reset_replica){
    g_message("Sending reset replica");
    execute_replication_commands(conn,replication_statements->reset_replica->str);
  }

  if (replication_statements->change_replication_source){
    g_message("Sending change replication source");
    execute_replication_commands(conn,replication_statements->change_replication_source->str);
  }

  if (replication_statements->start_replica){
    g_message("Sending start replica");
    execute_replication_commands(conn,replication_statements->start_replica->str);
  }

  g_async_queue_unref(conf.database_queue);
  g_async_queue_unref(conf.table_queue);
  g_async_queue_unref(conf.retry_queue);
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

  stop_pmm_thread();

//  g_hash_table_foreach(conf.table_hash,&show_dbt, NULL);
  free_table_hash(conf.table_hash);
  g_hash_table_remove_all(conf.table_hash);
  g_hash_table_unref(conf.table_hash);
  g_list_free_full(conf.checksum_list,g_free);

  free_set_names();
  print_errors();

  stop_signal_thread();

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

  if (key_file)  g_key_file_free(key_file);
  g_remove(fifo_directory);
  g_message("Restore completed");

  if (logoutfile) {
    fclose(logoutfile);
  }

  return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}

