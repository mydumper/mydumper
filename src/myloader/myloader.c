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
#include "myloader_process_filename.h"
#include "myloader_process_file_type.h"
#include "myloader_arguments.h"
#include "myloader_global.h"
#include "myloader_worker_index.h"
#include "myloader_worker_schema.h"
#include "myloader_worker_loader.h"
#include "myloader_worker_post.h"
#include "myloader_control_job.h"
#include "myloader_database.h"
#include "myloader_worker_loader_main.h"
#include "../logging.h"

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
gboolean skip_triggers = FALSE;
gboolean skip_constraints = FALSE;
gboolean skip_indexes = FALSE;
gboolean skip_post = FALSE;
gboolean serial_tbl_creation = FALSE;
gboolean resume = FALSE;
guint rows = 0;
guint num_sequences = 0;
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

extern GHashTable *database_hash;
extern gboolean shutdown_triggered;
extern gboolean local_infile;
extern guint64 max_transaction_size;
extern guint optimize_keys_batchsize;

const char DIRECTORY[] = "import";

//struct configuration_per_table conf_per_table = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
GHashTable *conf_per_table=NULL;
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
  struct M_ROW *mr = m_store_result_row(conn, "SHOW GLOBAL VARIABLES LIKE 'group_replication_transaction_size_limit'",m_message, m_message, "Using default transaction limit", NULL);
  if (mr->row)
    _max_transaction_size=strtoll(mr->row[0], NULL, 10)/1024/1024;
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
  initialize_connection_socket_dir(current_dir);
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

  if (load_data_tmp_directory){
    if (fifo_directory[0] != '/' ){
      gchar *tmp_load_data_tmp_directory=load_data_tmp_directory;
      load_data_tmp_directory=g_strdup_printf("%s/%s", current_dir, tmp_load_data_tmp_directory);
    }
  }else{
    load_data_tmp_directory=build_tmp_dir_name();
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
    print_int("optimize-keys-batchsize",optimize_keys_batchsize);
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
    print_string("replace-definer",replace_definer);
    print_bool("help",help);

    print_string("directory",input_directory);
    print_string("logfile",logfile);

    print_string("database",target_db);
    print_string("quote-character",identifier_quote_character_str);
    print_bool("resume",resume);
    print_int("threads",num_threads);
    print_bool("version",program_version);
    print_bool("verbose",verbose);
    print_bool("debug",debug);
    print_string("defaults-file",defaults_file);
    print_string("defaults-extra-file",defaults_extra_file);
    print_string("fifodir",fifo_directory);
    print_string("load-data-tmp-directory",load_data_tmp_directory);
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
  struct configuration conf = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL};
  GError *error = NULL;
  GOptionContext *context;
  gint64 process_started_at = g_get_monotonic_time();
  gint exit_code = EXIT_SUCCESS;

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

  if (machine_log_json) {
    configure_log_output(debug ? 4U : verbose);
  }

  // Loading the defaults file:
  initialize_common_options(context, "myloader");














  g_strfreev(tmpargv);

  if (target_db == NULL && source_db != NULL) {
    target_db = g_strdup(source_db);
  }

  if (overwrite_unsafe)
    overwrite_tables= TRUE;

  // Auto-scale schema/index threads based on CPU count
  // Only auto-scale if user hasn't explicitly set these values (default is 4)
  guint cpu_count = g_get_num_processors();
  if (max_threads_for_schema_creation == 4 && cpu_count > 4) {
    max_threads_for_schema_creation = cpu_count;
  }
  if (max_threads_for_index_creation == 4 && cpu_count > 4) {
    max_threads_for_index_creation = cpu_count;
  }

  check_num_threads();

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

  if (machine_log_json) {
    gchar *threads_text = g_strdup_printf("%u", num_threads);
    gchar *max_threads_text = g_strdup_printf("%u", max_threads_per_table);
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                     "MESSAGE", "effective restore configuration loaded",
                     "EVENT", "process_config",
                     "PHASE", "startup",
                     "STATUS", "started",
                     "MODE", "restore",
                     "INPUT_DIRECTORY", input_directory != NULL ? input_directory : "",
                     "THREADS", threads_text,
                     "MAX_THREADS_PER_TABLE", max_threads_text,
                     "STREAM", stream ? "true" : "false",
                     "LOGFILE", logfile != NULL ? logfile : "",
                     "SOURCE_DB", source_db != NULL ? source_db : "",
                     "TARGET_DB", target_db != NULL ? target_db : "",
                     NULL);
    g_free(threads_text);
    g_free(max_threads_text);
  }

  if (machine_log_json_enabled()) {
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "MyDumper restore version",
                      "EVENT", "process_version",
                      "PHASE", "startup",
                      "STATUS", "started",
                      "VERSION", VERSION,
                      NULL);
  } else {
    g_message("MyDumper restore version: %s", VERSION);
  }

  if (machine_log_json_enabled()) {
    gchar *threads_text = g_strdup_printf("%u", num_threads);
    gchar *max_threads_text = g_strdup_printf("%u", max_threads_per_table);
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "Using loader threads",
                      "EVENT", "loader_threads",
                      "PHASE", "startup",
                      "STATUS", "started",
                      "THREADS", threads_text,
                      "MAX_THREADS_PER_TABLE", max_threads_text,
                      NULL);
    g_free(max_threads_text);
    g_free(threads_text);
  } else if (num_threads > max_threads_per_table) {
    g_message("Using %u loader threads (%u per table)", num_threads, max_threads_per_table);
  } else {
    g_message("Using %u loader threads", num_threads);
  }

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
  create_dir(load_data_tmp_directory);

  g_message("Using %s as FIFO directory and %s as LOAD DATA temporary directory, please remove them if restoration fails", fifo_directory, load_data_tmp_directory);

  start_pmm_thread((void *)&conf);
  conf_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  g_chdir(directory);
  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);
  initialize_process(&conf);
  initialize_table(&conf);
  initialize_database();
  initialize_common();
  initialize_connection(MYLOADER);
  initialize_regex(NULL);
  if (!kill_at_once){
    m_thread_new("myloader_signal",signal_thread, &conf, "Signal thread could not be created");
  }
  initilize_checksum();
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
//	initialize_conf_per_table(&conf_per_table);
//  conf_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  load_per_table_info_from_key_file(key_file, conf_per_table, NULL );
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
  conf.ready_table_queue = g_async_queue_new();
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
  initialize_process_filename(&conf);

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
    if (machine_log_json_enabled()) {
      machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                        "MESSAGE", "Sending start replica until",
                        "EVENT", "replication_command",
                        "PHASE", "replication",
                        "STATUS", "started",
                        "COMMAND", "start_replica_until",
                        NULL);
    } else {
      g_message("Sending start replica until");
    }
    execute_replication_commands(conn,replication_statements->start_replica_until->str);
  }

  start_connection_pool();
  if (disable_redo_log){
    if ((get_major() == 8) && (get_secondary() == 0) && (get_revision() > 21)){
      if (machine_log_json_enabled()) {
        machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                          "MESSAGE", "Disabling redologs",
                          "EVENT", "redo_log",
                          "PHASE", "startup",
                          "STATUS", "started",
                          "ACTION", "disable",
                          NULL);
      } else {
        g_message("Disabling redologs");
      }
      m_query_critical(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", "DISABLE INNODB REDO LOG failed");
    }else{
      m_error("Disabling redologs is not supported for version %d.%d.%d", get_major(), get_secondary(), get_revision());
    }
  }
  if (machine_log_json_enabled()) {
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "start_database",
                      "EVENT", "restore_phase",
                      "PHASE", "start_database",
                      "STATUS", "started",
                      NULL);
  } else {
    g_message("start_database");
  }
  start_database(t);
  if (machine_log_json_enabled()) {
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "start_worker_schema",
                      "EVENT", "restore_phase",
                      "PHASE", "start_worker_schema",
                      "STATUS", "started",
                      NULL);
  } else {
    g_message("start_worker_schema");
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

  wait_schema_worker_to_finish(&conf);
  wait_worker_loader_main();
  enqueue_indexes_if_possible(&conf);
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
  trace("Starting checksums");
  gboolean checksum_ok=TRUE;
  if (machine_log_json) {
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "checksum verification started",
                      "EVENT", "checksum_run",
                      "PHASE", "checksum",
                      "STATUS", "started",
                      "MODE", checksum_mode == CHECKSUM_FAIL ? "fail" :
                              checksum_mode == CHECKSUM_WARN ? "warn" : "skip",
                      NULL);
  }
  tl=conf.table_list;
  struct db_table *dbt;
  while (tl != NULL){
    dbt=tl->data;
    if (!machine_log_json_enabled()) {
      g_message("DBT checksums: %s %s", dbt->database->target_database, dbt->source_table_name);
    }
    checksum_ok&=checksum_dbt(dbt->database->target_database, dbt->source_table_name, dbt->is_view, &dbt->checksum, conn);
    tl=tl->next;
  }
  trace("DBT checksums done");
  if (checksum_mode != CHECKSUM_SKIP) {
    GHashTableIter iter;
    gchar *lkey;
    g_hash_table_iter_init(&iter, database_hash);
    struct database *d= NULL;
    while (g_hash_table_iter_next(&iter, (gpointer *) &lkey, (gpointer *) &d)) {
      checksum_ok&=checksum_database(d->target_database, &d->checksum, conn);
    }
  }
  wait_restore_threads_to_close();

  if (!checksum_ok){
    if (machine_log_json) {
      machine_log_event(G_LOG_DOMAIN,
                        checksum_mode == CHECKSUM_WARN ? G_LOG_LEVEL_WARNING : G_LOG_LEVEL_CRITICAL,
                        "MESSAGE", "checksum verification failed",
                        "EVENT", "checksum_run",
                        "PHASE", "checksum",
                        "STATUS", "failed",
                        "MODE", checksum_mode == CHECKSUM_FAIL ? "fail" :
                                checksum_mode == CHECKSUM_WARN ? "warn" : "skip",
                        "RETRYABLE", "false",
                        "FATAL", checksum_mode == CHECKSUM_FAIL ? "true" : "false",
                        NULL);
    }
    if (checksum_mode==CHECKSUM_WARN)
      g_warning("Checksum failed");
    else
      g_error("Checksum failed");
  } else if (machine_log_json) {
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "checksum verification finished",
                      "EVENT", "checksum_run",
                      "PHASE", "checksum",
                      "STATUS", "finished",
                      "MODE", checksum_mode == CHECKSUM_FAIL ? "fail" :
                              checksum_mode == CHECKSUM_WARN ? "warn" : "skip",
                      NULL);
  }

  if (stream && no_delete == FALSE ){ //&& input_directory == NULL){
//    m_remove(directory,"metadata");
//    m_remove(directory, "metadata.header");
    if (g_rmdir(directory) < 0) {
      if (machine_log_json_enabled()) {
        machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_WARNING,
                          "MESSAGE", "Restore directory not removed",
                          "EVENT", "cleanup_directory",
                          "PHASE", "shutdown",
                          "STATUS", "failed",
                          "INPUT_DIRECTORY", directory,
                          "FATAL", "false",
                          "RETRYABLE", "false",
                          NULL);
      }
      g_warning("Restore directory not removed: %s (%s)", directory, strerror(errno));
    }
  }


  if (replication_statements->reset_replica){
    if (machine_log_json_enabled()) {
      machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                        "MESSAGE", "Sending reset replica",
                        "EVENT", "replication_command",
                        "PHASE", "replication",
                        "STATUS", "started",
                        "COMMAND", "reset_replica",
                        NULL);
    } else {
      g_message("Sending reset replica");
    }
    execute_replication_commands(conn,replication_statements->reset_replica->str);
  }

  if (replication_statements->change_replication_source){
    if (machine_log_json_enabled()) {
      machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                        "MESSAGE", "Sending change replication source",
                        "EVENT", "replication_command",
                        "PHASE", "replication",
                        "STATUS", "started",
                        "COMMAND", "change_replication_source",
                        NULL);
    } else {
      g_message("Sending change replication source");
    }
    execute_replication_commands(conn,replication_statements->change_replication_source->str);
  }

  if (replication_statements->gtid_purge){
    if (machine_log_json_enabled()) {
      machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                        "MESSAGE", "Sending GTID Purge",
                        "EVENT", "replication_command",
                        "PHASE", "replication",
                        "STATUS", "started",
                        "COMMAND", "gtid_purge",
                        NULL);
    } else {
      g_message("Sending GTID Purge");
    }
    execute_replication_commands(conn,replication_statements->gtid_purge->str);
  }

  if (replication_statements->start_replica){
    if (machine_log_json_enabled()) {
      machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                        "MESSAGE", "Sending start replica",
                        "EVENT", "replication_command",
                        "PHASE", "replication",
                        "STATUS", "started",
                        "COMMAND", "start_replica",
                        NULL);
    } else {
      g_message("Sending start replica");
    }
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
  gchar *summary_input_directory = directory != NULL ? g_strdup(directory) : g_strdup("");
  guint summary_tables = g_hash_table_size(conf.table_hash);
  guint summary_files = myloader_summary_get_files();
  guint64 summary_bytes = myloader_summary_get_bytes();
  guint64 summary_warnings = (guint64)machine_log_warning_count_get() + (guint64)detailed_errors.data_warnings;
  guint summary_retries = detailed_errors.retries;
  guint summary_skipped = detailed_errors.skip_errors;
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
    g_message("%s\t| %s\t| %s\t| `%s`.`%s`",print_time(diff1),print_time(diff2),print_time(diff1+diff2),dbt->target_database,dbt->source_table_name);
    tl=tl->next;
  }
*/

  if (key_file)  g_key_file_free(key_file);
  g_remove(fifo_directory);
  g_remove(load_data_tmp_directory);
  exit_code = errors ? EXIT_FAILURE : EXIT_SUCCESS;
  if (machine_log_json) {
    gchar *duration_ms = g_strdup_printf("%" G_GINT64_FORMAT,
                                         (g_get_monotonic_time() - process_started_at) / 1000);
    gchar *errors_text = g_strdup_printf("%u", errors);
    gchar *warnings_text = g_strdup_printf("%" G_GUINT64_FORMAT, summary_warnings);
    gchar *tables_text = g_strdup_printf("%u", summary_tables);
    gchar *files_text = g_strdup_printf("%u", summary_files);
    gchar *bytes_text = g_strdup_printf("%" G_GUINT64_FORMAT, summary_bytes);
    gchar *retries_text = g_strdup_printf("%u", summary_retries);
    gchar *skipped_text = g_strdup_printf("%u", summary_skipped);
    gchar *exit_code_text = g_strdup_printf("%d", exit_code);
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                     "MESSAGE", "restore completed",
                     "EVENT", "restore_completed",
                     "PHASE", "restore_finish",
                     "STATUS", "finished",
                     "MODE", "restore",
                     "INPUT_DIRECTORY", summary_input_directory,
                     "DURATION_MS", duration_ms,
                     "TABLES", tables_text,
                     "FILES", files_text,
                     "BYTES", bytes_text,
                     "ERRORS", errors_text,
                     "WARNINGS", warnings_text,
                     "RETRIES", retries_text,
                     "SKIPPED", skipped_text,
                     "EXIT_CODE", exit_code_text,
                     NULL);
    g_free(duration_ms);
    g_free(errors_text);
    g_free(warnings_text);
    g_free(tables_text);
    g_free(files_text);
    g_free(bytes_text);
    g_free(retries_text);
    g_free(skipped_text);
    g_free(exit_code_text);
  } else {
    g_message("Restore completed");
  }
  g_free(summary_input_directory);

  if (logoutfile) {
    fclose(logoutfile);
  }

  return exit_code;
}
