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
#if defined MARIADB_CLIENT_VERSION_STR && !defined MYSQL_SERVER_VERSION
#define MYSQL_SERVER_VERSION MARIADB_CLIENT_VERSION_STR
#endif

#include <string.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <glib-unix.h>
#include <locale.h>

#include "mydumper.h"
#include "mydumper_start_dump.h"
#include "mydumper_daemon_thread.h"
#include "mydumper_global.h"
#include "mydumper_arguments.h"
#include "mydumper_file_handler.h"
#include "../logging.h"

const char DIRECTORY[] = "export";

/* Program options */
gchar *output_directory = NULL;
gchar *dump_directory = NULL;
gboolean daemon_mode = FALSE;
gchar *disk_limits=NULL;
gboolean stream = FALSE;
gboolean no_delete = FALSE;

gboolean skip_constraints = FALSE;
gboolean skip_indexes = FALSE;
gboolean skip_metadata_sorting = FALSE;

//gboolean no_stream = FALSE;
// For daemon mode
gboolean shutdown_triggered = FALSE;

extern gboolean split_string_pk;
extern gboolean use_single_column;
extern guint max_time_per_select;
extern guint64 min_integer_chunk_step_size;
extern guint64 max_integer_chunk_step_size;
extern guint max_split_of_step_in_integer_chunk;
extern guint max_items_per_string_chunk;
extern guint max_char_size;
extern gchar *table_engine_for_view_dependency;
extern gchar *load_data_character_set;
extern guint ftwrl_timeout_retries;
extern guint ftwrl_max_wait_time;

void parse_disk_limits(){
  gchar ** strsplit = g_strsplit(disk_limits,":",3);
  if (g_strv_length(strsplit)!=2){
    m_critical("Parse limit failed");
  }
  set_disk_limits(atoi(strsplit[0]),atoi(strsplit[1]));
}

void print_help(){
  print_connection_help();

  print_list("regex",regex_list, NULL);
  print_string("database",source_db);
  print_string("ignore-engines",ignore_engines_str);
  print_string("where",where_option);
  print_int("updated-since",updated_since, updated_since==0);
  print_string("partition-regex",partition_regex);
  print_string("omit-from-file",tables_skiplist_file);
  print_string("tables-list",tables_list);

  print_string("tidb-snapshot",tidb_snapshot);
  print_string("sync-thread-lock-mode",syncthreadlockmode2str(sync_thread_lock_mode));
  print_bool("use-savepoints",use_savepoints);
  print_bool("no-backup-locks",no_backup_locks);
  print_int("trx-tables",trx_tables, FALSE);
  print_bool("no-trx-tables",trx_tables==0);
  print_bool("skip-ddl-locks",skip_ddl_locks);

  print_pmm_help();

  print_int("exec-threads",num_exec_threads, exec_command == NULL );
  print_string("exec",exec_command);
  print_string("exec-per-thread",exec_per_thread);
  print_string("exec-per-thread-extension",exec_per_thread_extension);

  print_int("long-query-retries",long_query_retries, long_query_retries==0 );
  print_int("long-query-retry-interval",long_query_retry_interval, long_query_retry_interval==0);
  print_int("long-query-guard",long_query, long_query==0);
  print_bool("kill-long-queries",killqueries);

  print_int("max-time-per-select", max_time_per_select, FALSE);
  print_int("max-threads-per-table", max_threads_per_table, FALSE);
  print_bool("use-single-column", use_single_column);
  print_bool("split-string-pk", split_string_pk);
  print_string("rows",g_strdup_printf("%"G_GUINT64_FORMAT":%"G_GUINT64_FORMAT":%"G_GUINT64_FORMAT,min_chunk_step_size, starting_chunk_step_size, max_chunk_step_size));
  print_string("rows-hard",g_strdup_printf("%"G_GUINT64_FORMAT":%"G_GUINT64_FORMAT,min_integer_chunk_step_size, max_integer_chunk_step_size));
  print_int("max-split-of-step-in-integer-chunk", max_split_of_step_in_integer_chunk, FALSE);
  print_int("max-char-size", max_char_size, FALSE);
  print_int("max-items-per-string-chunk", max_items_per_string_chunk, FALSE);
  print_bool("split-partitions",split_partitions);

  print_checksum_help();

  print_bool("no-schemas",no_schemas);
  print_bool("all-tablespaces",dump_tablespaces);
  print_bool("no-data",no_data);
  print_bool("triggers",dump_triggers);
  print_bool("events",dump_events);
  print_bool("routines",dump_routines);
  print_bool("skip-constraints",skip_constraints);
  print_bool("skip-indexes",skip_indexes);
  print_bool("skip-metadata-sorting",skip_metadata_sorting);
  print_bool("views-as-tables",views_as_tables);
  print_bool("no-views",no_dump_views);

  print_string("format", outputformat2str(output_format));
  print_bool("include-header",include_header);
  print_string("fields-terminated-by",fields_terminated_by_ld);
  print_string("fields-enclosed-by",fields_enclosed_by_ld);
  print_string("fields-escaped-by",fields_escaped_by);
  print_string("lines-starting-by",lines_starting_by_ld);
  print_string("lines-terminated-by",lines_terminated_by_ld);
  print_string("statement-terminated-by",statement_terminated_by_ld);
  print_bool("insert-ignore",insert_ignore);
  print_bool("replace",replace);
  print_bool("complete-insert",complete_insert);
  print_bool("hex-blob",hex_blob);
  print_bool("skip-definer",skip_definer);
  print_string("replace-definer",replace_definer);
  print_int("statement-size",statement_size, FALSE);
  print_bool("tz-utc",skip_tz);
  print_bool("skip-tz-utc",skip_tz);
  print_string("set-names", set_names_in_conn_by_default || set_names_in_conn_for_sct ? g_strdup_printf("%s,%s",set_names_in_conn_for_sct,set_names_in_conn_by_default):NULL);
  print_string("default-character-set", set_names_in_file_by_default || set_names_in_file_for_sct ? g_strdup_printf("%s,%s",set_names_in_file_for_sct,set_names_in_file_by_default):NULL);
  print_string("load-data-character-set", load_data_character_set);
  print_string("table-engine-for-view-dependency",table_engine_for_view_dependency);

  print_int("chunk-filesize",chunk_filesize, chunk_filesize==0);
  print_bool("exit-if-broken-table-found",exit_if_broken_table_found);
  print_bool("build-empty-files",build_empty_files);
  print_bool("no-check-generated-fields",ignore_generated_fields);
  print_bool("bulk-metadata-prefetch",bulk_metadata_prefetch);
  print_bool("order-by-primary",order_by_primary_key);
  print_bool("compact",compact);
  print_bool("compress",compress_method!=NULL);
  print_bool("use-defer",use_defer);
  print_bool("check-row-count",check_row_count);

  print_bool("daemon",daemon_mode);
  print_int("snapshot-interval",snapshot_interval, !daemon_mode);
  print_int("snapshot-count",snapshot_count, !daemon_mode);

  print_bool("help",help);
  print_string("outputdir",output_directory);
  print_bool("clear",clear_dumpdir);
  print_bool("dirty",dirty_dumpdir);
  print_bool("merge",merge_dumpdir);
  print_bool("stream",stream);
  print_string("logfile",logfile);
  print_string("disk-limits",disk_limits);
  print_bool("masquerade-filename", masquerade_filename);
  print_int("ftwrl-max-wait-time", ftwrl_max_wait_time, FALSE);
  print_int("ftwrl-timeout-retries", ftwrl_timeout_retries, ftwrl_timeout_retries==0);
  print_list("replica-data", build_list_from_replica_options(&replica_data), NULL);
  print_list("source-data", build_list_from_replica_options(&source_data), NULL);

  print_common();
  exit(EXIT_SUCCESS);
}

void initialize_directories(){
  if (!output_directory_str){
    GDateTime * datetime = g_date_time_new_now_local();
    char *datetimestr;
    datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");
    output_directory = g_strdup_printf("%s-%s", DIRECTORY, datetimestr);
    g_free(datetimestr);
    g_date_time_unref(datetime);
  }else{
    output_directory=output_directory_str;
  }
}

int main(int argc, char *argv[]) {


  GError *error = NULL;
  GOptionContext *context;
  gint64 process_started_at = g_get_monotonic_time();
  gint exit_code = EXIT_SUCCESS;

  setlocale(LC_ALL, "");
  g_thread_init(NULL);
  set_thread_name("MNT");



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
  initialize_common_options(context, "mydumper");

  if (tmpargc > 1 ){
    int pos=0;
    stream=TRUE;
    source_db=strdup(tmpargv[1]);
    if (tmpargc > 2 ){
      GString *s = g_string_new(tmpargv[2]);
      for (pos=3; pos<tmpargc;pos++){
        g_string_append_printf(s,",%s",tmpargv[pos]);
      }
      tables_list=g_strdup(s->str);
      g_string_free(s, TRUE);
    }
  }
  g_strfreev(tmpargv);

  // TODO: This must be removed when --csv and --load-data are deprecated
  if (load_data){
    output_format=LOAD_DATA;
		rows_file_extension=DAT;
  }
  if (csv){
    output_format=CSV;
		rows_file_extension=DAT;
  }

  initialize_directories();

  if (disk_limits!=NULL){
    parse_disk_limits();
  }

  if (num_threads < 2) {
    use_defer= FALSE;
  }

  if ((exec_per_thread_extension==NULL) && (exec_per_thread != NULL))
    m_critical("--exec-per-thread-extension needs to be set when --exec-per-thread (%s) is used", exec_per_thread);
  if ((exec_per_thread_extension!=NULL) && (exec_per_thread == NULL))
    m_critical("--exec-per-thread needs to be set when --exec-per-thread-extension (%s) is used", exec_per_thread_extension);

  if (compress_method==NULL && exec_per_thread==NULL) {
    exec_per_thread_extension=EMPTY_STRING;
  }else{
    set_pipe_backup();

    if (compress_method!=NULL && exec_per_thread!=NULL )
      m_critical("--compression and --exec-per-thread are not comptatible");

    if (compress_method){
      if ( g_ascii_strcasecmp(compress_method,GZIP)==0){
        exec_per_thread=g_strdup_printf("%s -c", GZIP);
        exec_per_thread_extension=GZIP_EXTENSION;
      }else if (g_ascii_strcasecmp(compress_method,ZSTD)==0){
        exec_per_thread=g_strdup_printf("%s -c", ZSTD);
        exec_per_thread_extension=ZSTD_EXTENSION;
      }
    }

    exec_per_thread_cmd=g_strsplit(exec_per_thread, " ", 0);
    gchar *tmpcmd=g_find_program_in_path(exec_per_thread_cmd[0]);
    if (!tmpcmd)
      m_critical("%s was not found in PATH, use --exec-per-thread for non default locations",exec_per_thread_cmd[0]);
    exec_per_thread_cmd[0]=tmpcmd;
  }

  initialize_set_names();

  if (debug) {
    set_debug();
    verbose=4;
  }

  if (help)
    printf("%s", g_option_context_get_help (context, FALSE, NULL));

  if (program_version) {
    print_version("mydumper");
    if (!help)
      exit(EXIT_SUCCESS);
    printf("\n");
  }

  if (help)
    print_help();

  set_verbose(verbose);

  if (machine_log_json) {
    gchar *threads_text = g_strdup_printf("%u", num_threads);
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                     "MESSAGE", "effective dump configuration loaded",
                     "EVENT", "process_config",
                     "PHASE", "startup",
                     "STATUS", "started",
                     "MODE", "dump",
                     "OUTPUT_DIRECTORY", output_directory != NULL ? output_directory : "",
                     "THREADS", threads_text,
                     "STREAM", stream ? "true" : "false",
                     "LOGFILE", logfile != NULL ? logfile : "",
                     "SOURCE_DB", source_db != NULL ? source_db : "",
                     "TABLES_LIST", tables_list != NULL ? tables_list : "",
                     NULL);
    g_free(threads_text);
  }

  if (machine_log_json_enabled()) {
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                      "MESSAGE", "MyDumper backup version",
                      "EVENT", "process_version",
                      "PHASE", "startup",
                      "STATUS", "started",
                      "VERSION", VERSION,
                      NULL);
  } else {
    g_message("MyDumper backup version: %s", VERSION);
  }

  // Startmodifying file in disk, creating objects and backup

  hide_password(argc, argv);
  ask_password();

  initialize_pmm();

  create_dir(output_directory);

  if (daemon_mode) {
    clear_dumpdir= TRUE;
    initialize_daemon_thread();
    run_daemon();
  }else{
    dump_directory = output_directory;
    struct configuration conf;
    start_pmm_thread((void *)&conf);
    start_dump(&conf, context);
  }

  free_set_names();

  g_option_context_free(context);
  gchar *summary_output_directory = output_directory != NULL ? g_strdup(output_directory) : g_strdup("");
  g_free(output_directory);
//  g_strfreev(tables);

  free_log_handlers();

  if (key_file)  g_key_file_free(key_file);
//  g_strfreev(argv);
  exit_code = errors ? EXIT_FAILURE : EXIT_SUCCESS;
  if (machine_log_json) {
    gchar *duration_ms = g_strdup_printf("%" G_GINT64_FORMAT,
                                         (g_get_monotonic_time() - process_started_at) / 1000);
    gchar *errors_text = g_strdup_printf("%u", errors);
    gchar *warnings_text = g_strdup_printf("%u", machine_log_warning_count_get());
    gchar *tables_text = g_strdup_printf("%u", dump_summary_get_tables());
    gchar *files_text = g_strdup_printf("%u", dump_summary_get_files());
    gchar *bytes_text = g_strdup_printf("%" G_GUINT64_FORMAT, dump_summary_get_bytes());
    gchar *retries_text = g_strdup_printf("%u", dump_summary_get_retries());
    gchar *skipped_text = g_strdup_printf("%u", dump_summary_get_skipped());
    gchar *exit_code_text = g_strdup_printf("%d", exit_code);
    machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_MESSAGE,
                     "MESSAGE", "dump completed",
                     "EVENT", "dump_completed",
                     "PHASE", "dump_finish",
                     "STATUS", "finished",
                     "MODE", "dump",
                     "OUTPUT_DIRECTORY", summary_output_directory,
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
  }
  if (logoutfile) {
    fclose(logoutfile);
  }
  g_free(summary_output_directory);
  exit(exit_code);
}
