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

//gboolean no_stream = FALSE;
// For daemon mode
gboolean shutdown_triggered = FALSE;

void parse_disk_limits(){
  gchar ** strsplit = g_strsplit(disk_limits,":",3);
  if (g_strv_length(strsplit)!=2){
    m_critical("Parse limit failed");
  }
  set_disk_limits(atoi(strsplit[0]),atoi(strsplit[1]));
}

int main(int argc, char *argv[]) {
  GError *error = NULL;
  GOptionContext *context;

  g_thread_init(NULL);
  setlocale(LC_ALL, "");
  context = load_contex_entries();

  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    m_critical("option parsing failed: %s, try --help\n", error->message);
  }

  // TODO: This must be removed when --csv and --load-data are deprecated
  if (load_data){
    output_format=LOAD_DATA;
		rows_file_extension=DAT;
  }
  if (csv){
    output_format=CSV;
		rows_file_extension=DAT;
  }

  if (help){
    printf("%s", g_option_context_get_help (context, FALSE, NULL));
//    exit(EXIT_SUCCESS);
  }

  if (program_version) {
    print_version("mydumper");
    if (!help)
      exit(EXIT_SUCCESS);
    printf("\n");
  }

  if (tmpargc > 1 ){
    int pos=0;
    stream=TRUE;
    db=strdup(tmpargv[1]);
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

  if (debug) {
    set_debug();
    set_verbose(3);
  } else {
    set_verbose(verbose);
  }

  g_message("MyDumper backup version: %s", VERSION);

  // Loading the defaults file:
  initialize_common_options(context, "mydumper");
//  initialize_start_dump();

  hide_password(argc, argv);
  ask_password();

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

  if (help){
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
    print_string("database",db);
    print_string("ignore-engines",ignore_engines_str);
    print_string("where",where_option);
    print_int("updated-since",updated_since);
    print_string("partition-regex",partition_regex);
    print_string("omit-from-file",tables_skiplist_file);
    print_string("tables-list",tables_list);
    print_string("tidb-snapshot",tidb_snapshot);
    print_bool("use-savepoints",use_savepoints);
    print_bool("no-backup-locks",no_backup_locks);
    print_int("trx-tables",trx_tables);
    print_bool("skip-ddl-locks",skip_ddl_locks);
    print_string("pmm-path",pmm_path);
    print_string("pmm-resolution",pmm_resolution);
    print_int("exec-threads",num_exec_threads);
    print_string("exec",exec_command);
    print_string("exec-per-thread",exec_per_thread);
    print_string("exec-per-thread-extension",exec_per_thread_extension);
    print_int("long-query-retries",longquery_retries);
    print_int("long-query-retry-interval",longquery_retry_interval);
    print_int("long-query-guard",longquery);
    print_bool("kill-long-queries",killqueries);
    print_int("max-threads-per-table",max_threads_per_table);
//    print_string("char-deep",);
//    print_string("char-chunk",);
    print_string("rows",g_strdup_printf("%"G_GUINT64_FORMAT":%"G_GUINT64_FORMAT":%"G_GUINT64_FORMAT,min_chunk_step_size, starting_chunk_step_size, max_chunk_step_size));
    print_bool("split-partitions",split_partitions);
    print_bool("checksum-all",dump_checksums);
    print_bool("data-checksums",data_checksums);
    print_bool("schema-checksums",schema_checksums);
    print_bool("routine-checksums",routine_checksums);
    print_bool("no-schemas",no_schemas);
    print_bool("all-tablespaces",dump_tablespaces);
    print_bool("no-data",no_data);
    print_bool("triggers",dump_triggers);
    print_bool("events",dump_events);
    print_bool("routines",dump_routines);
    print_bool("views-as-tables",views_as_tables);
    print_bool("no-views",no_dump_views);
    print_bool("load-data",load_data);
    print_bool("csv",csv);
    print_bool("clickhouse",clickhouse);
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
    print_int("statement-size",statement_size);
    print_bool("tz-utc",skip_tz);
    print_bool("skip-tz-utc",skip_tz);
    print_string("set-names",set_names_str);
    print_int("chunk-filesize",chunk_filesize);
    print_bool("exit-if-broken-table-found",exit_if_broken_table_found);
    print_bool("build-empty-files",build_empty_files);
    print_bool("no-check-generated-fields",ignore_generated_fields);
    print_bool("order-by-primary",order_by_primary_key);
    print_bool("compact",compact);
    print_bool("compress",compress_method!=NULL);
    print_bool("use-defer",use_defer);
    print_bool("check-row-count",check_row_count);
    print_bool("daemon",daemon_mode);
    print_int("snapshot-interval",snapshot_interval);
    print_int("snapshot-count",snapshot_count);
    print_bool("help",help);
    print_string("outputdir",output_directory);
    print_bool("clear",clear_dumpdir);
    print_bool("dirty",dirty_dumpdir);
    print_bool("merge",merge_dumpdir);
    print_bool("stream",stream);
    print_string("logfile",logfile);
    print_string("disk-limits",disk_limits);
    print_int("threads",num_threads);
    print_bool("version",program_version);
    print_bool("verbose",verbose);
    print_bool("debug",debug);
    print_string("defaults-file",defaults_file);
    print_string("defaults-extra-file",defaults_extra_file);
    exit(EXIT_SUCCESS);
  }
  
  create_dir(output_directory);

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


  if (daemon_mode) {
    clear_dumpdir= TRUE;
    initialize_daemon_thread();
    run_daemon();
  }else{
    dump_directory = output_directory;
    start_dump();
  }

  if (logoutfile) {
    fclose(logoutfile);
  }

  g_option_context_free(context);
  g_free(output_directory);
//  g_strfreev(tables);

  free_log_handlers();

  if (key_file)  g_key_file_free(key_file);
//  g_strfreev(argv);
  exit(errors ? EXIT_FAILURE : EXIT_SUCCESS);
}

