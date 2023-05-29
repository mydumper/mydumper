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
//#include "common_options.h"
#include "mydumper_global.h"
#include <glib/gstdio.h>
#include <gio/gio.h>
#include "common.h"
#include "connection.h"
#include "regex.h"
#include "mydumper_arguments.h"
const gchar *compress_method=NULL;

gboolean arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (g_strstr_len(option_name,10,"--compress") || g_strstr_len(option_name,2,"-c")){
    if (value==NULL || g_strstr_len(value,4,GZIP)){
      compress_method=GZIP;
      return TRUE;
    }
    if (g_strstr_len(value,4,ZSTD)){
      compress_method=ZSTD;
      return TRUE;
    }
  }
  return FALSE;
}

static GOptionEntry entries[] = {
    {"help", '?', 0, G_OPTION_ARG_NONE, &help, "Show help options", NULL},
    {"outputdir", 'o', 0, G_OPTION_ARG_FILENAME, &output_directory_param,
     "Directory to output files to", NULL},
    {"stream", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &stream_arguments_callback,
     "It will stream over STDOUT once the files has been written. Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given", NULL},
//    {"no-delete", 0, 0, G_OPTION_ARG_NONE, &no_delete,
//      "It will not delete the files after stream has been completed. It will be depercated and removed after v0.12.7-1. Used --stream", NULL},
    {"logfile", 'L', 0, G_OPTION_ARG_FILENAME, &logfile,
     "Log file name to use, by default stdout is used", NULL},
    { "disk-limits", 0, 0, G_OPTION_ARG_STRING, &disk_limits,
      "Set the limit to pause and resume if determines there is no enough disk space."
      "Accepts values like: '<resume>:<pause>' in MB."
      "For instance: 100:500 will pause when there is only 100MB free and will"
      "resume if 500MB are available", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry extra_entries[] = {
    {"chunk-filesize", 'F', 0, G_OPTION_ARG_INT, &chunk_filesize,
     "Split tables into chunks of this output file size. This value is in MB",
     NULL},
    {"exit-if-broken-table-found", 0, 0, G_OPTION_ARG_NONE, &exit_if_broken_table_found,
      "Exits if a broken table has been found", NULL},
    {"success-on-1146", 0, 0, G_OPTION_ARG_NONE, &success_on_1146,
     "Not increment error count and Warning instead of Critical in case of "
     "table doesn't exist",
     NULL},
    {"build-empty-files", 'e', 0, G_OPTION_ARG_NONE, &build_empty_files,
     "Build dump files even if no data available from table", NULL},
    { "no-check-generated-fields", 0, 0, G_OPTION_ARG_NONE, &ignore_generated_fields,
      "Queries related to generated fields are not going to be executed."
      "It will lead to restoration issues if you have generated columns", NULL },
    {"order-by-primary", 0, 0, G_OPTION_ARG_NONE, &order_by_primary_key,
     "Sort the data by Primary Key or Unique key if no primary key exists",
     NULL},
    {"compress", 'c', G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "Compress output files using: /usr/bin/gzip and /usr/bin/zstd. Options: GZIP and ZSTD. Default: GZIP", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry lock_entries[] = {
    {"tidb-snapshot", 'z', 0, G_OPTION_ARG_STRING, &tidb_snapshot,
     "Snapshot to use for TiDB", NULL},

    {"no-locks", 'k', 0, G_OPTION_ARG_NONE, &no_locks,
     "Do not execute the temporary shared read lock.  WARNING: This will cause "
     "inconsistent backups",
     NULL},
    {"use-savepoints", 0, 0, G_OPTION_ARG_NONE, &use_savepoints,
     "Use savepoints to reduce metadata locking issues, needs SUPER privilege",
     NULL},
    {"no-backup-locks", 0, 0, G_OPTION_ARG_NONE, &no_backup_locks,
     "Do not use Percona backup locks", NULL},
    {"lock-all-tables", 0, 0, G_OPTION_ARG_NONE, &lock_all_tables,
     "Use LOCK TABLE for all, instead of FTWRL", NULL},
    {"less-locking", 0, 0, G_OPTION_ARG_NONE, &less_locking,
     "Minimize locking time on InnoDB tables.", NULL},
    {"trx-consistency-only", 0, 0, G_OPTION_ARG_NONE, &trx_consistency_only,
     "Transactional consistency only", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


static GOptionEntry query_running_entries[] = {
    {"long-query-retries", 0, 0, G_OPTION_ARG_INT, &longquery_retries,
     "Retry checking for long queries, default 0 (do not retry)", NULL},
    {"long-query-retry-interval", 0, 0, G_OPTION_ARG_INT, &longquery_retry_interval,
     "Time to wait before retrying the long query check in seconds, default 60", NULL},
    {"long-query-guard", 'l', 0, G_OPTION_ARG_INT, &longquery,
     "Set long query timer in seconds, default 60", NULL},
    {"kill-long-queries", 'K', 0, G_OPTION_ARG_NONE, &killqueries,
     "Kill long running queries (instead of aborting)", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry exec_entries[] = {
    {"exec-threads", 0, 0, G_OPTION_ARG_INT, &num_exec_threads,
     "Amount of threads to use with --exec", NULL},
    {"exec", 0, 0, G_OPTION_ARG_STRING, &exec_command,
      "Command to execute using the file as parameter", NULL},
    {"exec-per-thread",0, 0, G_OPTION_ARG_STRING, &exec_per_thread,
     "Set the command that will receive by STDIN and write in the STDOUT into the output file", NULL},
    {"exec-per-thread-extension",0, 0, G_OPTION_ARG_STRING, &exec_per_thread_extension,
     "Set the extension for the STDOUT file when --exec-per-thread is used", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry pmm_entries[] = {
    { "pmm-path", 0, 0, G_OPTION_ARG_STRING, &pmm_path,
      "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution", NULL },
    { "pmm-resolution", 0, 0, G_OPTION_ARG_STRING, &pmm_resolution,
      "which default will be high", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


static GOptionEntry daemon_entries[] = {
    {"daemon", 'D', 0, G_OPTION_ARG_NONE, &daemon_mode, "Enable daemon mode",
     NULL},
    {"snapshot-interval", 'I', 0, G_OPTION_ARG_INT, &snapshot_interval,
     "Interval between each dump snapshot (in minutes), requires --daemon, "
     "default 60",
     NULL},
    {"snapshot-count", 'X', 0, G_OPTION_ARG_INT, &snapshot_count, "number of snapshots, default 2", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


static GOptionEntry chunks_entries[] = {
    {"max-rows", 0, 0, G_OPTION_ARG_INT64, &max_rows,
     "Limit the number of rows per block after the table is estimated, default 1000000. It has been deprecated, use --rows instead. Removed in future releases", NULL},
    {"char-deep", 0, 0, G_OPTION_ARG_INT64, &char_deep,
     "",NULL},
    {"char-chunk", 0, 0, G_OPTION_ARG_INT64, &char_chunk,
     "",NULL},
    {"rows", 'r', 0, G_OPTION_ARG_STRING, &rows_per_chunk,
     "Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds",
     NULL},
    { "split-partitions", 0, 0, G_OPTION_ARG_NONE, &split_partitions,
      "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
};


static GOptionEntry checksum_entries[] = {
    {"checksum-all", 'M', 0, G_OPTION_ARG_NONE, &dump_checksums,
     "Dump checksums for all elements", NULL},
    {"data-checksums", 0, 0, G_OPTION_ARG_NONE, &data_checksums,
     "Dump table checksums with the data", NULL},
    {"schema-checksums", 0, 0, G_OPTION_ARG_NONE, &schema_checksums,
     "Dump schema table and view creation checksums", NULL},
    {"routine-checksums", 0, 0, G_OPTION_ARG_NONE, &routine_checksums,
     "Dump triggers, functions and routines checksums", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry filter_entries[] = {
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db, "Database to dump", NULL},
    {"ignore-engines", 'i', 0, G_OPTION_ARG_STRING, &ignore_engines,
     "Comma delimited list of storage engines to ignore", NULL},
    { "where", 0, 0, G_OPTION_ARG_STRING, &where_option,
      "Dump only selected records.", NULL },
    {"updated-since", 'U', 0, G_OPTION_ARG_INT, &updated_since,
     "Use Update_time to dump only tables updated in the last U days", NULL},
    { "partition-regex", 0, 0, G_OPTION_ARG_STRING, &partition_regex,
      "Regex to filter by partition name.", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry objects_entries[] = {
    {"no-schemas", 'm', 0, G_OPTION_ARG_NONE, &no_schemas,
      "Do not dump table schemas with the data and triggers", NULL},
    {"all-tablespaces", 'Y', 0 , G_OPTION_ARG_NONE, &dump_tablespaces,
    "Dump all the tablespaces.", NULL},
    {"no-data", 'd', 0, G_OPTION_ARG_NONE, &no_data, "Do not dump table data",
     NULL},
    {"triggers", 'G', 0, G_OPTION_ARG_NONE, &dump_triggers, "Dump triggers. By default, it do not dump triggers",
     NULL},
    {"events", 'E', 0, G_OPTION_ARG_NONE, &dump_events, "Dump events. By default, it do not dump events", NULL},
    {"routines", 'R', 0, G_OPTION_ARG_NONE, &dump_routines,
     "Dump stored procedures and functions. By default, it do not dump stored procedures nor functions", NULL},
    {"views-as-tables", 0, 0, G_OPTION_ARG_NONE, &views_as_tables, "Export VIEWs as they were tables",
     NULL},
    {"no-views", 'W', 0, G_OPTION_ARG_NONE, &no_dump_views, "Do not dump VIEWs",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


static GOptionEntry statement_entries[] = {
    {"load-data", 0, 0, G_OPTION_ARG_NONE, &load_data,
     "", NULL },
    { "csv", 0, 0, G_OPTION_ARG_NONE, &csv,
      "Automatically enables --load-data and set variables to export in CSV format.", NULL },
    {"fields-terminated-by", 0, 0, G_OPTION_ARG_STRING, &fields_terminated_by_ld,"", NULL },
    {"fields-enclosed-by", 0, 0, G_OPTION_ARG_STRING, &fields_enclosed_by_ld,"", NULL },
    {"fields-escaped-by", 0, 0, G_OPTION_ARG_STRING, &fields_escaped_by,
      "Single character that is going to be used to escape characters in the"
      "LOAD DATA stament, default: '\\' ", NULL },
    {"lines-starting-by", 0, 0, G_OPTION_ARG_STRING, &lines_starting_by_ld,
      "Adds the string at the begining of each row. When --load-data is used"
      "it is added to the LOAD DATA statement. Its affects INSERT INTO statements"
      "also when it is used.", NULL },
    {"lines-terminated-by", 0, 0, G_OPTION_ARG_STRING, &lines_terminated_by_ld,
      "Adds the string at the end of each row. When --load-data is used it is"
       "added to the LOAD DATA statement. Its affects INSERT INTO statements"
       "also when it is used.", NULL },
    {"statement-terminated-by", 0, 0, G_OPTION_ARG_STRING, &statement_terminated_by_ld,
      "This might never be used, unless you know what are you doing", NULL },
    {"insert-ignore", 'N', 0, G_OPTION_ARG_NONE, &insert_ignore,
     "Dump rows with INSERT IGNORE", NULL},
    {"replace", 0, 0 , G_OPTION_ARG_NONE, &replace,
     "Dump rows with REPLACE", NULL},
    {"complete-insert", 0, 0, G_OPTION_ARG_NONE, &complete_insert,
     "Use complete INSERT statements that include column names", NULL},
    {"hex-blob", 0, 0, G_OPTION_ARG_NONE, &hex_blob,
      "Dump binary columns using hexadecimal notation", NULL},
    {"skip-definer", 0, 0, G_OPTION_ARG_NONE, &skip_definer,
     "Removes DEFINER from the CREATE statement. By default, statements are not modified", NULL},
    {"statement-size", 's', 0, G_OPTION_ARG_INT, &statement_size,
     "Attempted size of INSERT statement in bytes, default 1000000", NULL},
    {"tz-utc", 0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, &skip_tz,
     "SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data "
     "when a server has data in different time zones or data is being moved "
     "between servers with different time zones, defaults to on use "
     "--skip-tz-utc to disable.",
     NULL},
    {"skip-tz-utc", 0, 0, G_OPTION_ARG_NONE, &skip_tz, "", NULL},
    { "set-names",0, 0, G_OPTION_ARG_STRING, &set_names_str,
      "Sets the names, use it at your own risk, default binary", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


GOptionContext * load_contex_entries(){

  GOptionContext *context = g_option_context_new("multi-threaded MySQL dumping");

  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);

  GOptionGroup *connection_group = load_connection_entries(context);
  g_option_group_add_entries(connection_group, common_connection_entries);

  GOptionGroup *filter_group = load_regex_entries(context);
  g_option_group_add_entries(filter_group, filter_entries);
  g_option_group_add_entries(filter_group, common_filter_entries);

  GOptionGroup *lock_group=g_option_group_new("lock", "Lock Options", "Lock Options", NULL, NULL);
  g_option_group_add_entries(lock_group, lock_entries);
  g_option_context_add_group(context, lock_group);

  GOptionGroup *pmm_group=g_option_group_new("pmm", "PMM Options", "PMM Options", NULL, NULL);
  g_option_group_add_entries(pmm_group, pmm_entries);
  g_option_context_add_group(context, pmm_group);

  GOptionGroup *exec_group=g_option_group_new("exec", "Exec Options", "Exec Options", NULL, NULL);
  g_option_group_add_entries(exec_group, exec_entries);
  g_option_context_add_group(context, exec_group);

  GOptionGroup *query_running_group=g_option_group_new("query_running", "If long query running found:", "If long query running found:", NULL, NULL);
  g_option_group_add_entries(query_running_group, query_running_entries);
  g_option_context_add_group(context, query_running_group);

  GOptionGroup *chunks_group=g_option_group_new("job", "Job Options", "Job Options", NULL, NULL);
  g_option_group_add_entries(chunks_group, chunks_entries);
  g_option_context_add_group(context, chunks_group);

  GOptionGroup *checksum_group=g_option_group_new("checksum", "Checksum Options", "Checksum Options", NULL, NULL);
  g_option_group_add_entries(checksum_group, checksum_entries);
  g_option_context_add_group(context, checksum_group);

  GOptionGroup *objects_group=g_option_group_new("objects", "Objects Options", "Objects Options", NULL, NULL);
  g_option_group_add_entries(objects_group, objects_entries);
  g_option_context_add_group(context, objects_group);

  GOptionGroup *statement_group=g_option_group_new("statement", "Statement Options", "Statement Options", NULL, NULL);
  g_option_group_add_entries(statement_group, statement_entries);
  g_option_context_add_group(context, statement_group);

  GOptionGroup *extra_group=g_option_group_new("extra", "Extra Options", "Extra Options", NULL, NULL);
  g_option_group_add_entries(extra_group, extra_entries);
  g_option_context_add_group(context, extra_group);

  GOptionGroup *daemon_group = g_option_group_new("daemongroup", "Daemon Options", "daemon", NULL, NULL);
  g_option_group_add_entries(daemon_group, daemon_entries);
  g_option_context_add_group(context, daemon_group);

  g_option_context_set_help_enabled(context, FALSE);

  g_option_context_set_main_group(context, main_group);

return context;

}



