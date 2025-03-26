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

#include <glib/gstdio.h>
#include <gio/gio.h>

#include "mydumper.h"
#include "mydumper_global.h"
#include "mydumper_arguments.h"
#include "mydumper_common.h"

extern guint64 min_integer_chunk_step_size;
extern guint64 max_integer_chunk_step_size;

enum sync_thread_lock_mode sync_thread_lock_mode=AUTO;
const gchar *compress_method=NULL;
gboolean split_integer_tables=TRUE;
const gchar *rows_file_extension=SQL;
guint output_format=SQL_INSERT;
gchar *output_directory_str = NULL;
gboolean masquerade_filename=FALSE;
gboolean trx_tables=FALSE;
gboolean use_single_column=FALSE;
const gchar *table_engine_for_view_dependency=MEMORY;


gboolean arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
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
  if (g_strstr_len(option_name,11,"--rows-hard")){
    if (value!=NULL){
      if (!parse_rows_per_chunk(value, &min_integer_chunk_step_size,  &max_integer_chunk_step_size, &max_integer_chunk_step_size, "Invalid option on --rows-hard")){
      }
      return TRUE;
    }
    return FALSE;
  }
  if (g_strstr_len(option_name,6,"--rows") || g_strstr_len(option_name,2,"-r")){
    if (value!=NULL)
      split_integer_tables=parse_rows_per_chunk(value, &min_chunk_step_size, &starting_chunk_step_size, &max_chunk_step_size, "Invalid option on --rows");
    return TRUE;
  }
  if (g_strstr_len(option_name,8,"--format")){
    if (value==NULL)
      return FALSE;
    if (!g_ascii_strcasecmp(value,INSERT_ARG)){
			output_format=SQL_INSERT;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,LOAD_DATA_ARG)){
      load_data=TRUE;
      rows_file_extension=DAT;
			output_format=LOAD_DATA;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,CSV_ARG)){
      csv=TRUE;
      rows_file_extension=DAT;
			output_format=CSV;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,CLICKHOUSE_ARG)){
      clickhouse=TRUE;
      rows_file_extension=DAT;
			output_format=CLICKHOUSE;
      return TRUE;
    }
  }
  if (!strcmp(option_name,"--trx-consistency-only")){
    m_critical("--lock-all-tables is deprecated use --trx-tables instead");
  }
  if (!strcmp(option_name,"--less-locking")){
    m_critical("--less-locking is deprecated and its behaviour is the default which is useful if you don't have transaction tables. Use --trx-tables otherwise");
  }
  if (!strcmp(option_name,"--lock-all-tables")){
    m_critical("--lock-all-tables is deprecated use --sync-thread-lock-mode instead");
  }
  if (!strcmp(option_name,"--no-locks")){
    m_critical("--no-locks is deprecated use --sync-thread-lock-mode instead");
  }
  if (!strcmp(option_name,"--sync-thread-lock-mode")){
    if (!g_ascii_strcasecmp(value,"AUTO")){
      sync_thread_lock_mode=AUTO;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,"FTWRL")){
      sync_thread_lock_mode=FTWRL;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,"LOCK_ALL")){
      sync_thread_lock_mode=LOCK_ALL;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,"GTID")){
      sync_thread_lock_mode=GTID;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,"NO_LOCK")){
      sync_thread_lock_mode=NO_LOCK;
      return TRUE;
    }
  }
  if (!strcmp(option_name,"--success-on-1146")){
    ignore_errors_list=g_list_append(ignore_errors_list,GINT_TO_POINTER(1146));
    success_on_1146=TRUE;
    return TRUE;
  }

  return common_arguments_callback(option_name, value, data, error);
}

static GOptionEntry entries[] = {
    {"help", '?', 0, G_OPTION_ARG_NONE, &help, "Show help options", NULL},
    {"outputdir", 'o', 0, G_OPTION_ARG_FILENAME, &output_directory_str,
     "Directory to output files to", NULL},
    {"clear", 0, 0, G_OPTION_ARG_NONE, &clear_dumpdir,
     "Clear output directory before dumping", NULL},
    {"dirty", 0, 0, G_OPTION_ARG_NONE, &dirty_dumpdir,
     "Overwrite output directory without clearing (beware of leftower chunks)", NULL},
    {"stream", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &stream_arguments_callback,
     "It will stream over STDOUT once the files has been written. Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given and also NO_STREAM since v0.16.3-1", NULL},
    {"logfile", 'L', 0, G_OPTION_ARG_FILENAME, &logfile,
     "Log file name to use, by default stdout is used", NULL},
    { "disk-limits", 0, 0, G_OPTION_ARG_STRING, &disk_limits,
      "Set the limit to pause and resume if determines there is no enough disk space."
      "Accepts values like: '<resume>:<pause>' in MB."
      "For instance: 100:500 will pause when there is only 100MB free and will"
      "resume if 500MB are available", NULL },
    { "masquerade-filename", 0, 0, G_OPTION_ARG_NONE, &masquerade_filename, "Masquerades the filenames", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry extra_entries[] = {
    {"chunk-filesize", 'F', 0, G_OPTION_ARG_INT, &chunk_filesize,
     "Split data files into pieces of this size in MB. Useful for myloader multi-threading.",
     NULL},
    {"exit-if-broken-table-found", 0, 0, G_OPTION_ARG_NONE, &exit_if_broken_table_found,
      "Exits if a broken table has been found", NULL},
    {"isuccess-on-1146", 0, 0, G_OPTION_ARG_CALLBACK, &arguments_callback,
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
    {"compact", 0, 0, G_OPTION_ARG_NONE, &compact, "Give less verbose output. Disables header/footer constructs.", NULL},
    {"compress", 'c', G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "Compress output files using: /usr/bin/gzip and /usr/bin/zstd. Options: GZIP and ZSTD. Default: GZIP", NULL},
    {"use-defer", 0, 0, G_OPTION_ARG_NONE, &use_defer,
     "Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables)", NULL},
    {"check-row-count", 0, 0, G_OPTION_ARG_NONE, &check_row_count,
     "Run SELECT COUNT(*) and fail mydumper if dumped row count is different", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry lock_entries[] = {
    {"tidb-snapshot", 'z', 0, G_OPTION_ARG_STRING, &tidb_snapshot,
     "Snapshot to use for TiDB", NULL},
    {"no-locks", 'k', G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "This option is deprecated use --sync-thread-lock-mode instead",
     NULL},
    {"lock-all-tables", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "This option is deprecated use --sync-thread-lock-mode instead", NULL},
    {"sync-thread-lock-mode", 0, 0, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "There are 3 modes that can be use to sync: FTWRL, LOCK_ALL and GTID. If you don't need a consistent backup, use: NO_LOCK. More info https://mydumper.github.io/mydumper/docs/html/locks.html. Default: AUTO which uses the best option depending on the database vendor",
     NULL},
    {"use-savepoints", 0, 0, G_OPTION_ARG_NONE, &use_savepoints,
     "Use savepoints to reduce metadata locking issues, needs SUPER privilege",
     NULL},
    {"no-backup-locks", 0, 0, G_OPTION_ARG_NONE, &no_backup_locks,
     "Do not use Percona backup locks", NULL},
    {"less-locking", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "This option is deprecated and its behaviour is the default which is useful if you don't have transaction tables. Use --trx-tables otherwise", NULL},
    {"trx-consistency-only", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "This option is deprecated use --trx-tables instead", NULL},
    {"trx-tables", 0, 0, G_OPTION_ARG_NONE, &trx_tables, 
     "The backup process change if we known that we are exporitng transactional tables only", NULL},
    {"skip-ddl-locks", 0, 0, G_OPTION_ARG_NONE, &skip_ddl_locks, "Do not send DDL locks when possible", NULL},
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
    {"max-threads-per-table", 0, 0, G_OPTION_ARG_INT, &max_threads_per_table,
     "Maximum number of threads per table to use", NULL},
    {"use-single-column", 0, 0, G_OPTION_ARG_NONE, &use_single_column, 
     "It will ignore if the table has multiple columns and use only the first column to split the table", NULL},
    {"rows", 'r', 0, G_OPTION_ARG_CALLBACK, &arguments_callback,
     "Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds",
     NULL},
    {"rows-hard", 0, 0, G_OPTION_ARG_CALLBACK, &arguments_callback, "This set the MIN and MAX limit when even if --rows is 0", NULL},
    { "split-partitions", 0, 0, G_OPTION_ARG_NONE, &split_partitions,
      "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

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
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db,
      "Comma delimited list of databases to dump", NULL},
    {"ignore-engines", 'i', 0, G_OPTION_ARG_STRING, &ignore_engines_str,
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
    {"skip-constraints", 0, 0, G_OPTION_ARG_NONE, &skip_constraints, "Remove the constraints from the CREATE TABLE statement. By default, the statement is not modified",
     NULL },
    {"skip-indexes", 0, 0, G_OPTION_ARG_NONE, &skip_indexes, "Remove the indexes from the CREATE TABLE statement. By default, the statement is not modified",
     NULL},
    {"views-as-tables", 0, 0, G_OPTION_ARG_NONE, &views_as_tables, "Export VIEWs as they were tables",
     NULL},
    {"no-views", 'W', 0, G_OPTION_ARG_NONE, &no_dump_views, "Do not dump VIEWs",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


static GOptionEntry statement_entries[] = {
    {"load-data", 0, 0, G_OPTION_ARG_NONE, &load_data,
     "Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files. This option will be deprecated on future releases use --format", NULL },
    {"csv", 0, 0, G_OPTION_ARG_NONE, &csv,
      "Automatically enables --load-data and set variables to export in CSV format. This option will be deprecated on future releases use --format", NULL },
    {"format", 0, 0, G_OPTION_ARG_CALLBACK, &arguments_callback, "Set the output format which can be INSERT, LOAD_DATA, CSV or CLICKHOUSE. Default: INSERT", NULL },
    {"include-header", 0, 0, G_OPTION_ARG_NONE, &include_header, "When --load-data or --csv is used, it will include the header with the column name", NULL},
    {"fields-terminated-by", 0, 0, G_OPTION_ARG_STRING, &fields_terminated_by_ld,"Defines the character that is written between fields", NULL },
    {"fields-enclosed-by", 0, 0, G_OPTION_ARG_STRING, &fields_enclosed_by_ld,"Defines the character to enclose fields. Default: \"", NULL },
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
    {"skip-tz-utc", 0, 0, G_OPTION_ARG_NONE, &skip_tz, "Doesn't add SET TIMEZONE on the backup files", NULL},
    { "set-names",0, 0, G_OPTION_ARG_STRING, &set_names_str,
      "Sets the names, use it at your own risk, default binary", NULL },
    {"table-engine-for-view-dependency", 0, 0, G_OPTION_ARG_STRING, &table_engine_for_view_dependency, 
      "Table engine to be use for the CREATE TABLE statement for temporary tables when using views",NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

GOptionContext * load_contex_entries(){

  GOptionContext *context = g_option_context_new("multi-threaded MySQL dumping");

  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);

  load_connection_entries(context);

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

