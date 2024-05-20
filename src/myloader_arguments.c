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
#include "myloader_global.h"
#include "myloader_arguments.h"
#include "common.h"
#include "logging.h"
#include "connection.h"
#include "regex.h"
#include <strings.h>

extern gboolean enable_binlog;
extern gboolean show_warnings;
gchar *innodb_optimize_keys_str=NULL;
gchar *checksum_str=NULL;

gboolean arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (!strcmp(option_name, "--innodb-optimize-keys")) {
    innodb_optimize_keys = TRUE;
    innodb_optimize_keys_str=g_strdup(value);
    if (value==NULL){
      innodb_optimize_keys_per_table = TRUE;
      innodb_optimize_keys_all_tables = FALSE;
      return TRUE;
    }
    if (!strcasecmp(value, AFTER_IMPORT_PER_TABLE)) {
      innodb_optimize_keys_per_table = TRUE;
      innodb_optimize_keys_all_tables = FALSE;
      return TRUE;
    }
    if (!strcasecmp(value, AFTER_IMPORT_ALL_TABLES)) {
      innodb_optimize_keys_all_tables = TRUE;
      innodb_optimize_keys_per_table = FALSE;
      return TRUE;
    }
    g_critical("--innodb-optimize-keys accepts: after_import_per_table (default value), after_import_all_tables");
  } else if (!strcmp(option_name, "--quote-character")) {
    quote_character_cli= TRUE;
    if (!strcasecmp(value, "BACKTICK") || !strcasecmp(value, "BT") || !strcmp(value, "`")) {
      identifier_quote_character= BACKTICK;
      return TRUE;
    }
    if (!strcasecmp(value, "DOUBLE_QUOTE") || !strcasecmp(value, "DQ") || !strcmp(value, "\"")) {
      identifier_quote_character= DOUBLE_QUOTE;
      return TRUE;
    }
    g_critical("--quote-character accepts: backtick, bt, `, double_quote, dt, \"");
  } else if (!strcmp(option_name, "--checksum")) {
    checksum_str=g_strdup(value);
    if (value == NULL || !strcasecmp(value, "FAIL")) {
      checksum_mode= CHECKSUM_FAIL;
      return TRUE;
    }
    if (!strcasecmp(value, "WARN")) {
      checksum_mode= CHECKSUM_WARN;
      return TRUE;
    }
    if (!strcasecmp(value, "SKIP")) {
      checksum_mode= CHECKSUM_SKIP;
      return TRUE;
    }
    g_critical("--checksum accepts: fail (default), warn, skip");
  }

  return FALSE;
}

static GOptionEntry entries[] = {
    {"help", '?', 0, G_OPTION_ARG_NONE, &help, "Show help options", NULL},
    {"directory", 'd', 0, G_OPTION_ARG_STRING, &input_directory,
     "Directory of the dump to import", NULL},
    {"logfile", 'L', 0, G_OPTION_ARG_FILENAME, &logfile,
     "Log file name to use, by default stdout is used", NULL},
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db,
     "An alternative database to restore into", NULL},
    {"quote-character", 'Q', 0, G_OPTION_ARG_CALLBACK, &arguments_callback,
      "Identifier quote character used in INSERT statements. "
      "Posible values are: BACKTICK, bt, ` for backtick and DOUBLE_QUOTE, dt, \" for double quote. "
      "Default: detect from dump if possible, otherwise BACKTICK", NULL},
    {"show-warnings", 0,0, G_OPTION_ARG_NONE, &show_warnings, 
      "If enabled, during INSERT IGNORE the warnings will be printed", NULL},
    {"resume",0, 0, G_OPTION_ARG_NONE, &resume,
      "Expect to find resume file in backup dir and will only process those files",NULL},
    {"kill-at-once", 'k', 0, G_OPTION_ARG_NONE, &kill_at_once, "When Ctrl+c is pressed it immediately terminates the process", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry threads_entries[] = {
    {"max-threads-per-table", 0, 0, G_OPTION_ARG_INT, &max_threads_per_table,
     "Maximum number of threads per table to use, defaults to --threads", NULL},
    {"max-threads-for-index-creation", 0, 0, G_OPTION_ARG_INT, &max_threads_for_index_creation,
     "Maximum number of threads for index creation, default 4", NULL},
    {"max-threads-for-post-actions", 0, 0, G_OPTION_ARG_INT,&max_threads_for_post_creation,
     "Maximum number of threads for post action like: constraints, procedure, views and triggers, default 1", NULL},
    {"max-threads-for-schema-creation", 0, 0, G_OPTION_ARG_INT, &max_threads_for_schema_creation,
     "Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation, default 4", NULL},
    {"exec-per-thread",0, 0, G_OPTION_ARG_STRING, &exec_per_thread,
     "Set the command that will receive by STDIN from the input file and write in the STDOUT", NULL},
    {"exec-per-thread-extension",0, 0, G_OPTION_ARG_STRING, &exec_per_thread_extension,
     "Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry execution_entries[] = {
    {"enable-binlog", 'e', 0, G_OPTION_ARG_NONE, &enable_binlog,
     "Enable binary logging of the restore data", NULL},
    {"innodb-optimize-keys", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "Creates the table without the indexes and it adds them at the end. Options: AFTER_IMPORT_PER_TABLE and AFTER_IMPORT_ALL_TABLES. Default: AFTER_IMPORT_PER_TABLE", NULL},
    { "no-schema", 0, 0, G_OPTION_ARG_NONE, &no_schemas, "Do not import table schemas and triggers ", NULL},
    { "purge-mode", 0, 0, G_OPTION_ARG_STRING, &purge_mode_str,
      "This specify the truncate mode which can be: FAIL, NONE, DROP, TRUNCATE and DELETE. Default if not set: FAIL", NULL },
    { "disable-redo-log", 0, 0, G_OPTION_ARG_NONE, &disable_redo_log,
      "Disables the REDO_LOG and enables it after, doesn't check initial status", NULL },
    {"checksum", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &arguments_callback,
     "Treat checksums: skip, fail, warn (default).", NULL },

    {"overwrite-tables", 'o', 0, G_OPTION_ARG_NONE, &overwrite_tables,
     "Drop tables if they already exist", NULL},
    {"overwrite-unsafe", 0, 0, G_OPTION_ARG_NONE, &overwrite_unsafe,
     "Same as --overwrite-tables but starts data load as soon as possible. May cause InnoDB deadlocks for foreign keys.", NULL},
    {"retry-count", 0, 0, G_OPTION_ARG_INT, &retry_count,
     "Lock wait timeout exceeded retry count, default 10 (currently only for DROP TABLE)", NULL},

    {"serialized-table-creation",0, 0, G_OPTION_ARG_NONE, &serial_tbl_creation,
      "Table recreation will be executed in series, one thread at a time. This means --max-threads-for-schema-creation=1. This option will be removed in future releases",NULL},
    {"stream", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK , &stream_arguments_callback,
     "It will receive the stream from STDIN and creates the file in the disk before start processing. Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given", NULL},
//    {"no-delete", 0, 0, G_OPTION_ARG_NONE, &no_delete,
//      "It will not delete the files after stream has been completed", NULL},
    {"ignore-errors", 0, 0, G_OPTION_ARG_STRING, &ignore_errors,
     "Not increment error count and Warning instead of Critical in case of "
     "any of the comman separated error number list",
     NULL},

    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry pmm_entries[] = {
    { "pmm-path", 0, 0, G_OPTION_ARG_STRING, &pmm_path,
      "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution", NULL },
    { "pmm-resolution", 0, 0, G_OPTION_ARG_STRING, &pmm_resolution,
      "which default will be high", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


static GOptionEntry filter_entries[] ={
    {"source-db", 's', 0, G_OPTION_ARG_STRING, &source_db,
     "Database to restore", NULL},
    {"skip-triggers", 0, 0, G_OPTION_ARG_NONE, &skip_triggers, "Do not import triggers. By default, it imports triggers",
     NULL},
    {"skip-post", 0, 0, G_OPTION_ARG_NONE, &skip_post,
     "Do not import events, stored procedures and functions. By default, it imports events, stored procedures or functions", NULL},
    {"skip-constraints", 0, 0, G_OPTION_ARG_NONE, &skip_constraints, "Do not import constraints. By default, it imports contraints",
     NULL },
    {"skip-indexes", 0, 0, G_OPTION_ARG_NONE, &skip_indexes, "Do not import secondary index on InnoDB tables. By default, it import the indexes",
     NULL},
    {"no-data", 0, 0, G_OPTION_ARG_NONE, &no_data, "Do not dump or import table data",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry statement_entries[] ={
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows,
     "Split the INSERT statement into this many rows.", NULL},
    {"queries-per-transaction", 'q', 0, G_OPTION_ARG_INT, &commit_count,
     "Number of queries per transaction, default 1000", NULL},
    {"append-if-not-exist", 0, 0, G_OPTION_ARG_NONE,&append_if_not_exist,
      "Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented", NULL},
    { "set-names",0, 0, G_OPTION_ARG_STRING, &set_names_str,
      "Sets the names, use it at your own risk, default binary", NULL },
    {"skip-definer", 0, 0, G_OPTION_ARG_NONE, &skip_definer,
     "Removes DEFINER from the CREATE statement. By default, statements are not modified", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


GOptionContext * load_contex_entries(){


  GOptionContext * context = g_option_context_new("multi-threaded MySQL loader");
  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);
//  load_common_entries(main_group);
//  GOptionGroup *
  load_connection_entries(context);
//  g_option_group_add_entries(connection_group, common_connection_entries);
  GOptionGroup *filter_group = load_regex_entries(context);
  g_option_group_add_entries(filter_group, filter_entries);
  g_option_group_add_entries(filter_group, common_filter_entries);



  GOptionGroup *pmm_group=g_option_group_new("pmm", "PMM Options", "PMM Options", NULL, NULL);
  g_option_group_add_entries(pmm_group, pmm_entries);
  g_option_context_add_group(context, pmm_group);

  GOptionGroup *execution_group=g_option_group_new("execution", "Execution Options", "Execution Options", NULL, NULL);
  g_option_group_add_entries(execution_group, execution_entries);
  g_option_context_add_group(context, execution_group);

  GOptionGroup *threads_group=g_option_group_new("threads", "Threads Options", "Threads Options", NULL, NULL);
  g_option_group_add_entries(threads_group, threads_entries);
  g_option_context_add_group(context, threads_group);

  GOptionGroup *statement_group=g_option_group_new("statement", "Statement Options", "Statement Options", NULL, NULL);
  g_option_group_add_entries(statement_group, statement_entries);
  g_option_context_add_group(context, statement_group);


  g_option_context_set_help_enabled(context, FALSE);

  g_option_context_set_main_group(context, main_group);

  return context;
}

