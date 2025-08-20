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
*/
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <mysql.h>
#include "common.h"
#include "config.h"
#include "common_options.h"
char *db = NULL;
char *defaults_file = NULL;
char *defaults_extra_file = NULL;

gboolean help =FALSE;
GString *set_session = NULL;
GString *set_global = NULL;
GString *set_global_back = NULL;
gchar *sql_mode= NULL;
MYSQL *main_connection = NULL;
gboolean no_schemas = FALSE;
gboolean no_data = FALSE;
GKeyFile *key_file = NULL;

gchar *ignore_errors=NULL;

guint num_threads= 4;
guint verbose = 2;
gboolean debug = FALSE;
gboolean program_version = FALSE;

gchar *tables_list = NULL;
gchar *tables_skiplist_file = NULL;
char **tables = NULL;

gboolean no_stream = FALSE;
gboolean no_sync=FALSE;

gchar *set_names_in_conn_by_default=NULL;
gchar *set_names_statement=NULL;

gchar identifier_quote_character=BACKTICK;
const char *identifier_quote_character_str= "`";

gboolean schema_sequence_fix = FALSE;
guint max_threads_per_table= 4;

guint source_control_command = TRADITIONAL;

struct replication_settings source_data={FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE};
struct replication_settings replica_data={FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE};

gchar *throttle_variable=NULL;
guint throttle_value=0;

gchar *server_version_arg=NULL;

gchar **optimize_key_engines=NULL;


void parse_source_replica_options(const gchar *value, struct replication_settings *rep_set){
  rep_set->enabled=TRUE;
  if (value){
    gchar ** lp=g_strsplit(value, ",", 0);
    if (g_strv_length(lp)==1){
      guint64 _source_data;
       if (g_ascii_string_to_unsigned(value, 10, 0, 256, &_source_data, NULL)){
        rep_set->exec_reset_replica=((_source_data) & (1<<(0)))>0;
        rep_set->exec_change_source=((_source_data) & (1<<(1)))>0;
        rep_set->exec_start_replica=((_source_data) & (1<<(2)))>0;
        rep_set->source_ssl        =((_source_data) & (1<<(3)))>0;
        rep_set->auto_position     =((_source_data) & (1<<(4)))>0;
        rep_set->exec_start_replica_until=((_source_data) & (1<<(5)))>0;
        return;
       }
    }
    rep_set->exec_reset_replica=g_strv_contains((const char * const*)lp,"exec_reset_replica");
    rep_set->exec_change_source=g_strv_contains((const char * const*)lp,"exec_change_source");
    rep_set->exec_start_replica=g_strv_contains((const char * const*)lp,"exec_start_replica");
    rep_set->source_ssl=g_strv_contains((const char * const*)lp,"enable_ssl");
    rep_set->auto_position=g_strv_contains((const char * const*)lp,"use_auto_position");
    rep_set->exec_start_replica_until=g_strv_contains((const char * const*)lp,"exec_start_replica_until");
    g_strfreev(lp);
  }
}

gboolean common_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (!strcmp(option_name,"--throttle")){
    if (value){
      gchar ** tp;
      gchar ** tq=g_strsplit(value, ":", 2);
      if (tq[1]){
        throttle_max_usleep_limit=atoi(tq[0]);
        tp=g_strsplit(tq[1], "=", 2);
      }else{
        tp=g_strsplit(value, "=", 2);
      }
      throttle_variable=g_strdup(tp[0]);
      throttle_value = atoi(tp[1]);
      g_strfreev(tq);
      g_strfreev(tp);
    }else{
      throttle_variable=g_strdup("Threads_running");
      throttle_value = 0;
    }
    return TRUE;
  } else if (!strcmp(option_name, "--optimize-keys-engines")){
    if (value){
      optimize_key_engines = g_strsplit(value, ",", 0);
      return TRUE;
    }

  } else if (!strcmp(option_name, "--source-control-command")){
    if (!strcasecmp(value, "TRADITIONAL")) {
      source_control_command=TRADITIONAL;
      return TRUE;
    }
    if (!strcasecmp(value, "AWS")) {
      source_control_command=AWS;
      return TRUE;
    }
  } else if (!strcmp(option_name, "--ignore-errors")){
    guint n=0;
    gchar **tmp_ignore_errors_list = g_strsplit(value, ",", 0);
    while(tmp_ignore_errors_list[n]!=NULL){
      ignore_errors_list=g_list_append(ignore_errors_list,GINT_TO_POINTER(atoi(tmp_ignore_errors_list[n])));
      n++;
    }
    return TRUE;
  } else if (!strcmp(option_name, "--source-data")){
    parse_source_replica_options(value,&source_data);
    return TRUE;
  } else if (!strcmp(option_name, "--replica-data")){
    parse_source_replica_options(value,&replica_data);
    return TRUE;
  }
  return FALSE;
}

GOptionEntry common_entries[] = {
    {"source-data", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK, &common_arguments_callback,
      "It will include the options in the metadata file, to allow myloader to establish replication", NULL},
    {"threads", 't', 0, G_OPTION_ARG_INT, &num_threads,
      "Number of threads to use, 0 means to use number of CPUs. Default: 4, Minimum: 2", NULL},
    {"version", 'V', 0, G_OPTION_ARG_NONE, &program_version,
      "Show the program version and exit", NULL},
    {"verbose", 'v', 0, G_OPTION_ARG_INT, &verbose,
      "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, default 2", NULL},
    {"debug", 0, 0, G_OPTION_ARG_NONE, &debug, 
      "Turn on debugging output "
      "(automatically sets verbosity to 3)", NULL},
    {"ignore-errors", 0, 0, G_OPTION_ARG_CALLBACK, &common_arguments_callback,
      "Not increment error count and Warning instead of Critical in case of any of the comma-separated error number list", NULL},
    {"defaults-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_file,
      "Use a specific defaults file. Default: /etc/mydumper.cnf", NULL},
    {"defaults-extra-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_extra_file,
      "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values", NULL},
    {"source-control-command", 0, 0, G_OPTION_ARG_CALLBACK, &common_arguments_callback,
      "Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS", NULL},
    {"optimize-keys-engines", 0, 0, G_OPTION_ARG_CALLBACK , &common_arguments_callback,
      "List of engines that will be used to split the create table statement into multiple stages if possible. Default: InnoDB,ROCKSDB", NULL},
    {"server-version", 0, 0, G_OPTION_ARG_STRING, &server_version_arg,
      "Set the server version avoid automatic detection", NULL},
    {"throttle", 0, G_OPTION_FLAG_OPTIONAL_ARG, G_OPTION_ARG_CALLBACK, &common_arguments_callback,
      "Expects a string like Threads_running=10. It will check the SHOW GLOBAL STATUS and if it is higher, it will increase the sleep time between SELECT. "
      "If option is used without parameters it will use Threads_running and the amount of threads", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

GOptionEntry common_filter_entries[] = {
    {"omit-from-file", 'O', 0, G_OPTION_ARG_STRING, &tables_skiplist_file,
      "File containing a list of database.table entries to skip, one per line "
      "(skips before applying regex option)", NULL},
    {"tables-list", 'T', 0, G_OPTION_ARG_STRING, &tables_list,
      "Comma delimited table list to dump (does not exclude regex option). "
      "Table name must include database name. For instance: test.t1,test.t2", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};
