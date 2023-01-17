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
#include "config.h"
#include "connection.h"
#include "common_options.h"
#include "common.h"
#include "logging.h"
#include "set_verbose.h"
#include "tables_skiplist.h"
#include "regex.h"
#include "mydumper_start_dump.h"
#include "mydumper_daemon_thread.h"
const char DIRECTORY[] = "export";

/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

/* Program options */
gchar *output_directory = NULL;
gchar *output_directory_param = NULL;
gchar *dump_directory = NULL;
gboolean daemon_mode = FALSE;
gchar *disk_limits=NULL;
gboolean stream = FALSE;
gboolean no_delete = FALSE;
gboolean no_stream = FALSE;
// For daemon mode
gboolean shutdown_triggered = FALSE;

guint errors;

gboolean help =FALSE;
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


static GOptionEntry filter_entries[] ={
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db, "Database to dump", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

static GOptionEntry daemon_entries[] = {
    {"daemon", 'D', 0, G_OPTION_ARG_NONE, &daemon_mode, "Enable daemon mode",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


struct tm tval;

void parse_disk_limits(){
  gchar ** strsplit = g_strsplit(disk_limits,":",3);
  if (g_strv_length(strsplit)!=2){
    g_critical("Parse limit failed");
    exit(EXIT_FAILURE);
  }
  set_disk_limits(atoi(strsplit[0]),atoi(strsplit[1]));
}

void initialize_main(){

}


int main(int argc, char *argv[]) {
  GError *error = NULL;
  GOptionContext *context;

  g_thread_init(NULL);
  setlocale(LC_ALL, "");

  context = g_option_context_new("multi-threaded MySQL dumping");
  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);

  GOptionGroup *
    connection_group = load_connection_entries(context);
  g_option_group_add_entries(connection_group, common_connection_entries);

  GOptionGroup *filter_group = load_regex_entries(context);
  g_option_group_add_entries(filter_group, filter_entries);
  g_option_group_add_entries(filter_group, common_filter_entries);

  load_start_dump_entries(context, filter_group);
  GOptionGroup *
    daemon_group = load_daemon_entries(context);
  g_option_group_add_entries(daemon_group, daemon_entries);

  g_option_context_set_help_enabled(context, FALSE);

  g_option_context_set_main_group(context, main_group);
  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }

  if (help){
    printf("%s", g_option_context_get_help (context, FALSE, NULL));
    exit(0);
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
  gchar *mydumper = g_strdup("mydumper");
  initialize_common_options(context, mydumper);
  g_free(mydumper);
  g_option_context_free(context);

  initialize_main();
  initialize_start_dump();

  hide_password(argc, argv);
  ask_password();
  
  if (disk_limits!=NULL){
    parse_disk_limits();
  }

  if (program_version) {
    print_version("mydumper");
    exit(EXIT_SUCCESS);
  }

  GDateTime * datetime = g_date_time_new_now_local();

  g_message("MyDumper backup version: %s", VERSION);

  initialize_regex();
  time_t t;
  time(&t);
  localtime_r(&t, &tval);

  char *datetimestr;

  if (!output_directory_param){
    datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");
    output_directory = g_strdup_printf("%s-%s", DIRECTORY, datetimestr);
    g_free(datetimestr);
  }else{
    output_directory=output_directory_param;
  }
  g_date_time_unref(datetime);
  create_backup_dir(output_directory);
  if (daemon_mode) {
    initialize_daemon_thread();
  }else{
    dump_directory = output_directory;
  }

  /* Give ourselves an array of tables to dump */
  if (tables_list)
    tables = get_table_list(tables_list);
    
  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);

  /* Validate that thread count passed on CLI is a valid count */
  check_num_threads();

  if (daemon_mode) {
    run_daemon();
  } else {
    start_dump();
  }

  g_free(output_directory);
  g_strfreev(tables);

  if (logoutfile) {
    fclose(logoutfile);
  }
  if (key_file)  g_key_file_free(key_file);
//  g_strfreev(argv);
  g_free(compress_extension);
  exit(errors ? EXIT_FAILURE : EXIT_SUCCESS);
}

