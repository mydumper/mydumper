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
#include "mydumper_global.h"
#include "mydumper_arguments.h"
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
//gboolean no_stream = FALSE;
// For daemon mode
gboolean shutdown_triggered = FALSE;

guint errors;

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

  initialize_share_common();

  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    m_critical("option parsing failed: %s, try --help\n", error->message);
  }

  if (help){
    printf("%s", g_option_context_get_help (context, FALSE, NULL));
    exit(EXIT_SUCCESS);
  }

  if (program_version) {
    print_version("mydumper");
    exit(EXIT_SUCCESS);
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
  
  if (!output_directory_param){
    GDateTime * datetime = g_date_time_new_now_local();
    char *datetimestr;
    datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");
    output_directory = g_strdup_printf("%s-%s", DIRECTORY, datetimestr);
    g_free(datetimestr);
    g_date_time_unref(datetime);
  }else{
    output_directory=output_directory_param;
  }
//  char *current_dir=g_get_current_dir();
  create_backup_dir(output_directory, NULL);

  if (disk_limits!=NULL){
    parse_disk_limits();
  }

  if (num_threads < 2) {
    skip_defer= TRUE;
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

