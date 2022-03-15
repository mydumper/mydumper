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
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <mysql.h>

#if defined MARIADB_CLIENT_VERSION_STR && !defined MYSQL_SERVER_VERSION
#define MYSQL_SERVER_VERSION MARIADB_CLIENT_VERSION_STR
#endif

#include "string.h"
#include <glib/gstdio.h>
#include <gio/gio.h>
#include "src/config.h"
#include "src/connection.h"
#include "src/common_options.h"
#include "src/common.h"
#include <glib-unix.h>
#include "src/logging.h"
#include "src/set_verbose.h"
#include "locale.h"
#include "src/tables_skiplist.h"
#include "src/regex.h"
#include "src/mydumper_start_dump.h"
#include "src/mydumper_daemon_thread.h"
const char DIRECTORY[] = "export";

extern GAsyncQueue *stream_queue;
/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

/* Program options */
gchar *output_directory = NULL;
gchar *output_directory_param = NULL;
gchar *dump_directory = NULL;
guint statement_size = 1000000;
guint rows_per_file = 0;
guint chunk_filesize = 0;
int build_empty_files = 0;
guint snapshot_count= 2;
guint snapshot_interval = 60;
gboolean daemon_mode = FALSE;
gboolean have_snapshot_cloning = FALSE;

gboolean use_savepoints = FALSE;
gboolean load_data = FALSE;
gboolean csv = FALSE;

guint complete_insert = 0;

gchar *fields_terminated_by=NULL;
gchar *fields_enclosed_by=NULL;
gchar *fields_escaped_by=NULL;
gchar *lines_starting_by=NULL;
gchar *lines_terminated_by=NULL;
gchar *statement_terminated_by=NULL;

gchar *fields_enclosed_by_ld=NULL;
gchar *fields_terminated_by_ld=NULL;
gchar *lines_starting_by_ld=NULL;
gchar *lines_terminated_by_ld=NULL;
gchar *statement_terminated_by_ld=NULL;

gchar *disk_limits=NULL;

// For daemon mode
guint dump_number = 0;
gboolean shutdown_triggered = FALSE;
GAsyncQueue *start_scheduled_dump;
GMainLoop *m1;

guint errors;

static GOptionEntry entries[] = {
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db, "Database to dump", NULL},
    {"outputdir", 'o', 0, G_OPTION_ARG_FILENAME, &output_directory_param,
     "Directory to output files to", NULL},
    {"statement-size", 's', 0, G_OPTION_ARG_INT, &statement_size,
     "Attempted size of INSERT statement in bytes, default 1000000", NULL},
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows_per_file,
     "Try to split tables into chunks of this many rows. This option turns off "
     "--chunk-filesize",
     NULL},
    {"chunk-filesize", 'F', 0, G_OPTION_ARG_INT, &chunk_filesize,
     "Split tables into chunks of this output file size. This value is in MB",
     NULL},
    {"build-empty-files", 'e', 0, G_OPTION_ARG_NONE, &build_empty_files,
     "Build dump files even if no data available from table", NULL},
    {"no-data", 'd', 0, G_OPTION_ARG_NONE, &no_data, "Do not dump table data",
     NULL},
    {"daemon", 'D', 0, G_OPTION_ARG_NONE, &daemon_mode, "Enable daemon mode",
     NULL},
    {"snapshot-count", 'X', 0, G_OPTION_ARG_INT, &snapshot_count, "number of snapshots, default 2", NULL},
    {"snapshot-interval", 'I', 0, G_OPTION_ARG_INT, &snapshot_interval,
     "Interval between each dump snapshot (in minutes), requires --daemon, "
     "default 60",
     NULL},
    {"logfile", 'L', 0, G_OPTION_ARG_FILENAME, &logfile,
     "Log file name to use, by default stdout is used", NULL},
    {"use-savepoints", 0, 0, G_OPTION_ARG_NONE, &use_savepoints,
     "Use savepoints to reduce metadata locking issues, needs SUPER privilege",
     NULL},
    {"complete-insert", 0, 0, G_OPTION_ARG_NONE, &complete_insert,
     "Use complete INSERT statements that include column names", NULL},
    {"load-data", 0, 0, G_OPTION_ARG_NONE, &load_data,
     "", NULL },
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
    { "disk-limits", 0, 0, G_OPTION_ARG_STRING, &disk_limits,
      "Set the limit to pause and resume if determines there is no enough disk space."
      "Accepts values like: '<resume>:<pause>' in MB."
      "For instance: 100:500 will pause when there is only 100MB free and will"
      "resume if 500MB are available", NULL },
    { "csv", 0, 0, G_OPTION_ARG_NONE, &csv,
      "Automatically enables --load-data and set variables to export in CSV format.", NULL },
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

struct tm tval;

gboolean run_snapshot(gpointer *data) {
  (void)data;

  g_async_queue_push(start_scheduled_dump, GINT_TO_POINTER(1));

  return !shutdown_triggered;
}

void parse_disk_limits(){
  gchar ** strsplit = g_strsplit(disk_limits,":",3);
  if (g_strv_length(strsplit)!=2){
    g_critical("Parse limit failed");
    exit(EXIT_FAILURE);
  }
  set_disk_limits(atoi(strsplit[0]),atoi(strsplit[1]));
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
  load_connection_entries(main_group);
  load_regex_entries(main_group);
  load_start_dump_entries(main_group);
  g_option_context_set_main_group(context, main_group);
  gchar ** tmpargv=g_strdupv(argv);
  int tmpargc=argc;
  if (!g_option_context_parse(context, &tmpargc, &tmpargv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }

  if (tmpargc > 1 ){
    int pos=0;
    stream=TRUE;
    db=tmpargv[1];
    if (tmpargc > 2 ){
      GString *s = g_string_new(tmpargv[2]);
      for (pos=3; pos<tmpargc;pos++){
        g_string_append_printf(s,",%s",tmpargv[pos]);
      }
      tables_list=g_strdup(s->str);
    }
  }

  set_verbose(verbose);

  if (defaults_file != NULL){
    load_config_file(defaults_file, context, "mydumper");
  }
  g_option_context_free(context);


  initialize_start_dump();

  hide_password(argc, argv);
  ask_password();
  
  if (disk_limits!=NULL){
    parse_disk_limits();
  }

  if (csv){
    load_data=TRUE;
    if (!fields_terminated_by_ld) fields_terminated_by_ld=g_strdup(",");
    if (!fields_enclosed_by_ld) fields_enclosed_by_ld=g_strdup("\"");
    if (!fields_escaped_by) fields_escaped_by=g_strdup("\\");
    if (!lines_terminated_by_ld) lines_terminated_by_ld=g_strdup("\n");
  }
  if (load_data){
    if (!fields_enclosed_by_ld){
    	fields_enclosed_by=g_strdup("");
      fields_enclosed_by_ld=fields_enclosed_by;
    }else if(strlen(fields_enclosed_by_ld)>1){
	    g_error("--fields-enclosed-by must be a single character");
      exit(EXIT_FAILURE);
    }else{
      fields_enclosed_by=fields_enclosed_by_ld;
    }
  
    if (fields_escaped_by){
      if(strlen(fields_escaped_by)>1){
	      g_error("--fields-escaped-by must be a single character");
        exit(EXIT_FAILURE);
      }else if (strcmp(fields_escaped_by,"\\")==0){
        fields_escaped_by=g_strdup("\\\\");
      } 
    }else{
      fields_escaped_by=g_strdup("\\\\");
    }
  }

  if (!fields_terminated_by_ld){
    if (load_data){
      fields_terminated_by=g_strdup("\t");
      fields_terminated_by_ld=g_strdup("\\t");
    }else
      fields_terminated_by=g_strdup(",");
  }else
    fields_terminated_by=replace_escaped_strings(g_strdup(fields_terminated_by_ld));
  if (!lines_starting_by_ld){
    if (load_data){
      lines_starting_by=g_strdup("");
      lines_starting_by_ld=lines_starting_by;
    }else
  	  lines_starting_by=g_strdup("(");
  }else
    lines_starting_by=replace_escaped_strings(g_strdup(lines_starting_by_ld));
  if (!lines_terminated_by_ld){
    if (load_data){
      lines_terminated_by=g_strdup("\n");
      lines_terminated_by_ld=g_strdup("\\n");
    }else
  	  lines_terminated_by=g_strdup(")\n");
  }else
    lines_terminated_by=replace_escaped_strings(g_strdup(lines_terminated_by_ld));
  if (!statement_terminated_by_ld){
    if (load_data){
      statement_terminated_by=g_strdup("");
      statement_terminated_by_ld=statement_terminated_by;
    }else
  	  statement_terminated_by=g_strdup(";\n");
  }else
    statement_terminated_by=replace_escaped_strings(g_strdup(statement_terminated_by_ld));

  if (program_version) {
    g_print("mydumper %s, built against MySQL %s\n", VERSION,
            MYSQL_VERSION_STR);
    exit(EXIT_SUCCESS);
  }

  set_verbose(verbose);

  GDateTime * datetime = g_date_time_new_now_local();

  g_message("MyDumper backup version: %s", VERSION);

  initialize_regex();
  time_t t;
  time(&t);
  localtime_r(&t, &tval);

  // rows chunks have precedence over chunk_filesize
  if (rows_per_file > 0 && chunk_filesize > 0) {
//    chunk_filesize = 0;
//    g_warning("--chunk-filesize disabled by --rows option");
    g_warning("We are going to chunk by row and by filesize");
  }

  /* savepoints workaround to avoid metadata locking issues
     doesnt work for chuncks */
  if (rows_per_file && use_savepoints) {
    use_savepoints = FALSE;
    g_warning("--use-savepoints disabled by --rows");
  }

  char *datetimestr;

  if (!output_directory_param){
    datetimestr=g_date_time_format(datetime,"\%Y\%m\%d-\%H\%M\%S");
    output_directory = g_strdup_printf("%s-%s", DIRECTORY, datetimestr);
    g_free(datetimestr);
  }else{
    output_directory=output_directory_param;
  }
  create_backup_dir(output_directory);
  if (daemon_mode) {
    pid_t pid, sid;

    pid = fork();
    if (pid < 0)
      exit(EXIT_FAILURE);
    else if (pid > 0)
      exit(EXIT_SUCCESS);

    umask(0037);
    sid = setsid();

    if (sid < 0)
      exit(EXIT_FAILURE);

    char *d_d;
    for (dump_number = 0; dump_number < snapshot_count; dump_number++) {
        d_d= g_strdup_printf("%s/%d", output_directory, dump_number);
        create_backup_dir(d_d);
        g_free(d_d);
    }
    
    GFile *last_dump = g_file_new_for_path(
        g_strdup_printf("%s/last_dump", output_directory)
    );
    GFileInfo *last_dump_i = g_file_query_info(
        last_dump,
        G_FILE_ATTRIBUTE_STANDARD_TYPE ","
        G_FILE_ATTRIBUTE_STANDARD_SYMLINK_TARGET,
        G_FILE_QUERY_INFO_NOFOLLOW_SYMLINKS,
        NULL, NULL
    );
    if (last_dump_i != NULL &&
        g_file_info_get_file_type(last_dump_i) == G_FILE_TYPE_SYMBOLIC_LINK) {
        dump_number = atoi(g_file_info_get_symlink_target(last_dump_i));
        if (dump_number >= snapshot_count-1) dump_number = 0;
        else dump_number++;
        g_object_unref(last_dump_i);
    } else {
        dump_number = 0;
    }
    g_object_unref(last_dump);
  }else{
    dump_directory = output_directory;
  }

  /* Give ourselves an array of tables to dump */
  if (tables_list)
    tables = g_strsplit(tables_list, ",", 0);

  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);

  if (daemon_mode) {
    GError *terror;
    start_scheduled_dump = g_async_queue_new();
    GThread *ethread =
        g_thread_create(exec_thread, GINT_TO_POINTER(1), FALSE, &terror);
    if (ethread == NULL) {
      g_critical("Could not create exec thread: %s", terror->message);
      g_error_free(terror);
      exit(EXIT_FAILURE);
    }
    // Run initial snapshot
    run_snapshot(NULL);
#if GLIB_MINOR_VERSION < 14
    g_timeout_add(snapshot_interval * 60 * 1000, (GSourceFunc)run_snapshot,
                  NULL);
#else
    g_timeout_add_seconds(snapshot_interval * 60, (GSourceFunc)run_snapshot,
                          NULL);
#endif
    guint sigsource = g_unix_signal_add(SIGINT, sig_triggered_int, NULL);
    sigsource = g_unix_signal_add(SIGTERM, sig_triggered_term, NULL);
    m1 = g_main_loop_new(NULL, TRUE);
    g_main_loop_run(m1);
    g_source_remove(sigsource);
  } else {
    MYSQL *conn = create_main_connection();
    start_dump(conn);
  }


  mysql_thread_end();
  mysql_library_end();
  g_free(output_directory);
  g_strfreev(tables);

  if (logoutfile) {
    fclose(logoutfile);
  }

  exit(errors ? EXIT_FAILURE : EXIT_SUCCESS);
}

