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

#include <mysql.h>

#include <errno.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include "config.h"
#include "common.h"
#include <glib-unix.h>
#include "mydumper_start_dump.h"
#include "mydumper_common.h"

guint snapshot_interval = 60;
guint snapshot_count= 2;
GMainLoop *m1;
GAsyncQueue *start_scheduled_dump;
guint dump_number=0;

extern gchar *dump_directory;
extern gchar *output_directory;
extern gboolean shutdown_triggered;

static GOptionEntry daemon_entries[] = {
    {"snapshot-interval", 'I', 0, G_OPTION_ARG_INT, &snapshot_interval,
     "Interval between each dump snapshot (in minutes), requires --daemon, "
     "default 60",
     NULL},
    {"snapshot-count", 'X', 0, G_OPTION_ARG_INT, &snapshot_count, "number of snapshots, default 2", NULL},    
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

GOptionGroup * load_daemon_entries(GOptionContext *context){
  GOptionGroup *daemon_group =
      g_option_group_new("daemongroup", "Daemon Options", "daemon", NULL, NULL);
  g_option_group_add_entries(daemon_group, daemon_entries);
  g_option_context_add_group(context, daemon_group);
  return daemon_group;
}

void initialize_daemon_thread(){
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
}

gboolean run_snapshot(gpointer *data) {
    (void)data;

      g_async_queue_push(start_scheduled_dump, GINT_TO_POINTER(1));

        return !shutdown_triggered;
}


void *exec_thread(void *data) {
  (void)data;

  while (1) {
    g_async_queue_pop(start_scheduled_dump);
//    MYSQL *conn = create_main_connection();
    char *dump_number_str=g_strdup_printf("%d",dump_number);
    dump_directory = g_build_path("/", output_directory, dump_number_str, NULL);
    g_free(dump_number_str);
    clear_dump_directory(dump_directory);
    start_dump();
    // start_dump already closes mysql
    // mysql_close(conn);
    // mysql_thread_end();

    // Don't switch the symlink on shutdown because the dump is probably
    // incomplete.
    if (!shutdown_triggered) {
      char *dump_symlink_source= g_strdup_printf("%d", dump_number);
      char *dump_symlink_dest =
          g_strdup_printf("%s/last_dump", output_directory);

      // We don't care if this fails
      g_unlink(dump_symlink_dest);

      if (symlink(dump_symlink_source, dump_symlink_dest) == -1) {
        g_critical("error setting last good dump symlink %s, %d",
                   dump_symlink_dest, errno);
      }
      g_free(dump_symlink_dest);

      if (dump_number >= snapshot_count-1) dump_number = 0;
      else dump_number++;
    }
  }
  return NULL;
}


void run_daemon(){
    GError *terror;
    start_scheduled_dump = g_async_queue_new();
    GThread *ethread =
        g_thread_create(exec_thread, GINT_TO_POINTER(1), FALSE, &terror);
    if (ethread == NULL) {
      m_critical("Could not create exec thread: %s", terror->message);
      g_error_free(terror);
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
}
