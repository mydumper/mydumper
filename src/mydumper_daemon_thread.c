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

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include <pcre.h>
#include <signal.h>
#include <glib/gstdio.h>
#include <glib/gerror.h>
#include <gio/gio.h>
#include "config.h"
#include "server_detect.h"
#include "connection.h"
//#include "common_options.h"
#include "common.h"
#include <glib-unix.h>
#include <math.h>
#include "logging.h"
#include "set_verbose.h"
#include "locale.h"
#include <sys/statvfs.h>


#include "mydumper_start_dump.h"
#include "mydumper_common.h"
extern guint snapshot_count;
extern gboolean shutdown_triggered;
extern gchar *output_directory;
extern GAsyncQueue *start_scheduled_dump;
extern guint dump_number;
extern gchar *dump_directory;
//extern guint errors;
//
void *exec_thread(void *data) {
  (void)data;

  while (1) {
    g_async_queue_pop(start_scheduled_dump);
    MYSQL *conn = create_main_connection();
    char *dump_number_str=g_strdup_printf("%d",dump_number);
    dump_directory = g_build_path("/", output_directory, dump_number_str, NULL);
    g_free(dump_number_str);
    clear_dump_directory(dump_directory);
    start_dump(conn);
    // start_dump already closes mysql
    // mysql_close(conn);
    mysql_thread_end();

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

