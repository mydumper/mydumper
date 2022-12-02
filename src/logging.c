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

        Authors:        Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)

*/

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <zlib.h>
#include <pcre.h>
#include <signal.h>
#include <glib/gstdio.h>

gchar *logfile;
FILE *logoutfile;

void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data) {
  (void)log_domain;
  (void)log_level;
  (void)message;
  (void)user_data;
}

void write_log_file(const gchar *log_domain, GLogLevelFlags log_level,
                    const gchar *message, gpointer user_data) {
  (void)log_domain;
  (void)user_data;

  gchar date[20];
  time_t rawtime;
  struct tm timeinfo;

  // Don't log debug if debugging off
#if GLIB_CHECK_VERSION(2,68,0)
  if ((log_level & G_LOG_LEVEL_DEBUG) &&
      g_log_writer_default_would_drop(log_level, log_domain)) {
    return;
  }
#endif

  time(&rawtime);
  localtime_r(&rawtime, &timeinfo);
  strftime(date, 20, "%Y-%m-%d %H:%M:%S", &timeinfo);

  GString *message_out = g_string_new(date);
  if (log_level & G_LOG_LEVEL_DEBUG) {
    g_string_append(message_out, " [DEBUG] - ");
  } else if ((log_level & G_LOG_LEVEL_INFO) ||
             (log_level & G_LOG_LEVEL_MESSAGE)) {
    g_string_append(message_out, " [INFO] - ");
  } else if (log_level & G_LOG_LEVEL_WARNING) {
    g_string_append(message_out, " [WARNING] - ");
  } else if ((log_level & G_LOG_LEVEL_ERROR) ||
             (log_level & G_LOG_LEVEL_CRITICAL)) {
    g_string_append(message_out, " [ERROR] - ");
  }

  g_string_append_printf(message_out, "%s\n", message);
  if (write(fileno(logoutfile), message_out->str, message_out->len) <= 0) {
    fprintf(stderr, "Cannot write to log file with error %d.  Exiting...",
            errno);
  }
  g_string_free(message_out, TRUE);
}
