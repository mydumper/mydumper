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
//#include <zlib.h>
#include <pcre.h>
#include <signal.h>
#include <glib/gstdio.h>
#include "logging.h"
#include "common.h"

/* two handlers currently defined: no_log, write_log_file */
#define total_handlers 2
#define use_no_log 0
#define use_write_log_file 1

static guint log_handlers[total_handlers] = {};

void free_log_handlers() {
  int x = 0;
  for (x = 0; x < total_handlers; x++) {
    if (log_handlers[x] != 0) {
      g_log_remove_handler(NULL, log_handlers[x]);
      log_handlers[x] = 0;
    }
  }
}

void set_debug() {
#if GLIB_CHECK_VERSION(2,72,0)
  g_log_set_debug_enabled(TRUE);
#endif
}

void set_verbose(guint verbosity) {
  if (logfile) {
    logoutfile = g_fopen(logfile, "w");
    if (!logoutfile) {
      m_critical("Could not open log file '%s' for writing: %d", logfile,
                 errno);
    }
  }

  free_log_handlers();

  switch (verbosity) {
  case 0:
    log_handlers[use_no_log] = g_log_set_handler(
        NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK),
        no_log, NULL);
    break;
  case 1:
    log_handlers[use_no_log] = g_log_set_handler(
        NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_MESSAGE),
        no_log, NULL);
    if (logfile)
      log_handlers[use_write_log_file] = g_log_set_handler(
          NULL, (GLogLevelFlags)(G_LOG_LEVEL_ERROR | G_LOG_LEVEL_CRITICAL),
          write_log_file, NULL);
    break;
  case 2:
    log_handlers[use_no_log] = g_log_set_handler(
        NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE),
        no_log, NULL);
    if (logfile)
      log_handlers[use_write_log_file] = g_log_set_handler(
          NULL,
          (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR |
                           G_LOG_LEVEL_CRITICAL),
          write_log_file, NULL);
    break;
  default:
    if (logfile)
      log_handlers[use_write_log_file] = g_log_set_handler(
          NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK),
          write_log_file, NULL);
    break;
  }
}
