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
                        Mark Leith, Oracle Corporation (mark dot leith at oracle
   dot com) Andrew Hutchings, SkySQL (andrew at skysql dot com)

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

#include "logging.h"

void set_verbose(guint verbosity) {
  if (logfile) {
    logoutfile = g_fopen(logfile, "w");
    if (!logoutfile) {
      g_critical("Could not open log file '%s' for writing: %d", logfile,
                 errno);
      exit(EXIT_FAILURE);
    }
  }

  switch (verbosity) {
  case 0:
    g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK), no_log, NULL);
    break;
  case 1:
    g_log_set_handler(
        NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_MESSAGE),
        no_log, NULL);
    if (logfile)
      g_log_set_handler(
          NULL, (GLogLevelFlags)(G_LOG_LEVEL_ERROR | G_LOG_LEVEL_CRITICAL),
          write_log_file, NULL);
    break;
  case 2:
    g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE), no_log,
                      NULL);
    if (logfile)
      g_log_set_handler(
          NULL,
          (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR |
                           G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR |
                           G_LOG_LEVEL_CRITICAL),
          write_log_file, NULL);
    break;
  default:
    if (logfile)
      g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK),
                        write_log_file, NULL);
    break;
  }
}
