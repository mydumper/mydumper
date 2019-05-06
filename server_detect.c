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

    Authors:        Andrew Hutchings, SkySQL (andrew at skysql dot com)
*/

#include <pcre.h>
#include <glib.h>
#include <string.h>
#include "server_detect.h"

int detect_server(MYSQL *conn) {
  pcre *re = NULL;
  const char *error;
  int erroroffset;
  int ovector[9] = {0};
  int rc;
  const char *db_version = mysql_get_server_info(conn);

  // debug the version
  g_message("Server version reported as: %s", db_version);

  re = pcre_compile(DETECT_TIDB_REGEX, 0, &error, &erroroffset, NULL);
  if (!re) {
    g_critical("Regular expression fail: %s", error);
    exit(EXIT_FAILURE);
  }

  rc = pcre_exec(re, NULL, db_version, strlen(db_version), 0, 0, ovector, 9);
  pcre_free(re);

  if (rc > 0) {
    return SERVER_TYPE_TIDB;
  }

  re = pcre_compile(DETECT_MYSQL_REGEX, 0, &error, &erroroffset, NULL);
  if (!re) {
    g_critical("Regular expression fail: %s", error);
    exit(EXIT_FAILURE);
  }

  rc = pcre_exec(re, NULL, db_version, strlen(db_version), 0, 0, ovector, 9);
  pcre_free(re);

  if (rc > 0) {
    return SERVER_TYPE_MYSQL;
  }

  re = pcre_compile(DETECT_DRIZZLE_REGEX, 0, &error, &erroroffset, NULL);
  if (!re) {
    g_critical("Regular expression fail: %s", error);
    exit(EXIT_FAILURE);
  }

  rc = pcre_exec(re, NULL, db_version, strlen(db_version), 0, 0, ovector, 9);
  pcre_free(re);

  if (rc > 0) {
    return SERVER_TYPE_DRIZZLE;
  }

  re = pcre_compile(DETECT_MARIADB_REGEX, 0, &error, &erroroffset, NULL);
  if (!re) {
    g_critical("Regular expression fail: %s", error);
    exit(EXIT_FAILURE);
  }

  rc = pcre_exec(re, NULL, db_version, strlen(db_version), 0, 0, ovector, 9);
  pcre_free(re);

  if (rc > 0) {
    return SERVER_TYPE_MYSQL;
  }

  return SERVER_TYPE_UNKNOWN;
}
