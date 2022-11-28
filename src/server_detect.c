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

int product=0;
int major=0;
int secondary=0;
int revision=0;

void detect_server_version(MYSQL * conn) {
  mysql_query(conn, "SELECT @@version_comment, @@version");
  MYSQL_RES *res = mysql_store_result(conn);
  MYSQL_ROW ver;
  ver = mysql_fetch_row(res);
  
  if (g_str_has_prefix(ver[0], "Percona")){
    product = SERVER_TYPE_PERCONA;
  }else
  if (g_str_has_prefix(ver[0], "MySQL")){
    product = SERVER_TYPE_MYSQL;    
  }else 
  if (g_str_has_prefix(ver[0], "mariadb")){
    product = SERVER_TYPE_MARIADB;
  }
  gchar ** sver=g_strsplit(ver[1],".",3);
  major=strtol(sver[0], NULL, 10);
  secondary=strtol(sver[1], NULL, 10);
  revision=strtol(sver[2], NULL, 10);
  g_strfreev(sver);
  mysql_free_result(res);
}

int get_product(){
  return product;
}
int get_major(){
    return major;
}
int get_secondary(){
      return secondary;
}
int get_revision(){
      return revision;
}
