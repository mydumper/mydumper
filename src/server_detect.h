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
#ifndef _server_detect_h
#define _server_detect_h

#include <mysql.h>

enum server_type {
  SERVER_TYPE_UNKNOWN= 0,
  SERVER_TYPE_MYSQL,
  SERVER_TYPE_TIDB,
  SERVER_TYPE_MARIADB,
  SERVER_TYPE_PERCONA,
  SERVER_TYPE_CLICKHOUSE,
  SERVER_TYPE_DOLT
};
void server_detect(MYSQL * conn);
gboolean server_support_tablespaces();
int get_product();
int get_major();
int get_secondary();
int get_revision();
gboolean is_mysql_like();
const gchar * get_product_name();
#endif



