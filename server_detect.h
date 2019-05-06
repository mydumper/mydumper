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
#ifndef _server_detect_h
#define _server_detect_h

#include <mysql.h>

#define DETECT_MYSQL_REGEX "^([3-9]\\.[0-9]+\\.[0-9]+)"
#define DETECT_DRIZZLE_REGEX "^(20[0-9]{2}\\.(0[1-9]|1[012])\\.[0-9]+)"
#define DETECT_MARIADB_REGEX "^([0-9]{1,2}\\.[0-9]+\\.[0-9]+)"
#define DETECT_TIDB_REGEX "TiDB"

enum server_type {
  SERVER_TYPE_UNKNOWN,
  SERVER_TYPE_MYSQL,
  SERVER_TYPE_DRIZZLE,
  SERVER_TYPE_TIDB
};
int detect_server(MYSQL *conn);
#endif
