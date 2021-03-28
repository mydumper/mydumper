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

        Authors: 	Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle
   dot com) Andrew Hutchings, SkySQL (andrew at skysql dot com)

*/

#ifndef _binlog_h
#define _binlog_h
#include "mydumper.h"

void get_binlogs(MYSQL *conn, struct configuration *conf);
void get_binlog_file(MYSQL *conn, char *binlog_file,
                     const char *binlog_directory, guint64 start_position,
                     guint64 stop_position, gboolean continuous);
unsigned int get_event(const char *buf, unsigned int len);
void write_binlog(FILE *file, const char *data, guint64 len);

#endif
