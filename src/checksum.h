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

    Authors:        David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <mysql.h>
#ifndef _src_checksum_h
#define _src_checksum_h
char * checksum_table_structure(MYSQL *conn, char *database, char *table);
char * checksum_table(MYSQL *conn, char *database, char *table);
char * checksum_process_structure(MYSQL *conn, char *database, char *table);
char * checksum_trigger_structure(MYSQL *conn, char *database, char *table);
char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table);
char * checksum_events_structure_from_database(MYSQL *conn, char *database, char *table);
char * checksum_view_structure(MYSQL *conn, char *database, char *table);
char * checksum_database_defaults(MYSQL *conn, char *database, char *table);
char * checksum_table_indexes(MYSQL *conn, char *database, char *table);
#endif
