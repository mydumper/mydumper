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



extern enum checksum_modes checksum_mode;
extern gboolean skip_database_checksums;
extern gboolean skip_data_checksums;
extern gboolean skip_table_checksums;
extern gboolean skip_index_checksums;
extern gboolean skip_view_checksums;
extern gboolean skip_trigger_checksums;
extern gboolean skip_routine_checksums;
extern gboolean skip_event_checksums;

extern gboolean data_checksums;
extern gboolean dump_checksums;
extern gboolean routine_checksums;
extern gboolean schema_checksums;

enum checksum_modes {
  CHECKSUM_SKIP= 0,
  CHECKSUM_WARN,
  CHECKSUM_FAIL
};

struct database_level_checksum{
  gchar *schema;
  gchar *routine;
  gchar *trigger;
  gchar *event;
  gboolean skip_schema;
  gboolean skip_routine;
  gboolean skip_trigger;
  gboolean skip_event;

};

struct table_level_checksum{
  gchar *schema;
  gchar *index;
  gchar *trigger;
  gchar *data;
  gboolean skip_schema;
  gboolean skip_index;
  gboolean skip_trigger;
  gboolean skip_data;
};

char * checksum_table_structure(MYSQL *conn, char *database, char *table);
char * checksum_table(MYSQL *conn, char *database, char *table);
char * checksum_process_structure(MYSQL *conn, char *database, char *table);
char * checksum_trigger_structure(MYSQL *conn, char *database, char *table);
char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table);
char * checksum_events_structure_from_database(MYSQL *conn, char *database, char *table);
char * checksum_view_structure(MYSQL *conn, char *database, char *table);
char * checksum_database_defaults(MYSQL *conn, char *database, char *table);
char * checksum_table_indexes(MYSQL *conn, char *database, char *table);

gboolean should_write_database_checksum(struct database_level_checksum *database_checksum);
void write_database_checksum(FILE *mdfile, struct database_level_checksum *database_checksum);

gboolean checksum_database(gchar *target_database, struct database_level_checksum *database_checksum, MYSQL *conn);
gboolean checksum_dbt(gchar *target_database, gchar *source_table_name, gboolean is_view, struct table_level_checksum *table_checksum,  MYSQL *conn);
void initilize_checksum();
#endif
