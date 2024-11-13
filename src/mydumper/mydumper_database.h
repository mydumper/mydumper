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

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include <stdio.h>

struct database {
  char *name;
  char *filename;
  char *escaped;
  GMutex *ad_mutex;
  gboolean already_dumped;
  gchar *schema_checksum;
  gchar *post_checksum;
  gchar *triggers_checksum;
  gboolean dump_triggers;
};

void initialize_database();
struct database * new_database(MYSQL *conn, char *database_name, gboolean already_dumped);
gboolean get_database(MYSQL *conn, char *database_name, struct database ** database);
void free_databases();
void write_database_on_disk(FILE *mdfile);
