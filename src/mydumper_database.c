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
#include <mysql.h>
#include <glib.h>
#include "mydumper_common.h"
#include "mydumper_database.h"

GHashTable *database_hash=NULL;

void initialize_database(){
  database_hash=g_hash_table_new ( g_str_hash, g_str_equal );
}

struct database * new_database(MYSQL *conn, char *database_name, gboolean already_dumped){
  struct database * d=g_new(struct database,1);
  d->name = g_strdup(database_name);
  d->filename = get_ref_table(d->name);
  d->escaped = escape_string(conn,d->name);
  d->already_dumped = already_dumped;
  d->ad_mutex=g_mutex_new();
  g_hash_table_insert(database_hash, d->name,d);
  return d;
}

gboolean get_database(MYSQL *conn, char *database_name, struct database ** database){
  *database=g_hash_table_lookup(database_hash,database_name);
  if (*database == NULL){
    *database=new_database(conn,database_name,FALSE);
    return TRUE;
  }
  return FALSE;
}


