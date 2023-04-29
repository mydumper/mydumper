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
#include "mydumper_global.h"
GHashTable *database_hash = NULL;
GMutex * database_hash_mutex = NULL;
extern char *regex; 
void free_database(struct database * d){
//  g_free(d->name);
  if (d->escaped!=NULL){
    g_free(d->escaped);
    d->escaped=NULL;
  }
  if (d->ad_mutex){
    g_mutex_free(d->ad_mutex);
    d->ad_mutex=NULL;
  }
/*  if (d->name!=NULL){
    g_free(d->name);
    d->name=NULL;
  }
  if (d->filename){
    g_free(d->filename);
    d->filename=NULL;
  }*/
  g_free(d);
}

void initialize_database(){
  database_hash=g_hash_table_new_full( g_str_hash, g_str_equal,  &g_free, (GDestroyNotify) &free_database );
  database_hash_mutex=g_mutex_new(); 
}

struct database * new_database(MYSQL *conn, char *database_name, gboolean already_dumped){
  struct database * d=g_new(struct database,1);
  d->name = g_strdup(database_name);
  d->filename = get_ref_table(d->name);
  d->escaped = escape_string(conn,d->name);
  d->already_dumped = already_dumped;
  d->ad_mutex=g_mutex_new();
  d->schema_checksum=NULL;
  d->post_checksum=NULL;
  d->triggers_checksum=NULL;
  d->dump_triggers= regex == NULL && tables_list == NULL;
  g_hash_table_insert(database_hash, d->name,d);
  return d;
}

void free_databases(){
  g_mutex_lock(database_hash_mutex);
  g_hash_table_destroy(database_hash);
  g_mutex_unlock(database_hash_mutex);
  g_mutex_free(database_hash_mutex);
}


gboolean get_database(MYSQL *conn, char *database_name, struct database ** database){
  g_mutex_lock(database_hash_mutex);
  *database=g_hash_table_lookup(database_hash,database_name);
  if (*database == NULL){
    *database=new_database(conn,database_name,FALSE);
    g_mutex_unlock(database_hash_mutex);
    return TRUE;
  }
  g_mutex_unlock(database_hash_mutex);
  return FALSE;
}

void write_database_on_disk(FILE *mdfile){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, database_hash);
  struct database *d=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &d ) ) {
    if (d->schema_checksum != NULL || d->post_checksum != NULL || d->triggers_checksum)
      fprintf(mdfile, "\n[`%s`]\n", d->name);
    if (d->schema_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "schema_checksum", d->schema_checksum);
    if (d->post_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "post_checksum", d->post_checksum);
    if (d->triggers_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "triggers_checksum", d->triggers_checksum);
  }
}

