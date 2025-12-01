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

#include <glib.h>

#include "mydumper_common.h"
#include "mydumper_database.h"
#include "mydumper_global.h"
#include "mydumper_create_jobs.h"

GHashTable *database_hash = NULL;
static GMutex * database_hash_mutex = NULL;
gchar *source_db;

void free_database(struct database * _database){
  if (_database->source_database_escaped!=NULL){
    g_free(_database->source_database_escaped);
    _database->source_database_escaped=NULL;
  }
//  if (_database->ad_mutex){
//    g_mutex_free(_database->ad_mutex);
//    _database->ad_mutex=NULL;
//  }
  g_free(_database);
}

void initialize_database(){
  database_hash=g_hash_table_new_full( g_str_hash, g_str_equal,  &g_free, (GDestroyNotify) &free_database );
  database_hash_mutex=g_mutex_new();
}

static
struct database * new_database(MYSQL *conn, char *database_name){
  struct database * _database=g_new(struct database,1);
  _database->source_database = backtick_protect(database_name);
  _database->database_name_in_filename = get_ref_table(_database->source_database);
  _database->source_database_escaped = escape_string(conn,_database->source_database);
//  _database->already_dumped = already_dumped;
//  _database->ad_mutex=g_mutex_new();
  _database->schema_checksum=NULL;
  _database->post_checksum=NULL;
  _database->triggers_checksum=NULL;
  _database->events_checksum=NULL;
  _database->dump_triggers= !is_regex_being_used() && tables_list == NULL && g_hash_table_size(conf_per_table.all_object_to_export)==0;
  g_hash_table_insert(database_hash, _database->source_database,_database);
  return _database;
}

void free_databases(){
  g_mutex_lock(database_hash_mutex);
  g_hash_table_destroy(database_hash);
  g_mutex_unlock(database_hash_mutex);
  g_mutex_free(database_hash_mutex);
}

struct database * get_database(MYSQL *conn, char *database_name, gboolean create_job){
  g_mutex_lock(database_hash_mutex);
  struct database *database=g_hash_table_lookup(database_hash,database_name);
  if (database == NULL){
    database=new_database(conn,database_name);
    if (create_job)
      create_job_to_dump_schema(database);
  }
  g_mutex_unlock(database_hash_mutex);
  return database;
}

// see print_dbt_on_metadata_gstring() for table write to metadata
void write_database_on_disk(FILE *mdfile){
  const char q= identifier_quote_character;
  struct database *_database;
  GList *keys= g_hash_table_get_keys(database_hash);
  keys= g_list_sort(keys, key_strcmp);
  for (GList *it= keys; it; it= g_list_next(it)) {
    _database= (struct database *) g_hash_table_lookup(database_hash, it->data);
    g_assert(_database);
    if (_database->schema_checksum != NULL || _database->post_checksum != NULL || _database->triggers_checksum)
      fprintf(mdfile, "\n[%c%s%c]\n", q, _database->source_database, q);
    if (_database->schema_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "schema_checksum", _database->schema_checksum);
    if (_database->post_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "post_checksum", _database->post_checksum);
    if (_database->events_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "events_checksum", _database->events_checksum);
    if (_database->triggers_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "triggers_checksum", _database->triggers_checksum);
  }
  g_list_free(keys);
}

// OPTIMIZATION: Unsorted version - skips O(n*log(n)) sort for large database counts
void write_database_on_disk_unsorted(FILE *mdfile){
  const char q= identifier_quote_character;
  struct database *_database;
  GList *keys= g_hash_table_get_keys(database_hash);
  // Skip sorting - saves time on 1000+ databases
  for (GList *it= keys; it; it= g_list_next(it)) {
    _database= (struct database *) g_hash_table_lookup(database_hash, it->data);
    g_assert(_database);
    if (_database->schema_checksum != NULL || _database->post_checksum != NULL || _database->triggers_checksum)
      fprintf(mdfile, "\n[%c%s%c]\n", q, _database->source_database, q);
    if (_database->schema_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "schema_checksum", _database->schema_checksum);
    if (_database->post_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "post_checksum", _database->post_checksum);
    if (_database->events_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "events_checksum", _database->events_checksum);
    if (_database->triggers_checksum != NULL)
      fprintf(mdfile, "%s = %s\n", "triggers_checksum", _database->triggers_checksum);
  }
  g_list_free(keys);
}

