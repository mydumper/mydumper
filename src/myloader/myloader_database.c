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

#include "myloader_common.h"
#include "myloader_global.h"
#include "myloader_database.h"

GHashTable *database_hash=NULL;
static GMutex *database_hash_mutex = NULL;

struct database *database_db=NULL;

void initialize_database(){
  database_hash_mutex=g_mutex_new();
  database_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );
  if (db){
    database_db=get_database(g_strdup(db), g_strdup(db));
  }
}

struct database * new_database(gchar *database, gchar *filename){
  struct database * _database = g_new(struct database, 1);
  _database->source_database=database;
  _database->target_database = g_strdup(db ? db : _database->source_database);
  _database->database_name_in_filename = filename;
  _database->mutex=g_mutex_new();
  _database->sequence_queue= g_async_queue_new();
  _database->control_job_queue=g_async_queue_new();
  _database->schema_state=NOT_FOUND;
  _database->schema_checksum=NULL;
  _database->post_checksum=NULL;
  _database->triggers_checksum=NULL;
  _database->events_checksum=NULL;
  return _database;
}

struct database * get_database(gchar *filename, gchar *name){
  g_mutex_lock(database_hash_mutex);
  struct database * _database=g_hash_table_lookup(database_hash, filename);
  if (_database==NULL){
    _database=new_database(g_strdup(name), filename);
    g_hash_table_insert(database_hash, filename, _database);
    if (g_strcmp0(filename,name))
      g_hash_table_insert(database_hash, g_strdup(name), _database);
    _database=g_hash_table_lookup(database_hash, name);
  }else{
    if (filename != name){
      _database->source_database=g_strdup(name);
      _database->target_database = g_strdup(db ? db : _database->source_database);
    }
  }
  g_mutex_unlock(database_hash_mutex);
  return _database;
}

gboolean execute_use(struct connection_data *cd){
  if (cd->current_database){
    gchar *query = g_strdup_printf("USE `%s`", cd->current_database->target_database);
    if (m_query_warning(cd->thrconn, query, "Thread %d: Error switching to database `%s`", cd->thread_id, cd->current_database->target_database)) {
      g_free(query);
      return TRUE;
    }
    g_free(query);
  }else{
    g_warning("Thread %ld with connection %ld: Not able to switch database",cd->thread_id, cd->connection_id);
  }
  return FALSE;
}

void execute_use_if_needs_to(struct connection_data *cd, struct database *database, const gchar * msg){
  if ( database != NULL && (db == NULL || cd->current_database==NULL)){
    if (cd->current_database==NULL || g_strcmp0(database->target_database, cd->current_database->target_database) != 0){
      cd->current_database=database;
      if (execute_use(cd)){
        m_critical("Thread %ld with connection %ld: Error switching to database `%s` %s: %s", cd->thread_id, cd->connection_id, cd->current_database->target_database, msg, mysql_error(cd->thrconn));
      }
    }
  }
}

