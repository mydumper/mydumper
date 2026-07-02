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
#include "myloader_restore_job.h"
#include "myloader_restore.h"
#include "myloader_worker_schema.h"
//#include "../logging.h"

GHashTable *database_hash=NULL;
static GMutex *database_hash_mutex = NULL;
gchar *target_db=NULL;
//struct database *
GList *database_db=NULL;

gboolean has_been_defined_a_target_database(){
  return database_db!=NULL;
}

static
struct database * add_new_database(gchar *source_database, gchar *target_database);

void initialize_database(){
  database_hash_mutex=g_mutex_new();
  database_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );
  if (target_db){
    gchar **kv=NULL;
    GList *database_list=m_glistsplit(target_db);
    if (g_list_length(database_list) > 1){
      while (database_list){
        g_message("Working with %s", (gchar*)(database_list->data));
        kv=g_strsplit(database_list->data,":",2);
        if (g_strv_length(kv)!=2)
          m_error("Failed to parser element `%s` on database list: %s", database_list->data, target_db);
        database_db=g_list_prepend(database_db,add_new_database(g_strdup(kv[0]), g_strdup(kv[1])));
        database_list = g_list_delete_link(database_list, database_list);
        g_strfreev(kv);
      }

    }else{
      kv=g_strsplit(target_db,":",2);
      if (g_strv_length(kv)>1)
        database_db=g_list_prepend(database_db,add_new_database(g_strdup(kv[0]), g_strdup(kv[1])));
      else
        database_db=g_list_prepend(database_db,add_new_database(g_strdup(target_db), g_strdup(target_db)));
      database_list = g_list_delete_link(database_list, database_list);
      g_strfreev(kv);
    }
  }
}

gint find_database_with_source_database( gconstpointer a , gconstpointer b){
  const gchar *a_source_database=b;
  gchar *b_source_database=((struct database *)a)->source_database;
  return g_strcmp0(a_source_database, b_source_database);
}

static
struct database * new_database(gchar *filename, gchar *source_database, gchar *target_database){
  struct database * _database = g_new(struct database, 1);

  _database->source_database = g_strdup(source_database);
  _database->target_database = g_strdup(target_database); //first_stage? :get_target_db(_database->source_database, first_stage);
  _database->database_name_in_filename = g_strdup(filename);

  _database->schema_state=target_db?CREATED:NOT_FOUND;

  _database->mutex=g_mutex_new();
  _database->sequence_queue= g_async_queue_new();
  _database->table_queue=g_async_queue_new();

  _database->checksum.schema=NULL;
  _database->checksum.routine=NULL;
  _database->checksum.trigger=NULL;
  _database->checksum.event=NULL;
  gchar * any_table_config_file_dbt_key = build_config_file_dbt_key(_database->source_database,"");
  GHashTable *cpt = g_hash_table_lookup(conf_per_table,SKIP_DATABASE_CHECKSUMS);
  gboolean c=FALSE;
  if (cpt)
    c=GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c=FALSE;
  _database->checksum.skip_schema = c?c:skip_database_checksums;
  cpt = g_hash_table_lookup(conf_per_table,SKIP_ROUTINE_CHECKSUMS);
  if (cpt)
    c=GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c=FALSE;
  _database->checksum.skip_routine=c?c:skip_routine_checksums;
  cpt = g_hash_table_lookup(conf_per_table,SKIP_TRIGGER_CHECKSUMS);
  if (cpt)
    c=GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c=FALSE;
  _database->checksum.skip_trigger=c?c:skip_trigger_checksums;
  cpt = g_hash_table_lookup(conf_per_table,SKIP_EVENT_CHECKSUMS);
  if (cpt)
    c=GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c=FALSE;
  _database->checksum.skip_event=c?c:skip_event_checksums;
  g_free(any_table_config_file_dbt_key);
  return _database;
}


static
struct database * add_new_database0(gchar *source_database){
  struct database * _database=new_database(source_database, source_database, source_database);
  g_hash_table_insert(database_hash, g_strdup(source_database), _database);
  return _database;
}

static
struct database * add_new_database(gchar *source_database, gchar *target_database){
  struct database * _database=new_database(NULL, source_database, target_database);
  g_hash_table_insert(database_hash, source_database, _database);
  if (g_strcmp0(source_database,target_database))
    g_hash_table_insert(database_hash, g_strdup(target_database), _database);
  return _database;
}

struct database * get_database2(gchar *filename_database, gchar *founded_database){
  // This function is only used when the filename has prefix "mydumper_"
  g_mutex_lock(database_hash_mutex);
  // it will be faster to find the filename_database even if is is not the source_database
  struct database * _database=g_hash_table_lookup(database_hash, filename_database);
  if (_database==NULL){
//    _database=new_database(g_strdup(name), filename);
//    g_hash_table_insert(database_hash, filename, _database);
    if (target_db){
      if (g_hash_table_size(database_hash)==1){
        GHashTableIter iter;
        gpointer _key;
        struct database *__database;
        g_hash_table_iter_init(&iter, database_hash);
        g_hash_table_iter_next(&iter, &_key, (gpointer *)&__database);
        if (!g_strcmp0(__database->source_database, __database->target_database)){
          // means all tables goes to this database
          _database=add_new_database(filename_database, __database->target_database);
        }else{
          m_error("You defined multiple database relationships in -B but %s was not found in %s", filename_database, target_db);
        }
      }else{
        m_error("You defined multiple database relationships in -B but %s was not found in %s", filename_database, target_db);
      }
    }else
      _database=add_new_database(filename_database, founded_database);
//    if (g_strcmp0(filename,name))
//      g_hash_table_insert(database_hash, g_strdup(name), _database);
    _database=g_hash_table_lookup(database_hash, filename_database);
  }else{
    _database->source_database = g_strdup(founded_database);
    if (target_db)
      _database->target_database = g_strdup(target_db ? _database->target_database : _database->source_database);
  }
  g_mutex_unlock(database_hash_mutex);
  return _database;
}

struct database * get_database(gchar *source_database){
  g_mutex_lock(database_hash_mutex);
  struct database * _database=g_hash_table_lookup(database_hash, source_database);
  g_message("ACA1");
  if (_database==NULL){
    if (target_db){
      if (g_hash_table_size(database_hash)==1){
        GHashTableIter iter;
        gpointer _key;
        struct database *__database;
        g_hash_table_iter_init(&iter, database_hash);
        g_hash_table_iter_next(&iter, &_key, (gpointer *)&__database);
        if (!g_strcmp0(__database->source_database, __database->target_database)){
          // means all tables goes to this database
          _database=add_new_database(source_database, __database->target_database);
        }else{
          m_error("You defined multiple database relationships in -B but %s was not found in %s", source_database, target_db);
        }
      }else{
        m_error("You defined multiple database relationships in -B but %s was not found in %s", source_database, target_db);
      }
    }else
      _database=add_new_database0(source_database);
  }else{
    g_message("ACA2");
    _database=add_new_database0(source_database);
    g_message("ACA3");
  }
  g_message("ACA4");
  g_mutex_unlock(database_hash_mutex);
  g_message("ACA: %s", _database->target_database);
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
    if (machine_log_json_enabled()) {
      gchar *thread_id = g_strdup_printf("%lu", cd->thread_id);
      gchar *connection_id = g_strdup_printf("%lu", cd->connection_id);
      machine_log_event(G_LOG_DOMAIN, G_LOG_LEVEL_WARNING,
                        "MESSAGE", "Not able to switch database",
                        "EVENT", "database_switch",
                        "PHASE", "restore_schema",
                        "STATUS", "failed",
                        "THREAD_ID", thread_id,
                        "CONNECTION_ID", connection_id,
                        "RETRYABLE", "false",
                        "FATAL", "false",
                        NULL);
      g_free(thread_id);
      g_free(connection_id);
    }
    g_warning("Thread %ld with connection %ld: Not able to switch database",cd->thread_id, cd->connection_id);
  }
  return FALSE;
}

void execute_use_if_needs_to(struct connection_data *cd, struct database *database, const gchar * msg){
  if ( database != NULL && (target_db == NULL || cd->current_database==NULL)){
    if (cd->current_database==NULL || g_strcmp0(database->target_database, cd->current_database->target_database) != 0){
      cd->current_database=database;
      if (execute_use(cd)){
        m_critical("Thread %ld with connection %ld: Error switching to database `%s` %s: %s", cd->thread_id, cd->connection_id, cd->current_database->target_database, msg, mysql_error(cd->thrconn));
      }
    }
  }
}

void create_database(struct thread_data *td, gchar *database) {

  const gchar *filename =
      g_strdup_printf("%s-schema-create.sql%s", database, exec_per_thread_extension?exec_per_thread_extension:"");
  const gchar *filepath = g_strdup_printf("%s/%s",
                                            directory, filename);

  if (drop_database)
    execute_drop_database(td, database);

  if (g_file_test(filepath, G_FILE_TEST_EXISTS)) {
    trace("Creating database from %s", filename);
    g_atomic_int_add(&(detailed_errors.schema_errors), restore_data_from_mydumper_file(td, filename, TRUE, NULL));
  } else {
    GString *data = g_string_new("CREATE DATABASE IF NOT EXISTS ");
    g_string_append_printf(data,"`%s`", database);
    trace("Creating schema %s as %s not found", database, filepath);
    if (restore_data_in_gstring_extended(td, data , TRUE, NULL, m_critical, "Failed to create database: %s", database) )
      g_atomic_int_inc(&(detailed_errors.schema_errors));
    g_string_free(data, TRUE);
  }

  return;
}

void start_database(struct thread_data *td){
  if (database_db){
    GList *_database_db = database_db;
    while (_database_db){
      if (!no_schemas)
        create_database(td, ((struct database *)_database_db->data)->target_database);
      ((struct database *)_database_db->data)->schema_state=CREATED;
      _database_db=_database_db->next;
    }
  }
}

void set_all_databases_as_created(){
  struct database *_database;
  GHashTableIter iter;
  gpointer _key;
  g_hash_table_iter_init (&iter, database_hash);
  while (g_hash_table_iter_next (&iter, &_key, (gpointer) &_database)){
    g_mutex_lock(_database->mutex);
    set_db_schema_created(_database);
    g_mutex_unlock(_database->mutex);
  }
}

// _database is locked
void set_db_schema_created(struct database * _database)
{
  _database->schema_state= CREATED;

  struct schema_job *sj = g_async_queue_try_pop(_database->sequence_queue);
  while (sj){
    schema_job_queue_push(sj);
    sj = g_async_queue_try_pop(_database->sequence_queue);
  }
  sj = g_async_queue_try_pop(_database->table_queue);
  while (sj){
    schema_job_queue_push(sj);
    sj = g_async_queue_try_pop(_database->table_queue);
  }
}


