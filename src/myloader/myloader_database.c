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

#include "myloader/myloader_database.h"

#include "myloader/myloader_common.h"
#include "myloader/myloader_global.h"
#include "myloader/myloader_restore.h"
#include "myloader/myloader_restore_job.h"
#include "myloader_worker_schema.h"
#include "logging.h"

GHashTable    *database_hash = NULL;
static GMutex *database_hash_mutex = NULL;
gchar         *target_db = NULL;
// -B accepts a single database, where every database is restored into, or a
// comma-separated list of source:target pairs, where only the listed databases
// are renamed and the rest keep their own name.
static gchar      *single_target_database = NULL;
static GHashTable *target_database_by_source = NULL;

static struct database *add_new_database(gchar *filename_database, gchar *source_database);
extern int (*restore_data_from_file)(struct thread_data *, const char *, gboolean, struct database *, enum restore_job_statement_type restore_job_statement_type);

gboolean has_been_defined_a_target_database()
{
  return database_db != NULL;
}

void initialize_database()
{
  database_hash_mutex = g_mutex_new();
  database_hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
  target_database_by_source = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
  if (!target_db)
    return;
  if (!strchr(target_db, ':'))
  {
    if (strchr(target_db, ','))
      m_error("-B must be a single database or a list of source:target pairs: %s", target_db);
    single_target_database = g_strdup(target_db);
    return;
  }
  GList *database_list = m_glistsplit(target_db);
  while (database_list)
  {
    gchar **kv = g_strsplit(database_list->data, ":", 2);
    if (g_strv_length(kv) != 2 || !strlen(kv[0]) || !strlen(kv[1]))
      m_error("Failed to parse `%s` on -B, expected source:target: %s", (gchar *)database_list->data, target_db);
    if (g_hash_table_contains(target_database_by_source, kv[0]))
      m_error("Database `%s` is defined more than once on -B: %s", kv[0], target_db);
    g_hash_table_insert(target_database_by_source, g_strdup(kv[0]), g_strdup(kv[1]));
    g_strfreev(kv);
    g_free(database_list->data);
    database_list = g_list_delete_link(database_list, database_list);
  }
}

// @return the database defined with -B for source_database, or NULL when it
// must be restored with its own name
static gchar *get_target_database(gchar *source_database)
{
  if (single_target_database)
    return single_target_database;
  return g_hash_table_lookup(target_database_by_source, source_database);
}

static void set_target_database(struct database *_database)
{
  gchar *target_database = get_target_database(_database->source_database);
  _database->target_defined = target_database != NULL;
  _database->target_database = target_database ? g_strdup(target_database) : newline_unprotect(_database->source_database);
}

static struct database *new_database(gchar *filename, gchar *source_database)
{
  struct database *_database = g_new(struct database, 1);

  _database->source_database = g_strdup(source_database);
  _database->database_name_in_filename = g_strdup(filename);
  set_target_database(_database);

  _database->schema_state = _database->target_defined ? CREATED : NOT_FOUND;

  _database->mutex = g_mutex_new();
  _database->sequence_queue = g_async_queue_new();
  _database->table_queue = g_async_queue_new();
  _database->checksum.schema = NULL;
  _database->checksum.routine = NULL;
  _database->checksum.trigger = NULL;
  _database->checksum.event = NULL;
  gchar      *any_table_config_file_dbt_key = build_config_file_dbt_key(_database->source_database, "");
  GHashTable *cpt = g_hash_table_lookup(conf_per_table, SKIP_DATABASE_CHECKSUMS);
  gboolean    c = FALSE;
  if (cpt)
    c = GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c = FALSE;
  _database->checksum.skip_schema = c ? c : skip_database_checksums;
  cpt = g_hash_table_lookup(conf_per_table, SKIP_ROUTINE_CHECKSUMS);
  if (cpt)
    c = GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c = FALSE;
  // Routines/Events that are not imported can not be verified
  _database->checksum.skip_routine = c ? c : (skip_routine_checksums || skip_routines);
  cpt = g_hash_table_lookup(conf_per_table, SKIP_TRIGGER_CHECKSUMS);
  if (cpt)
    c = GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c = FALSE;
  _database->checksum.skip_trigger = c ? c : skip_trigger_checksums;
  cpt = g_hash_table_lookup(conf_per_table, SKIP_EVENT_CHECKSUMS);
  if (cpt)
    c = GPOINTER_TO_INT(g_hash_table_lookup(cpt, any_table_config_file_dbt_key));
  else
    c = FALSE;
  _database->checksum.skip_event = c ? c : (skip_event_checksums || skip_events);
  g_free(any_table_config_file_dbt_key);
  return _database;
}

static struct database *add_new_database(gchar *filename_database, gchar *source_database)
{
  trace("Adding database: %s -> %s", filename_database, source_database);
  struct database *_database = new_database(filename_database, source_database);
  g_hash_table_insert(database_hash, g_strdup(filename_database), _database);
  return _database;
}

struct database *get_database_with_prefix(gchar *filename_database, gchar *founded_database)
{
  // This function is only used when the filename has prefix "mydumper_"
  g_mutex_lock(database_hash_mutex);
  // it will be faster to find the filename_database even if is is not the source_database
  struct database *_database = g_hash_table_lookup(database_hash, filename_database);
  if (_database == NULL)
    _database = add_new_database(filename_database, founded_database);
  else if (g_strcmp0(_database->source_database, founded_database))
  {
    // created before its schema-create file was processed, with the name in
    // the filename as source_database
    _database->source_database = g_strdup(founded_database);
    set_target_database(_database);
  }
  g_mutex_unlock(database_hash_mutex);
  return _database;
}

struct database *get_database(gchar *source_database)
{
  g_mutex_lock(database_hash_mutex);
  struct database *_database = g_hash_table_lookup(database_hash, source_database);
  if (_database == NULL)
  {
    _database = add_new_database(source_database, source_database);
  }
  g_mutex_unlock(database_hash_mutex);
  return _database;
}

gboolean execute_use(struct connection_data *cd)
{
  if (cd->current_database)
  {
    gchar *query = g_strdup_printf("USE `%s`", cd->current_database->target_database);
    if (m_query_warning(cd->thrconn, query, "Thread %d: Error switching to database `%s`", cd->thread_id, cd->current_database->target_database))
    {
      g_free(query);
      return TRUE;
    }
    g_free(query);
  }
  else
  {
    if (machine_log_json_enabled())
    {
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
    g_warning("Thread %ld with connection %ld: Not able to switch database", cd->thread_id, cd->connection_id);
  }
  return FALSE;
}

void execute_use_if_needs_to(struct connection_data *cd, struct database *database, const gchar *msg)
{
  if (database != NULL && (cd->current_database == NULL || g_strcmp0(database->target_database, cd->current_database->target_database) != 0))
  {
    cd->current_database = database;
    if (execute_use(cd))
      m_critical("Thread %ld with connection %ld: Error switching to database `%s` %s: %s", cd->thread_id, cd->connection_id, cd->current_database->target_database, msg, mysql_error(cd->thrconn));
  }
}

static void create_database(struct thread_data *td, gchar *database)
{
  const gchar *filename =
      g_strdup_printf("%s-schema-create.sql%s", database, exec_per_thread_extension ? exec_per_thread_extension : "");
  const gchar *filepath = g_strdup_printf("%s/%s",
      directory, filename);

  if (drop_database)
    execute_drop_database(td, database);

  if (g_file_test(filepath, G_FILE_TEST_EXISTS))
  {
    trace("Creating database from %s", filename);
    g_atomic_int_add(&(detailed_errors.schema_errors), restore_data_from_file(td, filename, TRUE, NULL, CREATE_DATABASE));
  }
  else
  {
    GString *data = g_string_new("CREATE DATABASE IF NOT EXISTS ");
    g_string_append_printf(data, "`%s`", database);
    trace("Creating schema %s as %s not found", database, filepath);
    if (restore_data_in_gstring_extended(td, data, TRUE, NULL, m_critical, "Failed to create database: %s", database))
      g_atomic_int_inc(&(detailed_errors.schema_errors));
    g_string_free(data, TRUE);
  }

  return;
}

// Creates the target databases defined with -B before any schema job runs, as
// databases mapped with -B are considered CREATED and their tables are not
// waiting for a schema-create job.
void start_database(struct thread_data *td)
{
  if (no_schemas)
    return;
  if (single_target_database)
  {
    create_database(td, single_target_database);
    return;
  }
  GHashTable    *created = g_hash_table_new(g_str_hash, g_str_equal);
  GHashTableIter iter;
  gpointer       _key, _target_database;
  g_hash_table_iter_init(&iter, target_database_by_source);
  while (g_hash_table_iter_next(&iter, &_key, &_target_database))
  {
    if (g_hash_table_contains(created, _target_database))
      continue;
    create_database(td, _target_database);
    g_hash_table_add(created, _target_database);
  }
  g_hash_table_destroy(created);
}

// Databases without schema-create file never get a SCHEMA_CREATE_JOB, so the
// table and sequence jobs waiting for them are released here. Locking each
// database also waits for any SCHEMA_CREATE_JOB in progress.
void set_not_found_databases_as_created()
{
  struct database *_database;
  GHashTableIter   iter;
  gpointer         _key;
  g_mutex_lock(database_hash_mutex);
  g_hash_table_iter_init(&iter, database_hash);
  while (g_hash_table_iter_next(&iter, &_key, (gpointer)&_database))
  {
    g_mutex_lock(_database->mutex);
    if (_database->schema_state == NOT_FOUND)
      set_db_schema_created(_database);
    g_mutex_unlock(_database->mutex);
  }
  g_mutex_unlock(database_hash_mutex);
}

// _database is locked
void set_db_schema_created(struct database *_database)
{
  _database->schema_state = CREATED;

  struct schema_job *sj = g_async_queue_try_pop(_database->sequence_queue);
  while (sj)
  {
    schema_job_queue_push(sj);
    sj = g_async_queue_try_pop(_database->sequence_queue);
  }
  sj = g_async_queue_try_pop(_database->table_queue);
  while (sj)
  {
    schema_job_queue_push(sj);
    sj = g_async_queue_try_pop(_database->table_queue);
  }
}
