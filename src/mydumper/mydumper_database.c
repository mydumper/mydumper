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

#include "mydumper/mydumper_database.h"

#include "mydumper/mydumper.h"
#include "mydumper/mydumper_common.h"
#include "mydumper/mydumper_create_jobs.h"
#include "mydumper/mydumper_global.h"

GHashTable    *database_hash = NULL;
static GMutex *database_hash_mutex = NULL;
gchar         *source_db;

void free_database(struct database *_database)
{
  if (_database->source_database_escaped != NULL)
  {
    g_free(_database->source_database_escaped);
    _database->source_database_escaped = NULL;
  }
  //  if (_database->ad_mutex){
  //    g_mutex_free(_database->ad_mutex);
  //    _database->ad_mutex=NULL;
  //  }
  g_free(_database);
}

void initialize_database()
{
  database_hash = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, (GDestroyNotify)&free_database);
  database_hash_mutex = g_mutex_new();
}

static struct database *new_database(MYSQL *conn, char *database_name)
{
  struct database *_database = g_new(struct database, 1);
  _database->source_database = backtick_protect(database_name);
  _database->database_name_in_filename = get_ref_table(_database->source_database);
  _database->source_database_escaped = escape_string(conn, _database->source_database);
  //  _database->already_dumped = already_dumped;
  //  _database->ad_mutex=g_mutex_new();
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
  _database->checksum.skip_routine = c ? c : skip_routine_checksums;
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
  _database->checksum.skip_event = c ? c : skip_event_checksums;

  _database->dump_triggers = !is_regex_being_used() && tables_list == NULL && !g_hash_table_lookup(conf_per_table, OBJECT_TO_EXPORT);
  g_hash_table_insert(database_hash, _database->source_database, _database);
  g_free(any_table_config_file_dbt_key);
  return _database;
}

void free_databases()
{
  g_mutex_lock(database_hash_mutex);
  g_hash_table_destroy(database_hash);
  g_mutex_unlock(database_hash_mutex);
  g_mutex_free(database_hash_mutex);
}

struct database *get_database(MYSQL *conn, char *database_name, gboolean create_job)
{
  g_mutex_lock(database_hash_mutex);
  struct database *database = g_hash_table_lookup(database_hash, database_name);
  if (database == NULL)
  {
    database = new_database(conn, database_name);
    if (create_job)
    {
      create_job_to_dump_schema(database);
      database->schema_create_job_created = TRUE;
    }
  }
  g_mutex_unlock(database_hash_mutex);
  return database;
}

/* Called when a table of `database` has been elected to be dumped.

   --regex is evaluated against `database.table` for tables but against the bare
   database name for the database object, so a pattern such as '^mydb\.' selects
   every table while silently excluding the database itself.  The resulting
   backup has no <db>-schema-create.sql and cannot be restored into a server
   where the database does not already exist; in --stream mode myloader also
   holds every schema job until EOF.  mydumper exits 0 either way, so warn once
   per database instead of failing silently.  See issue #2329. */
void warn_if_schema_create_excluded(struct database *database)
{
  if (no_schemas || !is_regex_being_used() || database->schema_create_job_created)
    return;

  g_mutex_lock(database_hash_mutex);
  if (!database->regex_mismatch_warned)
  {
    database->regex_mismatch_warned = TRUE;
    g_warning("--regex matched tables in `%s` but not the database object itself: "
              "%s-schema-create.sql will not be dumped, and this backup will not be "
              "restorable unless the database already exists on the target. Database "
              "objects are matched against the bare database name, not `database.table` "
              "- use for instance '^(%s)(\\.|$)'.",
              database->source_database, database->database_name_in_filename,
              database->source_database);
  }
  g_mutex_unlock(database_hash_mutex);
}

// see print_dbt_on_metadata_gstring() for table write to metadata
static void write_list_of_database_on_disk(FILE *mdfile, GList *keys)
{
  const char       q = identifier_quote_character;
  struct database *_database;
  for (GList *it = keys; it; it = g_list_next(it))
  {
    _database = (struct database *)g_hash_table_lookup(database_hash, it->data);
    g_assert(_database);
    if (!should_write_database_checksum(&_database->checksum))
      continue;
    fprintf(mdfile, "\n[%c%s%c]\n", q, _database->source_database, q);
    write_database_checksum(mdfile, &_database->checksum);
  }
}

// see print_dbt_on_metadata_gstring() for table write to metadata
void write_database_on_disk(FILE *mdfile)
{
  GList *keys = g_hash_table_get_keys(database_hash);
  keys = g_list_sort(keys, key_strcmp);
  write_list_of_database_on_disk(mdfile, keys);
  g_list_free(keys);
}

// OPTIMIZATION: Unsorted version - skips O(n*log(n)) sort for large database counts
void write_database_on_disk_unsorted(FILE *mdfile)
{
  GList *keys = g_hash_table_get_keys(database_hash);
  // Skip sorting - saves time on 1000+ databases
  write_list_of_database_on_disk(mdfile, keys);
  g_list_free(keys);
}
