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


//#include <mysql.h>
#include <glib/gstdio.h>
#include "mydumper.h"
#include "mydumper_start_dump.h"
#include "mydumper_database.h"
#include "mydumper_table.h"
#include "mydumper_global.h"
#include "mydumper_chunks.h"
#include "mydumper_common.h"

// Extern
extern guint64 min_integer_chunk_step_size;
extern guint64 max_integer_chunk_step_size;
extern gboolean use_single_column;

static GMutex *all_dbts_mutex=NULL;
static GMutex *character_set_hash_mutex = NULL;
static GHashTable *character_set_hash=NULL;

// Bulk metadata prefetch caches (populated when --bulk-metadata-prefetch is used)
static GHashTable *json_fields_cache = NULL;       // "db.table" -> GINT_TO_POINTER(1) if has json
static GHashTable *generated_fields_cache = NULL;  // "db.table" -> GINT_TO_POINTER(1) if has generated
static GHashTable *primary_key_cache = NULL;       // "db.table" -> GList* of column names (PRIMARY or first UNIQUE)
static GHashTable *trigger_cache = NULL;           // "db.table" -> GINT_TO_POINTER(1) if has triggers
static GHashTable *selectable_columns_cache = NULL; // "db.table" -> GString* of comma-separated columns
static gboolean metadata_prefetch_done = FALSE;

// Free function for GList* values in primary_key_cache
static void free_primary_key_list(gpointer data) {
  g_list_free_full((GList *)data, g_free);
}

// Free function for GString* values in selectable_columns_cache
static void free_gstring(gpointer data) {
  g_string_free((GString *)data, TRUE);
}

void initialize_table(){
  all_dbts_mutex = g_mutex_new();
  character_set_hash_mutex = g_mutex_new();
  character_set_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free);
  // Initialize metadata caches (populated by prefetch_table_metadata when --bulk-metadata-prefetch)
  json_fields_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, NULL);
  generated_fields_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, NULL);
  primary_key_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_primary_key_list);
  trigger_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, NULL);
  selectable_columns_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_gstring);
}

void finalize_table(){
  g_hash_table_destroy(character_set_hash);
  g_mutex_free(all_dbts_mutex);
  g_mutex_free(character_set_hash_mutex);
  // Clean up metadata caches
  if (json_fields_cache) g_hash_table_destroy(json_fields_cache);
  if (generated_fields_cache) g_hash_table_destroy(generated_fields_cache);
  if (primary_key_cache) g_hash_table_destroy(primary_key_cache);
  if (trigger_cache) g_hash_table_destroy(trigger_cache);
  if (selectable_columns_cache) g_hash_table_destroy(selectable_columns_cache);
}

// Build schema filter clause for bulk prefetch queries
// Returns empty string if no filter, or " AND TABLE_SCHEMA IN (...)" if -B is used
static gchar *build_schema_filter(const gchar *column_name) {
  if (source_db == NULL || strlen(source_db) == 0)
    return g_strdup("");

  // Split by comma for multiple databases
  gchar **schemas = g_strsplit(source_db, ",", -1);
  GString *filter = g_string_new(" AND ");
  g_string_append(filter, column_name);

  guint count = g_strv_length(schemas);
  if (count == 1) {
    g_string_append_printf(filter, " = '%s'", schemas[0]);
  } else {
    g_string_append(filter, " IN (");
    for (guint i = 0; schemas[i] != NULL; i++) {
      if (i > 0) g_string_append(filter, ", ");
      g_string_append_printf(filter, "'%s'", schemas[i]);
    }
    g_string_append(filter, ")");
  }
  g_strfreev(schemas);
  return g_string_free(filter, FALSE);
}

// Prefetch JSON and generated column metadata in bulk queries
// Called once at startup when --bulk-metadata-prefetch is used
void prefetch_table_metadata(MYSQL *conn) {
  if (metadata_prefetch_done)
    return;

  g_message("Prefetching table metadata (collations, JSON fields, generated columns)...");
  GTimer *timer = g_timer_new();

  // Build schema filter once (empty string if no -B flag, otherwise "AND TABLE_SCHEMA IN (...)")
  gchar *schema_filter = build_schema_filter("TABLE_SCHEMA");
  if (source_db != NULL && strlen(source_db) > 0) {
    g_message("Filtering prefetch to schema(s): %s", source_db);
  }

  // Prefetch ALL collation->charset mappings in one query
  // This prevents per-table collation lookups later
  const char *collation_query =
      "SELECT COLLATION_NAME, CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS";
  MYSQL_RES *result = m_store_result(conn, collation_query, m_warning, "Failed to prefetch collation metadata", NULL);
  if (result) {
    MYSQL_ROW row;
    guint collation_count = 0;
    g_mutex_lock(character_set_hash_mutex);
    while ((row = mysql_fetch_row(result))) {
      if (row[0] && row[1] && !g_hash_table_contains(character_set_hash, row[0])) {
        g_hash_table_insert(character_set_hash, g_strdup(row[0]), g_strdup(row[1]));
        collation_count++;
      }
    }
    g_mutex_unlock(character_set_hash_mutex);
    mysql_free_result(result);
    g_message("Prefetched %u collation->charset mappings", collation_count);
  }

  // Prefetch all tables with JSON columns - single query (filtered by schema if -B used)
  gchar *json_query = g_strdup_printf(
      "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME FROM information_schema.COLUMNS "
      "WHERE COLUMN_TYPE = 'json'%s", schema_filter);
  result = m_store_result(conn, json_query, m_warning, "Failed to prefetch JSON column metadata", NULL);
  g_free(json_query);
  if (result) {
    MYSQL_ROW row;
    guint json_count = 0;
    while ((row = mysql_fetch_row(result))) {
      gchar *cache_key = g_strdup_printf("%s.%s", row[0], row[1]);
      g_hash_table_insert(json_fields_cache, cache_key, GINT_TO_POINTER(1));
      json_count++;
    }
    mysql_free_result(result);
    g_message("Prefetched %u tables with JSON columns", json_count);
  }

  // Prefetch all tables with generated columns - single query (filtered by schema if -B used)
  gchar *generated_query = g_strdup_printf(
      "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME FROM information_schema.COLUMNS "
      "WHERE extra LIKE '%%GENERATED%%' AND extra NOT LIKE '%%DEFAULT_GENERATED%%'%s", schema_filter);
  result = m_store_result(conn, generated_query, m_warning, "Failed to prefetch generated column metadata", NULL);
  g_free(generated_query);
  if (result) {
    MYSQL_ROW row;
    guint generated_count = 0;
    while ((row = mysql_fetch_row(result))) {
      gchar *cache_key = g_strdup_printf("%s.%s", row[0], row[1]);
      g_hash_table_insert(generated_fields_cache, cache_key, GINT_TO_POINTER(1));
      generated_count++;
    }
    mysql_free_result(result);
    g_message("Prefetched %u tables with generated columns", generated_count);
  }

  // Prefetch primary key / unique index columns from STATISTICS (filtered by schema if -B used)
  // Groups by table, stores column list for PRIMARY index (or first UNIQUE if no PRIMARY)
  gchar *index_query = g_strdup_printf(
      "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE "
      "FROM information_schema.STATISTICS "
      "WHERE NON_UNIQUE = 0%s "
      "ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX", schema_filter);
  result = m_store_result(conn, index_query, m_warning, "Failed to prefetch index metadata", NULL);
  g_free(index_query);
  if (result) {
    MYSQL_ROW row;
    guint pk_count = 0;
    gchar *last_key = NULL;
    gchar *last_index = NULL;
    GList *current_pk = NULL;
    gboolean is_primary = FALSE;

    while ((row = mysql_fetch_row(result))) {
      gchar *cache_key = g_strdup_printf("%s.%s", row[0], row[1]);
      gchar *index_name = row[2];

      // Check if we've moved to a new table
      if (last_key == NULL || g_strcmp0(last_key, cache_key) != 0) {
        // Save previous table's primary key (if we found one)
        if (last_key != NULL && current_pk != NULL) {
          g_hash_table_insert(primary_key_cache, last_key, g_list_reverse(current_pk));
          pk_count++;
        } else if (last_key != NULL) {
          g_free(last_key);
        }
        last_key = cache_key;
        last_index = NULL;
        current_pk = NULL;
        is_primary = FALSE;
      } else {
        g_free(cache_key);
      }

      // Prefer PRIMARY index, otherwise take first unique index encountered
      gboolean this_is_primary = (g_strcmp0(index_name, "PRIMARY") == 0);
      if (this_is_primary || (!is_primary && current_pk == NULL)) {
        // Starting a new index or continuing current one
        if (last_index == NULL || g_strcmp0(last_index, index_name) != 0) {
          // New index - if we were building a non-primary and found primary, switch
          if (this_is_primary && !is_primary && current_pk != NULL) {
            g_list_free_full(current_pk, g_free);
            current_pk = NULL;
          }
          last_index = index_name;
          is_primary = this_is_primary;
        }
        // Only add columns from the index we're tracking
        if (g_strcmp0(last_index, index_name) == 0) {
          current_pk = g_list_prepend(current_pk, g_strdup(row[4]));
        }
      }
    }
    // Don't forget the last table
    if (last_key != NULL && current_pk != NULL) {
      g_hash_table_insert(primary_key_cache, last_key, g_list_reverse(current_pk));
      pk_count++;
    } else if (last_key != NULL) {
      g_free(last_key);
    }
    mysql_free_result(result);
    g_message("Prefetched primary/unique keys for %u tables", pk_count);
  }

  // Prefetch triggers - just need to know which tables have triggers (filtered by schema if -B used)
  gchar *trigger_schema_filter = build_schema_filter("TRIGGER_SCHEMA");
  gchar *trigger_query = g_strdup_printf(
      "SELECT DISTINCT TRIGGER_SCHEMA, EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS WHERE 1=1%s",
      trigger_schema_filter);
  g_free(trigger_schema_filter);
  result = m_store_result(conn, trigger_query, m_warning, "Failed to prefetch trigger metadata", NULL);
  g_free(trigger_query);
  if (result) {
    MYSQL_ROW row;
    guint trigger_count = 0;
    while ((row = mysql_fetch_row(result))) {
      gchar *cache_key = g_strdup_printf("%s.%s", row[0], row[1]);
      g_hash_table_insert(trigger_cache, cache_key, GINT_TO_POINTER(1));
      trigger_count++;
    }
    mysql_free_result(result);
    g_message("Prefetched %u tables with triggers", trigger_count);
  }

  // Prefetch selectable columns (excluding virtual/stored generated) - filtered by schema if -B used
  // This eliminates per-table COLUMNS queries in get_selectable_fields()
  gchar *columns_query = g_strdup_printf(
      "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS "
      "WHERE ((extra NOT LIKE '%%VIRTUAL GENERATED%%' AND extra NOT LIKE '%%STORED GENERATED%%') "
      "OR extra IS NULL)%s "
      "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION", schema_filter);
  result = m_store_result(conn, columns_query, m_warning, "Failed to prefetch column metadata", NULL);
  g_free(columns_query);
  if (result) {
    MYSQL_ROW row;
    guint table_count = 0;
    gchar *last_key = NULL;
    GString *current_fields = NULL;

    while ((row = mysql_fetch_row(result))) {
      gchar *cache_key = g_strdup_printf("%s.%s", row[0], row[1]);

      // Check if we've moved to a new table
      if (last_key == NULL || g_strcmp0(last_key, cache_key) != 0) {
        // Save previous table's column list
        if (last_key != NULL && current_fields != NULL) {
          g_hash_table_insert(selectable_columns_cache, last_key, current_fields);
          table_count++;
        } else if (last_key != NULL) {
          g_free(last_key);
        }
        last_key = cache_key;
        current_fields = g_string_new("");
      } else {
        g_free(cache_key);
      }

      // Append column to current table's field list
      if (current_fields->len > 0) {
        g_string_append(current_fields, ",");
      }
      // Quote the column name
      char *field_name = identifier_quote_character_protect(row[2]);
      g_string_append_printf(current_fields, "%s%s%s",
                            identifier_quote_character_str, field_name, identifier_quote_character_str);
      g_free(field_name);
    }
    // Don't forget the last table
    if (last_key != NULL && current_fields != NULL) {
      g_hash_table_insert(selectable_columns_cache, last_key, current_fields);
      table_count++;
    } else if (last_key != NULL) {
      g_free(last_key);
    }
    mysql_free_result(result);
    g_message("Prefetched column lists for %u tables", table_count);
  }

  g_free(schema_filter);
  metadata_prefetch_done = TRUE;
  g_message("Metadata prefetch completed in %.2f seconds", g_timer_elapsed(timer, NULL));
  g_timer_destroy(timer);
}

void free_db_table(struct db_table * dbt){
  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_free(dbt->rows_lock);
  g_free(dbt->escaped_table);
  if (dbt->insert_statement)
    g_string_free(dbt->insert_statement,TRUE);
  if (dbt->select_fields)
    g_string_free(dbt->select_fields, TRUE);
  if (dbt->min!=NULL) g_free(dbt->min);
  if (dbt->max!=NULL) g_free(dbt->max);
  g_free(dbt->data_checksum);
  dbt->data_checksum=NULL;
  g_free(dbt->chunks_completed);

  g_free(dbt->table);
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_free(dbt->chunks_mutex);
  g_free(dbt);
}

static
gchar *get_character_set_from_collation(MYSQL *conn, gchar *collation){
  g_mutex_lock(character_set_hash_mutex);
  gchar *character_set = g_hash_table_lookup(character_set_hash, collation);
  if (character_set == NULL){
    gchar *query =
      g_strdup_printf("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS "
                      "WHERE collation_name='%s'",
                      collation);
    struct M_ROW *mr = m_store_result_row(conn, query, m_critical, m_warning, "Failed to get CHARACTER_SET from collation %s", collation);
    g_free(query);
    if (mr->row)
      g_hash_table_insert(character_set_hash, g_strdup(collation), character_set=g_strdup(mr->row[0]));
    m_store_result_row_free(mr);
  }
  g_mutex_unlock(character_set_hash_mutex);
  return character_set;
}

static
void get_primary_key(MYSQL *conn, struct db_table * dbt, struct configuration *conf){
  MYSQL_RES *indexes = NULL;
  MYSQL_ROW row;
  dbt->primary_key=NULL;

  // Check prefetched cache first (--bulk-metadata-prefetch)
  if (metadata_prefetch_done) {
    gchar *cache_key = g_strdup_printf("%s.%s", dbt->database->source_database, dbt->table);
    GList *cached_pk = g_hash_table_lookup(primary_key_cache, cache_key);
    g_free(cache_key);
    if (cached_pk != NULL) {
      // Deep copy the cached primary key list
      for (GList *l = cached_pk; l != NULL; l = l->next) {
        dbt->primary_key = g_list_append(dbt->primary_key, g_strdup(l->data));
      }
      return;
    }
    // No cached PK means no unique index found - fall through to try use_any_index
    if (!conf->use_any_index) {
      return;
    }
  }

  // Fall back to per-table SHOW INDEX query
  gchar *query = g_strdup_printf("SHOW INDEX FROM %s%s%s.%s%s%s",
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str);
  indexes = m_store_result(conn, query, m_warning, "Failed to execute SHOW INDEX over %s", dbt->database->source_database);
  g_free(query);

  if (indexes){
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[2], "PRIMARY") ) {
        // Pick first column in PK, cardinality doesn't matter
        dbt->primary_key=g_list_append(dbt->primary_key,g_strdup(row[4]));
      }
    }
    if (dbt->primary_key)
      goto cleanup;

    // If no PK found, try using first UNIQUE index
    mysql_data_seek(indexes, 0);
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[1], "0")) {
        // Again, first column of any unique index
        dbt->primary_key=g_list_append(dbt->primary_key,g_strdup(row[4]));
      }
    }

    if (dbt->primary_key)
      goto cleanup;

    // Still unlucky? Pick any high-cardinality index
    if (!dbt->primary_key && conf->use_any_index) {
      guint64 max_cardinality = 0;
      guint64 cardinality = 0;
      gchar *field=NULL;
      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[3], "1")) {
          if (row[6])
            cardinality = strtoul(row[6], NULL, 10);
          if (cardinality > max_cardinality) {
            field = g_strdup(row[4]);
            max_cardinality = cardinality;
          }
        }
      }
      if (field)
        dbt->primary_key=g_list_append(dbt->primary_key,field);
    }
  }

cleanup:
  if (indexes)
    mysql_free_result(indexes);
}

static
void get_primary_key_separated_by_comma(struct db_table * dbt) {
  GString *field_list = g_string_new("");
  GList *list=dbt->primary_key;
  gboolean first = TRUE;
  while (list){
    if (first) {
      first = FALSE;
    }else{
      g_string_append(field_list, ",");
    }
    char *field_name= identifier_quote_character_protect((char*) list->data);
    gchar *tb = g_strdup_printf("%s%s%s", identifier_quote_character_str, field_name, identifier_quote_character_str);
    g_free(field_name);
    g_string_append(field_list, tb);
    list=list->next;
  }
  if (field_list->len>0)
    dbt->primary_key_separated_by_comma = g_string_free(field_list, FALSE);
}

static
GString *get_selectable_fields(MYSQL *conn, char *database, char *table, char *raw_db, char *raw_table) {
  // Check prefetched cache first (--bulk-metadata-prefetch)
  // Cache key uses raw db.table names (same format as information_schema returns)
  if (metadata_prefetch_done && raw_db != NULL && raw_table != NULL) {
    gchar *cache_key = g_strdup_printf("%s.%s", raw_db, raw_table);
    GString *cached_fields = g_hash_table_lookup(selectable_columns_cache, cache_key);
    g_free(cache_key);
    if (cached_fields != NULL) {
      // Return a copy of the cached GString
      return g_string_new(cached_fields->str);
    }
  }

  // Fall back to per-table query
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra "
                      "not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%' ORDER BY ORDINAL_POSITION ASC",
                      database, table);
  MYSQL_RES *res=m_store_result_critical(conn, query, "Failed to get Selectable Fields", NULL);
  g_free(query);

  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else {
      g_string_append(field_list, ",");
    }

    char *field_name= identifier_quote_character_protect(row[0]);
    gchar *tb = g_strdup_printf("%s%s%s", identifier_quote_character_str, field_name, identifier_quote_character_str);
    g_free(field_name);
    g_string_append(field_list, tb);
    g_free(tb);
  }

  mysql_free_result(res);

  return field_list;
}

static
gboolean detect_generated_fields(MYSQL *conn, gchar *database, gchar* table) {
  gboolean result = FALSE;
  if (ignore_generated_fields)
    return FALSE;

  // Use prefetched cache if available (--bulk-metadata-prefetch)
  if (metadata_prefetch_done) {
    gchar *cache_key = g_strdup_printf("%s.%s", database, table);
    result = g_hash_table_contains(generated_fields_cache, cache_key);
    g_free(cache_key);
    return result;
  }

  // Fall back to per-table query
  gchar *query = g_strdup_printf(
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
      "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND extra LIKE '%%GENERATED%%' AND extra NOT LIKE '%%DEFAULT_GENERATED%%' "
      "LIMIT 1",
      database, table);

  struct M_ROW *mr = m_store_result_row(conn, query, m_warning, m_message, "Failed to detect generated fields", NULL);
  g_free(query);
  result=mr->row!=NULL;
  m_store_result_row_free(mr);
  return result;
}

static
gboolean has_json_fields(MYSQL *conn, char *database, char *table) {
  // Use prefetched cache if available (--bulk-metadata-prefetch)
  if (metadata_prefetch_done) {
    gchar *cache_key = g_strdup_printf("%s.%s", database, table);
    gboolean result = g_hash_table_contains(json_fields_cache, cache_key);
    g_free(cache_key);
    return result;
  }

  // Fall back to per-table query
  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and "
                      "COLUMN_TYPE ='json'",
                      database, table);
  struct M_ROW *mr = m_store_result_row(conn, query, m_critical, m_warning, "Failed to get JSON fields on %s.%s: %s", database, table, query);
  g_free(query);
  if (mr->row){
    m_store_result_row_free(mr);
    return TRUE;
  }
  m_store_result_row_free(mr);
  return FALSE;
}

// Check if table has triggers (used to skip per-table SHOW TRIGGERS query)
// Returns: -1 = prefetch not done (use SHOW TRIGGERS), 0 = no triggers, 1 = has triggers
int table_has_triggers_cached(char *database, char *table) {
  if (!metadata_prefetch_done) {
    return -1;  // Fall back to SHOW TRIGGERS
  }
  gchar *cache_key = g_strdup_printf("%s.%s", database, table);
  gboolean has = g_hash_table_contains(trigger_cache, cache_key);
  g_free(cache_key);
  return has ? 1 : 0;
}

gboolean new_db_table(struct db_table **d, MYSQL *conn, struct configuration *conf,
                      struct database *database, char *table, char *table_collation,
                      gboolean is_sequence, gboolean is_view)
{
  gchar * lkey = build_dbt_key(database->source_database,table);
  g_mutex_lock(all_dbts_mutex);
  struct db_table *dbt = g_hash_table_lookup(all_dbts, lkey);
  gboolean b;
  if (dbt){
    g_free(lkey);
    b=FALSE;
    g_mutex_unlock(all_dbts_mutex);
  }else{
    dbt = g_new(struct db_table, 1);
    dbt->key=lkey;
    dbt->status = UNDEFINED;
    g_hash_table_insert(all_dbts, lkey, dbt);
    g_mutex_unlock(all_dbts_mutex);
    dbt->database = database;
    dbt->table = identifier_quote_character_protect(table);
    dbt->table_filename = get_ref_table(dbt->table);
    dbt->is_sequence= is_sequence;
    dbt->is_view=is_view;
    if (table_collation==NULL)
      dbt->character_set = NULL;
    else{
      dbt->character_set=get_character_set_from_collation(conn, table_collation);
      if ( dbt->character_set == NULL)
        g_warning("Collation '%s' not found on INFORMATION_SCHEMA.COLLATIONS used by `%s`.`%s`",table_collation,database->source_database,table);
    }
    dbt->has_json_fields = has_json_fields(conn, dbt->database->source_database, dbt->table);
    dbt->rows_lock= g_mutex_new();
    dbt->rows_total=0;
    dbt->escaped_table = escape_string(conn,dbt->table);
    dbt->where=g_hash_table_lookup(conf_per_table.all_where_per_table, lkey);
    dbt->limit=g_hash_table_lookup(conf_per_table.all_limit_per_table, lkey);
    parse_object_to_export(&(dbt->object_to_export),g_hash_table_lookup(conf_per_table.all_object_to_export, lkey));

    dbt->partition_regex=g_hash_table_lookup(conf_per_table.all_partition_regex_per_table, lkey);
    dbt->max_threads_per_table=max_threads_per_table;
    dbt->current_threads_running=0;

    // Load chunk step size values
    gchar *rows_p_chunk=g_hash_table_lookup(conf_per_table.all_rows_per_table, lkey);
    if (rows_p_chunk )
      dbt->split_integer_tables=parse_rows_per_chunk(rows_p_chunk, &(dbt->min_chunk_step_size), &(dbt->starting_chunk_step_size), &(dbt->max_chunk_step_size),"Invalid option on rows in configuration file");
    else{
      dbt->split_integer_tables=split_integer_tables;
      dbt->min_chunk_step_size=min_chunk_step_size;
      dbt->starting_chunk_step_size=starting_chunk_step_size;
      dbt->max_chunk_step_size=max_chunk_step_size;
    }
    if (dbt->min_chunk_step_size == 1 && dbt->min_chunk_step_size == dbt->starting_chunk_step_size && dbt->starting_chunk_step_size != dbt->max_chunk_step_size ){
      dbt->min_chunk_step_size = 2;
      dbt->starting_chunk_step_size = 2;
      g_warning("Setting min and start rows per file to 2 on %s", lkey);
    }
    dbt->is_fixed_length=dbt->min_chunk_step_size != 0 && dbt->min_chunk_step_size == dbt->starting_chunk_step_size && dbt->starting_chunk_step_size == dbt->max_chunk_step_size;
    // We are disabling the file size limit when the user set a specific --rows size
    dbt->chunk_filesize=dbt->is_fixed_length?0:chunk_filesize;

    if ( dbt->min_chunk_step_size==0)
      dbt->min_chunk_step_size=MIN_CHUNK_STEP_SIZE;

    // honor --rows-hard
    if ( max_integer_chunk_step_size != 0 && (dbt->max_chunk_step_size > max_integer_chunk_step_size || dbt->max_chunk_step_size == 0 ))
      dbt->max_chunk_step_size=max_integer_chunk_step_size;
    if ( min_integer_chunk_step_size != 0 && (dbt->min_chunk_step_size < min_integer_chunk_step_size ))
      dbt->min_chunk_step_size=min_integer_chunk_step_size;


    dbt->num_threads=g_hash_table_lookup(conf_per_table.all_num_threads_per_table, lkey)?strtoul(g_hash_table_lookup(conf_per_table.all_num_threads_per_table, lkey), NULL, 10):num_threads;
    dbt->estimated_remaining_steps=1;
    dbt->min=NULL;
    dbt->max=NULL;
    dbt->chunks=NULL;
    dbt->load_data_header=NULL;
    dbt->load_data_suffix=NULL;
    dbt->insert_statement=NULL;
    dbt->chunks_mutex=g_mutex_new();
    dbt->chunks_queue=g_async_queue_new();
    dbt->chunks_completed=g_new(int,1);
    *(dbt->chunks_completed)=0;
    get_primary_key(conn,dbt,conf);
    dbt->primary_key_separated_by_comma = NULL;
    if (order_by_primary_key)
      get_primary_key_separated_by_comma(dbt);
    dbt->multicolumn = !use_single_column && g_list_length(dbt->primary_key) > 1;

    gchar *columns_on_select=g_hash_table_lookup(conf_per_table.all_columns_on_select_per_table, lkey);

    dbt->columns_on_insert=g_hash_table_lookup(conf_per_table.all_columns_on_insert_per_table, lkey);

    dbt->select_fields=NULL;

    if (columns_on_select){
      dbt->select_fields=g_string_new(columns_on_select);
      if (!dbt->columns_on_insert && complete_insert){
        g_warning("Ignoring complete-insert on %s.%s due usage of columns_on_select only", dbt->database->source_database,dbt->table);
      }

    }else if (!dbt->columns_on_insert){
      dbt->complete_insert = complete_insert || detect_generated_fields(conn, dbt->database->source_database_escaped, dbt->escaped_table);
      if (dbt->complete_insert) {
        dbt->select_fields = get_selectable_fields(conn, dbt->database->source_database_escaped, dbt->escaped_table,
                                                   dbt->database->source_database, dbt->table);
      }
    }

//    dbt->anonymized_function=get_anonymized_function_for(conn, dbt);
    dbt->anonymized_function=NULL;
    dbt->indexes_checksum=NULL;
    dbt->data_checksum=NULL;
    dbt->schema_checksum=NULL;
    dbt->triggers_checksum=NULL;
    dbt->rows=0;
 // dbt->chunk_functions.process=NULL;
    b=TRUE;
  }
  *d=dbt;
  return b;
}

