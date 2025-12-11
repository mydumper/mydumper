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
static GHashTable *partition_cache = NULL;         // "db.table" -> GList* of partition names
static gboolean metadata_prefetch_done = FALSE;

// Free a GList of partition names (for hash table value destructor)
static void free_partition_list(gpointer data) {
  GList *list = (GList *)data;
  g_list_free_full(list, g_free);
}

void initialize_table(){
  all_dbts_mutex = g_mutex_new();
  character_set_hash_mutex = g_mutex_new();
  character_set_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free);
  // Initialize metadata caches (populated by prefetch_table_metadata when --bulk-metadata-prefetch)
  json_fields_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, NULL);
  generated_fields_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, NULL);
  partition_cache = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_partition_list);
}

void finalize_table(){
  g_hash_table_destroy(character_set_hash);
  g_mutex_free(all_dbts_mutex);
  g_mutex_free(character_set_hash_mutex);
  // Clean up metadata caches
  if (json_fields_cache) g_hash_table_destroy(json_fields_cache);
  if (generated_fields_cache) g_hash_table_destroy(generated_fields_cache);
  if (partition_cache) g_hash_table_destroy(partition_cache);
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

  // Prefetch all partition names - single query (filtered by schema if -B used)
  // This eliminates per-table PARTITIONS queries during chunk building
  gchar *partition_query = g_strdup_printf(
      "SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME FROM information_schema.PARTITIONS "
      "WHERE PARTITION_NAME IS NOT NULL%s ORDER BY TABLE_SCHEMA, TABLE_NAME, PARTITION_ORDINAL_POSITION", schema_filter);
  result = m_store_result(conn, partition_query, m_warning, "Failed to prefetch partition metadata", NULL);
  g_free(partition_query);
  if (result) {
    MYSQL_ROW row;
    guint partition_count = 0;
    guint table_count = 0;
    gchar *last_key = NULL;
    GList *current_list = NULL;
    while ((row = mysql_fetch_row(result))) {
      gchar *cache_key = g_strdup_printf("%s.%s", row[0], row[1]);
      // Check if we've moved to a new table
      if (last_key == NULL || g_strcmp0(last_key, cache_key) != 0) {
        // Save previous table's partition list
        if (last_key != NULL && current_list != NULL) {
          g_hash_table_insert(partition_cache, last_key, g_list_reverse(current_list));
          table_count++;
        } else if (last_key != NULL) {
          g_free(last_key);
        }
        last_key = cache_key;
        current_list = NULL;
      } else {
        g_free(cache_key);  // Duplicate key, free it
      }
      // Append partition name to current table's list
      current_list = g_list_prepend(current_list, g_strdup(row[2]));
      partition_count++;
    }
    // Don't forget the last table
    if (last_key != NULL && current_list != NULL) {
      g_hash_table_insert(partition_cache, last_key, g_list_reverse(current_list));
      table_count++;
    } else if (last_key != NULL) {
      g_free(last_key);
    }
    mysql_free_result(result);
    g_message("Prefetched %u partitions across %u tables", partition_count, table_count);
  }

  g_free(schema_filter);
  metadata_prefetch_done = TRUE;
  g_message("Metadata prefetch completed in %.2f seconds", g_timer_elapsed(timer, NULL));
  g_timer_destroy(timer);
}

// Get cached partition list for a table (returns NULL if not in cache or prefetch not done)
// Caller must NOT free the returned list (it's owned by the cache)
// Returns a deep copy if cache hit, NULL if cache miss
GList *get_cached_partitions(const gchar *database, const gchar *table) {
  if (!metadata_prefetch_done || !partition_cache)
    return NULL;

  gchar *cache_key = g_strdup_printf("%s.%s", database, table);
  GList *cached = g_hash_table_lookup(partition_cache, cache_key);
  g_free(cache_key);

  if (cached == NULL)
    return NULL;

  // Return a deep copy so caller can modify/filter/free it
  GList *copy = NULL;
  for (GList *l = cached; l != NULL; l = l->next) {
    copy = g_list_prepend(copy, g_strdup(l->data));
  }
  return g_list_reverse(copy);
}

// Check if metadata prefetch has been done
gboolean is_metadata_prefetch_done(void) {
  return metadata_prefetch_done;
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
  // first have to pick index, in future should be able to preset in
  //    * configuration too
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
GString *get_selectable_fields(MYSQL *conn, char *database, char *table) {
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
        dbt->select_fields = get_selectable_fields(conn, dbt->database->source_database_escaped, dbt->escaped_table);
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

