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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot org)
                    Andrew Hutchings, MariaDB Foundation (andrew dot mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <mysql.h>
#include <glib/gstdio.h>

#include "mydumper.h"
#include "mydumper_start_dump.h"
#include "mydumper_database.h"
#include "mydumper_global.h"
#include "mydumper_chunks.h"
#include "mydumper_common.h"
#include "mydumper_string_planner.h"
#include "../logging.h"

extern gboolean split_string_pk;
extern guint max_char_size;
extern guint64 min_chunk_step_size;

extern gchar *string_pk_planner_strategy_str;
extern gboolean string_pk_planner_metadata_enabled;
extern guint string_pk_planner_timeout_seconds;
extern guint string_pk_planner_max_probes;
extern guint string_pk_planner_max_prefixes;
extern guint string_pk_planner_max_depth;
extern guint64 string_pk_planner_min_rows;
extern enum string_pk_planner_strategy string_pk_planner_strategy;

static const gchar *string_pk_probe_alphabet =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";

static gchar *escape_sql_string(MYSQL *conn, const gchar *value);

static gchar *escape_like_sql_string(MYSQL *conn, const gchar *value){
  GString *pattern = g_string_new("");
  for (const gchar *character = value; character != NULL && *character != '\0'; character++) {
    if (*character == '\\' || *character == '%' || *character == '_') {
      g_string_append_c(pattern, '\\');
    }
    g_string_append_c(pattern, *character);
  }

  gchar *escaped = escape_sql_string(conn, pattern->str);
  g_string_free(pattern, TRUE);
  return escaped;
}

static gchar *get_probe_alphabet(MYSQL *conn, struct db_table *dbt){
  gchar *schema = escape_sql_string(conn, dbt->database->source_database);
  gchar *table = escape_sql_string(conn, dbt->table);
  gchar *column = escape_sql_string(conn, (const gchar *)dbt->primary_key->data);
  gchar *query = g_strdup_printf(
      "SELECT COLLATION_NAME FROM information_schema.COLUMNS "
      "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s' LIMIT 1",
      schema, table, column);
  struct M_ROW *mr = m_store_result_row(conn, query, m_warning, m_warning,
                                        "Failed to determine collation for %s.%s",
                                        dbt->database->source_database, dbt->table);
  gchar *alphabet = g_strdup(string_pk_probe_alphabet);

  if (mr != NULL && mr->res != NULL && mr->row != NULL && mr->row[0] != NULL &&
      g_str_has_suffix(mr->row[0], "_ci")) {
    /*
     * LIKE uses the column collation.  Under a case-insensitive collation,
     * both A% and a% select the same rows.  Keep one representative so the
     * generated prefix roots remain disjoint.
     */
    g_free(alphabet);
    alphabet = g_strdup("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-");
  }

  if (mr != NULL) {
    m_store_result_row_free(mr);
  }
  g_free(query);
  g_free(schema);
  g_free(table);
  g_free(column);
  return alphabet;
}

static gboolean string_pk_planner_should_use_metadata_internal(guint64 rows){
  return string_pk_planner_should_use_metadata_mode(string_pk_planner_strategy,
                                                     string_pk_planner_metadata_enabled,
                                                     split_string_pk,
                                                     rows,
                                                     string_pk_planner_min_rows);
}

gboolean string_pk_planner_enabled_for_table(guint64 rows){
  return string_pk_planner_should_use_metadata_internal(rows);
}

void string_pk_planner_reset_for_table(struct db_table *dbt, guint64 rows){
  if (dbt == NULL) {
    return;
  }

  dbt->string_planner_probe_count = 0;
  dbt->string_planner_budget_exhausted = FALSE;
  dbt->string_planner_started_us = g_get_monotonic_time();
  dbt->string_planner_deadline_us =
      string_pk_planner_timeout_seconds > 0 ?
      dbt->string_planner_started_us + ((gint64)string_pk_planner_timeout_seconds * G_USEC_PER_SEC) :
      0;
  dbt->string_planner_estimated_rows = rows;
}

gboolean string_pk_planner_budget_exhausted(struct db_table *dbt){
  if (dbt == NULL) {
    return TRUE;
  }

  if (dbt->string_planner_budget_exhausted) {
    return TRUE;
  }

  if (dbt->string_planner_deadline_us > 0 && g_get_monotonic_time() > dbt->string_planner_deadline_us) {
    dbt->string_planner_budget_exhausted = TRUE;
    g_warning("String PK planner exceeded the per-table timeout on %s.%s; falling back to the recursive splitter",
              dbt->database->source_database, dbt->table);
    return TRUE;
  }

  if (string_pk_planner_max_probes > 0 && dbt->string_planner_probe_count >= string_pk_planner_max_probes) {
    dbt->string_planner_budget_exhausted = TRUE;
    g_warning("String PK planner exceeded the probe budget on %s.%s; falling back to the recursive splitter",
              dbt->database->source_database, dbt->table);
    return TRUE;
  }

  return FALSE;
}

gboolean string_pk_planner_note_probe(struct db_table *dbt){
  if (dbt == NULL) {
    return FALSE;
  }

  if (string_pk_planner_budget_exhausted(dbt)) {
    return FALSE;
  }

  dbt->string_planner_probe_count++;
  return TRUE;
}

static void free_prefix_list(GList *prefixes){
  GList *iter = prefixes;
  while (iter != NULL) {
    g_free(iter->data);
    iter = iter->next;
  }
  g_list_free(prefixes);
}

static gint compare_prefix_strings(gconstpointer a, gconstpointer b){
  return g_strcmp0((const gchar *)a, (const gchar *)b);
}

static gchar *escape_sql_string(MYSQL *conn, const gchar *value){
  gulong len = strlen(value);
  gchar *escaped = g_new(gchar, len * 2 + 1);
  mysql_real_escape_string(conn, escaped, value, len);
  return escaped;
}

static gchar *build_prefix_where_clause(MYSQL *conn, struct db_table *dbt, const gchar *prefix){
  gchar *escaped_prefix = escape_like_sql_string(conn, prefix);
  gchar *where_clause = g_strdup_printf("%s%s%s LIKE '%s%%'",
                                        identifier_quote_character_str,
                                        (const gchar *)dbt->primary_key->data,
                                        identifier_quote_character_str,
                                        escaped_prefix);
  gchar *escaped_where_clause = g_strdup_printf("%s ESCAPE '\\\\'", where_clause);
  g_free(where_clause);
  g_free(escaped_prefix);
  return escaped_where_clause;
}

static guint64 estimate_prefix_rows(MYSQL *conn, struct db_table *dbt, const gchar *prefix){
  if (!string_pk_planner_note_probe(dbt)) {
    return 0;
  }

  gchar *where_clause = build_prefix_where_clause(conn, dbt, prefix);
  GString *where = g_string_new(where_clause);
  guint64 rows = get_rows_from_explain(conn, dbt, where, (gchar *)dbt->primary_key->data);
  g_string_free(where, TRUE);
  g_free(where_clause);
  return rows;
}

static gboolean prefix_already_exists(GList *prefixes, const gchar *prefix){
  for (GList *iter = prefixes; iter != NULL; iter = iter->next) {
    if (g_strcmp0((const gchar *)iter->data, prefix) == 0) {
      return TRUE;
    }
  }
  return FALSE;
}

static gboolean collect_prefix_roots(MYSQL *conn,
                                     struct db_table *dbt,
                                     const gchar *current_prefix,
                                     guint current_depth,
                                     guint64 target_rows_per_root,
                                     const gchar *alphabet,
                                     GList **roots){
  if (string_pk_planner_budget_exhausted(dbt)) {
    return FALSE;
  }

  GList *local_roots = NULL;

  for (const gchar *character = alphabet; *character != '\0'; character++) {
    gchar candidate[64];
    g_snprintf(candidate, sizeof(candidate), "%s%c", current_prefix, *character);

    guint64 estimated_rows = estimate_prefix_rows(conn, dbt, candidate);
    /*
     * EXPLAIN is deliberately the only discovery operation here.  Running a
     * SELECT ... LIMIT 1 for every candidate can scan a very large string
     * index before returning, defeating the planner on large tables.
     */
    if (estimated_rows == 0) {
      continue;
    }

    if (estimated_rows > target_rows_per_root &&
        current_depth + 1 < max_char_size &&
        current_depth + 1 < string_pk_planner_max_depth) {
      GList *child_roots = NULL;
      if (!collect_prefix_roots(conn, dbt, candidate, current_depth + 1,
                                target_rows_per_root, alphabet, &child_roots)) {
        free_prefix_list(local_roots);
        return FALSE;
      }
      local_roots = g_list_concat(local_roots, child_roots);
    } else if (!prefix_already_exists(local_roots, candidate)) {
      local_roots = g_list_append(local_roots, g_strdup(candidate));
    }

    if (string_pk_planner_budget_exhausted(dbt)) {
      free_prefix_list(local_roots);
      return FALSE;
    }
  }

  if (string_pk_planner_max_prefixes > 0 &&
      g_list_length(local_roots) > string_pk_planner_max_prefixes) {
    /*
     * The descendants are too numerous for the configured root budget.  Do
     * not return a partial list: replace the complete subtree by its parent.
     * This preserves coverage and keeps sibling subtrees disjoint.
     */
    free_prefix_list(local_roots);
    if (current_prefix[0] == '\0') {
      local_roots = g_list_append(NULL, g_strdup(""));
    } else {
      local_roots = g_list_append(NULL, g_strdup(current_prefix));
    }
  }

  *roots = local_roots;
  return TRUE;
}

static GList *build_prefix_roots(MYSQL *conn, struct db_table *dbt, guint64 rows){
  GList *roots = NULL;
  guint64 target_rows_per_root = string_pk_planner_max_prefixes > 0 ?
      rows / string_pk_planner_max_prefixes : rows;
  gchar *alphabet = NULL;

  if (target_rows_per_root == 0) {
    target_rows_per_root = 1;
  }
  if (target_rows_per_root < dbt->min_chunk_step_size) {
    target_rows_per_root = dbt->min_chunk_step_size;
  }

  alphabet = get_probe_alphabet(conn, dbt);
  if (!collect_prefix_roots(conn, dbt, "", 0, target_rows_per_root,
                            alphabet, &roots)) {
    free_prefix_list(roots);
    roots = NULL;
  }
  g_free(alphabet);

  if (roots == NULL || g_list_length(roots) == 0) {
    return NULL;
  }

  roots = g_list_sort(roots, compare_prefix_strings);
  return roots;
}

static void append_string_chunk_root(struct db_table *dbt, GList **chunks, const gchar *prefix, guint64 part, guint64 step, guint64 rows_in_explain, guint prefix_len){
  struct chunk_step_item *csi = new_string_step_item(
      FALSE, NULL, (gchar *)dbt->primary_key->data, 0, dbt->is_fixed_length,
      prefix_len, g_strdup(prefix), g_strdup(prefix), step, part,
      FALSE, FALSE, NULL, 0, FALSE, rows_in_explain);
  csi->status = UNASSIGNED;
  *chunks = g_list_append(*chunks, csi);
}

gboolean string_pk_plan_prefix_chunks(MYSQL *conn, struct db_table *dbt, guint64 rows){
  if (!string_pk_planner_enabled_for_table(rows) || dbt == NULL || dbt->primary_key == NULL) {
    return FALSE;
  }

  if (string_pk_planner_budget_exhausted(dbt)) {
    return FALSE;
  }

  GList *prefixes = build_prefix_roots(conn, dbt, rows);
  if (prefixes == NULL || g_list_length(prefixes) == 0) {
    return FALSE;
  }

  guint prefix_count = g_list_length(prefixes);
  guint64 root_step = string_pk_planner_compute_root_step(rows, prefix_count, dbt->min_chunk_step_size);

  GList *chunks = NULL;
  guint64 part = 0;
  for (GList *iter = prefixes; iter != NULL; iter = iter->next) {
    const gchar *prefix = iter->data;
    guint64 rows_in_explain = prefix_count > 0 ? rows / prefix_count : 0;
    append_string_chunk_root(dbt, &chunks, prefix, part++, root_step, rows_in_explain, strlen(prefix));
  }
  free_prefix_list(prefixes);

  if (chunks == NULL) {
    return FALSE;
  }

  for (GList *iter = chunks; iter != NULL; iter = iter->next) {
    struct chunk_step_item *csi = iter->data;
    dbt->chunks = g_list_append(dbt->chunks, csi);
    g_async_queue_push(dbt->chunks_queue, csi);
  }
  dbt->status = READY;
  g_message("String PK planner selected metadata-assisted prefix chunks for %s.%s (%u roots, max prefix length %u)",
            dbt->database->source_database, dbt->table, g_list_length(chunks), max_char_size);
  g_list_free(chunks);
  return TRUE;
}
