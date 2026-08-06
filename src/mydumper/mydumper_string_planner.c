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
extern guint64 string_pk_planner_min_rows;
extern guint64 string_pk_planner_target_rows_per_prefix;
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

/*
 * A candidate prefix paired with the EXPLAIN row estimate that produced it.
 * Carrying the estimate lets the level-expansion decide whether a prefix is
 * still over the target without re-probing prefixes that were kept unchanged.
 */
struct prefix_estimate {
  gchar *prefix;
  guint64 rows;
  /*
   * FALSE once the prefix can no longer be usefully deepened (it reached the
   * max_char_size ceiling, has no non-empty children, or splitting it would
   * exceed the max_prefixes budget).  Such prefixes stay as final roots.
   */
  gboolean expandable;
};

static struct prefix_estimate *new_prefix_estimate(const gchar *prefix, guint64 rows){
  struct prefix_estimate *pe = g_new0(struct prefix_estimate, 1);
  pe->prefix = g_strdup(prefix);
  pe->rows = rows;
  pe->expandable = TRUE;
  return pe;
}

static void free_prefix_estimate_list(GList *estimates){
  for (GList *iter = estimates; iter != NULL; iter = iter->next) {
    struct prefix_estimate *pe = iter->data;
    g_free(pe->prefix);
    g_free(pe);
  }
  g_list_free(estimates);
}

static gint compare_prefix_estimate_prefix(gconstpointer a, gconstpointer b){
  const struct prefix_estimate *pa = a;
  const struct prefix_estimate *pb = b;
  return g_strcmp0(pa->prefix, pb->prefix);
}

/*
 * Seeds the coarsest complete cover: every single-character prefix that the
 * optimizer believes contains rows.  Returns NULL (and frees anything it
 * collected) when the probe/timeout budget is exhausted before the seed is
 * complete, so the caller can fall back to the recursive splitter with full
 * coverage rather than a partial plan.
 */
static GList *seed_prefix_level(MYSQL *conn, struct db_table *dbt, const gchar *alphabet){
  GList *level = NULL;
  for (const gchar *character = alphabet; *character != '\0'; character++) {
    gchar candidate[64];
    g_snprintf(candidate, sizeof(candidate), "%c", *character);
    guint64 estimated_rows = estimate_prefix_rows(conn, dbt, candidate);
    if (string_pk_planner_budget_exhausted(dbt)) {
      free_prefix_estimate_list(level);
      return NULL;
    }
    if (estimated_rows == 0) {
      continue;
    }
    level = g_list_append(level, new_prefix_estimate(candidate, estimated_rows));
  }
  return level;
}

/*
 * Probes every non-empty one-character child of victim->prefix, returning them
 * as a new prefix_estimate list.  *child_count receives the number of non-empty
 * children.  Because a prefix can only be split all-or-nothing (keeping only
 * some children would leave gaps in the key space), the probe stops early and
 * sets *over_budget as soon as accepting the children collected so far would
 * push the total prefix count above max_prefixes.  Sets *aborted when the
 * probe/timeout budget is exhausted mid-expansion.  Returns NULL (freeing any
 * partial children) whenever *over_budget or *aborted is set.
 */
static GList *probe_prefix_children(MYSQL *conn, struct db_table *dbt,
                                    const struct prefix_estimate *victim,
                                    guint current_count, const gchar *alphabet,
                                    guint *child_count, gboolean *over_budget,
                                    gboolean *aborted){
  GList *children = NULL;
  *child_count = 0;
  *over_budget = FALSE;
  *aborted = FALSE;

  for (const gchar *character = alphabet; *character != '\0'; character++) {
    gchar child[64];
    g_snprintf(child, sizeof(child), "%s%c", victim->prefix, *character);
    guint64 estimated_rows = estimate_prefix_rows(conn, dbt, child);
    if (string_pk_planner_budget_exhausted(dbt)) {
      *aborted = TRUE;
      free_prefix_estimate_list(children);
      return NULL;
    }
    if (estimated_rows == 0) {
      continue;
    }
    children = g_list_append(children, new_prefix_estimate(child, estimated_rows));
    (*child_count)++;
    /* Replacing one prefix with child_count children is a net +(child_count - 1). */
    if (!string_pk_planner_level_fits_budget(current_count - 1 + *child_count,
                                             string_pk_planner_max_prefixes)) {
      *over_budget = TRUE;
      free_prefix_estimate_list(children);
      return NULL;
    }
  }

  return children;
}

/*
 * Greedy best-first prefix planner.  Starts from the single-character cover and
 * repeatedly lengthens, by one character, the single over-target prefix with
 * the most estimated rows, replacing it with its non-empty children.  Only hot
 * prefixes are ever drilled into, so the number of EXPLAIN probes scales with
 * the number of chunks produced rather than with alphabet^depth.
 *
 * A prefix stops growing once it is at/under target_rows_per_root, once it
 * reaches the max_char_size length ceiling, or once it can no longer be split
 * without exceeding the max_prefixes budget; in the latter two cases it stays a
 * root as-is.  The single-character seed is always kept, so the planner never
 * collapses to a single empty-prefix root and always yields a full cover.
 * *chosen_depth reports the deepest prefix length produced.
 */
static GList *build_prefix_roots(MYSQL *conn, struct db_table *dbt, guint64 target_rows_per_root, guint *chosen_depth){
  gchar *alphabet = get_probe_alphabet(conn, dbt);

  GList *cover = seed_prefix_level(conn, dbt, alphabet);
  if (cover == NULL || g_list_length(cover) == 0) {
    free_prefix_estimate_list(cover);
    g_free(alphabet);
    if (chosen_depth != NULL) {
      *chosen_depth = 0;
    }
    return NULL;
  }

  guint deepest = 1;
  while (!string_pk_planner_budget_exhausted(dbt)) {
    /* Pick the hottest prefix that is still over target and can be deepened. */
    struct prefix_estimate *victim = NULL;
    for (GList *iter = cover; iter != NULL; iter = iter->next) {
      struct prefix_estimate *pe = iter->data;
      if (!pe->expandable || pe->rows <= target_rows_per_root) {
        continue;
      }
      if (strlen(pe->prefix) >= max_char_size) {
        /* Reached the length ceiling: keep it as a root even if over target. */
        pe->expandable = FALSE;
        continue;
      }
      if (victim == NULL || pe->rows > victim->rows) {
        victim = pe;
      }
    }
    if (victim == NULL) {
      /* Every prefix is at/under target or can no longer be deepened. */
      break;
    }

    guint child_count = 0;
    gboolean over_budget = FALSE;
    gboolean aborted = FALSE;
    GList *children = probe_prefix_children(conn, dbt, victim, g_list_length(cover),
                                            alphabet, &child_count, &over_budget, &aborted);
    if (aborted) {
      /* Out of probe/time budget: keep the complete cover we already have. */
      break;
    }
    if (over_budget || child_count == 0) {
      /*
       * Either splitting this prefix would exceed the prefix budget, or every
       * row equals the prefix itself, so it cannot be split.  Keep it as a root
       * and stop reconsidering it; smaller prefixes and single-child chains
       * (which never grow the total count) may still be split.
       */
      victim->expandable = FALSE;
      continue;
    }

    guint victim_depth = (guint)strlen(victim->prefix) + 1;
    if (victim_depth > deepest) {
      deepest = victim_depth;
    }
    cover = g_list_remove(cover, victim);
    g_free(victim->prefix);
    g_free(victim);
    cover = g_list_concat(cover, children);
  }

  g_free(alphabet);

  if (cover == NULL || g_list_length(cover) == 0) {
    free_prefix_estimate_list(cover);
    if (chosen_depth != NULL) {
      *chosen_depth = 0;
    }
    return NULL;
  }

  cover = g_list_sort(cover, compare_prefix_estimate_prefix);
  if (chosen_depth != NULL) {
    *chosen_depth = deepest;
  }
  return cover;
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

  guint64 target_rows_per_root = string_pk_planner_compute_target(
      rows, string_pk_planner_target_rows_per_prefix,
      string_pk_planner_max_prefixes, dbt->min_chunk_step_size);

  guint chosen_depth = 0;
  GList *prefixes = build_prefix_roots(conn, dbt, target_rows_per_root, &chosen_depth);
  if (prefixes == NULL || g_list_length(prefixes) == 0) {
    return FALSE;
  }

  /*
   * The chunk step doubles as the per-chunk row target for the runtime string
   * splitter: process_string_chunk_step() dumps a root whole only when its
   * EXPLAIN estimate is at/under the step, and subdivides it further otherwise.
   * Tying the step to target_rows_per_root therefore makes the emitted chunks
   * honor --string-pk-planner-target-rows-per-prefix even for roots the planner
   * had to leave over target (max-char-size or max-prefixes bound), instead of
   * dumping each root as one huge statement.
   */
  guint64 root_step = target_rows_per_root;

  GList *chunks = NULL;
  guint64 part = 0;
  for (GList *iter = prefixes; iter != NULL; iter = iter->next) {
    struct prefix_estimate *pe = iter->data;
    /* Carry the real per-prefix estimate so the runtime split decision is accurate. */
    append_string_chunk_root(dbt, &chunks, pe->prefix, part++, root_step, pe->rows, strlen(pe->prefix));
  }
  free_prefix_estimate_list(prefixes);

  if (chunks == NULL) {
    return FALSE;
  }

  for (GList *iter = chunks; iter != NULL; iter = iter->next) {
    struct chunk_step_item *csi = iter->data;
    dbt->chunks = g_list_append(dbt->chunks, csi);
    g_async_queue_push(dbt->chunks_queue, csi);
  }
  dbt->status = READY;
  g_message("String PK planner selected metadata-assisted prefix chunks for %s.%s "
            "(%u roots, prefix length %u, target %"G_GUINT64_FORMAT" rows/prefix, max prefix length %u)",
            dbt->database->source_database, dbt->table, g_list_length(chunks),
            chosen_depth, target_rows_per_root, max_char_size);
  g_list_free(chunks);
  return TRUE;
}
