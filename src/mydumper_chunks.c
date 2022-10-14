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
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include <mysql.h>
#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "mydumper_chunks.h"
#include "mydumper_database.h"
extern gchar *where_option;
extern int detected_server;
extern guint rows_per_file;
guint64 max_rows=1000000;

static GOptionEntry chunks_entries[] = {
    {"max-rows", 0, 0, G_OPTION_ARG_INT64, &max_rows,
     "Limit the number of rows per block after the table is estimated, default 1000000", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
};

void load_chunks_entries(GOptionGroup *main_group){
  g_option_group_add_entries(main_group, chunks_entries);
}

gchar * get_max_char( MYSQL *conn, struct db_table *dbt, char *field, gchar min){
  MYSQL_ROW row;
  MYSQL_RES *max = NULL;
  gchar *query = NULL;
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MAX(BINARY %s) FROM `%s`.`%s` WHERE BINARY %s like '%c%%'",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, dbt->database->name, dbt->table, field, min));
  g_free(query);
  max= mysql_store_result(conn);
  row = mysql_fetch_row(max);
  gchar * r = g_strdup(row[0]);
  mysql_free_result(max);
  return r;
}

gchar *get_next_min_char( MYSQL *conn, struct db_table *dbt, char *field, gchar *max){
  MYSQL_ROW row;
  MYSQL_RES *min = NULL;
  gchar *query = NULL;
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(BINARY %s) FROM `%s`.`%s` WHERE BINARY %s > '%s'",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, dbt->database->name, dbt->table, field, max));
  g_free(query);
  min= mysql_store_result(conn);
  row = mysql_fetch_row(min);
  gchar * r = g_strdup(row[0]);
  mysql_free_result(min);
  return r;
}


union chunk_step *new_char_step(gchar *prefix, gchar *field, gchar *cmin, gchar *cmax){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->char_step.prefix = g_strdup(prefix);
  cs->char_step.cmin = cmin;
  cs->char_step.cmax = cmax;
  cs->char_step.field = g_strdup(field);
  return cs;
}

union chunk_step *new_integer_step(gchar *prefix, gchar *field, guint64 nmin, guint64 nmax){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->integer_step.prefix = g_strdup(prefix);
  cs->integer_step.nmin = nmin;
  cs->integer_step.nmax = nmax;
  cs->integer_step.field = g_strdup(field);
  return cs;
}

void free_char_step(union chunk_step * cs){
  g_free(cs->char_step.field);
  g_free(cs->char_step.prefix);
  g_free(cs);
}

void free_integer_step(union chunk_step * cs){
  g_free(cs->integer_step.field);
  g_free(cs->integer_step.prefix);
  g_free(cs);
}


GList *get_chunks_for_table_by_rows(MYSQL *conn, struct db_table *dbt, char *field){
  GList *chunks = NULL;
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(`%s`),MAX(`%s`) FROM `%s`.`%s` %s %s",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, field, dbt->database->name, dbt->table, where_option ? "WHERE" : "", where_option ? where_option : ""));
  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax)
    goto cleanup;

  row = mysql_fetch_row(minmax);
  MYSQL_FIELD *fields = mysql_fetch_fields(minmax);

  /* Check if all values are NULL */
  if (row[0] == NULL)
    goto cleanup;

  char *min = row[0];
  char *max = row[1];
  guint64 estimated_chunks, estimated_step, nmin, nmax, cutoff, rows;
  char *new_max=NULL,*new_min=NULL;
  gchar *prefix=NULL;
  /* Support just bigger INTs for now, very dumb, no verify approach */
  switch (fields[0].type) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
    /* Got total number of rows, skip chunk logic if estimates are low */
    rows = estimate_count(conn, dbt->database->name, dbt->table, field, min, max);
    if (rows <= rows_per_file){
      g_message("Table %s.%s too small to split", dbt->database->name, dbt->table);
      goto cleanup;
    }

    /* This is estimate, not to use as guarantee! Every chunk would have eventual
 *      * adjustments */
    estimated_chunks = rows / rows_per_file;
    /* static stepping */
    nmin = strtoul(min, NULL, 10);
    nmax = strtoul(max, NULL, 10);
    estimated_step = (nmax - nmin) / estimated_chunks + 1;
    if (estimated_step > max_rows)
      estimated_step = max_rows;
    cutoff = nmin;
    dbt->chunk_type=INTEGER;
    prefix=g_strdup_printf("`%s` IS NULL OR ",field);
    while (cutoff <= nmax) {
      chunks = g_list_prepend(
          chunks,
          new_integer_step(prefix, field, cutoff, cutoff + estimated_step));
      cutoff += estimated_step;
      g_free(prefix);
      prefix=NULL;
    }
    chunks = g_list_reverse(chunks);
    break;
  case MYSQL_TYPE_STRING:
    /* static stepping */
    dbt->chunk_type=CHAR;
    new_min = g_strdup(min);
    prefix=g_strdup_printf("`%s` IS NULL OR ",field);
    while (g_strcmp0(new_max,max)) {
      new_max=get_max_char(conn, dbt, field, new_min[0]);
      chunks = g_list_prepend(
          chunks, 
          new_char_step(prefix, field, new_min, new_max));
      g_free(prefix);
      prefix=NULL;
      new_min = get_next_min_char(conn, dbt, field,new_max);
    }
    chunks = g_list_reverse(chunks);   
    break;
    default:
      ;
   }
cleanup:
  if (minmax)
    mysql_free_result(minmax);
  return chunks;
}

GList *get_chunks_for_table(MYSQL *conn, struct db_table * dbt,
                            struct configuration *conf) {

  GList *chunks = NULL;
  MYSQL_RES *indexes = NULL;
  MYSQL_ROW row;
  char *field = NULL;

  if (dbt->limit != NULL)
    return chunks;

  /* first have to pick index, in future should be able to preset in
   * configuration too */
  gchar *query = g_strdup_printf("SHOW INDEX FROM `%s`.`%s`", dbt->database->name, dbt->table);
  mysql_query(conn, query);
  g_free(query);
  indexes = mysql_store_result(conn);

  if (indexes){
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[2], "PRIMARY") && (!strcmp(row[3], "1"))) {
        /* Pick first column in PK, cardinality doesn't matter */
        field = row[4];
        break;
      }
    }

    /* If no PK found, try using first UNIQUE index */
    if (!field) {
      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[1], "0") && (!strcmp(row[3], "1"))) {
          /* Again, first column of any unique index */
          field = row[4];
          break;
        }
      }
    }
    /* Still unlucky? Pick any high-cardinality index */
    if (!field && conf->use_any_index) {
      guint64 max_cardinality = 0;
      guint64 cardinality = 0;

      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[3], "1")) {
          if (row[6])
            cardinality = strtoul(row[6], NULL, 10);
          if (cardinality > max_cardinality) {
            field = row[4];
            max_cardinality = cardinality;
          }
        }
      }
    }
  }
  /* Oh well, no chunks today - no suitable index */
  if (!field)
    goto cleanup;
  chunks = get_chunks_for_table_by_rows(conn,dbt,field);

cleanup:
  if (indexes)
    mysql_free_result(indexes);
  return chunks;
}

/* Try to get EXPLAIN'ed estimates of row in resultset */
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field,
                       char *from, char *to) {
  char *querybase, *query;
  int ret;

  g_assert(conn && database && table);

  querybase = g_strdup_printf("EXPLAIN SELECT `%s` FROM `%s`.`%s`",
                              (field ? field : "*"), database, table);
  if (from || to) {
    g_assert(field != NULL);
    char *fromclause = NULL, *toclause = NULL;
    char *escaped;
    if (from) {
      escaped = g_new(char, strlen(from) * 2 + 1);
      mysql_real_escape_string(conn, escaped, from, strlen(from));
      fromclause = g_strdup_printf(" `%s` >= %s ", field, escaped);
      g_free(escaped);
    }
    if (to) {
      escaped = g_new(char, strlen(to) * 2 + 1);
      mysql_real_escape_string(conn, escaped, to, strlen(to));
      toclause = g_strdup_printf(" `%s` <= %s", field, escaped);
      g_free(escaped);
    }
    query = g_strdup_printf("%s WHERE %s %s %s", querybase,
                            (from ? fromclause : ""),
                            ((from && to) ? "AND" : ""), (to ? toclause : ""));

    if (toclause)
      g_free(toclause);
    if (fromclause)
      g_free(fromclause);
    ret = mysql_query(conn, query);
    g_free(querybase);
    g_free(query);
  } else {
    ret = mysql_query(conn, querybase);
    g_free(querybase);
  }

  if (ret) {
    g_warning("Unable to get estimates for %s.%s: %s", database, table,
              mysql_error(conn));
  }

  MYSQL_RES *result = mysql_store_result(conn);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);

  guint i;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcmp(fields[i].name, "rows"))
      break;
  }

  MYSQL_ROW row = NULL;

  guint64 count = 0;

  if (result)
    row = mysql_fetch_row(result);

  if (row && row[i])
    count = strtoul(row[i], NULL, 10);

  if (result)
    mysql_free_result(result);

  return (count);
}
