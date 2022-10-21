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
#include <math.h>
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "mydumper_chunks.h"
#include "mydumper_database.h"
#include "mydumper_jobs.h"
extern gchar *where_option;
extern int detected_server;
extern guint rows_per_file;
extern gboolean split_partitions;
extern guint num_threads;
guint64 max_rows=1000000;
GAsyncQueue *give_me_another_innodb_chunk_step_queue;
GAsyncQueue *give_me_another_non_innodb_chunk_step_queue;

extern GList *innodb_table, *non_innodb_table;

static GOptionEntry chunks_entries[] = {
    {"max-rows", 0, 0, G_OPTION_ARG_INT64, &max_rows,
     "Limit the number of rows per block after the table is estimated, default 1000000", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
};

void initialize_chunk(){
  give_me_another_innodb_chunk_step_queue=g_async_queue_new();
  give_me_another_non_innodb_chunk_step_queue=g_async_queue_new();
}


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
  max= mysql_store_result(conn);
  row = mysql_fetch_row(max);
  gchar * r = g_strdup(row[0]);
  g_free(query);
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

union chunk_step *new_char_step(gchar *prefix, gchar *field, GList *list, guint deep, guint number){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->char_step.prefix = g_strdup(prefix);
//  cs->char_step.cmin = cmin;
//  cs->char_step.cmax = cmax;
  cs->char_step.assigned=FALSE;
  cs->char_step.deep = deep;
  cs->char_step.number = number;
  cs->char_step.mutex=g_mutex_new();
  cs->char_step.field = g_strdup(field);
  cs->char_step.list = list; 
  return cs;
}

union chunk_step *new_integer_step(gchar *prefix, gchar *field, guint64 nmin, guint64 nmax, guint deep, guint number){
  g_message("New Integer Step");
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->integer_step.prefix = g_strdup(prefix);
  cs->integer_step.nmin = nmin;
  cs->integer_step.step = cs->integer_step.nmin;
  cs->integer_step.deep = deep;
  cs->integer_step.number = number;
  cs->integer_step.nmax = nmax;
  cs->integer_step.step = rows_per_file;
  cs->integer_step.field = g_strdup(field);
  cs->integer_step.mutex = g_mutex_new(); 
  cs->integer_step.assigned = FALSE;
  return cs;
}

union chunk_step *new_real_partition_step(GList *partition, guint deep, guint number){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->partition_step.list = partition;
  cs->partition_step.assigned= FALSE;
  cs->partition_step.mutex = g_mutex_new();
  cs->partition_step.deep = deep;
  cs->partition_step.number = number;
  return cs;
}



void free_char_step(union chunk_step * cs){
  g_free(cs->char_step.field);
  g_free(cs->char_step.prefix);
  g_free(cs);
}

void free_integer_step(union chunk_step * cs){
  if (cs->integer_step.field!=NULL){
    g_free(cs->integer_step.field);
    cs->integer_step.field=NULL;
  }
  if (cs->integer_step.prefix)
    g_free(cs->integer_step.prefix);
  g_free(cs);
}


union chunk_step *get_next_integer_chunk(struct db_table *dbt){
  GList *l=dbt->chunks;
  g_mutex_lock(dbt->chunks_mutex);
  union chunk_step *cs=NULL;
  while (l!=NULL){
    g_message("IN WHILE");
    cs=l->data;
    g_mutex_lock(cs->integer_step.mutex);
    if (cs->integer_step.assigned==FALSE){
      g_message("Not assigned");
      cs->integer_step.assigned=TRUE;
      g_mutex_unlock(cs->integer_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return cs;
    }

    if (cs->integer_step.nmin + (5 * cs->integer_step.step) < cs->integer_step.nmax){
      guint64 new_minmax = cs->integer_step.nmin + (cs->integer_step.nmax - cs->integer_step.nmin)/2;
      cs->integer_step.deep--;
      union chunk_step * new_cs = new_integer_step(NULL, dbt->field, new_minmax, cs->integer_step.nmax, cs->integer_step.deep, cs->integer_step.number+pow(2,cs->integer_step.deep));
      dbt->chunks=g_list_append(dbt->chunks,new_cs);
      cs->integer_step.nmax = new_minmax;
      new_cs->integer_step.assigned=TRUE;
      g_mutex_unlock(cs->integer_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return new_cs;
    }
    g_mutex_unlock(cs->integer_step.mutex);
    l=l->next;
  }
  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

union chunk_step *get_next_char_chunk(struct db_table *dbt){
  GList *l=dbt->chunks;
  g_mutex_lock(dbt->chunks_mutex);
  union chunk_step *cs=NULL;
  while (l!=NULL){
    cs=l->data;
    g_mutex_lock(cs->char_step.mutex);
    if (!cs->char_step.assigned){
      cs->char_step.assigned=TRUE;
      g_mutex_unlock(cs->char_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return cs;
    }
     
    if (g_list_length (cs->char_step.list) > 3 ){
      guint pos=g_list_length (cs->char_step.list) / 2;
      GList *new_list=g_list_nth(cs->char_step.list,pos);
      new_list->prev->next=NULL;
      new_list->prev=NULL;
      cs->char_step.deep--;
      union chunk_step * new_cs = new_char_step(NULL, dbt->field, new_list, cs->char_step.deep, cs->char_step.number+pow(2,cs->char_step.deep));
      new_cs->char_step.assigned=TRUE;
      dbt->chunks=g_list_append(dbt->chunks,new_cs);

      g_mutex_unlock(cs->char_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return new_cs;
    }
    g_mutex_unlock(cs->char_step.mutex);
    l=l->next;
  }
  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

union chunk_step *get_next_partition_chunk(struct db_table *dbt){
  GList *l=dbt->chunks;
  g_mutex_lock(dbt->chunks_mutex);
  union chunk_step *cs=NULL;
  while (l!=NULL){
    cs=l->data;
    g_mutex_lock(cs->partition_step.mutex);
    if (!cs->partition_step.assigned){
      cs->partition_step.assigned=TRUE;
      g_mutex_unlock(cs->partition_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return cs;
    }

    if (g_list_length (cs->partition_step.list) > 3 ){
      guint pos=g_list_length (cs->partition_step.list) / 2;
      GList *new_list=g_list_nth(cs->partition_step.list,pos);
      new_list->prev->next=NULL;
      new_list->prev=NULL;
      cs->partition_step.deep--;
      union chunk_step * new_cs = new_real_partition_step(new_list, cs->partition_step.deep, cs->partition_step.number+pow(2,cs->partition_step.deep));
      new_cs->partition_step.assigned=TRUE;
      dbt->chunks=g_list_append(dbt->chunks,new_cs);

      g_mutex_unlock(cs->partition_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return new_cs;
    }
    g_mutex_unlock(cs->partition_step.mutex);
    l=l->next;
  }
  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

union chunk_step *get_next_chunk(struct db_table *dbt){
  switch (dbt->chunk_type){
    case CHAR: 
      return get_next_char_chunk(dbt);
      break;
    case INTEGER:
      g_message("get_next_integer_chunk INTEGER");
      return get_next_integer_chunk(dbt);
      break;
    case PARTITION:
      return get_next_partition_chunk(dbt);
      break;
    default:
      break;
  }
  return NULL;
}


GList * get_chunk_list_of_chars(MYSQL *conn, struct db_table *dbt){
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  GList *list=NULL;
  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s LEFT(`%s`,1) FROM `%s`.`%s` %s %s GROUP BY LEFT(`%s`,1)",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        dbt->field, dbt->database->name, dbt->table, where_option ? "WHERE" : "", where_option ? where_option : "", dbt->field));
  g_free(query);
  minmax = mysql_store_result(conn);
  if (!minmax)
    goto cleanup;

  while ((row = mysql_fetch_row(minmax))) {
    list=g_list_append(list,strdup(row[0]));
  }

cleanup:
  if (minmax)
    mysql_free_result(minmax);
  return list;
}

GList * get_partitions_for_table(MYSQL *conn, char *database, char *table){
  MYSQL_RES *res=NULL;
  MYSQL_ROW row;

  GList *partition_list = NULL;

  gchar *query = g_strdup_printf("select PARTITION_NAME from information_schema.PARTITIONS where PARTITION_NAME is not null and TABLE_SCHEMA='%s' and TABLE_NAME='%s'", database, table);
  mysql_query(conn,query);
  g_free(query);

  res = mysql_store_result(conn);
  if (res == NULL)
    //partitioning is not supported
    return partition_list;
  while ((row = mysql_fetch_row(res))) {
    partition_list = g_list_append(partition_list, strdup(row[0]));
  }
  mysql_free_result(res);

  return partition_list;
}

void set_chunk_strategy_for_dbt(MYSQL *conn, struct db_table *dbt){
  GList *partitions=NULL;
  if (split_partitions){
    partitions = get_partitions_for_table(conn, dbt->database->name, dbt->table);
  }

  if (partitions){
    dbt->chunk_type=PARTITION;
    dbt->chunks=g_list_prepend(dbt->chunks,new_real_partition_step(partitions,0,8));
    return;
  }

  if (rows_per_file>0){

  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(`%s`),MAX(`%s`) FROM `%s`.`%s` %s %s",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        dbt->field, dbt->field, dbt->database->name, dbt->table, where_option ? "WHERE" : "", where_option ? where_option : ""));
  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax)
    goto cleanup;

  row = mysql_fetch_row(minmax);
  MYSQL_FIELD *fields = mysql_fetch_fields(minmax);

  /* Check if all values are NULL */
  if (row[0] == NULL)
    goto cleanup;
  dbt->min = g_strdup(row[0]);
  dbt->max = g_strdup(row[1]);
  /* Support just bigger INTs for now, very dumb, no verify approach */
  guint64 nmin,nmax;
  switch (fields[0].type) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
    nmin = strtoul(dbt->min, NULL, 10);
    nmax = strtoul(dbt->max, NULL, 10) + 1;
    if ((nmax-nmin) > (4 * rows_per_file)){
      dbt->chunk_type=INTEGER;
      dbt->chunks=g_list_prepend(dbt->chunks,new_integer_step(g_strdup_printf("`%s` IS NULL OR ",dbt->field), dbt->field, nmin, nmax, 8, 0));
    }else{
      dbt->chunk_type=NONE;
    } 
    break;
  case MYSQL_TYPE_STRING:
    dbt->chunk_type=CHAR;
    dbt->chunks=g_list_prepend(dbt->chunks,new_char_step(g_strdup_printf("`%s` IS NULL OR ",dbt->field), dbt->field, get_chunk_list_of_chars(conn,dbt), 8, 0));
    break;
  default:
    break;
  }
cleanup:
  if (minmax)
    mysql_free_result(minmax);
}
}

GList *get_chunks_for_table_by_rows(MYSQL *conn, struct db_table *dbt){
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
                        dbt->field, dbt->field, dbt->database->name, dbt->table, where_option ? "WHERE" : "", where_option ? where_option : ""));
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
  guint64 estimated_chunks, estimated_step, nmin, nmax, cutoff, erows;
  char *new_max=NULL,*new_min=NULL;
  gchar *prefix=NULL;
  /* Support just bigger INTs for now, very dumb, no verify approach */
  switch (fields[0].type) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
    /* Got total number of rows, skip chunk logic if estimates are low */
    erows = estimate_count(conn, dbt->database->name, dbt->table, dbt->field, min, max);
    if (erows <= rows_per_file){
      g_message("Table %s.%s too small to split", dbt->database->name, dbt->table);
      goto cleanup;
    }

    /* This is estimate, not to use as guarantee! Every chunk would have eventual
 *      * adjustments */
    estimated_chunks = erows / rows_per_file;
    /* static stepping */
    nmin = strtoul(min, NULL, 10);
    nmax = strtoul(max, NULL, 10);
    estimated_step = (nmax - nmin) / estimated_chunks + 1;
    if (estimated_step > max_rows)
      estimated_step = max_rows;
    cutoff = nmin;
    dbt->chunk_type=INTEGER;
    prefix=g_strdup_printf("`%s` IS NULL OR ",dbt->field);
    while (cutoff <= nmax) {
      chunks = g_list_prepend(
          chunks,
          new_integer_step(prefix, dbt->field, cutoff, cutoff + estimated_step, 0, 0));
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
    prefix=g_strdup_printf("`%s` IS NULL OR ",dbt->field);
    while (g_strcmp0(new_max,max)) {
      new_max=get_max_char(conn, dbt, dbt->field, new_min[0]);
      chunks = g_list_prepend(
          chunks, 
          new_char_step(prefix, dbt->field, NULL, 0, 0));
      g_free(prefix);
      prefix=NULL;
      new_min = get_next_min_char(conn, dbt, dbt->field,new_max);
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

char *get_field_for_dbt(MYSQL *conn, struct db_table * dbt, struct configuration *conf){
  MYSQL_RES *indexes = NULL;
  MYSQL_ROW row;
  char *field = NULL;

  /* first have to pick index, in future should be able to preset in
 *    * configuration too */
  gchar *query = g_strdup_printf("SHOW INDEX FROM `%s`.`%s`", dbt->database->name, dbt->table);
  mysql_query(conn, query);
  g_free(query);
  indexes = mysql_store_result(conn);

  if (indexes){
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[2], "PRIMARY") && (!strcmp(row[3], "1"))) {
        /* Pick first column in PK, cardinality doesn't matter */
        field = g_strdup(row[4]);
        break;
      }
    }

    /* If no PK found, try using first UNIQUE index */
    if (!field) {
      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[1], "0") && (!strcmp(row[3], "1"))) {
          /* Again, first column of any unique index */
          field = g_strdup(row[4]);
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
            field = g_strdup(row[4]);
            max_cardinality = cardinality;
          }
        }
      }
    }
  }
  if (indexes)
    mysql_free_result(indexes);
  return field;
}


GList *get_chunks_for_table(MYSQL *conn, struct db_table * dbt,
                            struct configuration *conf) {

  GList *chunks = NULL;
  if (dbt->limit != NULL)
    return chunks;
  char *field=get_field_for_dbt(conn,dbt,conf);
  if (!field)
    return NULL;
  chunks = get_chunks_for_table_by_rows(conn,dbt);

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


void get_next_dbt_and_chunk(struct db_table **dbt,union chunk_step **cs, GList **dbt_list){
  GList *iter=*dbt_list;
  union chunk_step *lcs;
  struct db_table *d;

  while (iter){
    d=iter->data;
    g_message("Checking %s ", d->table);
    if (d->chunk_type == NONE){
      *dbt=iter->data;
      *dbt_list=g_list_remove(*dbt_list,d);
      break;
    }
    lcs=get_next_chunk(d);
    if (lcs!=NULL){
      *cs=lcs;
      *dbt=iter->data;
      break;
    }else{
      iter=iter->next;
      *dbt_list=g_list_remove(*dbt_list,d);
      continue;
    }
    iter=iter->next;
  }
}


void give_me_another_non_innodb_chunk_step(){
  g_async_queue_push(give_me_another_non_innodb_chunk_step_queue, GINT_TO_POINTER(1));
}

void give_me_another_innodb_chunk_step(){
  g_async_queue_push(give_me_another_innodb_chunk_step_queue, GINT_TO_POINTER(1));
}




/*void process_integer_chunk(struct thread_data *td, struct db_table *dbt, union chunk_step *cs){
  g_mutex_lock(cs->integer_step.mutex);
  cs->integer_step.cursor = cs->integer_step.nmin + cs->integer_step.step;
  g_mutex_unlock(cs->integer_step.mutex);
  struct table_job *tj = new_table_job(dbt, NULL, cs->integer_step.number, dbt->primary_key, cs);
  message_dumping_data(td,tj);
  write_table_job_into_file(td->thrconn, tj);
  g_mutex_lock(cs->integer_step.mutex);
  cs->integer_step.nmin = cs->integer_step.cursor;
  g_mutex_unlock(cs->integer_step.mutex);
  if (cs->integer_step.prefix)
    g_free(cs->integer_step.prefix);
  cs->integer_step.prefix=NULL;
  while ( cs->integer_step.nmax - cs->integer_step.nmin > rows_per_file ){
    g_mutex_lock(cs->integer_step.mutex);
    cs->integer_step.cursor = cs->integer_step.nmin + cs->integer_step.step;
    g_mutex_unlock(cs->integer_step.mutex);
    update_where_on_table_job(tj);
    message_dumping_data(td,tj);
    write_table_job_into_file(td->thrconn, tj);
    g_mutex_lock(cs->integer_step.mutex);
    cs->integer_step.nmin=cs->integer_step.cursor;
    g_mutex_unlock(cs->integer_step.mutex);
  }
  g_mutex_lock(cs->integer_step.mutex);
  cs->integer_step.cursor = cs->integer_step.nmax;
  g_mutex_unlock(cs->integer_step.mutex);
  update_where_on_table_job(tj);
  message_dumping_data(td,tj);
  write_table_job_into_file(td->thrconn, tj);
  g_mutex_lock(cs->integer_step.mutex);
  g_mutex_lock(dbt->chunks_mutex);
  dbt->chunks=g_list_remove(dbt->chunks,cs);
  if (g_list_length(dbt->chunks) == 0){
    g_message("Thread %d: Table %s completed ",td->thread_id,dbt->table);
    dbt->chunks=NULL;
  }
  g_message("Thread %d:Remaining 2 chunks: %d",td->thread_id,g_list_length(dbt->chunks));
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_unlock(cs->integer_step.mutex);
  free_table_job(tj);
}*/

void enqueue_shutdown_jobs(GAsyncQueue * queue){
  struct job *j=NULL;
  guint n;
  for (n = 0; n < num_threads; n++) {
    j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(queue, j);
  }
}

void table_job_enqueue(GAsyncQueue * pop_queue, GAsyncQueue * push_queue, GList **table_list){
  struct db_table *dbt;
  union chunk_step *cs;
  for (;;) {
    g_message("g_async_queue_pop(pop_queue)");
    g_async_queue_pop(pop_queue);
    dbt=NULL;
    cs=NULL;
    get_next_dbt_and_chunk(&dbt,&cs,table_list);

    if ((cs==NULL) && (dbt==NULL)){
      break;
    }
    switch (dbt->chunk_type) {
    case INTEGER:
      g_message("New INTEGER");
      create_job_to_dump_chunk(dbt, NULL, cs->integer_step.number, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    case CHAR:
      g_message("New CHAR");
      create_job_to_dump_chunk(dbt, NULL, cs->char_step.number, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    case PARTITION:
      g_message("New PARTITION");
      create_job_to_dump_chunk(dbt, NULL, cs->partition_step.number, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    case NONE:
     create_job_to_dump_chunk(dbt, NULL, 0, dbt->primary_key, cs, g_async_queue_push, push_queue);
    break;
    }
  }
}




void *chunk_builder_thread(struct configuration *conf){

  table_job_enqueue(give_me_another_non_innodb_chunk_step_queue, conf->non_innodb_queue, &non_innodb_table);

  enqueue_shutdown_jobs(conf->non_innodb_queue);

  g_message("chunk_builder_thread: Starting innodb tables");

  table_job_enqueue(give_me_another_innodb_chunk_step_queue, conf->innodb_queue, &innodb_table);

  g_message("innodb tables completed");
  enqueue_shutdown_jobs(conf->innodb_queue);

  return NULL;
}

