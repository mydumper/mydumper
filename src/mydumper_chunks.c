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
extern gchar *set_names_str;
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
  gchar *from = g_new(char, 2 + 1);
  from[0]=min;
  from[1]='\0';
  gchar *escaped = g_new(char, 2 + 1);
  mysql_real_escape_string(conn, escaped, from, 1);

  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s %s FROM `%s`.`%s` ORDER BY %s LIMIT 10000",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, dbt->database->name, dbt->table, field));
  max= mysql_store_result(conn);
  row = mysql_fetch_row(max);
  g_free(escaped);
  if (row[0] != NULL){
    escaped = g_new(char, strlen(row[0])*2 + 1);
    mysql_real_escape_string(conn, escaped, row[0], strlen(row[0]));
//    g_strdup_printf("%02x", )
//  gchar * r = g_strdup(row[0]);
  }else{
    g_message("ROW[0] is NULL");
    escaped=NULL;
  }
  g_free(query);
  mysql_free_result(max);
  return escaped;
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

union chunk_step *new_char_step(MYSQL *conn, gchar *field, /*GList *list,*/ guint deep, guint number, MYSQL_ROW row, gulong *lengths){
  union chunk_step * cs = g_new0(union chunk_step, 1);

  cs->char_step.step=rows_per_file;

  cs->char_step.cmin_clen = lengths[2];
  cs->char_step.cmin_len = lengths[0]+1;
  cs->char_step.cmin = g_new(char, cs->char_step.cmin_len);
  g_strlcpy(cs->char_step.cmin, row[0], cs->char_step.cmin_len);
  cs->char_step.cmin_escaped = g_new(char, lengths[0] * 2 + 1);
  mysql_real_escape_string(conn, cs->char_step.cmin_escaped, row[0], lengths[0]);

  cs->char_step.cmax_clen = lengths[3];
  cs->char_step.cmax_len = lengths[1]+1;
  cs->char_step.cmax = g_new(char, cs->char_step.cmax_len);
  g_strlcpy(cs->char_step.cmax, row[1], cs->char_step.cmax_len);
  cs->char_step.cmax_escaped = g_new(char, lengths[1] * 2 + 1);
  mysql_real_escape_string(conn, cs->char_step.cmax_escaped, row[1], lengths[1]);

//  g_message("new_char_step: cmin: `%s` | cmax: `%s`", cs->char_step.cmin, cs->char_step.cmax);
  cs->char_step.assigned=FALSE;
  cs->char_step.deep = deep;
  cs->char_step.number = number;
  cs->char_step.mutex=g_mutex_new();
  cs->char_step.field = g_strdup(field);
  cs->char_step.previous=NULL;
//  cs->char_step.list = list; 

  cs->char_step.prefix=g_strdup_printf("`%s` IS NULL OR `%s` = '%s' OR",field, field, cs->char_step.cmin_escaped);

//  g_message("new_char_step: min: %s | max: %s ", cs->char_step.cmin_escaped, cs->char_step.cmax_escaped);

  return cs;
}


void next_chunk_in_char_step(union chunk_step * cs){
  cs->char_step.cmin_clen = cs->char_step.cursor_clen;
  cs->char_step.cmin_len = cs->char_step.cursor_len;
  cs->char_step.cmin = cs->char_step.cursor;
  cs->char_step.cmin_escaped = cs->char_step.cursor_escaped;
}

union chunk_step *split_char_step( guint deep, guint number, union chunk_step *previous_cs){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->char_step.prefix = NULL;
  cs->char_step.assigned=TRUE;
  cs->char_step.deep = deep;
  cs->char_step.number = number;
  cs->char_step.mutex=g_mutex_new();
  cs->char_step.step=rows_per_file;
  cs->char_step.field = g_strdup(previous_cs->char_step.field);
  cs->char_step.previous=previous_cs;
//  cs->char_step.list = list;
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
//  g_message("Freeing free_char_step");
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
  g_mutex_lock(dbt->chunks_mutex);
  GList *l=dbt->chunks;
  union chunk_step *cs=NULL;
  while (l!=NULL){
//    g_message("IN WHILE");
    cs=l->data;
    g_mutex_lock(cs->integer_step.mutex);
    if (cs->integer_step.assigned==FALSE){
//      g_message("Not assigned");
      cs->integer_step.assigned=TRUE;
      g_mutex_unlock(cs->integer_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return cs;
    }

    if (cs->integer_step.nmin + (5 * cs->integer_step.step) < cs->integer_step.nmax){
      guint64 new_minmax = cs->integer_step.nmin + (cs->integer_step.nmax - cs->integer_step.nmin)/2;
      union chunk_step * new_cs = new_integer_step(NULL, dbt->field, new_minmax, cs->integer_step.nmax, cs->integer_step.deep + 1, cs->integer_step.number+pow(2,cs->integer_step.deep));
      cs->integer_step.deep++;
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
  g_mutex_lock(dbt->chunks_mutex);
  GList *l=dbt->chunks;
  union chunk_step *cs=NULL;
  while (l!=NULL){
    cs=l->data;
    if (cs->char_step.mutex == NULL){
      g_message("This should not happen");
      l=l->next;
      continue;
    }
    
    g_mutex_lock(cs->char_step.mutex);
    if (!cs->char_step.assigned){
      cs->char_step.assigned=TRUE;
      g_mutex_unlock(cs->char_step.mutex);
      g_mutex_unlock(dbt->chunks_mutex);
      return cs;
    }
    if (cs->char_step.deep < num_threads/2 && g_strcmp0(cs->char_step.cmax, cs->char_step.cursor)!=0){
      union chunk_step * new_cs = split_char_step(
          cs->char_step.deep + 1, cs->char_step.number+pow(2,cs->char_step.deep), cs);
      cs->char_step.deep++;
      new_cs->char_step.assigned=TRUE;
      return new_cs;
    }
    g_mutex_unlock(cs->char_step.mutex);
    l=l->next;
  }
  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

union chunk_step *get_next_partition_chunk(struct db_table *dbt){
  g_mutex_lock(dbt->chunks_mutex);
  GList *l=dbt->chunks;
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
      union chunk_step * new_cs = new_real_partition_step(new_list, cs->partition_step.deep+1, cs->partition_step.number+pow(2,cs->partition_step.deep));
      cs->partition_step.deep++;
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


gchar* update_cursor (MYSQL *conn, struct table_job *tj){
  union chunk_step *cs= tj->chunk_step;
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE '%s' <= `%s` AND `%s` <= '%s' ORDER BY `%s` LIMIT %ld,1",
                        (detected_server == SERVER_TYPE_MYSQL) ? "/*!40001 SQL_NO_CACHE */": "",
                        tj->dbt->field, tj->dbt->database->name, tj->dbt->table, cs->char_step.cmin_escaped, tj->dbt->field, tj->dbt->field, cs->char_step.cmax_escaped, tj->dbt->field, cs->char_step.step));
  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax){
    goto cleanup;
  }

  row = mysql_fetch_row(minmax);

  if (row==NULL){
cleanup:
    cs->char_step.cursor_clen = cs->char_step.cmax_clen;
    cs->char_step.cursor_len = cs->char_step.cmax_len;
    cs->char_step.cursor = cs->char_step.cmax;
    cs->char_step.cursor_escaped = cs->char_step.cmax_escaped;
    return NULL;
  }

  gulong *lengths = mysql_fetch_lengths(minmax);

  cs->char_step.cursor_clen = lengths[0];
  cs->char_step.cursor_len = lengths[0]+1;
  cs->char_step.cursor = g_new(char, cs->char_step.cursor_len);
  g_strlcpy(cs->char_step.cursor, row[0], cs->char_step.cursor_len);
  cs->char_step.cursor_escaped = g_new(char, lengths[0] * 2 + 1);
  mysql_real_escape_string(conn, cs->char_step.cursor_escaped, row[0], lengths[0]);


  return g_strdup(row[0]);
}


gboolean get_new_minmax (struct thread_data *td, struct table_job *tj){
 //
//  "SELECT `%s` FROM `%s`.`%s` WHERE `%s` > (SELECT hex(conv(hex('%s'),16,10)+(conv(hex('%s'),16,10)-conv(hex('%s'),16,10))/2)) ORDER BY `%s` LIMIT1"

  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  union chunk_step * previous=tj->chunk_step->char_step.previous, *cs=tj->chunk_step;
  /* Get minimum/maximum */
  guint i = previous->char_step.cmax_clen > previous->char_step.cmin_clen ? previous->char_step.cmin_clen : previous->char_step.cmax_clen;
  i=i==0?1:i;
  guint j =0;
  gchar c[4], d[4];
  for(j=0; j < i; j++){
    c[j]=abs(previous->char_step.cmax[j]-previous->char_step.cmin[j]);
  }
  c[j]='\0';
  mysql_real_escape_string(td->thrconn, d, c, i);
//  g_message("Middle point: `%s` | `%c` | %d", d, d[0], i);
  mysql_query(td->thrconn, query = g_strdup_printf(
//unhex(conv(((ord(left(MAX(`char_id`),1))-ord(@newmin))/2 + ord(@newmin)),10,16))
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE `%s` > (SELECT `%s` FROM `%s`.`%s` WHERE `%s` > '%s' ORDER BY `%s` LIMIT 1) AND `%s` < '%s' AND `%s` > '%s' ORDER BY `%s` LIMIT 1",
                        (detected_server == SERVER_TYPE_MYSQL) ? "/*!40001 SQL_NO_CACHE */": "",
                        tj->dbt->field, tj->dbt->database->name, tj->dbt->table, tj->dbt->field, tj->dbt->field, tj->dbt->database->name, tj->dbt->table, tj->dbt->field, d, tj->dbt->field, tj->dbt->field, previous->char_step.cmax_escaped, tj->dbt->field, previous->char_step.cursor_escaped, tj->dbt->field));
//  g_message("Query: %s", query);
  g_free(query);
  minmax = mysql_store_result(td->thrconn);

  if (!minmax){
//    g_message("get_new_minmax: NULL");
    g_mutex_unlock(tj->dbt->chunks_mutex);
    g_mutex_unlock(previous->char_step.mutex);
    return FALSE;
  }

  row = mysql_fetch_row(minmax);
  if (row == NULL){
//    g_message("get_new_minmax: NULL");
    g_mutex_unlock(tj->dbt->chunks_mutex);
    g_mutex_unlock(previous->char_step.mutex);
    return FALSE;
  }

  gulong *lengths = mysql_fetch_lengths(minmax);

//  g_message("Result: %s", row[0]);

  cs->char_step.cmax_clen = previous->char_step.cmax_clen;
  cs->char_step.cmax_len = previous->char_step.cmax_len;
  cs->char_step.cmax = previous->char_step.cmax;
  cs->char_step.cmax_escaped = previous->char_step.cmax_escaped;
  
  previous->char_step.cmax_clen = lengths[0];
  previous->char_step.cmax_len = lengths[0]+1;
  previous->char_step.cmax = g_new(char, previous->char_step.cmax_len);
  g_strlcpy(previous->char_step.cmax, row[0], previous->char_step.cmax_len);
  previous->char_step.cmax_escaped = g_new(char, lengths[0] * 2 + 1);
  mysql_real_escape_string(td->thrconn, previous->char_step.cmax_escaped, row[0], lengths[0]);

  cs->char_step.cmin_clen = lengths[0];
  cs->char_step.cmin_len = lengths[0]+1;
  cs->char_step.cmin = g_new(char, cs->char_step.cmin_len);
  g_strlcpy(cs->char_step.cmin, row[0], cs->char_step.cmin_len);
  cs->char_step.cmin_escaped = g_new(char, lengths[0] * 2 + 1);
  mysql_real_escape_string(td->thrconn, cs->char_step.cmin_escaped, row[0], lengths[0]);
  tj->dbt->chunks=g_list_append(tj->dbt->chunks,cs);
  g_mutex_unlock(tj->dbt->chunks_mutex);
  g_mutex_unlock(previous->char_step.mutex);
  return TRUE;
}

void set_chunk_strategy_for_dbt(MYSQL *conn, struct db_table *dbt){
  GList *partitions=NULL;
  if (split_partitions){
    partitions = get_partitions_for_table(conn, dbt->database->name, dbt->table);
  }

  if (partitions){
    dbt->chunk_type=PARTITION;
    dbt->chunks=g_list_prepend(dbt->chunks,new_real_partition_step(partitions,0,0));
    return;
  }

  mysql_query(conn, set_names_str);

  if (rows_per_file>0){
//  mysql_query(td->thrconn, set_names_str);
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(`%s`),MAX(`%s`),LEFT(MIN(`%s`),1),LEFT(MAX(`%s`),1) FROM `%s`.`%s` %s %s",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        dbt->field, dbt->field, dbt->field, dbt->field, dbt->database->name, dbt->table, where_option ? "WHERE" : "", where_option ? where_option : ""));
//  g_message("Query: %s", query);
  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax)
    goto cleanup;

  row = mysql_fetch_row(minmax);

  MYSQL_FIELD *fields = mysql_fetch_fields(minmax);
  gulong *lengths = mysql_fetch_lengths(minmax);
  /* Check if all values are NULL */
  if (row[0] == NULL)
    goto cleanup;
  gchar *escaped = g_new(char, lengths[0] * 2 + 1);
  g_strlcpy(escaped, row[0], lengths[0]);
  escaped[lengths[0]]='\0';
//  mysql_real_escape_string(conn, escaped, row[0], lengths[0]);
//  mysql_hex_string(escaped, row[0], lengths[0]);
//  g_message("Min: `%s` | `%s` | %ld",row[0], row[2], lengths[2]);
  dbt->min = escaped;
//  dbt->min_len = lengths[0];
  escaped = g_new(char, lengths[1] + 2);
  g_strlcpy(escaped, row[1], lengths[1]+1);
  escaped[lengths[1]+2]='\0';
  dbt->max = escaped;
//  g_message("Max: `%s` | `%s` | %ld | %s",row[1], row[3], lengths[3], dbt->max);
//  g_message("Min: `%s` | max: `%s` ", dbt->min, dbt->max);
//  write_my_data_into_file("david.log",escaped);
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
      dbt->chunks=g_list_prepend(dbt->chunks,new_integer_step(g_strdup_printf("`%s` IS NULL OR ",dbt->field), dbt->field, nmin, nmax, 0, 0));
    }else{
      dbt->chunk_type=NONE;
    } 
    break;
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
    dbt->chunk_type=CHAR;
    dbt->chunks=g_list_prepend(dbt->chunks,new_char_step(conn, dbt->field, 0, 0, row, lengths));
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
          chunks, NULL);
          //new_char_step(prefix, dbt->field, /*NULL,*/ 0, 0, dbt->cmin, dbt->cmax));
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
    g_async_queue_pop(pop_queue);
//    g_message("g_async_queue_pop(pop_queue!!!");
    dbt=NULL;
    cs=NULL;
    get_next_dbt_and_chunk(&dbt,&cs,table_list);

    if ((cs==NULL) && (dbt==NULL)){
      break;
    }
    switch (dbt->chunk_type) {
    case INTEGER:
      create_job_to_dump_chunk(dbt, NULL, cs->integer_step.number, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    case CHAR:
      create_job_to_dump_chunk(dbt, NULL, cs->char_step.number, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    case PARTITION:
      create_job_to_dump_chunk(dbt, NULL, cs->partition_step.number, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    case NONE:
      create_job_to_dump_chunk(dbt, NULL, 0, dbt->primary_key, cs, g_async_queue_push, push_queue);
      break;
    }
  }
}

void *chunk_builder_thread(struct configuration *conf){

  g_message("Starting Non-InnoDB tables");
  table_job_enqueue(give_me_another_non_innodb_chunk_step_queue, conf->non_innodb_queue, &non_innodb_table);
  g_message("Non-InnoDB tables completed");
  enqueue_shutdown_jobs(conf->non_innodb_queue);

  g_message("Starting InnoDB tables");
  table_job_enqueue(give_me_another_innodb_chunk_step_queue, conf->innodb_queue, &innodb_table);
  g_message("InnoDB tables completed");
  enqueue_shutdown_jobs(conf->innodb_queue);

  return NULL;
}

