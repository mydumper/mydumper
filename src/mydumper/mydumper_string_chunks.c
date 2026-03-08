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
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
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
#include "mydumper.h"
#include "mydumper_start_dump.h"
#include "mydumper_chunks.h"
#include "mydumper_database.h"
#include "mydumper_jobs.h"
#include "mydumper_global.h"
#include "mydumper_working_thread.h"
#include "mydumper_write.h"
#include "mydumper_string_chunks.h"
#include "mydumper_common.h"
#include "mydumper_chunks.h"

extern guint max_items_per_string_chunk;
extern guint max_time_per_select;
extern guint max_char_size;

static
void initialize_string_step(union chunk_step *cs, 
    gboolean is_step_fixed_length, 
    guint left_length, 
    gchar *str_min, gchar *str_max, 
    guint64 step, 
    gboolean check_min, gboolean check_max, 
    guint64 rows_in_explain){
  cs->string_step.step = step;
  cs->string_step.is_step_fixed_length = is_step_fixed_length;
  cs->string_step.check_max=check_max;
  cs->string_step.check_min=check_min;
  cs->string_step.rows_in_explain=rows_in_explain;
  cs->string_step.str_min=str_min;
  cs->string_step.str_max=str_max;
  cs->string_step.left_length=left_length;
  cs->string_step.cond=g_cond_new();
  cs->string_step.cond_mutex=g_mutex_new();
}

static
union chunk_step *new_string_step(gboolean is_step_fixed_length, guint left_length, gchar *str_min, gchar *str_max, guint64 step, gboolean check_min, gboolean check_max, guint64 rows_in_explain){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  initialize_string_step(cs, is_step_fixed_length, left_length, str_min, str_max, step, check_min, check_max, rows_in_explain);
  return cs;
}

void free_string_step_item(struct chunk_step_item * csi);

static
void initialize_string_step_item(struct chunk_step_item *csi, 
    gboolean include_null, GString *prefix, gchar *field, 
    guint deep, 
    gboolean is_step_fixed_length, 
    guint left_length, 
    gchar *str_min, gchar *str_max,
    guint64 step, guint64 part, 
    gboolean check_min, gboolean check_max, 
    struct chunk_step_item * next, 
    guint position, 
    gboolean multicolumn, 
    guint64 rows_in_explain){
  csi->chunk_step = new_string_step( is_step_fixed_length, left_length, str_min, str_max, step, check_min, check_max, rows_in_explain);
  csi->chunk_type=STRING;
  csi->position=position;
  csi->next=next;
  csi->status = UNASSIGNED;
  csi->chunk_functions.process = &process_string_chunk;
  csi->chunk_functions.free=&free_string_step_item;
//  csi->chunk_functions.update_where = &get_string_chunk_where;
  csi->chunk_functions.get_next = &get_next_string_chunk;
  csi->where=g_string_new("");
  csi->include_null = include_null;
  csi->prefix = prefix;
  csi->field = g_strdup(field);
  csi->mutex = g_mutex_new();
  csi->part = part;
  csi->deep = deep;
  csi->needs_refresh=FALSE;
  csi->multicolumn=multicolumn;
}

struct chunk_step_item *new_string_step_item(
    gboolean include_null, GString *prefix, gchar *field, 
    guint deep, 
    gboolean is_step_fixed_length, 
    guint left_length,
    gchar *str_min, gchar *str_max, 
    guint64 step, guint64 part,
    gboolean check_min, gboolean check_max,
    struct chunk_step_item * next,
    guint position, 
    gboolean multicolumn,
    guint64 rows_in_explain){
  struct chunk_step_item *csi = g_new0(struct chunk_step_item,1);
  initialize_string_step_item(
      csi, include_null, prefix, field, deep, is_step_fixed_length, left_length, str_min, str_max,
      step, part, check_min, check_max, next, position, 
      multicolumn, rows_in_explain);
  return csi;
}

void free_string_step(union chunk_step * cs){
  if (cs)
    g_free(cs);
}

void free_string_step_item(struct chunk_step_item * csi){
  if (csi && csi->chunk_step){
    free_string_step(csi->chunk_step);
    csi->chunk_step=NULL;
  }
  if (csi->where){
    g_string_free(csi->where, TRUE);
    csi->where=NULL;
  }
  if (csi->field){
    g_free(csi->field);
    csi->field=NULL;
  }
  if (csi->mutex){
    g_mutex_free(csi->mutex);
    csi->mutex=NULL;
  }

  // We cannot free it here, otherwise get_next_string_chunk() fails
  // g_free(csi);
}

struct chunk_step_item * split_string_chunk_step(struct chunk_step_item * csi){
  struct chunk_step_item * new_csi = NULL;
  guint64 part=csi->part;
  part+=pow(2,csi->deep);
  new_csi = new_string_step_item(FALSE, NULL, csi->field, csi->deep + 1, csi->chunk_step->string_step.is_step_fixed_length, csi->chunk_step->string_step.left_length, 
      csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max, 
      csi->chunk_step->string_step.step, part, FALSE, FALSE, NULL, csi->position, csi->multicolumn, 0);
  new_csi->status = UNASSIGNED;
  new_csi->chunk_step->string_step.str_max=csi->chunk_step->string_step.str_max;
  new_csi->chunk_step->string_step.str_min=csi->chunk_step->string_step.str_cur;

  csi->chunk_step->string_step.str_max=csi->chunk_step->string_step.str_prev_cur;

  csi->chunk_step->string_step.str_cur=csi->chunk_step->string_step.str_max;
  new_csi->chunk_step->string_step.str_cur=new_csi->chunk_step->string_step.str_max;


  csi->deep=csi->deep+1;


  return new_csi;
}


/*
static
gboolean is_last_step(struct chunk_step_item *csi){
  (void)csi;
  return TRUE;
}


static
gboolean is_splitable(struct chunk_step_item *csi){
  (void)csi;
  return FALSE;
}
*/

/*
gboolean has_only_one_level(struct chunk_step_item *csi){
return ( csi->chunk_step->string_step.is_step_fixed_length && (
(
  csi->chunk_step->string_step.is_unsigned && csi->chunk_step->string_step.type.unsign.max == csi->chunk_step->string_step.type.unsign.min
)||
(
 !csi->chunk_step->string_step.is_unsigned && csi->chunk_step->string_step.type.sign.max   == csi->chunk_step->string_step.type.sign.min
)
) );
}

static
gboolean is_splitable(struct chunk_step_item *csi){
return ( !csi->chunk_step->string_step.is_step_fixed_length  && (( csi->chunk_step->string_step.is_unsigned && (csi->chunk_step->string_step.type.unsign.cursor < csi->chunk_step->string_step.type.unsign.max
        && (
             ( csi->status == DUMPING_CHUNK && (csi->chunk_step->string_step.type.unsign.max - csi->chunk_step->string_step.type.unsign.cursor ) >= csi->chunk_step->string_step.step // As this chunk is dumping data, another thread can continue with the remaining rows
             ) ||
             ( csi->status == ASSIGNED      && (csi->chunk_step->string_step.type.unsign.max - csi->chunk_step->string_step.type.unsign.min    ) >= csi->chunk_step->string_step.step // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
       )) || ( ! csi->chunk_step->string_step.is_unsigned && (csi->chunk_step->string_step.type.sign.cursor < csi->chunk_step->string_step.type.sign.max // it is not the last chunk
        && (
             ( csi->status == DUMPING_CHUNK && gint64_abs(csi->chunk_step->string_step.type.sign.max - csi->chunk_step->string_step.type.sign.cursor) >= csi->chunk_step->string_step.step // As this chunk is dumping data, another thread can continue with the remaining rows
             ) ||
             ( csi->status == ASSIGNED      && gint64_abs(csi->chunk_step->string_step.type.sign.max - csi->chunk_step->string_step.type.sign.min)    >= csi->chunk_step->string_step.step // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
         )
)) ) || ( csi->chunk_step->string_step.is_step_fixed_length && csi->chunk_step->string_step.step > 0 && (   
(
  csi->chunk_step->string_step.is_unsigned && csi->chunk_step->string_step.type.unsign.max / csi->chunk_step->string_step.step > csi->chunk_step->string_step.type.unsign.min / csi->chunk_step->string_step.step + 1 
)||
(
 !csi->chunk_step->string_step.is_unsigned && csi->chunk_step->string_step.type.sign.max   / csi->chunk_step->string_step.step > csi->chunk_step->string_step.type.sign.min   / csi->chunk_step->string_step.step + 1
)

) );
}

static
gboolean is_last_step(struct chunk_step_item *csi){
return ( !csi->chunk_step->string_step.is_step_fixed_length  && (
      ( csi->chunk_step->string_step.is_unsigned && (csi->chunk_step->string_step.type.unsign.cursor < csi->chunk_step->string_step.type.unsign.max
        && (
             ( csi->status == DUMPING_CHUNK && (csi->chunk_step->string_step.type.unsign.max - csi->chunk_step->string_step.type.unsign.cursor ) <= csi->chunk_step->string_step.step
             )
           )
    )) || 
      ( ! csi->chunk_step->string_step.is_unsigned && (csi->chunk_step->string_step.type.sign.cursor < csi->chunk_step->string_step.type.sign.max
        && (
             ( csi->status == DUMPING_CHUNK && gint64_abs(csi->chunk_step->string_step.type.sign.max - csi->chunk_step->string_step.type.sign.cursor) <= csi->chunk_step->string_step.step
             )
           )
         )
)) ); 
  
//  || ( csi->chunk_step->string_step.is_step_fixed_length && csi->chunk_step->string_step.step > 0 && (
//(
//  csi->chunk_step->string_step.is_unsigned && csi->chunk_step->string_step.type.unsign.max / csi->chunk_step->string_step.step > csi->chunk_step->string_step.type.unsign.min / csi->chunk_step->string_step.step + 1
//)||
//(
// !csi->chunk_step->string_step.is_unsigned && csi->chunk_step->string_step.type.sign.max   / csi->chunk_step->string_step.step > csi->chunk_step->string_step.type.sign.min   / csi->chunk_step->string_step.step + 1
//)

//) );
}
*/


void update_where_on_string_step(struct chunk_step_item * csi);

struct chunk_step_item *clone_string_chunk_step_item(struct chunk_step_item *csi){
  return new_string_step_item(
      csi->include_null, csi->prefix, csi->field, csi->deep, csi->chunk_step->string_step.is_step_fixed_length, 
      csi->chunk_step->string_step.left_length, 
      csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max, csi->chunk_step->string_step.step, 
      csi->part, 
      csi->chunk_step->string_step.check_min, csi->chunk_step->string_step.check_max, NULL, csi->position, csi->multicolumn, 0);
}


// dbt->chunks_mutex is LOCKED
struct chunk_step_item *get_next_string_chunk(struct db_table *dbt){
  struct chunk_step_item *csi=NULL;

//  if (dbt->chunks!=NULL){

    csi = (struct chunk_step_item *)g_async_queue_try_pop(dbt->chunks_queue);
    trace("get_next_string_chunk"); 
    while (csi!=NULL){
      g_mutex_lock(csi->mutex);
      if (csi->status==UNASSIGNED){
        trace("get_next_string_chunk ASSIGNED");
        csi->status=ASSIGNED;
        g_async_queue_push(dbt->chunks_queue, csi);
        g_mutex_unlock(csi->mutex);
        return csi;
      }
      if (csi->status==UNSPLITTABLE){
        if (csi->next)
          g_async_queue_push(dbt->chunks_queue, csi);
        goto end;
      }
      if (csi->status==COMPLETED && csi->status==DUMPING_CHUNK){
        goto end;
      }
      
      g_mutex_lock(csi->chunk_step->string_step.cond_mutex);
      //g_mutex_unlock(dbt->chunks_mutex);
      trace("Waiting cond");
      while (csi->status == ASSIGNED){
        g_mutex_unlock(csi->mutex);
        g_cond_wait(csi->chunk_step->string_step.cond, csi->chunk_step->string_step.cond_mutex);
        g_mutex_lock(csi->mutex);
      }
      trace("Waiting cond ended");
      //g_mutex_lock(dbt->chunks_mutex);
      g_mutex_unlock(csi->chunk_step->string_step.cond_mutex);
      //
      // csi->status == ASSIGNED
      //

//      if (g_strcmp0(csi->chunk_step->string_step.str_min,csi->chunk_step->string_step.str_max)){
//        g_async_queue_push(dbt->chunks_queue, csi);
//       }



end:
      g_mutex_unlock(csi->mutex);
      csi = (struct chunk_step_item *)g_async_queue_try_pop(dbt->chunks_queue);
    }


//  }
  return csi;
}


gboolean refresh_string_min_max(MYSQL *conn, struct db_table *dbt, struct chunk_step_item *csi ){
//  struct string_step * ics = &(csi->chunk_step->string_step);
  gchar *query = NULL;
  // Get minimum/maximum 

  MYSQL_RES *minmax = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s MIN(%s%s%s),MAX(%s%s%s) FROM %s%s%s.%s%s%s%s%s",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str, identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        csi->prefix?" WHERE ":"", csi->prefix?csi->prefix->str:""), NULL, "Query to get a new min and max failed", NULL);
  trace("refresh_string_min_max: %s", query);
  g_free(query);

  if (!minmax){
    return FALSE;
  }

  MYSQL_ROW row = mysql_fetch_row(minmax);
  if (row==NULL || row[0]==NULL || row[1]==NULL){
    mysql_free_result(minmax);
    return FALSE;
  }

//    gint64 nmin = strtoll(row[0], NULL, 10);
//    gint64 nmax = strtoll(row[1], NULL, 10);
//    ics->type.sign.min = nmin;
//    ics->type.sign.max = nmax;
  csi->include_null=TRUE;
  mysql_free_result(minmax);
  return TRUE;
}
/*
static
gboolean update_string_min(MYSQL *conn, struct db_table *dbt, struct chunk_step_item *csi ){
//  struct string_step * ics = &(csi->chunk_step->string_step);
  gchar *query = NULL;

  GString *where = get_where_from_csi(csi);

  MYSQL_RES *min = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s ASC LIMIT 1",
                        is_mysql_like() ? "!40001 SQL_NO_CACHE ": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        csi->where->str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new max failed", NULL);

  g_string_free(where,TRUE);
  g_free(query);

  if (!min)
    return FALSE;

  MYSQL_ROW row = mysql_fetch_row(min);

  if (row==NULL || row[0]==NULL){
    mysql_free_result(min);
    return FALSE;
  }

//    gint64 nmin = strtoll(row[0], NULL, 10);
//    ics->type.sign.min = nmin;

  mysql_free_result(min);
  return TRUE;
}
*/

static
gboolean set_next_min(MYSQL *conn,struct db_table *dbt, struct chunk_step_item *csi){
  gchar *query = NULL;

  MYSQL_RES *max = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s LEFT(%s%s%s,%d) FROM %s%s%s.%s%s%s WHERE %s%s%s > '%s' AND %s%s%s NOT LIKE '%s%%' AND  %s%s%s < '%s' ORDER BY %s%s%s ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.left_length,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_min,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_min,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_max,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new min failed", NULL);
  trace("set_next_min: %s", query);
  g_free(query);

  if (!max)
    goto cleanup;

  MYSQL_ROW row = mysql_fetch_row(max);
  if (row==NULL || row[0]==NULL){
cleanup:
    if (max)
      mysql_free_result(max);
    return FALSE;
  }

  g_free(csi->chunk_step->string_step.str_min);
  csi->chunk_step->string_step.str_min=g_strdup(row[0]);

  mysql_free_result(max);
  return TRUE;
}

static
gboolean set_next_cur(MYSQL *conn,struct db_table *dbt, struct chunk_step_item *csi){
  gchar *query = NULL;

  MYSQL_RES *max = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s LEFT(%s%s%s,%d) FROM %s%s%s.%s%s%s WHERE %s%s%s > '%s' AND %s%s%s NOT LIKE '%s%%' AND  %s%s%s < '%s' ORDER BY %s%s%s ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.left_length,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_cur,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_cur,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_max,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new min failed", NULL);
  trace("set_next_cur: %s", query);
  g_free(query);

  if (csi->chunk_step->string_step.str_prev_cur) g_free(csi->chunk_step->string_step.str_prev_cur);
  csi->chunk_step->string_step.str_prev_cur=csi->chunk_step->string_step.str_cur;

  if (!max)
    goto cleanup;

  MYSQL_ROW row = mysql_fetch_row(max);
  if (row==NULL || row[0]==NULL){
cleanup:
    if (max)
      mysql_free_result(max);
    csi->chunk_step->string_step.str_cur=NULL;
    return FALSE;
  }
  csi->chunk_step->string_step.str_cur=g_strdup(row[0]);

  mysql_free_result(max);
  return TRUE;
}

gboolean set_prev_max(MYSQL *conn,struct db_table *dbt, struct chunk_step_item *csi){
  gchar *query = NULL;

  MYSQL_RES *max = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s LEFT(%s%s%s,1) FROM %s%s%s.%s%s%s WHERE %s%s%s < '%s' AND %s%s%s NOT LIKE '%s%%' AND  %s%s%s > '%s' ORDER BY %s%s%s DESC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_max,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_max,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_min,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new min failed", NULL);
  trace("set_prev_max: %s", query);
  g_free(query);

  if (!max)
    goto cleanup;

  MYSQL_ROW row = mysql_fetch_row(max);
  if (row==NULL || row[0]==NULL){
cleanup:
//      ics->type.sign.max = ics->type.sign.min;

    if (max)
      mysql_free_result(max);
    return FALSE;
  }

//    gint64 nmax = strtoll(row[0], NULL, 10);
//    ics->type.sign.max = nmax;

  g_free(csi->chunk_step->string_step.str_max);
  csi->chunk_step->string_step.str_max=g_strdup(row[0]);

  mysql_free_result(max);
  return TRUE;

}

static
gboolean renew_min_and_max(MYSQL *conn,struct db_table *dbt, struct chunk_step_item *csi){

  MYSQL_RES *max = m_store_result_free_query(conn, g_strdup_printf(
                        "SELECT %s LEFT(MIN(%s%s%s),%d), LEFT(MAX(%s%s%s),%d) FROM %s%s%s.%s%s%s WHERE  %s%s%s LIKE '%s%%'",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.left_length,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.left_length,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        csi->chunk_step->string_step.str_min), NULL, "Query to get a new min failed", NULL);

  if (!max)
    goto cleanup;

  MYSQL_ROW row = mysql_fetch_row(max);
  if (row==NULL || row[0]==NULL){
cleanup:
//      ics->type.sign.max = ics->type.sign.min;

    if (max)
      mysql_free_result(max);
    return FALSE;
  }

//    gint64 nmax = strtoll(row[0], NULL, 10);
//    ics->type.sign.max = nmax;

  g_free(csi->chunk_step->string_step.str_max);
  csi->chunk_step->string_step.str_min=g_strdup(row[0]);
  csi->chunk_step->string_step.str_max=g_strdup(row[1]);

  mysql_free_result(max);
  return TRUE;

}
/*
static
gboolean update_string_max(MYSQL *conn,struct db_table *dbt, struct chunk_step_item *csi ){
//  struct string_step * ics = &(csi->chunk_step->string_step);
  gchar *query = NULL;

  GString *where = get_where_from_csi(csi);

  MYSQL_RES *max = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s DESC LIMIT 1",
                        is_mysql_like() ? "!40001 SQL_NO_CACHE ": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        where->str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new max failed", NULL);
  g_string_free(where,TRUE);
  g_free(query);

  if (!max)
    goto cleanup;

  MYSQL_ROW row = mysql_fetch_row(max);
  if (row==NULL || row[0]==NULL){
cleanup:
//      ics->type.sign.max = ics->type.sign.min;

    if (max)
      mysql_free_result(max);
    return FALSE;
  }

//    gint64 nmax = strtoll(row[0], NULL, 10);
//    ics->type.sign.max = nmax;

  mysql_free_result(max);
  return TRUE;
}
*/

static
gboolean is_last(struct chunk_step_item *csi){
  g_mutex_lock(csi->mutex);
  gboolean r = TRUE;
//    csi->chunk_step->string_step.is_unsigned ?
//    csi->chunk_step->string_step.type.unsign.cursor == csi->chunk_step->string_step.type.unsign.max :
//    csi->chunk_step->string_step.type.sign.cursor   == csi->chunk_step->string_step.type.sign.max;
  g_mutex_unlock(csi->mutex);
  return r;
}

guint process_string_chunk_step(struct table_job *tj, struct chunk_step_item *csi){
  struct thread_data *td = tj->td;
  union chunk_step *cs = csi->chunk_step;

  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }

// Stage 1: Update min

// main chunk
// Tengo que calcular el min? hay situaciones en la que el chunk viene con el min que ya le corresponde a otro chunk y por lo tanto no debe ser tenido en cuenta, asi que debe ser recalculado.
  trace("Thread %d: I-Chunk 0: locking mutex", td->thread_id);
  g_mutex_lock(csi->mutex);
  trace("Thread %d: I-Chunk 0: locking mutex done", td->thread_id);
  csi->status=ASSIGNED;
  // check_min will tell us if the value of str_min is valid, if not, we need to recalculate
  if (cs->string_step.check_min /*&& tj->dbt->max_chunk_step_size!=0 && !cs->string_step.is_step_fixed_length */){
    set_next_min(td->thrconn, tj->dbt, csi);
    cs->string_step.check_min=FALSE;
    GString *_where;
    _where=get_where_from_csi(csi);
    trace("Thread %d: I-Chunk 0: Calculating rows from explain with where: %s", td->thread_id, _where->str);
    cs->string_step.rows_in_explain=get_rows_from_explain(td->thrconn, tj->dbt, _where, csi->field);
    g_string_free(_where, TRUE);
  }


// Stage 1: Can I split current level based in the amount of rows?
//  g_mutex_lock(csi->mutex);


// puedo hacerlo yo?
// check if step es mayor al explain
// si puedo, lo hago como un range string
// si no puedo  porque el step es menor al explain, que significa que el chunk se debe partir debo:
//    partir el chunk y ejectuarlo
//    partir el chunk implica count expalin primer caracter
//    check if step es mayor al explain
//    si es menor, entonces agarro otro caracter y ya seria un range string chunk, continuo iterando agarrando char hasta llegar al step, contar tambien remaining rows en explain, esta iteracion termina cuando el ultimo genera que sea mayor, este char va a ser el proximo str_min, que dicho y sea de paso, lo podria crear y encolar, total? si hay alguien esperando lo puede agarrar  || TENGO EL NEXT MIN
//    si es mayor, entonces tengo que ir deep (un char mas en el length) y creo el siguiente chunk clonando el actual a partir del siguiente char que va a ser calculado despues, tengo explain asi que lo guardo y tambien calculo el explain de lo queda, y encolo || NO TENGO EL NEXT MIN
//


  
retry_split_chunk:
//  csi->chunk_step->string_step.str_cur=g_strdup(csi->chunk_step->string_step.str_min);
//  csi->chunk_step->string_step.str_prev_cur=NULL;

  trace("Thread %d: I-Chunk 0: Initiaing process of CSI ['%s','%s'] Est. Rows: %d", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max, cs->string_step.rows_in_explain);

  if (cs->string_step.step > cs->string_step.rows_in_explain){
    csi->chunk_step->string_step.str_cur=csi->chunk_step->string_step.str_max;
    goto execute_string_chunk;
  }

  if (!g_strcmp0(csi->chunk_step->string_step.str_min,csi->chunk_step->string_step.str_max)){
    // str_min == str_max . Can I go deeper?
    if (csi->chunk_step->string_step.left_length >= max_char_size ){
      csi->chunk_step->string_step.str_cur=csi->chunk_step->string_step.str_max;
      goto execute_string_chunk;
    } 
    csi->chunk_step->string_step.left_length++;
    renew_min_and_max(td->thrconn, tj->dbt, csi);
    goto retry_split_chunk;
  }
  
  // range string



//max_items_per_string_chunk

  trace("Thread %d: I-Chunk 0: Should be splited ['%s','%s'] Step %d < %d rows reported in explain", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max, cs->string_step.step, cs->string_step.rows_in_explain);
  guint64 prev_rows_in_explain=0,rows_in_explain=0;
  guint i=0;

    
  csi->chunk_step->string_step.str_cur=g_strdup(csi->chunk_step->string_step.str_min);
  do {
    get_where_from_csi(csi);
    trace("Thread %d: I-Chunk 0: Calculating rows from explain ['%s','%s']. Where: %s", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max, csi->where->str);
    prev_rows_in_explain=rows_in_explain;
    rows_in_explain=get_rows_from_explain(td->thrconn, tj->dbt, csi->where, csi->field);
    set_next_cur(td->thrconn, tj->dbt, csi);
    i++;

  } while ( i < max_items_per_string_chunk &&
            cs->string_step.step > rows_in_explain  && 
            csi->chunk_step->string_step.str_cur
//          remaining_rows > 0
            );
  csi->chunk_step->string_step.rows_in_explain=prev_rows_in_explain;
  if (!g_strcmp0(csi->chunk_step->string_step.str_max,csi->chunk_step->string_step.str_cur)){
    goto execute_string_chunk;
  }
  g_assert(csi->chunk_step->string_step.str_prev_cur);
  if (!csi->chunk_step->string_step.str_prev_cur){
    trace("Thread %d: I-Chunk 2: as str_prev_cur is null using min: %s", td->thread_id, csi->chunk_step->string_step.str_min);
    csi->chunk_step->string_step.str_prev_cur=g_strdup(csi->chunk_step->string_step.str_min);
  }
  if (!csi->chunk_step->string_step.str_cur){
    trace("Thread %d: I-Chunk 2: as str_cur is null using max: %s", td->thread_id, csi->chunk_step->string_step.str_max);
    csi->chunk_step->string_step.str_cur=g_strdup(csi->chunk_step->string_step.str_max);
  }
  trace("Thread %d: I-Chunk 2: Cloning into ['%s','%s'] ['%s','%s']", 
      td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_prev_cur, 
      csi->chunk_step->string_step.str_cur, csi->chunk_step->string_step.str_max);
  struct chunk_step_item *new_csi=split_string_chunk_step(csi);
  get_where_from_csi(new_csi);
  new_csi->chunk_step->string_step.rows_in_explain=get_rows_from_explain(td->thrconn, tj->dbt, new_csi->where, new_csi->field);
  trace("Thread %d: I-Chunk 2: Exe CSI ['%s','%s']", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max);
  trace("Thread %d: I-Chunk 2: New CSI ['%s','%s'] with estimated rows: %d", td->thread_id, new_csi->chunk_step->string_step.str_min, new_csi->chunk_step->string_step.str_max, new_csi->chunk_step->string_step.rows_in_explain);
  g_async_queue_push(tj->dbt->chunks_queue, new_csi);
  goto execute_string_chunk;


/*      if (cs->string_step.rows_in_explain > _rows_in_explain)
        remaining_rows=cs->string_step.rows_in_explain - _rows_in_explain;
      else 
        remaining_rows=0;
      trace("Thread %d: I-Chunk 0: Calculated rows %"G_GUINT64_FORMAT" and remaining rows: %d", td->thread_id, _rows_in_explain, remaining_rows);
      g_string_free(_where, TRUE);
      csi->chunk_step->string_step.str_prev_cur=csi->chunk_step->string_step.str_cur;
      set_next_cur(td->thrconn, tj->dbt, csi);
      if (!csi->chunk_step->string_step.str_cur)
        csi->chunk_step->string_step.str_cur=csi->chunk_step->string_step.str_max;
      trace("Thread %d: I-Chunk 0: Subsets ['%s','%s'] ['%s','%s']", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_prev_cur, csi->chunk_step->string_step.str_cur, csi->chunk_step->string_step.str_max);
      i++;
    }
    csi->chunk_step->string_step.rows_in_explain=prev_rows_in_explain;

    if (!csi->chunk_step->string_step.str_cur){
      csi->chunk_step->string_step.str_cur=csi->chunk_step->string_step.str_max;
      goto process;
    }

    if (!g_strcmp0(csi->chunk_step->string_step.str_min,csi->chunk_step->string_step.str_prev_cur) && g_strcmp0(csi->chunk_step->string_step.str_min,csi->chunk_step->string_step.str_cur) && g_strcmp0(csi->chunk_step->string_step.str_cur, csi->chunk_step->string_step.str_max)){
      // MIN == CUR
      csi->chunk_step->string_step.rows_in_explain=_rows_in_explain;
      trace("Thread %d: I-Chunk 1: Cloning due MIN == CUR into ['%s','%s'] ['%s','%s']", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_prev_cur, csi->chunk_step->string_step.str_cur, csi->chunk_step->string_step.str_max);
      struct chunk_step_item *new_csi=split_string_chunk_step(csi);
      if (!new_csi->chunk_step->string_step.str_min) new_csi->chunk_step->string_step.str_min=new_csi->chunk_step->string_step.str_max;
      if (!remaining_rows){
        new_csi->chunk_step->string_step.str_cur=new_csi->chunk_step->string_step.str_max;
        _where=get_where_from_csi(new_csi);
        remaining_rows=get_rows_from_explain(td->thrconn, tj->dbt, _where, new_csi->field);
        g_string_free(_where, TRUE);
      }
      new_csi->chunk_step->string_step.rows_in_explain=remaining_rows;
      trace("Thread %d: I-Chunk 1: Exe CSI ['%s','%s'] Est. Rows: %d", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max, csi->chunk_step->string_step.rows_in_explain);
      trace("Thread %d: I-Chunk 1: New CSI ['%s','%s'] Est. Rows: %d", td->thread_id, new_csi->chunk_step->string_step.str_min, new_csi->chunk_step->string_step.str_max, new_csi->chunk_step->string_step.rows_in_explain);
//      m_error("MIN == CUR this shouldn't happen");
      g_async_queue_push(tj->dbt->chunks_queue, new_csi);
      if (!csi->chunk_step->string_step.rows_in_explain){
        _where=get_where_from_csi(csi);
        csi->chunk_step->string_step.rows_in_explain=get_rows_from_explain(td->thrconn, tj->dbt, _where, csi->field);
        trace("Thread %d: I-Chunk 1: We calculated rows %"G_GUINT64_FORMAT" and the step is %d for %s | Left_lenght %d", td->thread_id, csi->chunk_step->string_step.rows_in_explain, cs->string_step.step, _where->str, csi->chunk_step->string_step.left_length);
        g_string_free(_where, TRUE);
      }

      if (((2*cs->string_step.step) < csi->chunk_step->string_step.rows_in_explain) && (csi->chunk_step->string_step.left_length < 2)){
        trace("Trying to split");
        csi->chunk_step->string_step.left_length++;
        renew_min_and_max(td->thrconn, tj->dbt, csi);
        goto retry_split_chunk;
      }

    }
    if (!g_strcmp0(csi->chunk_step->string_step.str_max,csi->chunk_step->string_step.str_cur)){
      // MAX == CUR
      goto process;
    }
    if (i>0){
      trace("Thread %d: I-Chunk 2: Cloning into ['%s','%s'] ['%s','%s']", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_prev_cur, csi->chunk_step->string_step.str_cur, csi->chunk_step->string_step.str_max);
      struct chunk_step_item *new_csi=split_string_chunk_step(csi);
      _where=get_where_from_csi(new_csi);
      remaining_rows=get_rows_from_explain(td->thrconn, tj->dbt, _where, new_csi->field);
      new_csi->chunk_step->string_step.rows_in_explain=remaining_rows;
      trace("Thread %d: I-Chunk 2: Exe CSI ['%s','%s']", td->thread_id, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_max);
      trace("Thread %d: I-Chunk 2: New CSI ['%s','%s'] with estimated rows: %d", td->thread_id, new_csi->chunk_step->string_step.str_min, new_csi->chunk_step->string_step.str_max, new_csi->chunk_step->string_step.rows_in_explain);
      g_async_queue_push(tj->dbt->chunks_queue, new_csi);
//      g_mutex_lock(tj->dbt->chunks_mutex);
//      tj->dbt->chunks=g_list_prepend(tj->dbt->chunks,new_csi);
//      g_mutex_unlock(tj->dbt->chunks_mutex);

    }else{
      trace("Thread %d: I-Chunk 0: do not spliting", td->thread_id);
    
    }
    }
  }else{
    csi->chunk_step->string_step.str_cur=csi->chunk_step->string_step.str_max;
    trace("Thread %d: I-Chunk 0: do not spliting %s | %s | %s | %d | %d", td->thread_id, csi->chunk_step->string_step.str_min,csi->chunk_step->string_step.str_cur,csi->chunk_step->string_step.str_max,
      cs->string_step.step, cs->string_step.rows_in_explain);
  
  }
*/
execute_string_chunk:
  // Let's process the chunk

//  g_mutex_unlock(csi->mutex);

// Stage 1: Update min and max if needed

//  g_mutex_lock(csi->mutex);
//  if (tj->status == COMPLETED)
//    m_critical("Thread %d: Trying to process COMPLETED chunk",td->thread_id);
  csi->status = DUMPING_CHUNK;
  g_mutex_lock(csi->chunk_step->string_step.cond_mutex);
  trace("Thread %d: Signallinging cond", td->thread_id);
  g_cond_signal(csi->chunk_step->string_step.cond);
  g_mutex_unlock(csi->chunk_step->string_step.cond_mutex);
//  gboolean c_min=TRUE, c_max=TRUE;



// Stage 2: Setting cursor
  if (tj->dbt->multicolumn && csi->multicolumn && csi->next == NULL && !cs->string_step.is_step_fixed_length){
    guint64 string_step_step=cs->string_step.step;
retry:
  // We are setting cursor to build the WHERE clause for the EXPLAIN
/*      if (string_step_step > cs->string_step.type.unsign.max - cs->string_step.type.unsign.min + 1 )
        cs->string_step.type.unsign.cursor = cs->string_step.type.unsign.max;
      else
        cs->string_step.type.unsign.cursor = cs->string_step.type.unsign.min + string_step_step - 1;*/
    trace("Thread %d: I-Chunk 2: cs->string_step.type.unsign.cursor:", td->thread_id);
    update_where_on_string_step(csi);
    trace("Thread %d: I-Chunk 1: Calculating rows", td->thread_id);
    guint64 rows = check_row_count?
                   get_rows_from_count  (td->thrconn, tj->dbt, csi->where):
                   get_rows_from_explain(td->thrconn, tj->dbt, csi->where, csi->field);
    trace("Thread %d: I-Chunk 2: multicolumn and next == NULL with rows: %lld", td->thread_id, rows);

//    guint64 tmpstep = csi->chunk_step->string_step.is_unsigned?
//                 csi->chunk_step->string_step.type.unsign.cursor - csi->chunk_step->string_step.type.unsign.min:
//      gint64_abs(  csi->chunk_step->string_step.type.sign.cursor -   csi->chunk_step->string_step.type.sign.min);
//    tmpstep++;
//    string_step_step=string_step_step>tmpstep?tmpstep:string_step_step;

    if (string_step_step>1){
      // rows / num_threads > string_step_step
      if (rows > tj->dbt->min_chunk_step_size && ( rows > cs->string_step.step || (tj->num_rows_of_last_run>0 &&rows/100 > tj->num_rows_of_last_run))){
        trace("Thread %d: I-Chunk 2: string_step.step>1 then retrying", td->thread_id);
        string_step_step=string_step_step/2;
        goto retry;
      }
      trace("Thread %d: I-Chunk 2: string_step.step>1 not retrying as rows %lld <=  step %lld and string_step_step: %lld", td->thread_id, rows, cs->string_step.step, string_step_step);
      cs->string_step.step=string_step_step;
    }else{
      // at this poing cs->string_step.step == 1 always
      cs->string_step.step=1;
      if (rows > tj->dbt->min_chunk_step_size){
        csi->next = initialize_chunk_step_item(td->thrconn, tj->dbt, csi->position + 1, rows, csi->where);
        if (csi->next){
          csi->next->multicolumn=FALSE;
          trace("Thread %d: I-Chunk 2: New next with where %s | rows: %lld", td->thread_id, csi->where->str, rows);
        }
      }else{
        trace("Thread %d: I-Chunk 2: multicolumn=FALSE", td->thread_id);
        csi->multicolumn=FALSE;
      }
    }
  }

/*
    if (cs->string_step.step > cs->string_step.type.unsign.max - cs->string_step.type.unsign.min + 1 )
      cs->string_step.type.unsign.cursor = cs->string_step.type.unsign.max;
    else
      cs->string_step.type.unsign.cursor = cs->string_step.type.unsign.min + cs->string_step.step - 1;

    if (cs->string_step.type.unsign.cursor < cs->string_step.type.unsign.min)
      g_error("Thread %d: string_step.type.unsign.cursor: %"G_GUINT64_FORMAT"  | string_step.type.unsign.min %"G_GUINT64_FORMAT"  | cs->string_step.type.unsign.max : %"G_GUINT64_FORMAT" | cs->string_step.step %"G_GUINT64_FORMAT, td->thread_id, cs->string_step.type.unsign.cursor, cs->string_step.type.unsign.min, cs->string_step.type.unsign.max, cs->string_step.step);

    g_assert(cs->string_step.type.unsign.cursor >= cs->string_step.type.unsign.min);

    cs->string_step.estimated_remaining_steps=cs->string_step.step>0?(cs->string_step.type.unsign.max - cs->string_step.type.unsign.cursor) / cs->string_step.step:1;
    */


  if (csi->next !=NULL && csi->status==UNSPLITTABLE){
    // Could be possible that in previous iteration on a multicolumn table, the status changed ot UNSPLITTABLE, but on next iteration could be possible
    // to splittable, that is why we need to change back to ASSIGNED
    csi->status=ASSIGNED; 
  }

  g_mutex_unlock(csi->mutex);

  update_estimated_remaining_chunks_on_dbt(tj->dbt);

// Step 3: Executing query and writing data
  update_where_on_string_step(csi);
  if (csi->prefix)
    trace("Thread %d: I-Chunk 3: PREFIX: %s WHERE: %s", td->thread_id, csi->prefix->str, csi->where->str);
  else
    trace("Thread %d: I-Chunk 3: WHERE: %s", td->thread_id, csi->where->str); 

  if (csi->next !=NULL){
    trace("Thread %d: I-Chunk 3: going down", td->thread_id);
    // Multi column
    if (csi->next->needs_refresh)
      if (!refresh_string_min_max(td->thrconn, tj->dbt, csi->next)){
        trace("Thread %d: I-Chunk 3: No min and max found", td->thread_id);
        goto update_min;
      }

    csi->next->chunk_functions.process( tj , csi->next);
    csi->next->needs_refresh=TRUE;
  }else{
    g_string_set_size(tj->where,0);
    g_string_append(tj->where, csi->where->str);
    trace("Thread %d: I-Chunk 3: WHERE in TJ: %s", td->thread_id, tj->where->str);
    if (cs->string_step.is_step_fixed_length) {
      // tj->part= cs->string_step.type.sign.min   / cs->string_step.step + 1;      
      close_files(tj);
      write_table_job_into_file(tj);
    }else if (is_last(csi)) {
      trace("Thread %d: I-Chunk 3: Last chunk on `%s`.`%s` no need to calculate anything else after finish", td->thread_id, tj->dbt->database->source_database, tj->dbt->table);
      write_table_job_into_file(tj);
    }else{
      GDateTime *from = g_date_time_new_now_local();
      write_table_job_into_file(tj);
      GDateTime *to = g_date_time_new_now_local();

// Step 3.1: Updating Step length

      GTimeSpan diff=g_date_time_difference(to,from);
      g_date_time_unref(from);
      g_date_time_unref(to);
      g_mutex_lock(csi->mutex);


// Let's calculate last run
//
// and we also going to calculate the average
//
// if last_run is above, then we use it
// if it is not, we use the average
//      cs->string_step.rows_in_explain
//      tj->num_rows_of_last_run

      if (cs->string_step.rows_in_explain > tj->num_rows_of_last_run )
        cs->string_step.rows_in_explain-=tj->num_rows_of_last_run;
      else
        cs->string_step.rows_in_explain=0;

      if (diff>0 && tj->num_rows_of_last_run>0){
        cs->string_step.step=tj->num_rows_of_last_run*max_time_per_select*G_TIME_SPAN_SECOND/diff;
        trace("Thread %d: I-Chunk 3: Step size on `%s`.`%s` is %ld  ( %ld %ld)", td->thread_id, tj->dbt->database->source_database, tj->dbt->table, cs->string_step.step, tj->num_rows_of_last_run, diff);
      }else{
        cs->string_step.step*=2;
        cs->string_step.check_min=TRUE;
        trace("Thread %d: I-Chunk 3: During last query we get zero rows, duplicating the step size to %ld", td->thread_id, cs->string_step.step);
      }


/*
      cs->string_step.step = csi->chunk_step->string_step.max_chunk_step_size !=0 && cs->string_step.step > csi->chunk_step->string_step.max_chunk_step_size ? 
                              csi->chunk_step->string_step.max_chunk_step_size :
                              cs->string_step.step;

      cs->string_step.step = csi->chunk_step->string_step.min_chunk_step_size !=0 && cs->string_step.step < csi->chunk_step->string_step.min_chunk_step_size ?
                              csi->chunk_step->string_step.min_chunk_step_size :
                              cs->string_step.step;
*/

//      trace("After checking: %ld == %ld | max_string_chunk_step_size=%ld | min_string_chunk_step_size=%ld", ant, cs->string_step.step, max_string_chunk_step_size, min_string_chunk_step_size);
      g_mutex_unlock(csi->mutex);
    }
  }

// Step 5: Updating min
update_min:
  g_mutex_lock(csi->mutex);
  if (csi->status != COMPLETED)
    csi->status = ASSIGNED;
/*
    if ( cs->string_step.type.unsign.cursor+1 < cs->string_step.type.unsign.min){
      // Overflow
      trace("Thread %d: I-Chunk 5: Overflow due string_step.type.unsign.cursor: %"G_GUINT64_FORMAT"  | string_step.type.unsign.min %"G_GUINT64_FORMAT, td->thread_id, cs->string_step.type.unsign.cursor, cs->string_step.type.unsign.min);
      cs->string_step.type.unsign.min=cs->string_step.type.unsign.max;
      cs->string_step.type.unsign.max--;
    }else
      cs->string_step.type.unsign.min=cs->string_step.type.unsign.cursor+1;
    */

//  trace("Thread %d: I-Chunk 5: string_step.type.sign.cursor: %"G_GINT64_FORMAT"  | string_step.type.sign.min %"G_GINT64_FORMAT"  | cs->string_step.type.sign.max : %"G_GINT64_FORMAT" | cs->string_step.step %ld | tmpstep: %d", td->thread_id, cs->string_step.type.sign.cursor, cs->string_step.type.sign.min, cs->string_step.type.sign.max, cs->string_step.step, tmpstep);
//  cs->string_step.step=cs->string_step.step>tmpstep?tmpstep:cs->string_step.step;

//  g_message("Thread %d: I-Chunk 5: string_step.type.sign.cursor: %"G_GINT64_FORMAT"  | string_step.type.sign.min %"G_GINT64_FORMAT"  | cs->string_step.type.sign.max : %"G_GINT64_FORMAT" | cs->string_step.step %ld", td->thread_id, cs->string_step.type.sign.cursor, cs->string_step.type.sign.min, cs->string_step.type.sign.max, cs->string_step.step);


//  trace("Thread %d: I-Chunk 5: string_step current min %s", td->thread_id, csi->chunk_step->string_step.str_min);
//  set_next_min(td->thrconn, tj->dbt, csi);
//  trace("Thread %d: I-Chunk 5: string_step next min %s", td->thread_id, csi->chunk_step->string_step.str_min);




//end_process:

  if (csi->position==0)
    csi->multicolumn=tj->dbt->multicolumn;

  if (csi->next!=NULL){
    free_string_step_item(csi->next);
    csi->next=NULL;
  }
  g_mutex_unlock(csi->mutex);
  return 0;
}

void process_string_chunk(struct table_job *tj, struct chunk_step_item *csi){
  struct thread_data *td = tj->td;
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = csi->chunk_step;
//  gboolean multicolumn_process=FALSE;


  // First step, we need this to process the one time prefix
  g_string_set_size(tj->where,0);
  if (process_string_chunk_step(tj, csi)){
    g_message("Thread %d: Job has been cacelled",td->thread_id);
    return;
  }
  g_atomic_int_inc(dbt->chunks_completed);
//  if (csi->prefix)
//    g_free(csi->prefix);
//  csi->prefix=NULL;
  csi->include_null=FALSE;

  // Processing the remaining steps
  g_mutex_lock(csi->mutex);
    // Remaining unsigned steps
//g_message("cs->string_step.type.unsign.min: %"G_GUINT64_FORMAT" | cs->string_step.type.unsign.max: %"G_GUINT64_FORMAT, cs->string_step.type.unsign.min, cs->string_step.type.unsign.max);

  if (csi->position==0)
    cs->string_step.estimated_remaining_steps=0;
  csi->status=COMPLETED;
  g_mutex_unlock(csi->mutex);

}

void update_string_where_on_gstring(GString *where, gboolean include_null, GString *prefix, gchar * field, gchar *str_min, gchar *str_max){
  if (prefix && prefix->len>0){
//    g_message("update_string_where_on_gstring:: Prefix: %s", prefix->str);
    g_string_append_printf(where,"(%s AND ",
                          prefix->str);
  }
  if (include_null){
//    g_message("update_string_where_on_gstring:: with_null");
    g_string_append_printf(where,"(%s%s%s IS NULL OR", identifier_quote_character_str, field, identifier_quote_character_str);
  }
  g_string_append(where,"(");

  if (!g_strcmp0(str_min,str_max))
    g_string_append_printf(where, "%s%s%s LIKE '%s%%'",identifier_quote_character_str, field, identifier_quote_character_str, str_min);
  else
    g_string_append_printf(where, "(%s%s%s >= '%s' AND %s%s%s <= '%s') OR %s%s%s LIKE '%s%%'",
        identifier_quote_character_str, field, identifier_quote_character_str, str_min, 
        identifier_quote_character_str, field, identifier_quote_character_str, str_max,
        identifier_quote_character_str, field, identifier_quote_character_str, str_max);
  if (include_null)
    g_string_append(where,")");
  g_string_append(where,")");
  if (prefix && prefix->len>0)
    g_string_append(where,")");
//  g_message("update_string_where_on_gstring:: where = |%s|", where->str);
}

void update_where_on_string_step(struct chunk_step_item * csi){
  g_string_set_size(csi->where,0);
  update_string_where_on_gstring(csi->where, csi->include_null, csi->prefix, csi->field, csi->chunk_step->string_step.str_min, csi->chunk_step->string_step.str_cur);
}

void determine_if_we_can_go_deeper_in_string_chunk_step_item( struct chunk_step_item * csi, guint64 rows){
  (void) rows;
  if (csi->multicolumn && csi->position == 0){
//    trace("is_unsigned: %d | rows: %lld | max - min: %lld", csi->chunk_step->string_step.is_unsigned, rows, csi->chunk_step->string_step.is_unsigned?(csi->chunk_step->string_step.type.unsign.max - csi->chunk_step->string_step.type.unsign.min):gint64_abs(csi->chunk_step->string_step.type.sign.max -   csi->chunk_step->string_step.type.sign.min));

    /*
    if (
        // In a multi column table, we will use the first column to split the table.
        // This calculation will let us know how many rows are we getting on average per first column value
        // we need to have have at least 1 chunk size per first column to perform multi column spliting
        ( csi->chunk_step->string_step.is_unsigned && (rows /         (csi->chunk_step->string_step.type.unsign.max - csi->chunk_step->string_step.type.unsign.min) > 1))||
        (!csi->chunk_step->string_step.is_unsigned && (rows / gint64_abs(csi->chunk_step->string_step.type.sign.max -   csi->chunk_step->string_step.type.sign.min) > 1))
       ){
      csi->chunk_step->string_step.min_chunk_step_size=1;
      csi->chunk_step->string_step.is_step_fixed_length=TRUE;
      csi->chunk_step->string_step.max_chunk_step_size=1;
      csi->chunk_step->string_step.step=1;
    }else */
      csi->multicolumn=FALSE;
     
  }
}
