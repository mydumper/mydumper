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
#include "mydumper_integer_chunks.h"
#include "mydumper_common.h"


guint64 min_integer_chunk_step_size=0;
guint64 max_integer_chunk_step_size=0;

guint max_time_per_select=MAX_TIME_PER_QUERY;

guint64 gint64_abs(gint64 a){
  if (a >= 0)
    return a;
  return -a;
}

static
GString * get_where_from_csi(struct chunk_step_item * csi);

static
void initialize_integer_step(union chunk_step *cs, gboolean is_unsigned, union type type, gboolean is_step_fixed_length, guint64 step, guint64 min_css, guint64 max_css, gboolean check_min, gboolean check_max, guint64 rows_in_explain){
  cs->integer_step.is_unsigned = is_unsigned;
  cs->integer_step.min_chunk_step_size = min_css;
  cs->integer_step.max_chunk_step_size = max_css;
  if (cs->integer_step.is_unsigned){
    cs->integer_step.type.unsign.min = type.unsign.min;
    cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.min;
    cs->integer_step.type.unsign.max = type.unsign.max;
    cs->integer_step.step = step;
    cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min) / cs->integer_step.step;
  }else{
    cs->integer_step.type.sign.min = type.sign.min;
    cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.min;
    cs->integer_step.type.sign.max = type.sign.max;
    cs->integer_step.step = step;
    cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min) / cs->integer_step.step;
  }
  cs->integer_step.is_step_fixed_length = is_step_fixed_length;
  cs->integer_step.check_max=check_max;
  cs->integer_step.check_min=check_min;
  cs->integer_step.rows_in_explain=rows_in_explain;
}

static
union chunk_step *new_integer_step(gboolean is_unsigned, union type type, gboolean is_step_fixed_length, guint64 step, guint64 min_css, guint64 max_css, gboolean check_min, gboolean check_max, guint64 rows_in_explain){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  initialize_integer_step(cs,is_unsigned, type, is_step_fixed_length, step, min_css, max_css, check_min, check_max, rows_in_explain);
  return cs;
}

void free_integer_step_item(struct chunk_step_item * csi);

static
void initialize_integer_step_item(struct chunk_step_item *csi, gboolean include_null, GString *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, gboolean is_step_fixed_length, guint64 step, guint64 min_css, guint64 max_css, guint64 part, gboolean check_min, gboolean check_max, struct chunk_step_item * next, guint position, gboolean multicolumn, guint64 rows_in_explain){
  csi->chunk_step = new_integer_step(is_unsigned, type, is_step_fixed_length, step, min_css, max_css, check_min, check_max, rows_in_explain);
  csi->chunk_type=INTEGER;
  csi->position=position;
  csi->next=next;
  csi->status = UNASSIGNED;
  csi->chunk_functions.process = &process_integer_chunk;
  csi->chunk_functions.free=&free_integer_step_item;
//  csi->chunk_functions.update_where = &get_integer_chunk_where;
  csi->chunk_functions.get_next = &get_next_integer_chunk;
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

struct chunk_step_item *new_integer_step_item(gboolean include_null, GString *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, gboolean is_step_fixed_length, guint64 step, guint64 min_css, guint64 max_css, guint64 part, gboolean check_min, gboolean check_max, struct chunk_step_item * next, guint position, gboolean multicolumn, guint64 rows_in_explain){
  struct chunk_step_item *csi = g_new0(struct chunk_step_item,1);
  initialize_integer_step_item(csi, include_null, prefix, field, is_unsigned, type, deep, is_step_fixed_length, step, min_css, max_css, part, check_min, check_max, next, position, multicolumn, rows_in_explain);
  return csi;
}



void free_integer_step(union chunk_step * cs){
  if (cs)
    g_free(cs);
}

void free_integer_step_item(struct chunk_step_item * csi){
  if (csi && csi->chunk_step){
    free_integer_step(csi->chunk_step);
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

  // We cannot free it here, otherwise get_next_integer_chunk() fails
  // g_free(csi);
}

void print_type(union type * type, gboolean is_unsigned){
  if (is_unsigned)
    g_message("new_integer_step_item: min: %"G_GUINT64_FORMAT "| max: %"G_GINT64_FORMAT, type->sign.min,type->sign.max);
  else
    g_message("new_integer_step_item: min: %"G_GINT64_FORMAT "| max: %"G_GINT64_FORMAT, type->sign.min,type->sign.max);
}

struct chunk_step_item * split_chunk_step(struct chunk_step_item * csi){
  struct chunk_step_item * new_csi = NULL;
  guint64 part=csi->part;
  gint64 new_minmax_signed = 0;
  guint64 new_minmax_unsigned = 0;
  union type type;
  struct integer_step *ics=&(csi->chunk_step->integer_step);
  if (ics->is_unsigned){
    type.unsign.max = ics->type.unsign.max;
    if (csi->status == DUMPING_CHUNK)
      type.unsign.min=ics->type.unsign.cursor;
    else
      type.unsign.min=ics->type.unsign.min;
  }else{
    type.sign.max   = ics->type.sign.max;
    if (csi->status == DUMPING_CHUNK)
      type.sign.min=ics->type.sign.cursor;
    else
      type.sign.min=ics->type.sign.min;
  }

  if ( ics->is_step_fixed_length ){
    if (ics->is_unsigned){
      new_minmax_unsigned = (type.unsign.min/ics->step)*ics->step +          ics->step    *
                  (((( ics->type.unsign.max /          ics->step )  -
                      (     type.unsign.min /          ics->step )) / 2 ) + 1);

      if ((type.unsign.min / ics->step) == (new_minmax_unsigned / ics->step))
        return NULL;

      if (new_minmax_unsigned == type.unsign.min)
        return NULL;
      
      type.unsign.min = new_minmax_unsigned;
      part= type.unsign.min / csi->chunk_step->integer_step.step + 1;
    }else{
      new_minmax_signed   =   (type.sign.min/ics->step)*ics->step + (signed) ics->step    *
                  ((((   ics->type.sign.max / (signed) ics->step )  -
                     (        type.sign.min / (signed) ics->step )) / 2 ) + 1);
      if ((type.sign.min / ics->step) == (new_minmax_signed / ics->step))
        return NULL;
      if (new_minmax_signed == type.sign.min)
        return NULL;
//      trace("Signed Chunk split like this: min: %"G_GINT64_FORMAT " | Mid: %"G_GINT64_FORMAT" | max: %"G_GINT64_FORMAT, type.sign.min, new_minmax_signed, type.sign.max);
      type.sign.min = new_minmax_signed;
      part= type.sign.min / csi->chunk_step->integer_step.step + 1;
    }
  }else{
    part+=pow(2,csi->deep);
    if (ics->is_unsigned){
      new_minmax_unsigned = type.unsign.min + ics->type.unsign.max/2 - type.unsign.min/2 ;
      if ( new_minmax_unsigned == type.unsign.min )
        new_minmax_unsigned++;
      type.unsign.min = new_minmax_unsigned;
    }else{
      new_minmax_signed = type.sign.min   + ics->type.sign.max/2   - type.sign.min/2 ;
      if ( new_minmax_signed == type.sign.min   )
        new_minmax_signed++;
      type.sign.min = new_minmax_signed;
      trace("Signed Chunk split like this: min: %"G_GINT64_FORMAT " | Mid: %"G_GINT64_FORMAT" | max: %"G_GINT64_FORMAT, type.sign.min, new_minmax_signed, type.sign.max);
    }
  }
  // print_type(&type, ics->is_unsigned);
  new_csi = new_integer_step_item(FALSE, NULL, csi->field, csi->chunk_step->integer_step.is_unsigned, type, csi->deep + 1, csi->chunk_step->integer_step.is_step_fixed_length, csi->chunk_step->integer_step.step, csi->chunk_step->integer_step.min_chunk_step_size, csi->chunk_step->integer_step.max_chunk_step_size, part, TRUE, TRUE /*csi->chunk_step->integer_step.check_max*/, NULL, csi->position, csi->multicolumn, 0);
  new_csi->status=ASSIGNED;

  csi->chunk_step->integer_step.check_max=TRUE;
  if (ics->is_unsigned){
//    csi->chunk_step->integer_step.rows_in_explain = csi->chunk_step->integer_step.rows_in_explain * gint64_abs( new_minmax_unsigned - 1 - csi->chunk_step->integer_step.type.unsign.min ) / gint64_abs(csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.min);
//    new_csi->chunk_step->integer_step.rows_in_explain = csi->chunk_step->integer_step.rows_in_explain * gint64_abs( new_csi->chunk_step->integer_step.type.unsign.max - new_minmax_unsigned - 1 ) / gint64_abs(new_csi->chunk_step->integer_step.type.unsign.max - new_csi->chunk_step->integer_step.type.unsign.min);

    csi->chunk_step->integer_step.type.unsign.max = new_minmax_unsigned - 1;
  }
  else{
//    csi->chunk_step->integer_step.rows_in_explain = csi->chunk_step->integer_step.rows_in_explain * ( new_minmax_signed - 1 - csi->chunk_step->integer_step.type.sign.min ) / (csi->chunk_step->integer_step.type.sign.max - csi->chunk_step->integer_step.type.sign.min);
//    new_csi->chunk_step->integer_step.rows_in_explain = csi->chunk_step->integer_step.rows_in_explain * gint64_abs( new_csi->chunk_step->integer_step.type.sign.max - new_minmax_signed - 1 ) / gint64_abs(new_csi->chunk_step->integer_step.type.sign.max - new_csi->chunk_step->integer_step.type.sign.min);

    csi->chunk_step->integer_step.type.sign.max = new_minmax_signed - 1;
  }
  csi->deep=csi->deep+1;


  return new_csi;
}


gboolean has_only_one_level(struct chunk_step_item *csi){
return ( csi->chunk_step->integer_step.is_step_fixed_length && (
(
  csi->chunk_step->integer_step.is_unsigned && csi->chunk_step->integer_step.type.unsign.max == csi->chunk_step->integer_step.type.unsign.min
)||
(
 !csi->chunk_step->integer_step.is_unsigned && csi->chunk_step->integer_step.type.sign.max   == csi->chunk_step->integer_step.type.sign.min
)
) );
}

static
gboolean is_splitable(struct chunk_step_item *csi){
return ( !csi->chunk_step->integer_step.is_step_fixed_length  && (( csi->chunk_step->integer_step.is_unsigned && (csi->chunk_step->integer_step.type.unsign.cursor < csi->chunk_step->integer_step.type.unsign.max
        && (
             ( csi->status == DUMPING_CHUNK && (csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.cursor ) >= csi->chunk_step->integer_step.step // As this chunk is dumping data, another thread can continue with the remaining rows
             ) ||
             ( csi->status == ASSIGNED      && (csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.min    ) >= csi->chunk_step->integer_step.step // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
       )) || ( ! csi->chunk_step->integer_step.is_unsigned && (csi->chunk_step->integer_step.type.sign.cursor < csi->chunk_step->integer_step.type.sign.max // it is not the last chunk
        && (
             ( csi->status == DUMPING_CHUNK && gint64_abs(csi->chunk_step->integer_step.type.sign.max - csi->chunk_step->integer_step.type.sign.cursor) >= csi->chunk_step->integer_step.step // As this chunk is dumping data, another thread can continue with the remaining rows
             ) ||
             ( csi->status == ASSIGNED      && gint64_abs(csi->chunk_step->integer_step.type.sign.max - csi->chunk_step->integer_step.type.sign.min)    >= csi->chunk_step->integer_step.step // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
         )
)) ) || ( csi->chunk_step->integer_step.is_step_fixed_length && csi->chunk_step->integer_step.step > 0 && (   
(
  csi->chunk_step->integer_step.is_unsigned && csi->chunk_step->integer_step.type.unsign.max / csi->chunk_step->integer_step.step > csi->chunk_step->integer_step.type.unsign.min / csi->chunk_step->integer_step.step + 1 
)||
(
 !csi->chunk_step->integer_step.is_unsigned && csi->chunk_step->integer_step.type.sign.max   / csi->chunk_step->integer_step.step > csi->chunk_step->integer_step.type.sign.min   / csi->chunk_step->integer_step.step + 1
)

) );
}

static
gboolean is_last_step(struct chunk_step_item *csi){
return ( !csi->chunk_step->integer_step.is_step_fixed_length  && (
      ( csi->chunk_step->integer_step.is_unsigned && (csi->chunk_step->integer_step.type.unsign.cursor < csi->chunk_step->integer_step.type.unsign.max
        && (
             ( csi->status == DUMPING_CHUNK && (csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.cursor ) <= csi->chunk_step->integer_step.step
             )
           )
    )) || 
      ( ! csi->chunk_step->integer_step.is_unsigned && (csi->chunk_step->integer_step.type.sign.cursor < csi->chunk_step->integer_step.type.sign.max
        && (
             ( csi->status == DUMPING_CHUNK && gint64_abs(csi->chunk_step->integer_step.type.sign.max - csi->chunk_step->integer_step.type.sign.cursor) <= csi->chunk_step->integer_step.step
             )
           )
         )
)) ); 
  /*
  || ( csi->chunk_step->integer_step.is_step_fixed_length && csi->chunk_step->integer_step.step > 0 && (
(
  csi->chunk_step->integer_step.is_unsigned && csi->chunk_step->integer_step.type.unsign.max / csi->chunk_step->integer_step.step > csi->chunk_step->integer_step.type.unsign.min / csi->chunk_step->integer_step.step + 1
)||
(
 !csi->chunk_step->integer_step.is_unsigned && csi->chunk_step->integer_step.type.sign.max   / csi->chunk_step->integer_step.step > csi->chunk_step->integer_step.type.sign.min   / csi->chunk_step->integer_step.step + 1
)

) );*/
}



void update_where_on_integer_step(struct chunk_step_item * csi);

struct chunk_step_item *clone_chunk_step_item(struct chunk_step_item *csi){
  return new_integer_step_item(csi->include_null, csi->prefix, csi->field, csi->chunk_step->integer_step.is_unsigned, csi->chunk_step->integer_step.type, csi->deep, csi->chunk_step->integer_step.is_step_fixed_length, csi->chunk_step->integer_step.step, csi->chunk_step->integer_step.min_chunk_step_size, csi->chunk_step->integer_step.max_chunk_step_size, csi->part, csi->chunk_step->integer_step.check_min, csi->chunk_step->integer_step.check_max, NULL, csi->position, csi->multicolumn, 0);
}


// dbt->chunks_mutex is LOCKED
struct chunk_step_item *get_next_integer_chunk(struct db_table *dbt){
  struct chunk_step_item *csi=NULL, *new_csi=NULL, *new_csi_next=NULL;
  if (dbt->chunks!=NULL){
    csi = (struct chunk_step_item *)g_async_queue_try_pop(dbt->chunks_queue);      
    while (csi!=NULL){
      g_mutex_lock(csi->mutex);
      if (csi->status==UNASSIGNED){
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
      if (csi->status==COMPLETED){
        goto end;
      }
      if (is_last_step(csi)){
        trace("Last chunk on step in `%s`.`%s` assigned", dbt->database->name, dbt->table);
        csi->status=UNSPLITTABLE;
        csi->deep=csi->deep+1;
        new_csi=clone_chunk_step_item(csi);
        new_csi->status=ASSIGNED;

        if (csi->chunk_step->integer_step.is_unsigned){
          csi->chunk_step->integer_step.type.unsign.max=csi->chunk_step->integer_step.type.unsign.cursor;
          new_csi->chunk_step->integer_step.type.unsign.min=csi->chunk_step->integer_step.type.unsign.cursor+1;
        }else{
          csi->chunk_step->integer_step.type.sign.max=csi->chunk_step->integer_step.type.sign.cursor;
          new_csi->chunk_step->integer_step.type.sign.min=csi->chunk_step->integer_step.type.sign.cursor+1;
        }

        new_csi->part+=pow(2,csi->deep);
        update_where_on_integer_step(new_csi);

        dbt->chunks=g_list_append(dbt->chunks,new_csi);
        // should I push them again? isn't it pointless?
//        g_async_queue_push(dbt->chunks_queue, csi);
//        g_async_queue_push(dbt->chunks_queue, new_csi);
        //
        g_mutex_unlock(csi->mutex);
        return new_csi;
      
      }
      if (!is_splitable(csi)){
        if (csi->multicolumn && csi->next && csi->next->chunk_type==INTEGER){
          trace("Multicolumn table checking next");
          g_mutex_lock(csi->next->mutex);
          if (csi->next->status==UNSPLITTABLE || csi->next->status==COMPLETED){
            trace("Multicolumn table is not splittable: %d Ref: COMPLETED=%d", csi->next->status, COMPLETED);
            csi->status=UNSPLITTABLE;
            g_mutex_unlock(csi->next->mutex);
            goto end;
          }
          if (!is_splitable(csi->next)){
            trace("Multicolumn table is not splittable");
            csi->next->status=UNSPLITTABLE;
            g_mutex_unlock(csi->next->mutex);
            goto end;
          }

//          if (has_only_one_level(csi)){

            new_csi_next=split_chunk_step(csi->next);

            if (new_csi_next){
              trace("Multicolumn table is splittable");
              new_csi_next->multicolumn=FALSE;
              csi->deep=csi->deep+1;
              new_csi=clone_chunk_step_item(csi);
              new_csi->status=ASSIGNED;
//              if ( csi->chunk_step->integer_step.is_step_fixed_length ){
                new_csi->part+=pow(2,csi->deep);
//              }
              update_where_on_integer_step(new_csi);
 
              new_csi->next=new_csi_next;

              new_csi->next->prefix = new_csi->where;
              dbt->chunks=g_list_append(dbt->chunks,new_csi);
              g_async_queue_push(dbt->chunks_queue, csi);
              g_async_queue_push(dbt->chunks_queue, new_csi);
              g_mutex_unlock(csi->next->mutex);
              g_mutex_unlock(csi->mutex);
              return new_csi;
            }else{
              trace("Multicolumn table: not able to split?");
            }
//          }
          g_mutex_unlock(csi->next->mutex);
        }

        csi->status=UNSPLITTABLE;
        goto end;
      }

      // it should be splittable, let's do it
      new_csi = split_chunk_step(csi);
      if (new_csi){
        if (new_csi->chunk_step->integer_step.is_unsigned)
          trace("Multicolumn table splited min: %lld max: %lld ", new_csi->chunk_step->integer_step.type.unsign.min, new_csi->chunk_step->integer_step.type.unsign.max);
        else
          trace("Multicolumn table splited min: %lld max: %lld ", new_csi->chunk_step->integer_step.type.sign.min, new_csi->chunk_step->integer_step.type.sign.max);
        dbt->chunks=g_list_append(dbt->chunks,new_csi);
        g_async_queue_push(dbt->chunks_queue, csi);
        g_async_queue_push(dbt->chunks_queue, new_csi);
        g_mutex_unlock(csi->mutex);
        return new_csi;
      }
  
end:
      g_mutex_unlock(csi->mutex);
      csi = (struct chunk_step_item *)g_async_queue_try_pop(dbt->chunks_queue);
    }
  }
  return NULL;
}

gboolean refresh_integer_min_max(MYSQL *conn, struct db_table *dbt, struct chunk_step_item *csi ){
  struct integer_step * ics = &(csi->chunk_step->integer_step);
  gchar *query = NULL;
  /* Get minimum/maximum */

  MYSQL_RES *minmax = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s MIN(%s%s%s),MAX(%s%s%s) FROM %s%s%s.%s%s%s%s%s",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str, identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->name, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        csi->prefix?" WHERE ":"", csi->prefix?csi->prefix->str:""), NULL, "Query to get a new min and max failed", NULL);
  trace("refresh_integer_min_max: %s", query);
  g_free(query);

  if (!minmax){
    return FALSE;
  }

  MYSQL_ROW row = mysql_fetch_row(minmax);
  if (row==NULL || row[0]==NULL || row[1]==NULL){
    mysql_free_result(minmax);
    return FALSE;
  }

  if (ics->is_unsigned) {
    guint64 nmin = strtoull(row[0], NULL, 10);
    guint64 nmax = strtoull(row[1], NULL, 10);
    ics->type.unsign.min = nmin;
    ics->type.unsign.max = nmax;
  }else{
    gint64 nmin = strtoll(row[0], NULL, 10);
    gint64 nmax = strtoll(row[1], NULL, 10);
    ics->type.sign.min = nmin;
    ics->type.sign.max = nmax;
  }
  csi->include_null=TRUE;
  mysql_free_result(minmax);
  return TRUE;
}

static
gboolean update_integer_min(MYSQL *conn, struct db_table *dbt, struct chunk_step_item *csi ){
  struct integer_step * ics = &(csi->chunk_step->integer_step);
  gchar *query = NULL;

  GString *where = get_where_from_csi(csi);

  MYSQL_RES *min = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->name, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        where->str, 
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new min failed", NULL);
  g_string_free(where,TRUE);
  g_free(query);

  if (!min)
    return FALSE;

  MYSQL_ROW row = mysql_fetch_row(min);

  if (row==NULL || row[0]==NULL){
    mysql_free_result(min);
    return FALSE;
  }

  if (ics->is_unsigned) {
    guint64 nmin = strtoull(row[0], NULL, 10);
    ics->type.unsign.min = nmin;
  }else{
    gint64 nmin = strtoll(row[0], NULL, 10);
    ics->type.sign.min = nmin;
  }

  mysql_free_result(min);
  return TRUE;
}

static
gboolean update_integer_max(MYSQL *conn,struct db_table *dbt, struct chunk_step_item *csi ){
  struct integer_step * ics = &(csi->chunk_step->integer_step);
  gchar *query = NULL;

  GString *where = get_where_from_csi(csi);

  MYSQL_RES *max = m_store_result(conn, query = g_strdup_printf(
                        "SELECT %s %s%s%s FROM %s%s%s.%s%s%s WHERE %s ORDER BY %s%s%s DESC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        identifier_quote_character_str, csi->field, identifier_quote_character_str,
                        identifier_quote_character_str, dbt->database->name, identifier_quote_character_str, identifier_quote_character_str, dbt->table, identifier_quote_character_str,
                        where->str,
                        identifier_quote_character_str, csi->field, identifier_quote_character_str), NULL, "Query to get a new max failed", NULL);
  g_string_free(where,TRUE);
  g_free(query);

  if (!max)
    goto cleanup;

  MYSQL_ROW row = mysql_fetch_row(max);
  if (row==NULL || row[0]==NULL){
cleanup:
    if (ics->is_unsigned) {
      ics->type.unsign.max = ics->type.unsign.min;
    }else{
      ics->type.sign.max = ics->type.sign.min;
    }

    if (max)
      mysql_free_result(max);
    return FALSE;
  }

  if (ics->is_unsigned) {
    guint64 nmax = strtoull(row[0], NULL, 10);
    ics->type.unsign.max = nmax;
  }else{
    gint64 nmax = strtoll(row[0], NULL, 10);
    ics->type.sign.max = nmax;
  }

  mysql_free_result(max);
  return TRUE;
}

static
gboolean is_last(struct chunk_step_item *csi){
  g_mutex_lock(csi->mutex);
  gboolean r = 
    csi->chunk_step->integer_step.is_unsigned ?
    csi->chunk_step->integer_step.type.unsign.cursor == csi->chunk_step->integer_step.type.unsign.max :
    csi->chunk_step->integer_step.type.sign.cursor   == csi->chunk_step->integer_step.type.sign.max;
  g_mutex_unlock(csi->mutex);
  return r;
}

guint process_integer_chunk_step(struct table_job *tj, struct chunk_step_item *csi){
  struct thread_data *td = tj->td;
  union chunk_step *cs = csi->chunk_step;

  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }

// Stage 1: Update min and max if needed

  g_mutex_lock(csi->mutex);
//  if (tj->status == COMPLETED)
//    m_critical("Thread %d: Trying to process COMPLETED chunk",td->thread_id);
  csi->status = DUMPING_CHUNK;

  gboolean c_min=TRUE, c_max=TRUE;

  if (!cs->integer_step.is_step_fixed_length){  

    if (cs->integer_step.check_max /*&& tj->dbt->max_chunk_step_size!=0*/ && !cs->integer_step.is_step_fixed_length){
      trace("Thread %d: I-Chunk 1: Updating MAX", td->thread_id);
      if (cs->integer_step.is_unsigned)
        trace("Thread %d: I-Chunk 1: Updating MAX: %ld", td->thread_id, cs->integer_step.type.unsign.max);
      else
        trace("Thread %d: I-Chunk 1: Updating MAX: %ld", td->thread_id, cs->integer_step.type.sign.max);
      c_max=update_integer_max(td->thrconn, tj->dbt, csi);
      if (cs->integer_step.is_unsigned)
        trace("Thread %d: I-Chunk 1: New MAX: %ld", td->thread_id, cs->integer_step.type.unsign.max);
    else
      trace("Thread %d: I-Chunk 1: New MAX: %ld", td->thread_id, cs->integer_step.type.sign.max);
    cs->integer_step.check_max=FALSE;
  }
  if (cs->integer_step.check_min /*&& tj->dbt->max_chunk_step_size!=0*/ && !cs->integer_step.is_step_fixed_length){
    if (cs->integer_step.is_unsigned)
      trace("Thread %d: I-Chunk 1: Updating MIN: %ld", td->thread_id, cs->integer_step.type.unsign.min);
    else
      trace("Thread %d: I-Chunk 1: Updating MIN: %ld", td->thread_id, cs->integer_step.type.sign.min);
    c_min=update_integer_min(td->thrconn, tj->dbt, csi);
    if (cs->integer_step.is_unsigned)
      trace("Thread %d: I-Chunk 1: New MIN: %ld", td->thread_id, cs->integer_step.type.unsign.min);
    else
      trace("Thread %d: I-Chunk 1: New MIN: %ld", td->thread_id, cs->integer_step.type.sign.min);
    cs->integer_step.check_min=FALSE;
  }
  if (!c_min && !c_max){
    trace("Thread %d: I-Chunk 1: both min and max doesn't exists", td->thread_id);
    close_files(tj);
    g_mutex_unlock(csi->mutex);
    goto update_min;
//    goto end_process; 
  }


  if ( cs->integer_step.rows_in_explain == 0){
    GString *_where=get_where_from_csi(csi);
    cs->integer_step.rows_in_explain=get_rows_from_explain(td->thrconn, tj->dbt, _where, csi->field); 
    trace("Thread %d: I-Chunk 1: We calculated rows %"G_GUINT64_FORMAT" for %s", td->thread_id, cs->integer_step.rows_in_explain, _where->str);
    g_string_free(_where, TRUE);
  }
  }


// Stage 2: Setting cursor
  if (tj->dbt->multicolumn && csi->multicolumn && csi->next == NULL && !cs->integer_step.is_step_fixed_length){
    guint64 integer_step_step=cs->integer_step.step;
retry:
  // We are setting cursor to build the WHERE clause for the EXPLAIN
    if (cs->integer_step.is_unsigned){
      if (integer_step_step > cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min + 1 )
        cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.max;
      else
        cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.min + integer_step_step - 1;
      trace("Thread %d: I-Chunk 2: cs->integer_step.type.unsign.cursor: %lld", td->thread_id, cs->integer_step.type.unsign.cursor);
    }else{
      if (integer_step_step > gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min) + 1)
        cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.max;
      else
        cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.min + integer_step_step - 1;
      trace("Thread %d: I-Chunk 2: cs->integer_step.type.sign.cursor: %lld", td->thread_id, cs->integer_step.type.sign.cursor);
    }
    update_where_on_integer_step(csi);
    guint64 rows = check_row_count?
                   get_rows_from_count  (td->thrconn, tj->dbt, csi->where):
                   get_rows_from_explain(td->thrconn, tj->dbt, csi->where, csi->field);
    trace("Thread %d: I-Chunk 2: multicolumn and next == NULL with rows: %lld", td->thread_id, rows);

    guint64 tmpstep = csi->chunk_step->integer_step.is_unsigned?
                 csi->chunk_step->integer_step.type.unsign.cursor - csi->chunk_step->integer_step.type.unsign.min:
      gint64_abs(  csi->chunk_step->integer_step.type.sign.cursor -   csi->chunk_step->integer_step.type.sign.min);
    tmpstep++;
    integer_step_step=integer_step_step>tmpstep?tmpstep:integer_step_step;

    if (integer_step_step>1){
      // rows / num_threads > integer_step_step
      if (rows > tj->dbt->min_chunk_step_size && ( rows > cs->integer_step.step || (tj->num_rows_of_last_run>0 &&rows/100 > tj->num_rows_of_last_run))){
        trace("Thread %d: I-Chunk 2: integer_step.step>1 then retrying", td->thread_id);
        integer_step_step=integer_step_step/2;
        goto retry;
      }
      trace("Thread %d: I-Chunk 2: integer_step.step>1 not retrying as rows %lld <=  step %lld and integer_step_step: %lld", td->thread_id, rows, cs->integer_step.step, integer_step_step);
      cs->integer_step.step=integer_step_step;
    }else{
      // at this poing cs->integer_step.step == 1 always
      cs->integer_step.step=1;
      if (cs->integer_step.is_unsigned){
        trace("Thread %d: I-Chunk 2: integer_step.step==1 min: %"G_GUINT64_FORMAT" | max: %"G_GUINT64_FORMAT, td->thread_id, csi->chunk_step->integer_step.type.unsign.min, csi->chunk_step->integer_step.type.unsign.max);
      }else{
        trace("Thread %d: I-Chunk 2: integer_step.step==1 min: %"G_GINT64_FORMAT" | max: %"G_GINT64_FORMAT, td->thread_id, csi->chunk_step->integer_step.type.sign.min, csi->chunk_step->integer_step.type.sign.max);
      }
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

  if (cs->integer_step.is_unsigned){

    if (cs->integer_step.step > cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min + 1 )
      cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.max;
    else
      cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.min + cs->integer_step.step - 1;

    if (cs->integer_step.type.unsign.cursor < cs->integer_step.type.unsign.min)
      g_error("Thread %d: integer_step.type.unsign.cursor: %"G_GUINT64_FORMAT"  | integer_step.type.unsign.min %"G_GUINT64_FORMAT"  | cs->integer_step.type.unsign.max : %"G_GUINT64_FORMAT" | cs->integer_step.step %ld", td->thread_id, cs->integer_step.type.unsign.cursor, cs->integer_step.type.unsign.min, cs->integer_step.type.unsign.max, cs->integer_step.step);

    g_assert(cs->integer_step.type.unsign.cursor >= cs->integer_step.type.unsign.min);

    cs->integer_step.estimated_remaining_steps=cs->integer_step.step>0?(cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.cursor) / cs->integer_step.step:1;
  }else{

    if (cs->integer_step.step > gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min) + 1)
      cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.max;
    else
      cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.min + cs->integer_step.step - 1;

    if (cs->integer_step.type.sign.cursor < cs->integer_step.type.sign.min)
      g_error("Thread %d: integer_step.type.sign.cursor: %"G_GINT64_FORMAT"  | integer_step.type.sign.min %"G_GINT64_FORMAT"  | cs->integer_step.type.sign.max : %"G_GINT64_FORMAT" | cs->integer_step.step %ld", td->thread_id, cs->integer_step.type.sign.cursor, cs->integer_step.type.sign.min, cs->integer_step.type.sign.max, cs->integer_step.step);

    g_assert(cs->integer_step.type.sign.cursor >= cs->integer_step.type.sign.min);

    cs->integer_step.estimated_remaining_steps=cs->integer_step.step>0?(cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor) / cs->integer_step.step:1;
  }


  if (csi->next !=NULL && csi->status==UNSPLITTABLE){
    // Could be possible that in previous iteration on a multicolumn table, the status changed ot UNSPLITTABLE, but on next iteration could be possible
    // to splittable, that is why we need to change back to ASSIGNED
    csi->status=ASSIGNED; 
  }

  g_mutex_unlock(csi->mutex);

  update_estimated_remaining_chunks_on_dbt(tj->dbt);

// Step 3: Executing query and writing data
  update_where_on_integer_step(csi);
  if (csi->prefix)
    trace("Thread %d: I-Chunk 3: PREFIX: %s WHERE: %s", td->thread_id, csi->prefix->str, csi->where->str);
  else
    trace("Thread %d: I-Chunk 3: WHERE: %s", td->thread_id, csi->where->str); 

  if (csi->next !=NULL){
    trace("Thread %d: I-Chunk 3: going down", td->thread_id);
    // Multi column
    if (csi->next->needs_refresh)
      if (!refresh_integer_min_max(td->thrconn, tj->dbt, csi->next)){
        trace("Thread %d: I-Chunk 3: No min and max found", td->thread_id);
        goto update_min;
      }

    csi->next->chunk_functions.process( tj , csi->next);
    csi->next->needs_refresh=TRUE;
  }else{
    g_string_set_size(tj->where,0);
    g_string_append(tj->where, csi->where->str);
    trace("Thread %d: I-Chunk 3: WHERE in TJ: %s", td->thread_id, tj->where->str);
    if (cs->integer_step.is_step_fixed_length) {
      if (cs->integer_step.is_unsigned)
        tj->part= cs->integer_step.type.unsign.min / cs->integer_step.step + 1;
      else
        tj->part= cs->integer_step.type.sign.min   / cs->integer_step.step + 1;      
      close_files(tj);
      write_table_job_into_file(tj);
    }else if (is_last(csi)) {
      trace("Thread %d: I-Chunk 3: Last chunk on `%s`.`%s` no need to calculate anything else after finish", td->thread_id, tj->dbt->database->name, tj->dbt->table);
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
//      cs->integer_step.rows_in_explain
//      tj->num_rows_of_last_run

      if (cs->integer_step.rows_in_explain > tj->num_rows_of_last_run )
        cs->integer_step.rows_in_explain-=tj->num_rows_of_last_run;
      else
        cs->integer_step.rows_in_explain=0;

      if (diff>0 && tj->num_rows_of_last_run>0){
        cs->integer_step.step=tj->num_rows_of_last_run*max_time_per_select*G_TIME_SPAN_SECOND/diff;
        trace("Thread %d: I-Chunk 3: Step size on `%s`.`%s` is %ld  ( %ld %ld)", td->thread_id, tj->dbt->database->name, tj->dbt->table, cs->integer_step.step, tj->num_rows_of_last_run, diff);
      }else{
        cs->integer_step.step*=2;
        cs->integer_step.check_min=TRUE;
        trace("Thread %d: I-Chunk 3: During last query we get zero rows, duplicating the step size to %ld", td->thread_id, cs->integer_step.step);
      }



      cs->integer_step.step = csi->chunk_step->integer_step.max_chunk_step_size !=0 && cs->integer_step.step > csi->chunk_step->integer_step.max_chunk_step_size ? 
                              csi->chunk_step->integer_step.max_chunk_step_size :
                              cs->integer_step.step;

      cs->integer_step.step = csi->chunk_step->integer_step.min_chunk_step_size !=0 && cs->integer_step.step < csi->chunk_step->integer_step.min_chunk_step_size ?
                              csi->chunk_step->integer_step.min_chunk_step_size :
                              cs->integer_step.step;

//      trace("After checking: %ld == %ld | max_integer_chunk_step_size=%ld | min_integer_chunk_step_size=%ld", ant, cs->integer_step.step, max_integer_chunk_step_size, min_integer_chunk_step_size);
      g_mutex_unlock(csi->mutex);
    }
  }

// Step 5: Updating min
update_min:
  g_mutex_lock(csi->mutex);
  if (csi->status != COMPLETED)
    csi->status = ASSIGNED;
  if (cs->integer_step.is_unsigned){
    if ( cs->integer_step.type.unsign.cursor+1 < cs->integer_step.type.unsign.min){
      // Overflow
      trace("Thread %d: I-Chunk 5: Overflow due integer_step.type.unsign.cursor: %"G_GUINT64_FORMAT"  | integer_step.type.unsign.min %"G_GUINT64_FORMAT, td->thread_id, cs->integer_step.type.unsign.cursor, cs->integer_step.type.unsign.min);
      cs->integer_step.type.unsign.min=cs->integer_step.type.unsign.max;
      cs->integer_step.type.unsign.max--;
    }else
      cs->integer_step.type.unsign.min=cs->integer_step.type.unsign.cursor+1;
  }else{
    if ( cs->integer_step.type.sign.cursor+1 < cs->integer_step.type.sign.min){
      trace("Thread %d: I-Chunk 5: Overflow due integer_step.type.unsign.cursor: %"G_GUINT64_FORMAT"  | integer_step.type.unsign.min %"G_GINT64_FORMAT, td->thread_id, cs->integer_step.type.sign.cursor, cs->integer_step.type.sign.min);
      cs->integer_step.type.sign.min=cs->integer_step.type.sign.max;
      cs->integer_step.type.sign.max--;
    }else
      cs->integer_step.type.sign.min=cs->integer_step.type.sign.cursor+1;
  }
  guint64 tmpstep = csi->chunk_step->integer_step.is_unsigned?
                    csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.min:
         gint64_abs(  csi->chunk_step->integer_step.type.sign.max -   csi->chunk_step->integer_step.type.sign.min);
  tmpstep++;

  trace("Thread %d: I-Chunk 5: integer_step.type.sign.cursor: %"G_GINT64_FORMAT"  | integer_step.type.sign.min %"G_GINT64_FORMAT"  | cs->integer_step.type.sign.max : %"G_GINT64_FORMAT" | cs->integer_step.step %ld | tmpstep: %d", td->thread_id, cs->integer_step.type.sign.cursor, cs->integer_step.type.sign.min, cs->integer_step.type.sign.max, cs->integer_step.step, tmpstep);
  cs->integer_step.step=cs->integer_step.step>tmpstep?tmpstep:cs->integer_step.step;

//  g_message("Thread %d: I-Chunk 5: integer_step.type.sign.cursor: %"G_GINT64_FORMAT"  | integer_step.type.sign.min %"G_GINT64_FORMAT"  | cs->integer_step.type.sign.max : %"G_GINT64_FORMAT" | cs->integer_step.step %ld", td->thread_id, cs->integer_step.type.sign.cursor, cs->integer_step.type.sign.min, cs->integer_step.type.sign.max, cs->integer_step.step);

//end_process:

  if (csi->position==0)
    csi->multicolumn=tj->dbt->multicolumn;

  if (csi->next!=NULL){
    free_integer_step_item(csi->next);
    csi->next=NULL;
  }
  g_mutex_unlock(csi->mutex);
  return 0;
}

void process_integer_chunk(struct table_job *tj, struct chunk_step_item *csi){
  struct thread_data *td = tj->td;
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = csi->chunk_step;
//  gboolean multicolumn_process=FALSE;

/*
  if (csi->next==NULL && csi->multicolumn){
    if (csi->position == 0){
      
    
    
    }else{
      g_string_set_size(csi->where,0);
      update_integer_where_on_gstring(csi->where, csi->include_null, csi->prefix, csi->field, csi->chunk_step->integer_step.is_unsigned, csi->chunk_step->integer_step.type, FALSE);
      guint64 rows = check_row_count?
                   get_rows_from_count(td->thrconn, dbt, NULL): 
                   get_rows_from_explain(td->thrconn, dbt, csi->where, csi->field);


//    determine_if_we_can_go_deeper(csi,rows);

      if (rows > csi->chunk_step->integer_step.min_chunk_step_size ){
        struct chunk_step_item *next_csi = initialize_chunk_step_item(td->thrconn, dbt, csi->position + 1, csi->where);
        if (next_csi && next_csi->chunk_type!=NONE){
          csi->next=next_csi;
          multicolumn_process=TRUE;
        }
      }
    }
  }
*/



  // First step, we need this to process the one time prefix
  g_string_set_size(tj->where,0);
  if (process_integer_chunk_step(tj, csi)){
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
//g_message("cs->integer_step.type.unsign.min: %"G_GUINT64_FORMAT" | cs->integer_step.type.unsign.max: %"G_GUINT64_FORMAT, cs->integer_step.type.unsign.min, cs->integer_step.type.unsign.max);
    while ( 
              (  cs->integer_step.is_unsigned && cs->integer_step.type.unsign.min <= cs->integer_step.type.unsign.max )
           || ( !cs->integer_step.is_unsigned && cs->integer_step.type.sign.min   <= cs->integer_step.type.sign.max   )
          ){
      g_mutex_unlock(csi->mutex);
      g_string_set_size(tj->where,0);
      if (process_integer_chunk_step(tj, csi)){
        g_message("Thread %d: Job has been cacelled",td->thread_id);
        return;
      }
      g_atomic_int_inc(dbt->chunks_completed);
      g_mutex_lock(csi->mutex);
    }

  if (csi->position==0)
    cs->integer_step.estimated_remaining_steps=0;
  csi->status=COMPLETED;
  g_mutex_unlock(csi->mutex);

}

void update_integer_where_on_gstring(GString *where, gboolean include_null, GString *prefix, gchar * field, gboolean is_unsigned, union type type, gboolean use_cursor){
  union type t;  
  if (prefix && prefix->len>0){
//    g_message("update_integer_where_on_gstring:: Prefix: %s", prefix->str);
    g_string_append_printf(where,"(%s AND ",
                          prefix->str);
  }
  if (include_null){
//    g_message("update_integer_where_on_gstring:: with_null");
    g_string_append_printf(where,"(%s%s%s IS NULL OR", identifier_quote_character_str, field, identifier_quote_character_str);
  }
  g_string_append(where,"(");
  if (is_unsigned){
      t.unsign.min = type.unsign.min;
      if (!use_cursor)
        t.unsign.cursor = type.unsign.max;
      else
        t.unsign.cursor = type.unsign.cursor;
      if (t.unsign.min == t.unsign.cursor) {
                g_string_append_printf(where, "%s%s%s = %"G_GUINT64_FORMAT,
                          identifier_quote_character_str, field, identifier_quote_character_str, t.unsign.cursor);
      }else{
                g_string_append_printf(where,"%"G_GUINT64_FORMAT" <= %s%s%s AND %s%s%s <= %"G_GUINT64_FORMAT,
                          t.unsign.min,
	                  identifier_quote_character_str, field, identifier_quote_character_str, identifier_quote_character_str, field, identifier_quote_character_str,
                          t.unsign.cursor);
      }
  }else{
      t.sign.min = type.sign.min;
      if (!use_cursor)
        t.sign.cursor = type.sign.max;
      else
        t.sign.cursor = type.sign.cursor;
      if (t.sign.min == t.sign.cursor){
                g_string_append_printf(where,"%s%s%s = %"G_GINT64_FORMAT,
                          identifier_quote_character_str, field, identifier_quote_character_str, t.sign.cursor);
      }else{
                g_string_append_printf(where,"%"G_GINT64_FORMAT" <= %s%s%s AND %s%s%s <= %"G_GINT64_FORMAT,
                          t.sign.min,
                          identifier_quote_character_str, field, identifier_quote_character_str, identifier_quote_character_str, field, identifier_quote_character_str,
                          t.sign.cursor);
      }
  }
  if (include_null)
    g_string_append(where,")");
  g_string_append(where,")");
  if (prefix && prefix->len>0)
    g_string_append(where,")");
//  g_message("update_integer_where_on_gstring:: where = |%s|", where->str);
}

void update_where_on_integer_step(struct chunk_step_item * csi){
  struct integer_step *chunk_step=&(csi->chunk_step->integer_step);
  g_string_set_size(csi->where,0);
  update_integer_where_on_gstring(csi->where, csi->include_null, csi->prefix, csi->field, chunk_step->is_unsigned, chunk_step->type, TRUE);
}

static
GString * get_where_from_csi(struct chunk_step_item * csi){
  GString *where = g_string_new("");
  update_integer_where_on_gstring(where, FALSE, csi->prefix, csi->field, csi->chunk_step->integer_step.is_unsigned, csi->chunk_step->integer_step.type, FALSE);
  return where;
}

void determine_if_we_can_go_deeper( struct chunk_step_item * csi, guint64 rows){
  if (csi->multicolumn && csi->position == 0){
    trace("is_unsigned: %d | rows: %lld | max - min: %lld", csi->chunk_step->integer_step.is_unsigned, rows, csi->chunk_step->integer_step.is_unsigned?(csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.min):gint64_abs(csi->chunk_step->integer_step.type.sign.max -   csi->chunk_step->integer_step.type.sign.min));

    if (
        // In a multi column table, we will use the first column to split the table.
        // This calculation will let us know how many rows are we getting on average per first column value
        // we need to have have at least 1 chunk size per first column to perform multi column spliting
        ( csi->chunk_step->integer_step.is_unsigned && (rows /         (csi->chunk_step->integer_step.type.unsign.max - csi->chunk_step->integer_step.type.unsign.min) > 1))||
        (!csi->chunk_step->integer_step.is_unsigned && (rows / gint64_abs(csi->chunk_step->integer_step.type.sign.max -   csi->chunk_step->integer_step.type.sign.min) > 1))
       ){
      csi->chunk_step->integer_step.min_chunk_step_size=1;
      csi->chunk_step->integer_step.is_step_fixed_length=TRUE;
      csi->chunk_step->integer_step.max_chunk_step_size=1;
      csi->chunk_step->integer_step.step=1;
    }else
      csi->multicolumn=FALSE;
  }
}
