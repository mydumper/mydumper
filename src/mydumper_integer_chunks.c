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
#include "common.h"
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "mydumper_chunks.h"
#include "mydumper_database.h"
#include "mydumper_jobs.h"
#include "mydumper_global.h"
#include "regex.h"
#include "mydumper_working_thread.h"
#include "mydumper_write.h"
#include "mydumper_integer_chunks.h"

guint64 gint64_abs(gint64 a){
  if (a >= 0)
    return a;
  return -a;
}

union chunk_step *new_integer_step(gboolean include_null, gchar *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, guint64 step, guint64 number, gboolean check_min, gboolean check_max){
//  g_message("New Integer Step with step size: %d", step);
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->integer_step.where=g_string_new("");
  if (include_null)
    cs->integer_step.include_null = g_strdup_printf("`%s` IS NULL OR ", field); 
  cs->integer_step.is_unsigned = is_unsigned;
  cs->integer_step.prefix = prefix;
  cs->integer_step.step = step;
  if (cs->integer_step.is_unsigned){
//    g_message("New unsigned %"G_GUINT64_FORMAT" %"G_GUINT64_FORMAT, type.unsign.min, type.unsign.max);
    cs->integer_step.type.unsign.min = type.unsign.min;
    cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.min;
    cs->integer_step.type.unsign.max = type.unsign.max;
    cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min) / cs->integer_step.step;
  }else{
//    g_message("New signed %"G_GINT64_FORMAT" %"G_GINT64_FORMAT, type.sign.min, type.sign.max);
    cs->integer_step.type.sign.min = type.sign.min;
    cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.min;
    cs->integer_step.type.sign.max = type.sign.max;
    cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min) / cs->integer_step.step;
  }
  cs->integer_step.deep = deep;
  cs->integer_step.number = number;
  cs->integer_step.field = g_strdup(field);
  cs->integer_step.mutex = g_mutex_new(); 
  cs->integer_step.status = UNASSIGNED;
  cs->integer_step.check_max=check_max;
  cs->integer_step.check_min=check_min;
  return cs;
}

void free_integer_step(union chunk_step * cs){
  if (cs->integer_step.field!=NULL){
    g_free(cs->integer_step.field);
    cs->integer_step.field=NULL;
  }
  if (cs->integer_step.prefix!=NULL)
    g_free(cs->integer_step.prefix);
  g_mutex_free(cs->integer_step.mutex); 
  g_free(cs);
}



void common_to_chunk_step(struct db_table *dbt, union chunk_step * cs, union chunk_step * new_cs){
  cs->integer_step.deep++;
  dbt->chunks=g_list_append(dbt->chunks,new_cs);

  new_cs->integer_step.status=ASSIGNED;

  g_async_queue_push(dbt->chunks_queue, cs);
  g_async_queue_push(dbt->chunks_queue, new_cs);

  g_mutex_unlock(cs->integer_step.mutex);
//  g_mutex_unlock(dbt->chunks_mutex);
}

union chunk_step * split_unsigned_chunk_step(struct db_table *dbt, union chunk_step * cs){
  guint64 new_minmax = 0;
  union type type;
  type.unsign.max = cs->integer_step.type.unsign.max;
  union chunk_step * new_cs = NULL;
  if ( dbt->min_rows_per_file == dbt->start_rows_per_file && dbt->max_rows_per_file == dbt->start_rows_per_file){
    if ( cs->integer_step.status == DUMPING_CHUNK ){
      new_minmax = cs->integer_step.type.unsign.cursor + cs->integer_step.step *
                (( cs->integer_step.type.unsign.max    / cs->integer_step.step - 
                   cs->integer_step.type.unsign.cursor / cs->integer_step.step ) / 2 ) + 1;

      if (new_minmax == cs->integer_step.type.unsign.cursor)
      new_minmax = cs->integer_step.type.unsign.cursor + cs->integer_step.step * (
                 ( cs->integer_step.type.unsign.max    / cs->integer_step.step -
                   cs->integer_step.type.unsign.cursor / cs->integer_step.step ) / 2  + 1) + 1;

    }else{
      new_minmax = cs->integer_step.type.unsign.min    + cs->integer_step.step *
                (( cs->integer_step.type.unsign.max    / cs->integer_step.step - 
                   cs->integer_step.type.unsign.min    / cs->integer_step.step ) / 2 );

      if (new_minmax == cs->integer_step.type.unsign.min)
      new_minmax = cs->integer_step.type.unsign.min    + cs->integer_step.step * (
                 ( cs->integer_step.type.unsign.max    / cs->integer_step.step -
                   cs->integer_step.type.unsign.min    / cs->integer_step.step ) / 2 + 1);


    }
    type.unsign.min = new_minmax;
    new_cs = new_integer_step(FALSE, NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number, TRUE, cs->integer_step.check_max);
  }else{
    if ( cs->integer_step.status == DUMPING_CHUNK ){
      new_minmax = cs->integer_step.type.unsign.cursor + (cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.cursor)/2;
      if ( new_minmax == cs->integer_step.type.unsign.cursor )
        new_minmax++;
    }else{
      new_minmax = cs->integer_step.type.unsign.min    + (cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min)/2;
      if ( new_minmax == cs->integer_step.type.unsign.min )
        new_minmax++;
    }
    type.unsign.min = new_minmax;
    new_cs = new_integer_step(FALSE, NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number+pow(2,cs->integer_step.deep), TRUE, cs->integer_step.check_max);
    cs->integer_step.check_max=TRUE;
    
  }

  cs->integer_step.type.unsign.max = new_minmax - 1;

  common_to_chunk_step(dbt, cs, new_cs);
  return new_cs;
}

union chunk_step * split_signed_chunk_step(struct db_table *dbt, union chunk_step * cs){
  gint64 new_minmax = 0;
  union type type;

  type.sign.max = cs->integer_step.type.sign.max;

  union chunk_step * new_cs = NULL;
  if ( dbt->min_rows_per_file == dbt->start_rows_per_file && dbt->max_rows_per_file == dbt->start_rows_per_file){
    if ( cs->integer_step.status == DUMPING_CHUNK ){
       new_minmax = cs->integer_step.type.sign.cursor + (signed) cs->integer_step.step *
                 (( cs->integer_step.type.sign.max    / (signed) cs->integer_step.step - 
                    cs->integer_step.type.sign.cursor / (signed) cs->integer_step.step ) / 2 ) + 1;
      if (new_minmax == cs->integer_step.type.sign.min)
       new_minmax = cs->integer_step.type.sign.min    + (signed) cs->integer_step.step *
                 (( cs->integer_step.type.sign.max    / (signed) cs->integer_step.step -
                    cs->integer_step.type.sign.min    / (signed) cs->integer_step.step ) / 2 + 1 ) + 1; 
   }else{
       new_minmax = cs->integer_step.type.sign.min    + (signed) cs->integer_step.step *
                 (( cs->integer_step.type.sign.max    / (signed) cs->integer_step.step - 
                    cs->integer_step.type.sign.min    / (signed) cs->integer_step.step ) / 2 );
      if (new_minmax == cs->integer_step.type.sign.min)
       new_minmax = cs->integer_step.type.sign.min    + (signed) cs->integer_step.step *
                 (( cs->integer_step.type.sign.max    / (signed) cs->integer_step.step -
                    cs->integer_step.type.sign.min    / (signed) cs->integer_step.step ) / 2 + 1);
    }
    type.sign.min = new_minmax;

    new_cs = new_integer_step(FALSE, NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number, TRUE, cs->integer_step.check_max);
  }else{
    if ( cs->integer_step.status == DUMPING_CHUNK ){
      new_minmax = gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor) > cs->integer_step.step ?
                     cs->integer_step.type.sign.cursor + (cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor)/2 :
                     cs->integer_step.type.sign.cursor + 1;
      if ( new_minmax == cs->integer_step.type.sign.cursor )
        new_minmax++;
    }else{
      new_minmax = gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min) > cs->integer_step.step ?
                     cs->integer_step.type.sign.min + (cs->integer_step.type.sign.max - cs->integer_step.type.sign.min)/2 :
                     cs->integer_step.type.sign.min + 1;
      if ( new_minmax == cs->integer_step.type.sign.min )
        new_minmax++;
    }
    type.sign.min = new_minmax;

    new_cs = new_integer_step(FALSE, NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number+pow(2,cs->integer_step.deep), TRUE, cs->integer_step.check_max);

    cs->integer_step.check_max=TRUE;
  }

  cs->integer_step.type.sign.max = new_minmax - 1;

//  g_message("New sign max: %"G_GINT64_FORMAT, cs->integer_step.type.sign.max);

  common_to_chunk_step(dbt, cs, new_cs);

  return new_cs;
}

union chunk_step *get_next_integer_chunk(struct db_table *dbt){
//  g_mutex_lock(dbt->chunks_mutex);
//  GList *l=dbt->chunks;
  union chunk_step *cs=NULL;
  if (dbt->chunks!=NULL){
//    cs=l->data;
    cs = (union chunk_step *)g_async_queue_try_pop(dbt->chunks_queue);      
    while (cs!=NULL){
      g_mutex_lock(cs->integer_step.mutex);
      if (cs->integer_step.status==UNASSIGNED){
//      g_message("Not assigned");
        cs->integer_step.status=ASSIGNED;
        g_async_queue_push(dbt->chunks_queue, cs);
        g_mutex_unlock(cs->integer_step.mutex);
//        g_mutex_unlock(dbt->chunks_mutex);
        return cs;
      }
      if (cs->integer_step.is_unsigned) {

        if (cs->integer_step.type.unsign.cursor < cs->integer_step.type.unsign.max // it is not the last chunk
        && (  
             ( cs->integer_step.status == DUMPING_CHUNK && cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.cursor > cs->integer_step.step // As this chunk is dumping data, another thread can continue with the remaining rows
             ) || 
             ( cs->integer_step.status == ASSIGNED      && cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min    > cs->integer_step.step // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
         ){
          return split_unsigned_chunk_step(dbt,cs);
        }else{
//        g_message("Not able to split min %"G_GUINT64_FORMAT" step: %"G_GUINT64_FORMAT" max: %"G_GUINT64_FORMAT, cs->integer_step.nmin, cs->integer_step.step, cs->integer_step.nmax);
          g_mutex_unlock(cs->integer_step.mutex);
          if (cs->integer_step.status==COMPLETED){
            free_integer_step(cs);
          }
        }
      }else{
        if (cs->integer_step.type.sign.cursor < cs->integer_step.type.sign.max // it is not the last chunk
        && (  
             ( cs->integer_step.status == DUMPING_CHUNK && gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor) > cs->integer_step.step // As this chunk is dumping data, another thread can continue with the remaining rows
             ) || 
             ( cs->integer_step.status == ASSIGNED      && gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min)    > cs->integer_step.step // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
         ){
          return split_signed_chunk_step(dbt,cs);
        }else{
//        g_message("Not able to split min %"G_GUINT64_FORMAT" step: %"G_GUINT64_FORMAT" max: %"G_GUINT64_FORMAT, cs->integer_step.nmin, cs->integer_step.step, cs->integer_step.nmax);
          g_mutex_unlock(cs->integer_step.mutex);
          if (cs->integer_step.status==COMPLETED){
            free_integer_step(cs);
          }
        }
      }
      cs = (union chunk_step *)g_async_queue_try_pop(dbt->chunks_queue);
    }

//    g_mutex_unlock(cs->integer_step.mutex);
//    l=l->next;
  }
//  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

void update_integer_min(MYSQL *conn,struct db_table *dbt, struct integer_step * ics ){
//  union chunk_step *cs= tj->chunk_step;
  gchar *query = NULL;
  MYSQL_ROW row = NULL;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */


  if (ics->is_unsigned) {

    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %s %"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT" ORDER BY `%s` ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        ics->field, dbt->database->name, dbt->table, ics->prefix,  ics->type.unsign.min, ics->field, ics->field, ics->type.unsign.max, ics->field));

  }else{

    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %s %"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT" ORDER BY `%s` ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        ics->field, dbt->database->name, dbt->table, ics->prefix,  ics->type.sign.min, ics->field, ics->field, ics->type.sign.max, ics->field));

  }

  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax){
    return;
  }
  row = mysql_fetch_row(minmax);

  if (row==NULL || row[0]==NULL){
    return;
  }
  if (ics->is_unsigned) {
    guint64 nmin = strtoull(row[0], NULL, 10);
    ics->type.unsign.min = nmin;
  }else{
    gint64 nmin = strtoll(row[0], NULL, 10);
    ics->type.sign.min = nmin;
  }
}

void update_integer_max(MYSQL *conn,struct db_table *dbt, struct integer_step * ics ){
  gchar *query = NULL;
  MYSQL_ROW row = NULL;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */


  if (ics->is_unsigned) {
    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %s %"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT" ORDER BY `%s` DESC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        ics->field, dbt->database->name, dbt->table, ics->prefix?ics->prefix:"",  ics->type.unsign.min, ics->field, ics->field, ics->type.unsign.max, ics->field));
  }else{
    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %s %"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT" ORDER BY `%s` DESC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        ics->field, dbt->database->name, dbt->table, ics->prefix?ics->prefix:"",  ics->type.sign.min, ics->field, ics->field, ics->type.sign.max, ics->field));
  }

  minmax = mysql_store_result(conn);
  g_free(query);

  if (!minmax){
//    g_message("No middle point");
    goto cleanup;
  }
  row = mysql_fetch_row(minmax);

  if (row==NULL || row[0]==NULL){
//    g_message("No middle point");
cleanup:

    if (ics->is_unsigned) {
      ics->type.unsign.max = ics->type.unsign.min;
    }else{
      ics->type.sign.max = ics->type.sign.min;
    }

    mysql_free_result(minmax);
    return;
  }

  if (ics->is_unsigned) {
    guint64 nmax = strtoull(row[0], NULL, 10);
    ics->type.unsign.max = nmax;
  }else{
    gint64 nmax = strtoll(row[0], NULL, 10);
    ics->type.sign.max = nmax;
  }

  mysql_free_result(minmax);
}


guint process_integer_chunk_step(struct table_job *tj){
  struct thread_data *td = tj->td;
  union chunk_step *cs = tj->chunk_step;
  struct integer_step *ics = &(cs->integer_step);

  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }

// Stage 1: Update min and max if needed

  g_mutex_lock(cs->integer_step.mutex);
//  if (tj->chunk_step->integer_step.status == COMPLETED)
//    m_critical("Thread %d: Trying to process COMPLETED chunk",td->thread_id);
  cs->integer_step.status = DUMPING_CHUNK;

  if (cs->integer_step.check_max){
    g_message("thread: %d Updating MAX", td->thread_id);
    update_integer_max(td->thrconn, tj->dbt, ics);
    cs->integer_step.check_max=FALSE;
  }
  if (cs->integer_step.check_min){
    g_message("thread: %d Updating MIN", td->thread_id);
    update_integer_min(td->thrconn, tj->dbt, ics);
//    g_message("thread: %d New MIN: %ld", td->thread_id, tj->chunk_step->integer_step.nmin);
    cs->integer_step.check_min=FALSE;
  }

// Stage 2: Setting cursor

if (cs->integer_step.is_unsigned){

//  tj->chunk_step->integer_step.type.unsign.cursor = (tj->chunk_step->integer_step.type.unsign.min + tj->chunk_step->integer_step.step) > tj->chunk_step->integer_step.type.unsign.max ? tj->chunk_step->integer_step.type.unsign.max : tj->chunk_step->integer_step.type.unsign.min + tj->chunk_step->integer_step.step;
  if (cs->integer_step.step -1 > cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min)
    cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.max;
  else
    cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.min + cs->integer_step.step -1;
  cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.cursor) / cs->integer_step.step;

}else{

//  tj->chunk_step->integer_step.type.sign.cursor = ((gint64)(tj->chunk_step->integer_step.type.sign.min + tj->chunk_step->integer_step.step)) > tj->chunk_step->integer_step.type.sign.max ? tj->chunk_step->integer_step.type.sign.max : tj->chunk_step->integer_step.type.sign.min + (gint64) tj->chunk_step->integer_step.step;
  if (cs->integer_step.step - 1 > gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.min))
    cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.max;
  else
    cs->integer_step.type.sign.cursor = cs->integer_step.type.sign.min + cs->integer_step.step - 1;
//g_message("cs->integer_step.type.sign.min: %"G_GINT64_FORMAT" | cs->integer_step.type.sign.cursor: %"G_GINT64_FORMAT "| cs->integer_step.type.sign.max: %"G_GINT64_FORMAT, cs->integer_step.type.sign.min,cs->integer_step.type.sign.cursor, cs->integer_step.type.sign.max);

  cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor) / cs->integer_step.step;
}

  g_mutex_unlock(cs->integer_step.mutex);
/*  if (tj->chunk_step->integer_step.nmin == tj->chunk_step->integer_step.nmax){
    return;
  }*/
//  g_message("CONTINUE");



/*
  if (cs->integer_step.step != 1){

// If we determine that cursor and min are equal, we must cancel

    if (cs->integer_step.is_unsigned){
      if (cs->integer_step.type.unsign.cursor == cs->integer_step.type.unsign.min){
g_message("cs->integer_step.type.unsign.cursor == cs->integer_step.type.unsign.min");
        return 0;
      }
    }else{
      if (cs->integer_step.type.sign.cursor == cs->integer_step.type.sign.min){
g_message("cs->integer_step.type.sign.cursor == cs->integer_step.type.sign.min");
        return 0;
      }
    }
  }
*/

//TODO: We need to include this line
//update_estimated_remaining_chunks_on_dbt(tj->dbt);

  g_string_append(tj->where,update_integer_where(cs));

// Step 3: Executing query and writing data


    if (cs->integer_step.is_unsigned){
g_message("unsign.min: %"G_GUINT64_FORMAT" | unsign.cursor: %"G_GUINT64_FORMAT"| unsign.max: %"G_GUINT64_FORMAT, cs->integer_step.type.unsign.min, cs->integer_step.type.unsign.cursor, cs->integer_step.type.unsign.max);
}else{
g_message("sign.min: %"G_GINT64_FORMAT" | sign.cursor: %"G_GINT64_FORMAT "| sign.max: %"G_GINT64_FORMAT, cs->integer_step.type.sign.min, cs->integer_step.type.sign.cursor, cs->integer_step.type.sign.max);
}


  GDateTime *from = g_date_time_new_now_local();
  write_table_job_into_file(tj);
  GDateTime *to = g_date_time_new_now_local();

// Step 4: Updating Step size

  GTimeSpan diff=g_date_time_difference(to,from)/G_TIME_SPAN_SECOND;
  g_date_time_unref(from);
  g_date_time_unref(to);
  if (diff > 2){
    cs->integer_step.step=cs->integer_step.step  / 2;
    cs->integer_step.step=cs->integer_step.step<min_rows_per_file?min_rows_per_file:cs->integer_step.step;
//    g_message("Decreasing time: %ld | %ld", diff, tj->chunk_step->integer_step.step);
  }else if (diff < 1){
    cs->integer_step.step=cs->integer_step.step  * 2 == 0?cs->integer_step.step:cs->integer_step.step  * 2;
    if (max_rows_per_file!=0)
      cs->integer_step.step=cs->integer_step.step>max_rows_per_file?max_rows_per_file:cs->integer_step.step;
//    g_message("Increasing time: %ld | %ld", diff, tj->chunk_step->integer_step.step);
  }


// Step 5: Updating min

  g_mutex_lock(cs->integer_step.mutex);
  if (cs->integer_step.status != COMPLETED)
    cs->integer_step.status = ASSIGNED;
  if (cs->integer_step.is_unsigned){
    cs->integer_step.type.unsign.min=cs->integer_step.type.unsign.cursor+1;
  }else{
    cs->integer_step.type.sign.min=cs->integer_step.type.sign.cursor+1;
  }
  g_mutex_unlock(cs->integer_step.mutex);
  return 0;
}

void process_integer_chunk(struct table_job *tj){
  struct thread_data *td = tj->td;
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step;
  // First step, we need this to process the one time prefix
  g_string_set_size(tj->where,0);
  if (process_integer_chunk_step(tj)){
    g_message("Thread %d: Job has been cacelled",td->thread_id);
    return;
  }
  g_atomic_int_inc(dbt->chunks_completed);
  if (cs->integer_step.prefix)
    g_free(cs->integer_step.prefix);
  cs->integer_step.prefix=NULL;
  cs->integer_step.include_null=FALSE;

  // Processing the remaining steps
  if (cs->integer_step.is_unsigned){
    g_mutex_lock(tj->chunk_step->integer_step.mutex);
    // Remaining unsigned steps
//g_message("cs->integer_step.type.unsign.min: %"G_GUINT64_FORMAT" | cs->integer_step.type.unsign.max: %"G_GUINT64_FORMAT, cs->integer_step.type.unsign.min, cs->integer_step.type.unsign.max);
    while ( cs->integer_step.type.unsign.min <= cs->integer_step.type.unsign.max ){
      g_mutex_unlock(tj->chunk_step->integer_step.mutex);
      g_string_set_size(tj->where,0);
      if (process_integer_chunk_step(tj)){
        g_message("Thread %d: Job has been cacelled",td->thread_id);
        return;
      }
      g_atomic_int_inc(dbt->chunks_completed);
      g_mutex_lock(tj->chunk_step->integer_step.mutex);
    }
    g_mutex_unlock(tj->chunk_step->integer_step.mutex);
  }else{
    g_mutex_lock(tj->chunk_step->integer_step.mutex);
    // Remaining signed steps
//g_message("cs->integer_step.type.sign.min: %"G_GINT64_FORMAT" | cs->integer_step.type.sign.max: %"G_GINT64_FORMAT, cs->integer_step.type.sign.min, cs->integer_step.type.sign.max);
    while ( cs->integer_step.type.sign.min <= cs->integer_step.type.sign.max ){
      g_mutex_unlock(tj->chunk_step->integer_step.mutex);
      g_string_set_size(tj->where,0);
      if (process_integer_chunk_step(tj)){
        g_message("Thread %d: Job has been cacelled",td->thread_id);
        return;
      }
      g_atomic_int_inc(dbt->chunks_completed);
      g_mutex_lock(tj->chunk_step->integer_step.mutex);
    }
    g_mutex_unlock(tj->chunk_step->integer_step.mutex);
  }

/*
  g_mutex_lock(tj->chunk_step->integer_step.mutex);
  if (cs->integer_step.step == 1){
    g_mutex_unlock(tj->chunk_step->integer_step.mutex);
    g_string_set_size(tj->where,0);
    if (process_integer_chunk_step(tj)){
      g_message("Thread %d: Job has been cacelled",td->thread_id);
      return;
    }
    g_atomic_int_inc(dbt->chunks_completed);
  }else{
    g_mutex_unlock(tj->chunk_step->integer_step.mutex);
  }
*/

  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_lock(cs->integer_step.mutex);
  dbt->chunks=g_list_remove(dbt->chunks,cs);
  tj->chunk_step->integer_step.estimated_remaining_steps=0;
  if (g_list_length(dbt->chunks) == 0){
    g_message("Thread %d: Table %s completed ",td->thread_id,dbt->table);
    dbt->chunks=NULL;
  }
  // Chunk completed
  cs->integer_step.status=COMPLETED;
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_unlock(cs->integer_step.mutex);
}


void update_where_on_integer_step(struct integer_step * chunk_step){
  g_string_set_size(chunk_step->where,0);
  if (chunk_step->is_unsigned){
      if (chunk_step->type.unsign.min == chunk_step->type.unsign.cursor) {
                g_string_append_printf(chunk_step->where, "`%s` = %"G_GUINT64_FORMAT,
                          chunk_step->field, chunk_step->type.unsign.cursor);
      }else{
                g_string_append_printf(chunk_step->where,"%"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT,
                          chunk_step->type.unsign.min, chunk_step->field,
                          chunk_step->field, chunk_step->type.unsign.cursor);
      }
  }else{
      if (chunk_step->type.sign.min == chunk_step->type.sign.cursor){
                g_string_append_printf(chunk_step->where,"`%s` = %"G_GINT64_FORMAT,
                          chunk_step->field, chunk_step->type.sign.cursor);
      }else{
                g_string_append_printf(chunk_step->where,"%"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT,
                          chunk_step->type.sign.min, chunk_step->field,
                          chunk_step->field, chunk_step->type.sign.cursor);
      }
  }
}


gchar * update_integer_where(union chunk_step * chunk_step){
//  (void)td;
  update_where_on_integer_step(&(chunk_step->integer_step));

                return g_strdup_printf("(%s(%s%s%s))",
                          chunk_step->integer_step.include_null?chunk_step->integer_step.include_null:"",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"", chunk_step->integer_step.prefix?" AND ":"",
                          chunk_step->integer_step.where->str);

/*
  gchar *where=NULL;
  if (chunk_step->integer_step.is_unsigned){
      if (chunk_step->integer_step.type.unsign.min == chunk_step->integer_step.type.unsign.max) {
                where=g_strdup_printf("(%s ( %s %s `%s` = %"G_GUINT64_FORMAT"))",
                          chunk_step->integer_step.include_null?chunk_step->integer_step.include_null:"",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"", chunk_step->integer_step.prefix?"AND":"",
                          chunk_step->integer_step.field, chunk_step->integer_step.type.unsign.cursor);
      }else{
                where=g_strdup_printf("( %s ( %s %s  %"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT"))",
                          chunk_step->integer_step.include_null?chunk_step->integer_step.include_null:"",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"", chunk_step->integer_step.prefix?"AND":"",
                          chunk_step->integer_step.type.unsign.min, chunk_step->integer_step.field,
                          chunk_step->integer_step.field, chunk_step->integer_step.type.unsign.cursor);
      }
  }else{
      if (chunk_step->integer_step.type.sign.min == chunk_step->integer_step.type.sign.max){
                where=g_strdup_printf("(%s ( %s %s `%s` = %"G_GINT64_FORMAT"))",
                          chunk_step->integer_step.include_null?chunk_step->integer_step.include_null:"",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"", chunk_step->integer_step.prefix?"AND":"",
                          chunk_step->integer_step.field, chunk_step->integer_step.type.sign.cursor);
      }else{
                where=g_strdup_printf("( %s ( %s %s  %"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT"))",
                          chunk_step->integer_step.include_null?chunk_step->integer_step.include_null:"",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"", chunk_step->integer_step.prefix?"AND":"",
                          chunk_step->integer_step.type.sign.min, chunk_step->integer_step.field,
                          chunk_step->integer_step.field, chunk_step->integer_step.type.sign.cursor);
      }
  }

  return where;
*/
}
