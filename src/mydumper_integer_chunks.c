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

union chunk_step *new_integer_step(gchar *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, guint64 step, guint64 number, gboolean check_min, gboolean check_max){
//  g_message("New Integer Step with step size: %d", step);
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->integer_step.is_unsigned = is_unsigned;
  cs->integer_step.prefix = prefix;
  cs->integer_step.step = step;
  if (cs->integer_step.is_unsigned){
//    g_message("Is unsigned... ");
    cs->integer_step.type.unsign.min = type.unsign.min;
    cs->integer_step.type.unsign.cursor = cs->integer_step.type.unsign.min;
    cs->integer_step.type.unsign.max = type.unsign.max;
    cs->integer_step.estimated_remaining_steps=(cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.min) / cs->integer_step.step;
  }else{
//    g_message("Is signed... ");
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
  g_mutex_unlock(dbt->chunks_mutex);
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
    new_cs = new_integer_step(NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number, TRUE, cs->integer_step.check_max);
  }else{
    new_minmax = cs->integer_step.type.unsign.cursor + (cs->integer_step.type.unsign.max - cs->integer_step.type.unsign.cursor)/2;
    if ( new_minmax == cs->integer_step.type.unsign.cursor )
      new_minmax++;
    type.unsign.min = new_minmax;
    new_cs = new_integer_step(NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number+pow(2,cs->integer_step.deep), TRUE, cs->integer_step.check_max);
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

    new_cs = new_integer_step(NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number, TRUE, cs->integer_step.check_max);
  }else{
    new_minmax = gint64_abs(cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor) > cs->integer_step.step ?
                   cs->integer_step.type.sign.cursor + (cs->integer_step.type.sign.max - cs->integer_step.type.sign.cursor)/2 :
                   cs->integer_step.type.sign.cursor + 1;
    if ( new_minmax == cs->integer_step.type.sign.cursor )
      new_minmax++;
    type.sign.min = new_minmax;

    new_cs = new_integer_step(NULL, dbt->primary_key->data, cs->integer_step.is_unsigned, type, cs->integer_step.deep + 1, cs->integer_step.step, cs->integer_step.number+pow(2,cs->integer_step.deep), TRUE, cs->integer_step.check_max);

    cs->integer_step.check_max=TRUE;
  }

  cs->integer_step.type.sign.max = new_minmax - 1;

  common_to_chunk_step(dbt, cs, new_cs);

  return new_cs;
}



union chunk_step *get_next_integer_chunk(struct db_table *dbt){
  g_mutex_lock(dbt->chunks_mutex);
//  GList *l=dbt->chunks;
  union chunk_step *cs=NULL;
  if (dbt->chunks!=NULL){
//    g_message("IN WHILE");
//    cs=l->data;
    cs = (union chunk_step *)g_async_queue_try_pop(dbt->chunks_queue);      
    while (cs!=NULL){
      g_mutex_lock(cs->integer_step.mutex);
      if (cs->integer_step.status==UNASSIGNED){
//      g_message("Not assigned");
        cs->integer_step.status=ASSIGNED;
        g_async_queue_push(dbt->chunks_queue, cs);
        g_mutex_unlock(cs->integer_step.mutex);
        g_mutex_unlock(dbt->chunks_mutex);
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
  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

void update_integer_min(MYSQL *conn, struct table_job *tj){
  union chunk_step *cs= tj->chunk_step;
  gchar *query = NULL;
  MYSQL_ROW row = NULL;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */


if (cs->integer_step.is_unsigned) {

  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %s %"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT" ORDER BY `%s` ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        (gchar*)tj->dbt->primary_key->data, tj->dbt->database->name, tj->dbt->table, cs->integer_step.prefix,  cs->integer_step.type.unsign.min,(gchar*) tj->dbt->primary_key->data,(gchar*) tj->dbt->primary_key->data, cs->integer_step.type.unsign.max,(gchar*) tj->dbt->primary_key->data));

}else{

  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %s %"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT" ORDER BY `%s` ASC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        (gchar*)tj->dbt->primary_key->data, tj->dbt->database->name, tj->dbt->table, cs->integer_step.prefix,  cs->integer_step.type.sign.min, (gchar*)tj->dbt->primary_key->data,(gchar*) tj->dbt->primary_key->data, cs->integer_step.type.sign.max, (gchar*)tj->dbt->primary_key->data));

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
if (cs->integer_step.is_unsigned) {
  guint64 nmin = strtoull(row[0], NULL, 10);
  cs->integer_step.type.unsign.min = nmin;
}else{
  gint64 nmin = strtoll(row[0], NULL, 10);
  cs->integer_step.type.sign.min = nmin;

}
}

void update_integer_max(MYSQL *conn, struct table_job *tj){
  union chunk_step *cs= tj->chunk_step;
  gchar *query = NULL;
  MYSQL_ROW row = NULL;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */


  if (cs->integer_step.is_unsigned) {
    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT" ORDER BY `%s` DESC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        (gchar*)tj->dbt->primary_key->data, tj->dbt->database->name, tj->dbt->table, cs->integer_step.type.unsign.min, (gchar*)tj->dbt->primary_key->data, (gchar*)tj->dbt->primary_key->data, cs->integer_step.type.unsign.max, (gchar*)tj->dbt->primary_key->data));

  }else{
    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE %"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT" ORDER BY `%s` DESC LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        (gchar*)tj->dbt->primary_key->data, tj->dbt->database->name, tj->dbt->table, cs->integer_step.type.sign.min, (gchar*)tj->dbt->primary_key->data, (gchar*)tj->dbt->primary_key->data, cs->integer_step.type.sign.max, (gchar*)tj->dbt->primary_key->data));
  }

//  g_free(query);
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

    if (cs->integer_step.is_unsigned) {
      cs->integer_step.type.unsign.max = cs->integer_step.type.unsign.min;
    }else{
      cs->integer_step.type.sign.max = cs->integer_step.type.sign.min;
    }

    mysql_free_result(minmax);
    return;
  }

if (cs->integer_step.is_unsigned) {
  guint64 nmax = strtoull(row[0], NULL, 10);
  cs->integer_step.type.unsign.max = nmax;
}else{
  gint64 nmax = strtoll(row[0], NULL, 10);
  cs->integer_step.type.sign.max = nmax;

}


  mysql_free_result(minmax);
}


guint process_integer_chunk_step(struct thread_data *td, struct table_job *tj){
  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }
  g_mutex_lock(tj->chunk_step->integer_step.mutex);
//  if (tj->chunk_step->integer_step.status == COMPLETED)
//    m_critical("Thread %d: Trying to process COMPLETED chunk",td->thread_id);
  tj->chunk_step->integer_step.status = DUMPING_CHUNK;

  if (tj->chunk_step->integer_step.check_max){
//    g_message("thread: %d Updating MAX", td->thread_id);
    update_integer_max(td->thrconn, tj);
    tj->chunk_step->integer_step.check_max=FALSE;
  }
  if (tj->chunk_step->integer_step.check_min){
//    g_message("thread: %d Updating MIN", td->thread_id);
    update_integer_min(td->thrconn, tj);
//    g_message("thread: %d New MIN: %ld", td->thread_id, tj->chunk_step->integer_step.nmin);
    tj->chunk_step->integer_step.check_min=FALSE;
  }

if (tj->chunk_step->integer_step.is_unsigned){

//  tj->chunk_step->integer_step.type.unsign.cursor = (tj->chunk_step->integer_step.type.unsign.min + tj->chunk_step->integer_step.step) > tj->chunk_step->integer_step.type.unsign.max ? tj->chunk_step->integer_step.type.unsign.max : tj->chunk_step->integer_step.type.unsign.min + tj->chunk_step->integer_step.step;
  if (tj->chunk_step->integer_step.step -1 > tj->chunk_step->integer_step.type.unsign.max - tj->chunk_step->integer_step.type.unsign.min)
    tj->chunk_step->integer_step.type.unsign.cursor = tj->chunk_step->integer_step.type.unsign.max;
  else
    tj->chunk_step->integer_step.type.unsign.cursor = tj->chunk_step->integer_step.type.unsign.min + tj->chunk_step->integer_step.step -1;
  tj->chunk_step->integer_step.estimated_remaining_steps=(tj->chunk_step->integer_step.type.unsign.max - tj->chunk_step->integer_step.type.unsign.cursor) / tj->chunk_step->integer_step.step;

}else{

//  tj->chunk_step->integer_step.type.sign.cursor = ((gint64)(tj->chunk_step->integer_step.type.sign.min + tj->chunk_step->integer_step.step)) > tj->chunk_step->integer_step.type.sign.max ? tj->chunk_step->integer_step.type.sign.max : tj->chunk_step->integer_step.type.sign.min + (gint64) tj->chunk_step->integer_step.step;
  if (tj->chunk_step->integer_step.step - 1 > gint64_abs(tj->chunk_step->integer_step.type.sign.max - tj->chunk_step->integer_step.type.sign.min))
    tj->chunk_step->integer_step.type.sign.cursor = tj->chunk_step->integer_step.type.sign.max;
  else
    tj->chunk_step->integer_step.type.sign.cursor = tj->chunk_step->integer_step.type.sign.min + tj->chunk_step->integer_step.step - 1;
  tj->chunk_step->integer_step.estimated_remaining_steps=(tj->chunk_step->integer_step.type.sign.max - tj->chunk_step->integer_step.type.sign.cursor) / tj->chunk_step->integer_step.step;
}

  g_mutex_unlock(tj->chunk_step->integer_step.mutex);
/*  if (tj->chunk_step->integer_step.nmin == tj->chunk_step->integer_step.nmax){
    return;
  }*/
//  g_message("CONTINUE");

  if (tj->chunk_step->integer_step.is_unsigned){
    if (tj->chunk_step->integer_step.type.unsign.cursor == tj->chunk_step->integer_step.type.unsign.min)
      return 0;
  }else{
    if (tj->chunk_step->integer_step.type.sign.cursor == tj->chunk_step->integer_step.type.sign.min)
      return 0;
  }

  update_estimated_remaining_chunks_on_dbt(tj->dbt);
  if (tj->where)
    g_free(tj->where);
  tj->where=update_integer_where(td,tj->chunk_step);

//  update_where_on_table_job(td, tj);
//  message_dumping_data(td,tj);

  GDateTime *from = g_date_time_new_now_local();
  write_table_job_into_file(td->thrconn, tj);
  GDateTime *to = g_date_time_new_now_local();

  GTimeSpan diff=g_date_time_difference(to,from)/G_TIME_SPAN_SECOND;
  g_date_time_unref(from);
  g_date_time_unref(to);
  if (diff > 2){
    tj->chunk_step->integer_step.step=tj->chunk_step->integer_step.step  / 2;
    tj->chunk_step->integer_step.step=tj->chunk_step->integer_step.step<min_rows_per_file?min_rows_per_file:tj->chunk_step->integer_step.step;
//    g_message("Decreasing time: %ld | %ld", diff, tj->chunk_step->integer_step.step);
  }else if (diff < 1){
    tj->chunk_step->integer_step.step=tj->chunk_step->integer_step.step  * 2 == 0?tj->chunk_step->integer_step.step:tj->chunk_step->integer_step.step  * 2;
    if (max_rows_per_file!=0)
      tj->chunk_step->integer_step.step=tj->chunk_step->integer_step.step>max_rows_per_file?max_rows_per_file:tj->chunk_step->integer_step.step;
//    g_message("Increasing time: %ld | %ld", diff, tj->chunk_step->integer_step.step);
  }

  g_mutex_lock(tj->chunk_step->integer_step.mutex);
  if (tj->chunk_step->integer_step.status != COMPLETED)
    tj->chunk_step->integer_step.status = ASSIGNED;
  if (tj->chunk_step->integer_step.is_unsigned){
    tj->chunk_step->integer_step.type.unsign.min=tj->chunk_step->integer_step.type.unsign.cursor+1;
  }else{
    tj->chunk_step->integer_step.type.sign.min=tj->chunk_step->integer_step.type.sign.cursor+1;
  }
  g_mutex_unlock(tj->chunk_step->integer_step.mutex);
  return 0;
}

void process_integer_chunk(struct thread_data *td, struct table_job *tj){
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step;
  // First step, we need this to process the one time prefix
  if (process_integer_chunk_step(td,tj)){
    g_message("Thread %d: Job has been cacelled",td->thread_id);
    return;
  }
  g_atomic_int_inc(dbt->chunks_completed);
  if (cs->integer_step.prefix)
    g_free(cs->integer_step.prefix);
  cs->integer_step.prefix=NULL;

  // Processing the remaining steps
  if (cs->integer_step.is_unsigned){
    g_mutex_lock(tj->chunk_step->integer_step.mutex);
    // Remaining unsigned steps
    while ( cs->integer_step.type.unsign.min < cs->integer_step.type.unsign.max ){
      g_mutex_unlock(tj->chunk_step->integer_step.mutex);
      if (process_integer_chunk_step(td,tj)){
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
    while ( cs->integer_step.type.sign.min < cs->integer_step.type.sign.max ){
      g_mutex_unlock(tj->chunk_step->integer_step.mutex);
      if (process_integer_chunk_step(td,tj)){
        g_message("Thread %d: Job has been cacelled",td->thread_id);
        return;
      }
      g_atomic_int_inc(dbt->chunks_completed);
      g_mutex_lock(tj->chunk_step->integer_step.mutex);
    }
    g_mutex_unlock(tj->chunk_step->integer_step.mutex);
  }
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


gchar * update_integer_where(struct thread_data *td, union chunk_step * chunk_step){
  (void)td;
  gchar *where=NULL;
  if (chunk_step->integer_step.is_unsigned){
      if (chunk_step->integer_step.type.unsign.min == chunk_step->integer_step.type.unsign.max) {
                where=g_strdup_printf("(%s ( `%s` = %"G_GUINT64_FORMAT"))",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"",
                          chunk_step->integer_step.field, chunk_step->integer_step.type.unsign.cursor);
      }else{
                where=g_strdup_printf("( %s ( %"G_GUINT64_FORMAT" <= `%s` AND `%s` <= %"G_GUINT64_FORMAT"))",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"",
                          chunk_step->integer_step.type.unsign.min, chunk_step->integer_step.field,
                          chunk_step->integer_step.field, chunk_step->integer_step.type.unsign.cursor);
      }
  }else{
      if (chunk_step->integer_step.type.sign.min == chunk_step->integer_step.type.sign.max){
                where=g_strdup_printf("(%s ( `%s` = %"G_GINT64_FORMAT"))",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"",
                          chunk_step->integer_step.field, chunk_step->integer_step.type.sign.cursor);
      }else{
                where=g_strdup_printf("( %s ( %"G_GINT64_FORMAT" <= `%s` AND `%s` <= %"G_GINT64_FORMAT"))",
                          chunk_step->integer_step.prefix?chunk_step->integer_step.prefix:"",
                          chunk_step->integer_step.type.sign.min, chunk_step->integer_step.field,
                          chunk_step->integer_step.field, chunk_step->integer_step.type.sign.cursor);
      }
  }

  return where;
}
