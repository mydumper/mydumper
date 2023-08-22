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
#include "mydumper_integer_chunks.h"
#include "mydumper_working_thread.h"
#include "mydumper_write.h"

union chunk_step *new_multicolumn_integer_step(gchar *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, guint64 number){
  g_message("New Multi Integer Step");
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->multicolumn_integer_step.is_unsigned = is_unsigned;
  cs->multicolumn_integer_step.prefix = prefix;
  if (cs->multicolumn_integer_step.is_unsigned){
    cs->multicolumn_integer_step.type.unsign.min = type.unsign.min;
    cs->multicolumn_integer_step.type.unsign.cursor = cs->multicolumn_integer_step.type.unsign.min;
    cs->multicolumn_integer_step.type.unsign.max = type.unsign.max;
    cs->multicolumn_integer_step.estimated_remaining_steps=(cs->multicolumn_integer_step.type.unsign.max - cs->multicolumn_integer_step.type.unsign.min);
  }else{
    cs->multicolumn_integer_step.type.sign.min = type.sign.min;
    cs->multicolumn_integer_step.type.sign.cursor = cs->multicolumn_integer_step.type.sign.min;
    cs->multicolumn_integer_step.type.sign.max = type.sign.max;
    cs->multicolumn_integer_step.estimated_remaining_steps=(cs->multicolumn_integer_step.type.sign.max - cs->multicolumn_integer_step.type.sign.min);
  }
  cs->multicolumn_integer_step.deep = deep;
  cs->multicolumn_integer_step.number = number;
  cs->multicolumn_integer_step.field = g_strdup(field);
  cs->multicolumn_integer_step.mutex = g_mutex_new();
  cs->multicolumn_integer_step.status = UNASSIGNED;
  return cs;
}


union chunk_step *get_next_multicolumn_integer_chunk(struct db_table *dbt){
  g_mutex_lock(dbt->chunks_mutex);
//  GList *l=dbt->chunks;
  union chunk_step *cs=NULL;
  if (dbt->chunks!=NULL){
//    g_message("IN WHILE");
//    cs=l->data;
    cs = (union chunk_step *)g_async_queue_try_pop(dbt->chunks_queue);
    while (cs!=NULL){
      g_mutex_lock(cs->multicolumn_integer_step.mutex);
      if (cs->multicolumn_integer_step.status==UNASSIGNED){
//      g_message("Not assigned");
        cs->multicolumn_integer_step.status=ASSIGNED;
        g_async_queue_push(dbt->chunks_queue, cs);
        g_mutex_unlock(cs->multicolumn_integer_step.mutex);
        g_mutex_unlock(dbt->chunks_mutex);
        return cs;
      }
      if (cs->multicolumn_integer_step.is_unsigned) {

        if (cs->multicolumn_integer_step.type.unsign.cursor < cs->multicolumn_integer_step.type.unsign.max // it is not the last chunk
        && (
             ( cs->multicolumn_integer_step.status == DUMPING_CHUNK && cs->multicolumn_integer_step.type.unsign.max - cs->multicolumn_integer_step.type.unsign.cursor > 1 // As this chunk is dumping data, another thread can continue with the remaining rows
             ) ||
             ( cs->multicolumn_integer_step.status == ASSIGNED      && cs->multicolumn_integer_step.type.unsign.max - cs->multicolumn_integer_step.type.unsign.min    > 1 // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
         ){
//          return split_unsigned_chunk_step(dbt,cs);
        }else{
//        g_message("Not able to split min %"G_GUINT64_FORMAT" step: %"G_GUINT64_FORMAT" max: %"G_GUINT64_FORMAT, cs->integer_step.nmin, cs->integer_step.step, cs->integer_step.nmax);
          g_mutex_unlock(cs->multicolumn_integer_step.mutex);
          if (cs->multicolumn_integer_step.status==COMPLETED){
//            free_multicolumn_integer_step(cs);
          }
        }
      }else{
        if (cs->multicolumn_integer_step.type.sign.cursor < cs->multicolumn_integer_step.type.sign.max // it is not the last chunk
        && (
             ( cs->multicolumn_integer_step.status == DUMPING_CHUNK && gint64_abs(cs->multicolumn_integer_step.type.sign.max - cs->multicolumn_integer_step.type.sign.cursor) > 1 // As this chunk is dumping data, another thread can continue with the remaining rows
             ) ||
             ( cs->multicolumn_integer_step.status == ASSIGNED      && gint64_abs(cs->multicolumn_integer_step.type.sign.max - cs->multicolumn_integer_step.type.sign.min)    > 1 // As this chunk is going to process another step, another thread can continue with the remaining rows
             )
           )
         ){
//          return split_signed_chunk_step(dbt,cs);
        }else{
//        g_message("Not able to split min %"G_GUINT64_FORMAT" step: %"G_GUINT64_FORMAT" max: %"G_GUINT64_FORMAT, cs->integer_step.nmin, cs->integer_step.step, cs->integer_step.nmax);
          g_mutex_unlock(cs->integer_step.mutex);
          if (cs->multicolumn_integer_step.status==COMPLETED){
//            free_multicolumn_integer_step(cs);
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



gchar * update_multicolumn_integer_where(struct thread_data *td, union chunk_step * cs){
  (void)td;
  g_message("Updating Multicolumn Where");
  char *where = g_strdup_printf("%s AND %s" , cs->multicolumn_integer_step.prefix, cs->multicolumn_integer_step.chunk_functions.update_where(td,cs->multicolumn_integer_step.next_chunk_step));
  
  return where;
}

guint process_multicolumn_integer_chunk_step(struct thread_data *td, struct table_job *tj){
  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }
  g_mutex_lock(tj->chunk_step->multicolumn_integer_step.mutex);
  tj->chunk_step->multicolumn_integer_step.status = DUMPING_CHUNK;

  if (tj->chunk_step->integer_step.is_unsigned){
    tj->chunk_step->multicolumn_integer_step.type.unsign.cursor = tj->chunk_step->multicolumn_integer_step.type.unsign.min;
    tj->chunk_step->multicolumn_integer_step.estimated_remaining_steps = tj->chunk_step->multicolumn_integer_step.type.unsign.max - tj->chunk_step->multicolumn_integer_step.type.unsign.cursor;

  }else{
    tj->chunk_step->multicolumn_integer_step.type.sign.cursor = tj->chunk_step->multicolumn_integer_step.type.sign.min;
    tj->chunk_step->multicolumn_integer_step.estimated_remaining_steps = tj->chunk_step->multicolumn_integer_step.type.sign.max - tj->chunk_step->multicolumn_integer_step.type.sign.cursor;
  }

  g_mutex_unlock(tj->chunk_step->multicolumn_integer_step.mutex);
/*  if (tj->chunk_step->integer_step.nmin == tj->chunk_step->integer_step.nmax){
    return;
  }*/
//  g_message("CONTINUE");

  update_estimated_remaining_chunks_on_dbt(tj->dbt);
  if (tj->where)
    g_free(tj->where);
  tj->where=update_multicolumn_integer_where(td,tj->chunk_step);

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

  g_mutex_lock(tj->chunk_step->multicolumn_integer_step.mutex);
  if (tj->chunk_step->multicolumn_integer_step.status != COMPLETED)
    tj->chunk_step->multicolumn_integer_step.status = ASSIGNED;
  if (tj->chunk_step->multicolumn_integer_step.is_unsigned){
    tj->chunk_step->multicolumn_integer_step.type.unsign.min=tj->chunk_step->multicolumn_integer_step.type.unsign.cursor+1;
  }else{
    tj->chunk_step->multicolumn_integer_step.type.sign.min=tj->chunk_step->multicolumn_integer_step.type.sign.cursor+1;
  }
  g_mutex_unlock(tj->chunk_step->multicolumn_integer_step.mutex);
  return 0;
}



void process_multicolumn_integer_chunk(struct thread_data *td, struct table_job *tj){
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step;
  g_warning("Processing Multi Integer Step");

  // First step, we need this to process the one time prefix
  if (process_multicolumn_integer_chunk_step(td,tj)){
    g_message("Thread %d: Job has been cacelled",td->thread_id);
    return;
  }
  g_atomic_int_inc(dbt->chunks_completed);
  if (cs->multicolumn_integer_step.prefix)
    g_free(cs->multicolumn_integer_step.prefix);
  cs->multicolumn_integer_step.prefix=NULL;

  // Processing the remaining steps
  if (cs->multicolumn_integer_step.is_unsigned){
    g_mutex_lock(cs->multicolumn_integer_step.mutex);
    // Remaining unsigned steps
    while ( cs->multicolumn_integer_step.type.unsign.min < cs->multicolumn_integer_step.type.unsign.max ){
      g_mutex_unlock(cs->multicolumn_integer_step.mutex);
      if (process_multicolumn_integer_chunk_step(td,tj)){
        g_message("Thread %d: Job has been cacelled",td->thread_id);
        return;
      }
      g_atomic_int_inc(dbt->chunks_completed);
      g_mutex_lock(cs->multicolumn_integer_step.mutex);
    }
    g_mutex_unlock(cs->multicolumn_integer_step.mutex);
  }else{
    g_mutex_lock(cs->multicolumn_integer_step.mutex);
    // Remaining signed steps
    while ( cs->multicolumn_integer_step.type.sign.min < cs->multicolumn_integer_step.type.sign.max ){
      g_mutex_unlock(cs->multicolumn_integer_step.mutex);
      if (process_multicolumn_integer_chunk_step(td,tj)){
        g_message("Thread %d: Job has been cacelled",td->thread_id);
        return;
      }
      g_atomic_int_inc(dbt->chunks_completed);
      g_mutex_lock(cs->multicolumn_integer_step.mutex);
    }
    g_mutex_unlock(cs->multicolumn_integer_step.mutex);
  }
  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_lock(cs->multicolumn_integer_step.mutex);
  dbt->chunks=g_list_remove(dbt->chunks,cs);
  cs->multicolumn_integer_step.estimated_remaining_steps=0;
  if (g_list_length(dbt->chunks) == 0){
    g_message("Thread %d: Table %s completed ",td->thread_id,dbt->table);
    dbt->chunks=NULL;
  }
  // Chunk completed
  cs->integer_step.status=COMPLETED;
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_unlock(cs->multicolumn_integer_step.mutex);
}


