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

union chunk_step *new_multicolumn_integer_step(gboolean include_null, gchar *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, guint64 number){
  g_message("New Multi Integer Step");
  union chunk_step * cs = g_new0(union chunk_step, 1);
  if (include_null)
    cs->integer_step.include_null = g_strdup_printf("`%s` IS NULL OR", field);
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
//  g_mutex_lock(dbt->chunks_mutex);
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
//        g_mutex_unlock(dbt->chunks_mutex);
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
          g_mutex_unlock(cs->multicolumn_integer_step.mutex);
        }else{
          g_mutex_unlock(cs->multicolumn_integer_step.mutex);
          if (cs->multicolumn_integer_step.status==COMPLETED){
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
          g_mutex_unlock(cs->multicolumn_integer_step.mutex);
        }else{
          g_mutex_unlock(cs->multicolumn_integer_step.mutex);
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
//  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}



gchar * update_multicolumn_integer_where(union chunk_step * cs){

//  cs->multicolumn_integer_step.next_chunk_step->prefix = 


  if (cs->multicolumn_integer_step.is_unsigned){
     ((struct integer_step *) cs->multicolumn_integer_step.next_chunk_step)->prefix = g_strdup_printf("%s %s = %"G_GUINT64_FORMAT,
                                cs->multicolumn_integer_step.prefix ? cs->multicolumn_integer_step.prefix: "",
                                cs->multicolumn_integer_step.field, cs->multicolumn_integer_step.type.unsign.cursor
                                );
  }else{
     ((struct integer_step *)cs->multicolumn_integer_step.next_chunk_step)->prefix = g_strdup_printf("%s %s = %"G_GINT64_FORMAT ,
                                cs->multicolumn_integer_step.prefix ? cs->multicolumn_integer_step.prefix: "",
                                cs->multicolumn_integer_step.field, cs->multicolumn_integer_step.type.sign.cursor
                                );
  }


  return cs->multicolumn_integer_step.chunk_functions.update_where(cs->multicolumn_integer_step.next_chunk_step) ;

}

guint process_multicolumn_integer_chunk_step(struct table_job *tj){
  struct thread_data *td = tj->td;
  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }
  g_mutex_lock(tj->chunk_step->multicolumn_integer_step.mutex);
  tj->chunk_step->multicolumn_integer_step.status = DUMPING_CHUNK;

  if (tj->chunk_step->multicolumn_integer_step.is_unsigned){
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
//  if (tj->where)
//    g_free(tj->where);

  g_string_set_size(tj->where,0);



//  g_string_append(tj->where,update_multicolumn_integer_where(td,tj->chunk_step));

//  update_where_on_table_job(td, tj);
//  message_dumping_data(td,tj);

//  write_table_job_into_file(tj);

  update_multicolumn_integer_where(tj->chunk_step);

  tj->chunk_step->multicolumn_integer_step.chunk_functions.process(tj); 


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



void process_multicolumn_integer_chunk(struct table_job *tj){
  struct thread_data *td = tj->td;
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step;

  // First step, we need this to process the one time prefix
  if (process_multicolumn_integer_chunk_step(tj)){
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
      if (process_multicolumn_integer_chunk_step(tj)){
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
      if (process_multicolumn_integer_chunk_step(tj)){
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
  cs->multicolumn_integer_step.status=COMPLETED;
  g_mutex_unlock(dbt->chunks_mutex);
  g_mutex_unlock(cs->multicolumn_integer_step.mutex);
}


