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
#include "mydumper_char_chunks.h"

guint char_chunk=0;
guint char_deep=0;


void initialize_char_chunk(){

  if (rows_per_file>0){
    char_chunk=char_chunk==0?num_threads:char_chunk;
    char_deep=char_deep==0?num_threads:char_deep;
  }
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

  cs->char_step.estimated_remaining_steps=1;
  cs->char_step.prefix=g_strdup_printf("`%s` IS NULL OR `%s` = '%s' OR", field, field, cs->char_step.cmin_escaped);

//  g_message("new_char_step: min: %s | max: %s ", cs->char_step.cmin_escaped, cs->char_step.cmax_escaped);

  cs->char_step.status = 0;
  return cs;
}


struct chunk_step_item *new_char_step_item(MYSQL *conn, gchar *field, /*GList *list,*/ guint deep, guint number, MYSQL_ROW row, gulong *lengths){
  struct chunk_step_item * csi = g_new0(struct chunk_step_item, 1);
  csi->chunk_step = new_char_step(conn, field, deep, number, row, lengths);
  csi->chunk_type=CHAR;
  csi->chunk_functions.process = &process_char_chunk;
  csi->chunk_functions.update_where = &update_char_where;
  csi->chunk_functions.get_next = &get_next_char_chunk; 
  return csi;
}


void next_chunk_in_char_step(union chunk_step * cs){
  cs->char_step.cmin_clen = cs->char_step.cursor_clen;
  cs->char_step.cmin_len = cs->char_step.cursor_len;
  cs->char_step.cmin = cs->char_step.cursor;
  cs->char_step.cmin_escaped = cs->char_step.cursor_escaped;
}

struct chunk_step_item *split_char_step( guint deep, guint number, union chunk_step *previous_cs){
  struct chunk_step_item *csi = g_new0(struct chunk_step_item,1);
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->char_step.prefix = NULL;
  cs->char_step.assigned=TRUE;
  cs->char_step.deep = deep;
  cs->char_step.number = number;
  cs->char_step.mutex=g_mutex_new();
  cs->char_step.step=rows_per_file;
  cs->char_step.field = g_strdup(previous_cs->char_step.field);
  cs->char_step.previous=previous_cs;
  cs->char_step.status = 0;
//  cs->char_step.list = list;
  csi->chunk_step=cs;
  csi->chunk_type=CHAR;
  csi->chunk_functions.process = &process_char_chunk;
  csi->chunk_functions.update_where = &update_char_where;
  csi->chunk_functions.get_next = &get_next_char_chunk; 
  return csi;
}

void free_char_step(union chunk_step * cs){
  g_mutex_lock(cs->char_step.mutex);
  g_free(cs->char_step.field);
  g_free(cs->char_step.prefix);
  g_mutex_unlock(cs->char_step.mutex);
  g_mutex_free(cs->char_step.mutex);
  g_free(cs);
}

// dbt->chunks_mutex is LOCKED
struct chunk_step_item *get_next_char_chunk(struct db_table *dbt){
  GList *l=dbt->chunks;
  struct chunk_step_item *csi=NULL;
  while (l!=NULL){
    csi=l->data;
    if (csi->chunk_step->char_step.mutex == NULL){
      g_message("This should not happen");
      l=l->next;
      continue;
    }
    
    g_mutex_lock(csi->chunk_step->char_step.mutex);
    if (!csi->chunk_step->char_step.assigned){
      csi->chunk_step->char_step.assigned=TRUE;
      g_mutex_unlock(csi->chunk_step->char_step.mutex);
      return csi;
    }

    if (csi->chunk_step->char_step.deep <= char_deep && g_strcmp0(csi->chunk_step->char_step.cmax, csi->chunk_step->char_step.cursor)!=0 && csi->chunk_step->char_step.status == 0){
      struct chunk_step_item * new_cs = split_char_step(
          csi->chunk_step->char_step.deep + 1, csi->chunk_step->char_step.number+pow(2,csi->chunk_step->char_step.deep), csi->chunk_step);
      csi->chunk_step->char_step.deep++;
      csi->chunk_step->char_step.status = 1;
      new_cs->chunk_step->char_step.assigned=TRUE;
      g_mutex_unlock(csi->chunk_step->char_step.mutex);
      return new_cs;
    }else{
//      g_message("Not able to split because %d > %d | %s == %s | %d != 0", cs->char_step.deep,num_threads, cs->char_step.cmax, cs->char_step.cursor, cs->char_step.status);
    }
    g_mutex_unlock(csi->chunk_step->char_step.mutex);
    l=l->next;
  }
  return NULL;
}

gchar * get_escaped_middle_char(MYSQL *conn, gchar *c1, guint c1len, gchar *c2, guint c2len, guint part){
  guint cresultlen = c1len < c2len ? c1len: c2len;
  gchar *cresult = g_new(gchar, cresultlen + 1);
  guint i =0;
  guchar cu1=c1[0],cu2=c2[0];
//  g_message("get_escaped_middle_char: %u %u %u %d", cu1, abs(cu2-cu1) , cu2, part);
  for(i=0; i < cresultlen; i++){
    cu1=c1[i];
    cu2=c2[i];
    if (cu2!=cu1)
      cresult[i]=(cu2>cu1?cu1:cu2)+abs(cu2-cu1)/part;
    else
      cresultlen=i;
  }
  cu1=c1[0];cu2=c2[0];
//  guchar cur=cresult[0];
//  g_message("get_escaped_middle_char: %u %u %u %d", cu1, cur , cu2, part);
  cresult[cresultlen]='\0';

  gchar *escapedresult=g_new(char, cresultlen * 2 + 1);
  mysql_real_escape_string(conn, escapedresult, cresult, cresultlen);
  g_free(cresult);
  return escapedresult;
}

gchar* update_cursor (MYSQL *conn, struct table_job *tj){
  struct chunk_step_item *csi= tj->chunk_step_item;
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  /* Get minimum/maximum */
  gchar * middle = get_escaped_middle_char(conn, csi->chunk_step->char_step.cmax, csi->chunk_step->char_step.cmax_clen, csi->chunk_step->char_step.cmin, csi->chunk_step->char_step.cmin_clen, tj->char_chunk_part>0?tj->char_chunk_part:1);//num_threads*(num_threads - cs->char_step.deep>0?num_threads-cs->char_step.deep:1));
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE '%s' <= `%s` AND '%s' <= `%s` AND `%s` <= '%s' ORDER BY `%s` LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        (gchar*)tj->dbt->primary_key->data, tj->dbt->database->name, tj->dbt->table, csi->chunk_step->char_step.cmin_escaped, (gchar*)tj->dbt->primary_key->data, middle, (gchar*)tj->dbt->primary_key->data, (gchar*)tj->dbt->primary_key->data, csi->chunk_step->char_step.cmax_escaped, (gchar*)tj->dbt->primary_key->data));
  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax){
//    g_message("No middle point");
    goto cleanup;
  }
  row = mysql_fetch_row(minmax);

  if (row==NULL){
//    g_message("No middle point");
cleanup:
    csi->chunk_step->char_step.cursor_clen = csi->chunk_step->char_step.cmax_clen;
    csi->chunk_step->char_step.cursor_len = csi->chunk_step->char_step.cmax_len;
    csi->chunk_step->char_step.cursor = csi->chunk_step->char_step.cmax;
    csi->chunk_step->char_step.cursor_escaped = csi->chunk_step->char_step.cmax_escaped;
    return NULL;
  }
//  guchar d=middle[0];
//  g_message("updated point: `%s` | `%c` %u", middle, middle[0], d);
  gulong *lengths = mysql_fetch_lengths(minmax);

  tj->char_chunk_part--;

  if (g_strcmp0(row[0], csi->chunk_step->char_step.cmax)!=0 && g_strcmp0(row[0], csi->chunk_step->char_step.cmin)!=0){
    csi->chunk_step->char_step.cursor_clen = lengths[0];
    csi->chunk_step->char_step.cursor_len = lengths[0]+1;
    csi->chunk_step->char_step.cursor = g_new(char, csi->chunk_step->char_step.cursor_len);
    g_strlcpy(csi->chunk_step->char_step.cursor, row[0], csi->chunk_step->char_step.cursor_len);
    csi->chunk_step->char_step.cursor_escaped = g_new(char, lengths[0] * 2 + 1);
    mysql_real_escape_string(conn, csi->chunk_step->char_step.cursor_escaped, row[0], lengths[0]);
  }else{
    csi->chunk_step->char_step.cursor_clen = csi->chunk_step->char_step.cmax_clen;
    csi->chunk_step->char_step.cursor_len = csi->chunk_step->char_step.cmax_len;
    csi->chunk_step->char_step.cursor = csi->chunk_step->char_step.cmax;
    csi->chunk_step->char_step.cursor_escaped = csi->chunk_step->char_step.cmax_escaped;
  }

  return NULL;
}

gboolean get_new_minmax (struct thread_data *td, struct db_table *dbt, union chunk_step *cs){
//  g_message("Thread %d: get_new_minmax", td->thread_id);
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  union chunk_step * previous=cs->char_step.previous;
  /* Get minimum/maximum */

  gchar *middle=get_escaped_middle_char(td->thrconn, previous->char_step.cmax, previous->char_step.cmax_clen, previous->char_step.cursor != NULL ? previous->char_step.cursor: previous->char_step.cmin, previous->char_step.cursor != NULL ?previous->char_step.cursor_len:previous->char_step.cmin_clen, char_chunk);
//  guchar d=middle[0];
//  g_message("Middle point: `%s` | `%c` %u", middle, middle[0], d);
  mysql_query(td->thrconn, query = g_strdup_printf(
                        "SELECT %s `%s` FROM `%s`.`%s` WHERE `%s` > (SELECT `%s` FROM `%s`.`%s` WHERE `%s` > '%s' ORDER BY `%s` LIMIT 1) AND '%s' < `%s` AND `%s` < '%s' ORDER BY `%s` LIMIT 1",
                        is_mysql_like() ? "/*!40001 SQL_NO_CACHE */": "",
                        (gchar*)dbt->primary_key->data, dbt->database->name, dbt->table, (gchar*)dbt->primary_key->data, (gchar*)dbt->primary_key->data, dbt->database->name, dbt->table, (gchar*)dbt->primary_key->data, middle, (gchar*)dbt->primary_key->data, previous->char_step.cursor_escaped!=NULL?previous->char_step.cursor_escaped:previous->char_step.cmin_escaped, (gchar*)dbt->primary_key->data, (gchar*)dbt->primary_key->data, previous->char_step.cmax_escaped, (gchar*)dbt->primary_key->data));

//g_message("get_new_minmax Query: %s", query);

  g_free(query);
  minmax = mysql_store_result(td->thrconn);

  if (!minmax){
    mysql_free_result(minmax);
//    g_message("No middle point");
    return FALSE;
  }

  row = mysql_fetch_row(minmax);
  if (row == NULL){
    mysql_free_result(minmax);
//    g_message("No middle point");
    return FALSE;
  }
//  guchar c=row[0][0];
//  g_message("First char %u ", c);
  gulong *lengths = mysql_fetch_lengths(minmax);

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

  previous->char_step.status=0;

  cs->char_step.cmin_clen = lengths[0];
  cs->char_step.cmin_len = lengths[0]+1;
  cs->char_step.cmin = g_new(char, cs->char_step.cmin_len);
  g_strlcpy(cs->char_step.cmin, row[0], cs->char_step.cmin_len);
  cs->char_step.cmin_escaped = g_new(char, lengths[0] * 2 + 1);
  mysql_real_escape_string(td->thrconn, cs->char_step.cmin_escaped, row[0], lengths[0]);

  mysql_free_result(minmax);
  return TRUE;
}

guint process_char_chunk_step(struct thread_data *td, struct table_job *tj){
  check_pause_resume(td);
  if (shutdown_triggered) {
    return 1;
  }
  g_mutex_lock(tj->chunk_step_item->chunk_step->char_step.mutex);
//  update_estimated_remaining_chunks_on_dbt(tj->dbt);

  update_where_on_table_job(td, tj);
  g_mutex_unlock(tj->chunk_step_item->chunk_step->char_step.mutex);

//  message_dumping_data(td,tj);

  GDateTime *from = g_date_time_new_now_local();
  write_table_job_into_file(tj);
  GDateTime *to = g_date_time_new_now_local();



  GTimeSpan diff=g_date_time_difference(to,from)/G_TIME_SPAN_SECOND;


  if (diff > 2){
    tj->chunk_step_item->chunk_step->char_step.step=tj->chunk_step_item->chunk_step->char_step.step  / 2;
    tj->chunk_step_item->chunk_step->char_step.step=tj->chunk_step_item->chunk_step->char_step.step<min_chunk_step_size?min_chunk_step_size:tj->chunk_step_item->chunk_step->char_step.step;
//    g_message("Decreasing time: %ld | %ld", diff, tj->chunk_step->char_step.step);
  }else if (diff < 1){
    tj->chunk_step_item->chunk_step->char_step.step=tj->chunk_step_item->chunk_step->char_step.step  * 2;
    if (max_chunk_step_size!=0)
      tj->chunk_step_item->chunk_step->char_step.step=tj->chunk_step_item->chunk_step->char_step.step>max_chunk_step_size?max_chunk_step_size:tj->chunk_step_item->chunk_step->char_step.step;
//    g_message("Increasing time: %ld | %ld", diff, tj->chunk_step->char_step.step);
  }


  if (tj->chunk_step_item->chunk_step->char_step.prefix)
    g_free(tj->chunk_step_item->chunk_step->char_step.prefix);
  tj->chunk_step_item->chunk_step->char_step.prefix=NULL;
  g_mutex_lock(tj->chunk_step_item->chunk_step->char_step.mutex);
  next_chunk_in_char_step(tj->chunk_step_item->chunk_step);
  g_mutex_unlock(tj->chunk_step_item->chunk_step->char_step.mutex);
  return 0;
}


void process_char_chunk(struct table_job *tj){
  struct thread_data *td = tj->td;
  struct db_table *dbt = tj->dbt;
  union chunk_step *cs = tj->chunk_step_item->chunk_step, *previous = cs->char_step.previous;
  gboolean cont=FALSE;
  while ((cs->char_step.previous != NULL) || (g_strcmp0(cs->char_step.cmax, cs->char_step.cursor) )){

    if (cs->char_step.previous != NULL){
      g_mutex_lock(cs->char_step.mutex);
      cont=get_new_minmax(td, dbt, cs);
      g_mutex_unlock(cs->char_step.mutex);
      if (cont == TRUE){
        
        cs->char_step.previous=NULL;
        g_mutex_lock(dbt->chunks_mutex);
        dbt->chunks=g_list_append(dbt->chunks,cs);
        g_mutex_unlock(dbt->chunks_mutex);
//        g_mutex_unlock(previous->char_step.mutex);
//        g_mutex_unlock(cs->char_step.mutex);
      }else{
        g_mutex_lock(dbt->chunks_mutex);
        previous->char_step.status=0;
        g_mutex_unlock(dbt->chunks_mutex);
//        g_mutex_unlock(previous->char_step.mutex);
        return;
      }
    }else{
      if (g_strcmp0(cs->char_step.cmax, cs->char_step.cursor)!=0){
        if (process_char_chunk_step(td,tj)){
          g_message("Thread %d: Job has been cacelled",td->thread_id);
          return;
        }
      }else{
        g_mutex_lock(cs->char_step.mutex);
        cs->char_step.status=2;
        g_mutex_unlock(cs->char_step.mutex);
        break;
      }
    }
  }
  if (g_strcmp0(cs->char_step.cursor, cs->char_step.cmin)!=0)
    if (process_char_chunk_step(td,tj)){
      g_message("Thread %d: Job has been cacelled",td->thread_id);
      return;
    }
  g_mutex_lock(dbt->chunks_mutex);
  g_mutex_lock(cs->char_step.mutex);
//  dbt->chunks=g_list_remove(dbt->chunks,cs);
  g_mutex_unlock(cs->char_step.mutex);
  g_mutex_unlock(dbt->chunks_mutex);
}

gchar * update_char_where(union chunk_step * chunk_step){
  gchar *where=NULL;
//  if (td != NULL){
    if (chunk_step->char_step.cmax == NULL){
      where=g_strdup_printf("(%s(`%s` >= '%s'))",
                        chunk_step->char_step.prefix?chunk_step->char_step.prefix:"",
                        chunk_step->char_step.field, chunk_step->char_step.cmin_escaped
                        );
    }else{
      where=g_strdup_printf("(%s('%s' < `%s` AND `%s` <= '%s'))",
                        chunk_step->char_step.prefix?chunk_step->char_step.prefix:"",
                        chunk_step->char_step.cmin_escaped, chunk_step->char_step.field,
                        chunk_step->char_step.field, chunk_step->char_step.cursor_escaped
                        );
    }
//  }
  return where;
}


