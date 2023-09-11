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
#include "mydumper_write.h"

#include "mydumper_integer_chunks.h"
#include "mydumper_char_chunks.h"
#include "mydumper_partition_chunks.h"
#include "mydumper_multicolumn_integer_chunks.h"

GAsyncQueue *give_me_another_innodb_chunk_step_queue;
GAsyncQueue *give_me_another_non_innodb_chunk_step_queue;

void initialize_chunk(){
  give_me_another_innodb_chunk_step_queue=g_async_queue_new();
  give_me_another_non_innodb_chunk_step_queue=g_async_queue_new();
  initialize_char_chunk();
}

void finalize_chunk(){
  g_async_queue_unref(give_me_another_innodb_chunk_step_queue); 
  g_async_queue_unref(give_me_another_non_innodb_chunk_step_queue);
}

void process_none_chunk(struct table_job *tj){
  write_table_job_into_file(tj);
}

/*
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
    case MULTICOLUMN_INTEGER:
      return get_next_multicolumn_integer_chunk(dbt);
      break;
    default:
      break;
  }
  return NULL;
}
*/


union chunk_step *get_initial_chunk (MYSQL *conn, enum chunk_type *chunk_type,  struct chunk_functions * chunk_functions, struct db_table *dbt, guint position, gchar *local_where) {

//  if (dbt->start_rows_per_file>0 && dbt->rows_in_sts > dbt->min_rows_per_file ){
    gchar *field=g_list_nth_data(dbt->primary_key, position);
    gchar *query = NULL;
    MYSQL_ROW row;
    MYSQL_RES *minmax = NULL;
    /* Get minimum/maximum */
    mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(`%s`),MAX(`%s`),LEFT(MIN(`%s`),1),LEFT(MAX(`%s`),1) FROM `%s`.`%s` %s %s %s %s",
                        is_mysql_like()
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, field, field, field, dbt->database->name, dbt->table, where_option || local_where ? "WHERE" : "", where_option ? where_option : "", where_option && local_where ? "AND" : "", local_where ? local_where : ""));
//  g_message("Query: %s", query);
    g_free(query);
    minmax = mysql_store_result(conn);

    if (!minmax){
      *chunk_type=NONE;
      goto cleanup;
    }

    row = mysql_fetch_row(minmax);

    MYSQL_FIELD *fields = mysql_fetch_fields(minmax);
    gulong *lengths = mysql_fetch_lengths(minmax);
    /* Check if all values are NULL */
    if (row[0] == NULL){
      *chunk_type=NONE;
      goto cleanup;
    }
  /* Support just bigger INTs for now, very dumb, no verify approach */
    guint64 abs;
    guint64 unmin, unmax;
    gint64 nmin, nmax;
    gchar *prefix=NULL;
    union chunk_step *cs = NULL;
    switch (fields[0].type) {
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_SHORT:

        unmin = strtoull(row[0], NULL, 10);
        unmax = strtoull(row[1], NULL, 10);
        nmin  = strtoll (row[0], NULL, 10);
        nmax  = strtoll (row[1], NULL, 10);

//        prefix= g_strdup_printf("`%s` IS NULL OR ", field) ;

        if (fields[0].flags & UNSIGNED_FLAG){
          abs=gint64_abs(unmax-unmin);
g_message("unsign.min: %"G_GUINT64_FORMAT" | unsign.max: %"G_GUINT64_FORMAT, unmin, unmax);

        }else{
          abs=gint64_abs(nmax-nmin);
g_message("sign.min: %"G_GINT64_FORMAT" | sign.max: %"G_GINT64_FORMAT, nmin, nmax);
        }

        if ( !local_where && dbt->multicolumn && FALSE){
          g_message("MULTICOLUMN??");
          if (dbt->rows_in_sts / abs > dbt->min_rows_per_file){
            *chunk_type=MULTICOLUMN_INTEGER;
            union type type;
            gchar *this_where=NULL;
            if ((fields[0].flags & UNSIGNED_FLAG)){
              type.unsign.min=unmin;
              type.unsign.cursor=type.unsign.min;
              type.unsign.max=unmax;
              this_where=g_strdup_printf(" `%s` = %"G_GUINT64_FORMAT, field, type.unsign.cursor);
            }else{
              type.sign.min=nmin;
              type.sign.cursor=type.sign.min;
              type.sign.max=nmax;
              this_where=g_strdup_printf(" `%s` = %"G_GINT64_FORMAT , field, type.sign.cursor);
            }
            cs=new_multicolumn_integer_step(TRUE, prefix, field, fields[0].flags & UNSIGNED_FLAG, type, 0, 0);
            cs->multicolumn_integer_step.status = UNASSIGNED;
            chunk_functions->update_where = &update_multicolumn_integer_where;
            chunk_functions->process = &process_multicolumn_integer_chunk;
            chunk_functions->get_next = &get_next_multicolumn_integer_chunk;
            cs->multicolumn_integer_step.next_chunk_step = get_initial_chunk(conn, &(cs->multicolumn_integer_step.chunk_type), &(cs->multicolumn_integer_step.chunk_functions), dbt, position+1, this_where);
            g_free(this_where);
            if (!cs->multicolumn_integer_step.next_chunk_step)
              m_error("cs->multicolumn_integer_step.next_chunk_step null");
            if (cs->multicolumn_integer_step.chunk_type == NONE){
              g_warning("Reverting integer");
              // revert multicolumn_integer and set INTEGER
            }
            if (minmax) mysql_free_result(minmax);
            return cs;
          }
        }

        if ( abs > dbt->min_rows_per_file){

          union type type;
          if ((fields[0].flags & UNSIGNED_FLAG)){
            type.unsign.min=unmin;
            type.unsign.max=unmax;
            cs=new_integer_step( TRUE, prefix, field, fields[0].flags & UNSIGNED_FLAG, type, 0, dbt->start_rows_per_file, 0, FALSE, FALSE);
          }else{
            type.sign.min=nmin;
            type.sign.max=nmax;
            cs=new_integer_step(TRUE, prefix, field, fields[0].flags & UNSIGNED_FLAG, type, 0, dbt->start_rows_per_file, 0, FALSE, FALSE);
          }
//          dbt->chunks=g_list_prepend(dbt->chunks,cs);
//          g_async_queue_push(dbt->chunks_queue, cs);
          *chunk_type=INTEGER;
          chunk_functions->process = &process_integer_chunk;
          chunk_functions->update_where = &update_integer_where;
          chunk_functions->get_next = &get_next_integer_chunk;
//          dbt->estimated_remaining_steps=cs->integer_step.estimated_remaining_steps;
        }else{
          *chunk_type=NONE;
        }
        if (minmax) mysql_free_result(minmax);
        return cs;
        break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VAR_STRING:
        *chunk_type=NONE;
        break;

        cs=new_char_step(conn, dbt->primary_key->data, 0, 0, row, lengths);
//        dbt->chunks=g_list_prepend(dbt->chunks,cs);
//        g_async_queue_push(dbt->chunks_queue, cs);
        *chunk_type=CHAR;
        chunk_functions->process = &process_char_chunk;
        chunk_functions->update_where = &update_char_where;
        chunk_functions->get_next = &get_next_char_chunk;
        if (minmax) mysql_free_result(minmax);
        return cs;
        break;
      default:
        *chunk_type=NONE;
        break;
      }
    cleanup:
      if (minmax)
        mysql_free_result(minmax);
//  }else
//    *chunk_type=NONE;
  return NULL;
}



void set_chunk_strategy_for_dbt(MYSQL *conn, struct db_table *dbt){

  dbt->chunk_functions.process=&process_none_chunk;
  dbt->chunk_functions.update_where=NULL;

/*
  GList *partitions=NULL;
  if (split_partitions || dbt->partition_regex){
    partitions = get_partitions_for_table(conn, dbt);
  }

*/
  g_mutex_lock(dbt->chunks_mutex);

  union chunk_step * cs=NULL;
  GList *partitions=NULL;
  if (split_partitions || dbt->partition_regex){
    partitions = get_partitions_for_table(conn, dbt);
    dbt->chunk_functions.process = &process_partition_chunk;
    dbt->chunk_type=PARTITION;
    dbt->chunk_functions.get_next = &get_next_partition_chunk;
    cs=new_real_partition_step(partitions,0,0);
  }else{
    if (dbt->start_rows_per_file>0 && dbt->rows_in_sts > dbt->min_rows_per_file ){
      cs = get_initial_chunk(conn, &(dbt->chunk_type), &(dbt->chunk_functions), dbt, 0, NULL);
    }else{
      dbt->chunk_type=NONE;
    }

//    g_debug("chunk_type: %d %p %p", dbt->chunk_type, dbt->chunk_functions.process, process_multicolumn_integer_chunk);

    dbt->chunks=g_list_prepend(dbt->chunks,cs);

    if ( dbt->chunk_type==INTEGER &&  dbt->min_rows_per_file==dbt->start_rows_per_file && dbt->max_rows_per_file==dbt->start_rows_per_file)
      dbt->chunk_filesize=0;
  }
  if (cs)
    g_async_queue_push(dbt->chunks_queue, cs);

  g_debug("Estrategy for `%s`.`%s` is: %d %d", dbt->database->name, dbt->table, dbt->chunk_type, INTEGER); 

    g_mutex_unlock(dbt->chunks_mutex);
}

void get_primary_key(MYSQL *conn, struct db_table * dbt, struct configuration *conf){
  MYSQL_RES *indexes = NULL;
  MYSQL_ROW row;
//  char *field = NULL;
  dbt->primary_key=NULL;
  /* first have to pick index, in future should be able to preset in
 *    * configuration too */
  gchar *query = g_strdup_printf("SHOW INDEX FROM `%s`.`%s`", dbt->database->name, dbt->table);
  mysql_query(conn, query);
  g_free(query);
  indexes = mysql_store_result(conn);

  if (indexes){
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[2], "PRIMARY") ) {
        /* Pick first column in PK, cardinality doesn't matter */
        dbt->primary_key=g_list_append(dbt->primary_key,g_strdup(row[4]));
//        field = g_strdup(row[4]);
//        break;
      }
    }
    if (dbt->primary_key)
      goto cleanup;

    /* If no PK found, try using first UNIQUE index */
    mysql_data_seek(indexes, 0);
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[1], "0")) {
        /* Again, first column of any unique index */
        dbt->primary_key=g_list_append(dbt->primary_key,g_strdup(row[4]));
//          field = g_strdup(row[4]);
//          break;
      }
    }
    
    if (dbt->primary_key)
      goto cleanup;

    /* Still unlucky? Pick any high-cardinality index */
    if (!dbt->primary_key && conf->use_any_index) {
      guint64 max_cardinality = 0;
      guint64 cardinality = 0;
      gchar *field=NULL;
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
      if (field)
        dbt->primary_key=g_list_append(dbt->primary_key,field);
    }
  }

cleanup:
  if (indexes)
    mysql_free_result(indexes);
//  return field;
}


gboolean get_next_dbt_and_chunk(struct db_table **dbt,union chunk_step **cs, GList **dbt_list){
  GList *iter=*dbt_list;
  union chunk_step *lcs;
  struct db_table *d;
  gboolean are_there_jobs_defining=FALSE;
  while (iter){
    d=iter->data;
    g_mutex_lock(d->chunks_mutex);
    g_message("Checking table: %s.%s", d->database->name, d->table);
    if (d->chunk_type != DEFINING){
      if (d->chunk_type == NONE){
        *dbt=iter->data;
        *dbt_list=g_list_remove(*dbt_list,d);
        g_mutex_unlock(d->chunks_mutex);
        break;
      }
      if (d->chunk_type == UNDEFINED){
        *dbt=iter->data;
        d->chunk_type = DEFINING;
        are_there_jobs_defining=TRUE;
        g_mutex_unlock(d->chunks_mutex);
        break;
      }
      g_message("Getting next from: %p", d->chunk_functions.get_next);
      lcs=d->chunk_functions.get_next(d);
      if (lcs!=NULL){
        *cs=lcs;
        *dbt=iter->data;
        g_mutex_unlock(d->chunks_mutex);
        break;
      }else{
        iter=iter->next;
        *dbt_list=g_list_remove(*dbt_list,d);
        g_mutex_unlock(d->chunks_mutex);
        continue;
      }
    }else{
      g_mutex_unlock(d->chunks_mutex);
      are_there_jobs_defining=TRUE;
    }
    iter=iter->next;
  }
  return are_there_jobs_defining;
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
  gboolean are_there_jobs_defining=FALSE;
  for (;;) {
    g_async_queue_pop(pop_queue);
    if (shutdown_triggered) {
      return; 
    }
    dbt=NULL;
    cs=NULL;
    are_there_jobs_defining=FALSE;
    are_there_jobs_defining=get_next_dbt_and_chunk(&dbt,&cs,table_list);

    if ((cs==NULL) && (dbt==NULL)){
      if (are_there_jobs_defining){
        g_debug("chunk_builder_thread: Are jobs defining... should we wait and try again later?");
        g_async_queue_push(pop_queue, GINT_TO_POINTER(1));
        usleep(1);
        continue;
      }
      g_debug("chunk_builder_thread: There were not job defined");
      break;
    }
    g_debug("chunk_builder_thread: Job will be enqueued");
    switch (dbt->chunk_type) {
    case INTEGER:
      create_job_to_dump_chunk(dbt, NULL, cs->integer_step.number, dbt->primary_key_separated_by_comma, cs, g_async_queue_push, push_queue, TRUE);
      break;
    case MULTICOLUMN_INTEGER:
      create_job_to_dump_chunk(dbt, NULL, cs->multicolumn_integer_step.number, dbt->primary_key_separated_by_comma, cs, g_async_queue_push, push_queue, TRUE);
      break;
    case CHAR:
      create_job_to_dump_chunk(dbt, NULL, cs->char_step.number, dbt->primary_key_separated_by_comma, cs, g_async_queue_push, push_queue, FALSE);
      break;
    case PARTITION:
      create_job_to_dump_chunk(dbt, NULL, cs->partition_step.number, dbt->primary_key_separated_by_comma, cs, g_async_queue_push, push_queue, TRUE);
      break;
    case NONE:
      create_job_to_dump_chunk(dbt, NULL, 0, dbt->primary_key_separated_by_comma, cs, g_async_queue_push, push_queue, TRUE);
      break;
    case DEFINING:
      create_job_to_determine_chunk_type(dbt, g_async_queue_push, push_queue);
      break;
    default:
      m_error("This should not happen");
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

