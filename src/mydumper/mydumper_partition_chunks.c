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
#include "mydumper_write.h"


gboolean split_partitions = FALSE;
gchar *partition_regex = FALSE;

struct chunk_step_item *get_next_partition_chunk(struct db_table *dbt);

void process_partition_chunk(struct table_job *tj, struct chunk_step_item *csi){
  union chunk_step *cs = csi->chunk_step;
  gchar *partition=NULL;
  while (cs->partition_step.list != NULL){
    if (shutdown_triggered) {
      return;
    }
    g_mutex_lock(csi->mutex);
    partition=g_strdup_printf(" PARTITION (%s) ",(char*)(cs->partition_step.list->data));
    cs->partition_step.list= cs->partition_step.list->next;
    g_mutex_unlock(csi->mutex);
    tj->partition = partition;
    write_table_job_into_file(tj);
    g_free(partition);
  }
}

union chunk_step *new_real_partition_step(GList *partition){
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->partition_step.list = partition;
  return cs;
}

struct chunk_step_item *new_real_partition_step_item(GList *partition, guint deep, guint part){
  struct chunk_step_item *csi = g_new0(struct chunk_step_item, 1);
  csi->chunk_type=PARTITION;
  csi->chunk_step = new_real_partition_step(partition);
  csi->chunk_functions.process = &process_partition_chunk;
  csi->chunk_functions.get_next = &get_next_partition_chunk;
  csi->chunk_functions.free=NULL;
  csi->status= UNASSIGNED;
  csi->mutex = g_mutex_new();
  csi->deep = deep;
  csi->part = part;
  return csi;
}


struct chunk_step_item *get_next_partition_chunk(struct db_table *dbt){
//  g_mutex_lock(dbt->chunks_mutex);
  GList *l=dbt->chunks;
  struct chunk_step_item *csi=NULL;
  while (l!=NULL){
    csi=l->data;
    g_mutex_lock(csi->mutex);
    if (csi->status==UNASSIGNED){
      csi->status=ASSIGNED;
      g_mutex_unlock(csi->mutex);
//      g_mutex_unlock(dbt->chunks_mutex);
      return csi;
    }

    if (g_list_length (csi->chunk_step->partition_step.list) > 3 ){
      guint pos=g_list_length (csi->chunk_step->partition_step.list) / 2;
      GList *new_list=g_list_nth(csi->chunk_step->partition_step.list,pos);
      new_list->prev->next=NULL;
      new_list->prev=NULL;
      struct chunk_step_item * new_csi = new_real_partition_step_item(new_list, csi->deep+1, csi->part+pow(2,csi->deep));
      csi->deep++;
      new_csi->status=ASSIGNED;
      dbt->chunks=g_list_append(dbt->chunks,new_csi);

      g_mutex_unlock(csi->mutex);
 //     g_mutex_unlock(dbt->chunks_mutex);
      return new_csi;
    }
    g_mutex_unlock(csi->mutex);
    l=l->next;
  }
//  g_mutex_unlock(dbt->chunks_mutex);
  return NULL;
}

GList * get_partitions_for_table(MYSQL *conn, struct db_table *dbt){

  gchar *query = g_strdup_printf("select PARTITION_NAME from information_schema.PARTITIONS where PARTITION_NAME is not null and TABLE_SCHEMA='%s' and TABLE_NAME='%s'", dbt->database->name, dbt->table);
  MYSQL_RES *res=m_store_result(conn,query, NULL,"Partitioning is not supported", NULL);
  g_free(query);

  if (res == NULL)
    //partitioning is not supported
    return NULL;

  GList *partition_list = NULL;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    if ( (!dbt->partition_regex && eval_partition_regex(row[0])) || (dbt->partition_regex && eval_pcre_regex(dbt->partition_regex, row[0]) ) )
      partition_list = g_list_append(partition_list, strdup(row[0]));
  }
  mysql_free_result(res);

  return partition_list;
}
