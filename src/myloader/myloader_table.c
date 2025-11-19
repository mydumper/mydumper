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

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include "myloader.h"
#include "myloader_stream.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_control_job.h"
#include "myloader_restore_job.h"
#include "myloader_global.h"
#include "myloader_arguments.h"
#include "myloader_database.h"
#include "myloader_directory.h"
#include "myloader_worker_schema.h"


//GString *change_master_statement=NULL;
struct configuration *__conf;
extern gboolean schema_sequence_fix;


void initialize_table(struct configuration *c){
  __conf=c;
}

gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash){
  gchar *a_key=build_dbt_key(((struct db_table *)a)->database->target_database,((struct db_table *)a)->table_filename);
  gchar *b_key=build_dbt_key(((struct db_table *)b)->database->target_database,((struct db_table *)b)->table_filename);
  struct db_table * a_val=g_hash_table_lookup(table_hash,a_key);
  struct db_table * b_val=g_hash_table_lookup(table_hash,b_key);
  g_free(a_key);
  g_free(b_key);
  return a_val->rows < b_val->rows;
}

gint compare_dbt_short(gconstpointer a, gconstpointer b){
  return ((struct db_table *)a)->rows < ((struct db_table *)b)->rows;
}

gboolean append_new_db_table( struct db_table **p_dbt, struct database *_database, gchar *source_table_name, gchar *table_filename){
  struct db_table *dbt=NULL;
  gchar *lkey=build_dbt_key(_database->database_name_in_filename, table_filename);
  trace("Searching for dbt with key: %s", lkey);
//  dbt=g_hash_table_lookup(__conf->table_hash,lkey);
  gboolean r = dbt == NULL;
  if (r){
    g_mutex_lock(__conf->table_hash_mutex);
    dbt=g_hash_table_lookup(__conf->table_hash,lkey);
    r = dbt == NULL;
    if (r){
      trace("New dbt: %s %s %s", _database->target_database, table_filename,source_table_name);
      dbt=g_new(struct db_table,1);
      dbt->database=_database;
      dbt->create_table_name=NULL;
      dbt->table_filename=g_strdup(table_filename);
      dbt->source_table_name=g_strdup(source_table_name);
//      dbt->rows=number_rows;
      dbt->rows=0;
      dbt->rows_inserted=0;
      dbt->restore_job_list = NULL;
      parse_object_to_export(&(dbt->object_to_export),g_hash_table_lookup(conf_per_table.all_object_to_export, lkey));
			dbt->current_threads=0;
      dbt->max_threads=max_threads_per_table>num_threads?num_threads:max_threads_per_table;
      dbt->max_connections_per_job=0;
      dbt->retry_count= retry_count;
      dbt->mutex=g_mutex_new();
//      dbt->indexes=alter_table_statement;
      dbt->indexes=NULL;
      dbt->start_data_time=NULL;
      dbt->finish_data_time=NULL;
      dbt->start_index_time=NULL;
      dbt->finish_time=NULL;
      dbt->schema_state=NOT_FOUND;
      dbt->index_enqueued=FALSE;
      dbt->remaining_jobs = 0;
      dbt->constraints=NULL;
      dbt->count=0;
      g_hash_table_insert(__conf->table_hash, lkey, dbt);
      trace("g_hash_table_insert(conf->table_hash, %s", lkey);
      refresh_table_list_without_table_hash_lock(__conf, FALSE);
      dbt->schema_checksum=NULL;
      dbt->triggers_checksum=NULL;
      dbt->indexes_checksum=NULL;
      dbt->data_checksum=NULL;
      dbt->is_view=FALSE;
      dbt->is_sequence=FALSE;
    }else{
//      g_free(source_table_name);
      g_free(lkey);
//      if (number_rows>0) dbt->rows=number_rows;
//      if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
    }
    g_mutex_unlock(__conf->table_hash_mutex);
  }else{
    //g_free(source_table_name);
      g_free(lkey);
//      if (number_rows>0) dbt->rows=number_rows;
//      if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
  }

  if (!dbt->source_table_name && source_table_name){
    dbt->source_table_name=g_strdup(source_table_name);
    trace("Setting source_table_name on dbt: %s %s %s", _database->target_database, table_filename, dbt->source_table_name);
  }
  g_free(source_table_name);
  *p_dbt=dbt;
  return r;
}

void table_lock(struct db_table *dbt){
  trace("table_lock:: %s %s", dbt->database->target_database, dbt->source_table_name);
  g_mutex_lock(dbt->mutex);
}

void table_unlock(struct db_table *dbt){
  trace("table_unlock:: %s %s", dbt->database->target_database, dbt->source_table_name);
  g_mutex_unlock(dbt->mutex);
}


void free_dbt(struct db_table * dbt){
  g_free(dbt->table_filename);
//  if (dbt->constraints!=NULL) g_string_free(dbt->constraints,TRUE);
  dbt->constraints = NULL; // It should be free after constraint is executed
//  g_async_queue_unref(dbt->queue);
  g_mutex_clear(dbt->mutex); 
  
}

void free_table_hash(GHashTable *table_hash){
  g_mutex_lock(__conf->table_hash_mutex);
  GHashTableIter iter;
  gchar * lkey;
  if (table_hash){
    g_hash_table_iter_init ( &iter, table_hash );
    struct db_table *dbt=NULL;
    while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
      free_dbt(dbt);
      g_free((gchar*)lkey);
      g_free(dbt);
    }
  } 
  g_mutex_unlock(__conf->table_hash_mutex);
}

