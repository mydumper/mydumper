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


#ifndef _src_myloader_table_h
#define _src_myloader_table_h

#include <stdio.h>
#include "myloader.h"

struct db_table {
  struct database * database;
  gchar *source_table_name;
  gchar *table_filename;
  gchar *create_table_name;
  struct object_to_export object_to_export;
  guint64 rows;
  guint64 rows_inserted;
  GList * restore_job_list;
  guint current_threads;
  guint max_threads;
  guint max_connections_per_job;
  guint retry_count;
  GMutex *mutex;
  GString *indexes;
  GString *constraints;
  guint count;
  enum schema_status schema_state;
  gboolean index_enqueued;
  GDateTime * start_data_time;
  GDateTime * finish_data_time;
  GDateTime * start_index_time;
  GDateTime * finish_time;
  gint remaining_jobs;
  gchar *data_checksum;
  gchar *schema_checksum;
  gchar *indexes_checksum;
  gchar *triggers_checksum;
  gboolean is_view;
  gboolean is_sequence;
};


void free_table_hash(GHashTable *table_hash);
gboolean append_new_db_table( struct db_table **p_dbt, struct database *_database, gchar *source_table_name, gchar *table_filename);
gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash);
gint compare_dbt_short(gconstpointer a, gconstpointer b);
#endif
