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


#ifndef _src_mydumper_table_h
#define _src_mydumper_table_h
#include "mydumper_start_dump.h"
enum db_table_states{
  UNDEFINED,
  DEFINING,
  READY
};


struct db_table {
  gchar *key;
  struct database *database;
  char *table;
  char *table_filename;
  char *escaped_table;
  char *min;
  char *max;
  struct object_to_export object_to_export;
  GString *select_fields;
  gboolean complete_insert;
  GString *insert_statement;
  GString *load_data_header;
  GString *load_data_suffix;
  gboolean is_transactional;
  gboolean is_sequence;
  gboolean has_json_fields;
  char *character_set;
  guint64 rows_total;
  guint64 rows;
  guint64 estimated_remaining_steps;
  GMutex *rows_lock;
  struct function_pointer ** anonymized_function;
  gchar *where;
  gchar *limit;
  gchar *columns_on_insert;
  pcre2_code *partition_regex;
  guint num_threads;
  GList *chunks;
  GMutex *chunks_mutex;
  GAsyncQueue *chunks_queue;
  GList *primary_key;
  gchar *primary_key_separated_by_comma;
  gboolean multicolumn;
  gint * chunks_completed;
  gchar *data_checksum;
  gchar *schema_checksum;
  gchar *indexes_checksum;
  gchar *triggers_checksum;
  guint chunk_filesize;
  gboolean split_integer_tables;
  guint64 min_chunk_step_size;
  guint64 starting_chunk_step_size;
  guint64 max_chunk_step_size;
  gboolean is_fixed_length;
  enum db_table_states status;
  guint max_threads_per_table;
  guint current_threads_running;
};

#endif
void initialize_table();
void finalize_table();
void free_db_table(struct db_table * dbt);
gboolean new_db_table(struct db_table **d, MYSQL *conn, struct configuration *conf,
                      struct database *database, char *table, char *table_collation,
                      gboolean is_sequence);

