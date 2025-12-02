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

#include <glib/gstdio.h>

#include "mydumper_global.h"
#include "mydumper_stream.h"

void append_pmm_entry(GString *content, const gchar *metric, const gchar *_key, guint64 value){
  g_string_append_printf(content,"mydumper_%s{name=\"%s\"} %"G_GUINT64_FORMAT"\n",metric, _key, value);
}

void append_pmm_entry_queue(GString *content, const gchar *_key, GAsyncQueue * queue){
  if (queue != NULL)
    append_pmm_entry(content, "queue", _key, g_async_queue_length(queue));
}

void append_pmm_entry_all_tables(GString *content){
  struct db_table *dbt=NULL;
  GHashTableIter iter;
  g_hash_table_iter_init ( &iter, all_dbts );
  gchar *lkey;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
    append_pmm_entry(content,"table",dbt->table, dbt->estimated_remaining_steps);
  }
}

void write_mydumper_pmm_entries(const gchar* filename, GString *content, struct configuration* conf){
  g_string_set_size(content,0);
  append_pmm_entry_queue(content,"schema_queue",      conf->schema_queue);
  append_pmm_entry_queue(content,"non_transactional_queue",  conf->non_transactional.queue);
  append_pmm_entry_queue(content,"non_transactional_defer_queue", conf->non_transactional.defer);
  append_pmm_entry_queue(content,"transactional_queue",      conf->transactional.queue);
  append_pmm_entry_queue(content,"integer_queue",     conf->transactional.defer);
  append_pmm_entry_queue(content,"post_data_queue",   conf->post_data_queue);
  append_pmm_entry_queue(content,"ready",             conf->ready);
  append_pmm_entry_queue(content,"unlock_tables",     conf->unlock_tables);
  append_pmm_entry_queue(content,"pause_resume",      conf->pause_resume);
  append_pmm_entry(content,"queueu", "stream",            get_stream_queue_length());
  append_pmm_entry(content,"object", "all_tables",        g_hash_table_size(all_dbts));
  // Use cached count for O(1) access instead of O(n) g_list_length()
  g_mutex_lock(transactional_table->mutex);
  guint trans_count = transactional_table->count;
  g_mutex_unlock(transactional_table->mutex);
  g_mutex_lock(non_transactional_table->mutex);
  guint non_trans_count = non_transactional_table->count;
  g_mutex_unlock(non_transactional_table->mutex);
  append_pmm_entry(content,"object", "transactional_tables",     trans_count);
  append_pmm_entry(content,"object", "non_transactional_tables", non_trans_count);
  append_pmm_entry_all_tables(content);
  g_file_set_contents( filename , content->str, content->len, NULL);
}

