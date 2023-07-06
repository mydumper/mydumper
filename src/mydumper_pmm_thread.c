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

#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <glib/gerror.h>
#include <gio/gio.h>
#include <mysql.h>
#include "mydumper_start_dump.h"
#include "mydumper_global.h"
#include "mydumper_stream.h"
gint kill_pmm = 0;
GMutex *pmm_mutex=NULL;
const gchar* filename=NULL;

void kill_pmm_thread(){
  g_mutex_lock(pmm_mutex);
  kill_pmm=1;
  remove(filename);
  g_mutex_unlock(pmm_mutex);
}

void append_pmm_entry(GString *content, const gchar *metric, const gchar *key, guint64 value){
  g_string_append_printf(content,"mydumper_%s{name=\"%s\"} %"G_GUINT64_FORMAT"\n",metric, key, value);
}

void append_pmm_entry_queue(GString *content, const gchar *key, GAsyncQueue * queue){
  if (queue != NULL)
    append_pmm_entry(content, "queue", key, g_async_queue_length(queue));
}

void append_pmm_entry_all_tables(GString *content){
  GList *tl=all_dbts;
  struct db_table *dbt=NULL;
  while (tl!=NULL){
    dbt=((struct db_table *)(tl->data));
    append_pmm_entry(content,"table",dbt->table, dbt->estimated_remaining_steps);
    tl=tl->next;
  }
}

void write_pmm_entries(GString *content, struct configuration* conf){
  g_string_set_size(content,0);
  append_pmm_entry_queue(content,"schema_queue",      conf->schema_queue);
  append_pmm_entry_queue(content,"non_innodb_queue",  conf->non_innodb_queue);
  append_pmm_entry_queue(content,"innodb_queue",      conf->innodb_queue);
  append_pmm_entry_queue(content,"post_data_queue",   conf->post_data_queue);
  append_pmm_entry_queue(content,"ready",             conf->ready);
  append_pmm_entry_queue(content,"unlock_tables",     conf->unlock_tables);
  append_pmm_entry_queue(content,"pause_resume",      conf->pause_resume);
  append_pmm_entry(content,"queueu", "stream",            get_stream_queue_length());
  append_pmm_entry(content,"object", "all_tables",        g_list_length(all_dbts));
  append_pmm_entry(content,"object", "innodb_tables",     g_list_length(innodb_table));
  append_pmm_entry(content,"object", "non_innodb_tables", g_list_length(non_innodb_table));
  append_pmm_entry_all_tables(content);
  g_file_set_contents( filename , content->str, content->len, NULL);
}

void *pmm_thread(void *conf){
  pmm_mutex = g_mutex_new();
  g_mutex_lock(pmm_mutex);
  filename=g_strdup_printf("%s/mydumper.prom",pmm_path);
  g_mutex_unlock(pmm_mutex);
  GString *content = g_string_sized_new(200);
  pmm_mutex = g_mutex_new();
  while (!kill_pmm){
    g_mutex_lock(pmm_mutex);
    if (kill_pmm){
      g_mutex_unlock(pmm_mutex);
      break;
    }
    write_pmm_entries(content, (struct configuration*)conf);
    g_mutex_unlock(pmm_mutex);
    sleep(1);
  }
  return NULL;
}
