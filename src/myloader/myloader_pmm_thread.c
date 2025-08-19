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
#include "myloader.h"
#include "myloader_global.h"

gint kill_pmm = 0;

void kill_pmm_thread(){
  kill_pmm=1;
}

void append_pmm_entry(GString *content, const gchar *_key, GAsyncQueue * queue){
  if (queue != NULL)
    g_string_append_printf(content,"myloader_queue{name=\"%s\"} %d\n",_key,g_async_queue_length(queue));
}

void append_pmm_entry_tables(GString *content,struct configuration *conf){
  (void) content;
  GHashTableIter iter;
  gchar * lkey;
  if (conf->table_hash){
    g_hash_table_iter_init ( &iter, conf->table_hash );
    struct db_table *dbt=NULL;
    while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
//      g_string_append_printf(content,"myloader_table{name=\"%s\"} %d\n",lkey,stream?(guint)g_list_length(dbt->restore_job_list):(guint)g_async_queue_length(dbt->queue));
    }
  }
}

void write_pmm_entries(const gchar* filename, GString *content, struct configuration* conf){
  g_string_set_size(content,0);
  append_pmm_entry(content,"database_queue",    conf->database_queue);
  append_pmm_entry(content,"table_queue",       conf->table_queue);
  append_pmm_entry(content,"retry_queue",       conf->retry_queue);
  append_pmm_entry(content,"ready",             conf->ready);
  append_pmm_entry(content,"data_queue",        conf->data_queue);
  append_pmm_entry(content,"post_table_queue",  conf->post_table_queue);
  append_pmm_entry(content,"post_queue",        conf->post_queue);
  append_pmm_entry(content,"pause_resume",      conf->pause_resume);
//  append_pmm_entry(content,"stream_queue",      conf->stream_queue);
  append_pmm_entry(content,"ready",             conf->ready);
  append_pmm_entry_tables(content,conf);
  g_file_set_contents( filename , content->str, content->len, NULL);
}

void *pmm_thread(void *conf){
  const gchar* filename=g_strdup_printf("%s/myloader.prom",pmm_path);
  GString *content = g_string_sized_new(200);
  while (!kill_pmm){
    write_pmm_entries(filename, content, (struct configuration*)conf);
    sleep(1);
  }
  write_pmm_entries(filename, content, conf);
  return NULL;
}
