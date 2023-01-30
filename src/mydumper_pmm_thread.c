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

gint kill_pmm = 0;

void kill_pmm_thread(){
  kill_pmm=1;
}

void append_pmm_entry(GString *content, const gchar *key, GAsyncQueue * queue){
  if (queue != NULL)
    g_string_append_printf(content,"mydumper_queue{name=\"%s\"} %d\n",key,g_async_queue_length(queue));
}

void write_pmm_entries(const gchar* filename, GString *content, struct configuration* conf){
  g_string_set_size(content,0);
  append_pmm_entry(content,"schema_queue",      conf->schema_queue);
  append_pmm_entry(content,"non_innodb_queue",  conf->non_innodb_queue);
  append_pmm_entry(content,"innodb_queue",      conf->innodb_queue);
  append_pmm_entry(content,"post_data_queue",   conf->post_data_queue);
  append_pmm_entry(content,"ready",             conf->ready);
  append_pmm_entry(content,"unlock_tables",     conf->unlock_tables);
  append_pmm_entry(content,"pause_resume",      conf->pause_resume);
  append_pmm_entry(content,"stream_queue",      stream_queue);
  g_file_set_contents( filename , content->str, content->len, NULL);
}

void *pmm_thread(void *conf){
  const gchar* filename=g_strdup_printf("%s/mydumper.prom",pmm_path);
  GString *content = g_string_sized_new(200);
  while (!kill_pmm){
    write_pmm_entries(filename, content, (struct configuration*)conf);
    sleep(1);
  }
  write_pmm_entries(filename, content, conf);
  return NULL;
}
