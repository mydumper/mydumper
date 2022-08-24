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

    Authors:        David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <glib.h>
#include "myloader_control_job.h"
#include "myloader_intermediate_queue.h"

extern gchar *directory;
extern guint num_threads;

void *process_directory(struct configuration *conf){
  GError *error = NULL;
  const gchar *filename = NULL;
  conf->table_hash = g_hash_table_new ( g_str_hash, g_str_equal );
  GDir *dir = g_dir_open(directory, 0, &error);
  while ((filename = g_dir_read_name(dir))){
    intermediate_queue_new(g_strdup(filename));
  }
  intermediate_queue_end();
  guint n=0;
  for (n = 0; n < num_threads ; n++) {
    g_async_queue_push(conf->data_queue,       new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->post_table_queue, new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->post_queue,       new_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->view_queue,       new_job(JOB_SHUTDOWN,NULL,NULL));
  }
  return NULL;
}

