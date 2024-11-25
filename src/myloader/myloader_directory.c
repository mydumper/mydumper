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

#include <stdio.h>
#include <glib/gstdio.h>
#include <string.h>
#include <glib.h>

#include "myloader.h"
#include "myloader_control_job.h"
#include "myloader_intermediate_queue.h"
#include "myloader_process.h"
#include "myloader_common.h"
#include "myloader_global.h"

GAsyncQueue *metadata_sync_queue=NULL;

void initialize_directory(){
  metadata_sync_queue=g_async_queue_new();
}

void wait_directory_to_process_metadata(){
  g_async_queue_pop(metadata_sync_queue);
  g_async_queue_unref(metadata_sync_queue);
}

void *process_directory(struct configuration *conf){
  GError *error = NULL;
  const gchar *filename = NULL;
  /*
    set_db_schema_created() depends on sequences variable. It will not be
    updated until metadata is read. If DB schema is processed before metadata
    we will get wrong condition (sequences == sequences_processed == 0).
  */
  if (g_file_test("metadata", G_FILE_TEST_IS_REGULAR)){
    process_metadata_global("metadata");
    g_async_queue_push(metadata_sync_queue,GINT_TO_POINTER(1));
    g_message("metadata pushed");
  }else
    g_error("metadata file was not found");
  if (resume){
    g_message("Using resume file");
    FILE *file = g_fopen("resume", "r");
    GString *data=g_string_sized_new(256);
    gboolean eof = FALSE;
    guint line=0;
    read_data(file, data, &eof, &line);
    gchar **split=NULL;
    guint i=0;
    while (!eof){
      read_data(file, data, &eof, &line);
      split=g_strsplit(data->str,"\n",0);
      for (i=0; i<g_strv_length(split);i++){
        if (strlen(split[i])>2){
          filename=split[i];
          intermediate_queue_new(g_strdup(filename));
        }
      }
      g_string_set_size(data, 0);
    } 
    fclose(file);
  }else{
    GDir *dir = g_dir_open(directory, 0, &error);
    while ((filename = g_dir_read_name(dir))){
      if (strcmp(filename, "metadata"))
        intermediate_queue_new(filename);
    }
  }
  intermediate_queue_end();
  guint n=0;
  for (n = 0; n < num_threads ; n++) {
    g_async_queue_push(conf->data_queue,       new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->post_table_queue, new_control_job(JOB_SHUTDOWN,NULL,NULL));
//    g_async_queue_push(conf->post_queue,       new_control_job(JOB_SHUTDOWN,NULL,NULL));
    g_async_queue_push(conf->view_queue,       new_control_job(JOB_SHUTDOWN,NULL,NULL));
  }
  return NULL;
}

