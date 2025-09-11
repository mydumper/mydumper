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
#include <unistd.h>

#include "common.h"

const gchar* filename=NULL;

gboolean pmm = FALSE;
gboolean pause_pmm=FALSE;
gchar *pmm_resolution = NULL;
gchar *pmm_path = NULL;

gint kill_pmm = 0;
GMutex *pmm_mutex=NULL;
GThread *pmm_thread = NULL;

void (*write_pmm_entries)(GString *content, void* conf);

void *worker_pmm_thread(void *conf);

void initialize_pmm(void _write_pmm_entries(GString *content, void* conf)){
  write_pmm_entries=_write_pmm_entries;
  if (pmm_path){
    pmm=TRUE;
    if (!pmm_resolution){
      pmm_resolution=g_strdup("high");
    }
  }else if (pmm_resolution){
    pmm=TRUE;
    pmm_path=g_strdup_printf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution",pmm_resolution);
  }
}

void stop_pmm_thread(){
  if (pmm){
    g_mutex_lock(pmm_mutex);
    kill_pmm=1;
    remove(filename);
    g_mutex_unlock(pmm_mutex);
    g_thread_join(pmm_thread);
  }
}

void pause_pmm_thread(){
  if (pmm){
    g_mutex_lock(pmm_mutex);
    pause_pmm=TRUE;
    g_mutex_unlock(pmm_mutex);
  }
}

void *worker_pmm_thread(void *conf){
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
    if (pause_pmm)
      write_pmm_entries(content, conf);
    g_mutex_unlock(pmm_mutex);
    sleep(1);
  }
  return NULL;
}

void start_pmm_thread(void *conf){
  if (pmm){
    if (pause_pmm){

    }else{
      g_message("Using PMM resolution %s at %s", pmm_resolution, pmm_path);
      pmm_thread = m_thread_new("pmm", worker_pmm_thread, conf,"PMM thread could not be created");
      if (pmm_thread == NULL)
        m_critical("Could not create pmm thread");
    }
  }
}
