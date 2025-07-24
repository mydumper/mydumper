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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include <stdio.h>
#include <stdlib.h>


struct filename_queue_element{
  struct db_table *dbt;
  gchar *filename;
  GAsyncQueue *done;
};


struct fifo{
  gchar *filename;
  gchar *stdout_filename;
  GAsyncQueue * queue;
  float size;
  struct db_table *dbt;
  int fdout;
  GPid gpid;
  int child_pid;
  int pipe[2];
  GMutex *out_mutex;
  int error_number;
};

void set_pipe_backup();
void initialize_file_handler();
int m_open_pipe(char **filename, const char *type);
void release_pid();
void child_process_ended(int child_pid);
void wait_close_files();
struct filename_queue_element * new_filename_queue_element(struct db_table *dbt,gchar *filename,GAsyncQueue *done);
