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

        Authors: 	Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle
   dot com) Andrew Hutchings, SkySQL (andrew at skysql dot com)

*/

#ifndef _myloader_h
#define _myloader_h

enum job_type { JOB_SHUTDOWN, JOB_RESTORE };

struct configuration {
  GAsyncQueue *queue;
  GAsyncQueue *ready;
  GMutex *mutex;
  int done;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
};

struct job {
  enum job_type type;
  void *job_data;
  struct configuration *conf;
};

struct restore_job {
  char *database;
  char *table;
  char *filename;
  guint part;
};

#endif
