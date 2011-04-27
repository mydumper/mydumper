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
			Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
			Andrew Hutchings, SkySQL (andrew at skysql dot com)

*/

#ifndef _mydumper_h
#define _mydumper_h

enum job_type { JOB_SHUTDOWN, JOB_DUMP, JOB_BINLOG };

struct configuration {
	char use_any_index;
	GAsyncQueue* queue;
	GAsyncQueue* ready;
	GMutex* mutex;
	int done;
};

struct job {
	enum job_type type;
        void *job_data;
	struct configuration *conf;
};

struct table_job {
	char *database;
	char *table;
	char *filename;
	char *where;
};

struct binlog_job {
	char *filename;
	guint64 start_position;
	guint64 stop_position;
};

#endif
