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
};

#endif
