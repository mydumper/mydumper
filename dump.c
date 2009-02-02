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

	Authors: 	Domas Mituzas, Sun Microsystems ( domas at sun dot com )
			Mark Leith, Sun Microsystems (leith at sun dot com)
*/

#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <glib/gstdio.h>

struct configuration {
	char use_any_index;
	GAsyncQueue* queue;
	GAsyncQueue* ready;
	GMutex* mutex;
	int done;
};

/* Database options */
char *hostname="";
char *username="root";
char *password;
char *db="test";
guint port=3306;

#define DIRECTORY "export"

/* Program options */
guint num_threads = 16;
gchar *directory = DIRECTORY;
guint statement_size = 1000000;
guint rows_per_file = 1000000;

static GOptionEntry entries[] =
{
	{ "host", 'h', 0, G_OPTION_ARG_STRING, &hostname, "The host to connect to", NULL },
	{ "user", 'u', 0, G_OPTION_ARG_STRING, &username, "Username with privileges to run the dump", NULL },
	{ "password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL },
	{ "port", 'P', 0, G_OPTION_ARG_INT, &port, "TCP/IP port to connect to", NULL },
	{ "database", 'B', 0, G_OPTION_ARG_STRING, &db, "Database to dump", NULL },
	{ "threads", 't', 0, G_OPTION_ARG_INT, &num_threads, "Number of parallel threads", NULL },
	{ "outputdir", 'o', 0, G_OPTION_ARG_FILENAME, &directory, "Directory to output files to, default: ./" DIRECTORY,  NULL },
	{ "statement-size", 's', 0, G_OPTION_ARG_INT, &statement_size, "Attempted size of INSERT statement in bytes", NULL},
	{ "rows", 'r', 0, G_OPTION_ARG_INT, &rows_per_file, "Try to split tables into chunks of this many rows", NULL},
	{ NULL }
};

enum job_type { JOB_SHUTDOWN, JOB_DUMP };

struct job {
	enum job_type type;
	char *database;
	char *table;
	char *filename;
	char *where;
	struct configuration *conf;
};

void dump_table(MYSQL *conn, char *database, char *table, struct configuration *conf);
void dump_table_data(MYSQL *, FILE *, char *, char *, char *, struct configuration *conf);
void dump_database(MYSQL *, char *, struct configuration *conf);
GList * get_chunks_for_table(MYSQL *, char *, char*,  struct configuration *conf);
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field, char *from, char *to);
void dump_table_data_file(MYSQL *conn, char *database, char *table, char *where, char *filename, struct configuration *conf);
void create_backup_dir(char *directory);

void *process_queue(struct configuration * conf) {
	mysql_thread_init();
	MYSQL *thrconn = mysql_init(NULL);
	mysql_real_connect(thrconn, hostname, username, password, db, port, NULL, 0);
	mysql_query(thrconn, "START TRANSACTION WITH CONSISTENT SNAPSHOT");
	mysql_query(thrconn, "SET NAMES binary");

	g_async_queue_push(conf->ready,GINT_TO_POINTER(1));
	
	struct job* job;
	for(;;) {
		GTimeVal tv;
		g_get_current_time(&tv);
		g_time_val_add(&tv,1000*1000*1);
		job=g_async_queue_pop(conf->queue);
		switch (job->type) {
			case JOB_DUMP:
				dump_table_data_file(thrconn, job->database, job->table, job->where, job->filename, job->conf);
				break;
			case JOB_SHUTDOWN:
				return NULL;
				break;
		}
		if(job->table) g_free(job->table);
		if(job->where) g_free(job->where);
		if(job->filename) g_free(job->filename);
		g_free(job);
	}
	return NULL;
}

int main(int argc, char *argv[])
{
	mysql_thread_init();
	struct configuration conf = { 1, NULL, NULL, NULL, 0 };

        GError *error = NULL;
        GOptionContext *context;
        context = g_option_context_new("multi-threaded MySQL dumping");
        g_option_context_add_main_entries(context, entries, NULL);
        if (!g_option_context_parse(context, &argc, &argv, &error))
        {
                g_print ("option parsing failed: %s\n", error->message);
                exit (1);
        }
        g_option_context_free(context);
	
	create_backup_dir(directory);

	g_thread_init(NULL);
	
	MYSQL *conn;
	conn = mysql_init(NULL);
	mysql_real_connect(conn, hostname, username, password, db, port, NULL, 0);
	mysql_query(conn, "FLUSH TABLES WITH READ LOCK");
	mysql_query(conn, "START TRANSACTION WITH CONSISTENT SNAPSHOT");
	mysql_query(conn, "SET NAMES binary");
	
	conf.queue = g_async_queue_new();
	conf.ready = g_async_queue_new();
	conf.mutex = g_mutex_new();
	
	int n;
	GThread **threads = g_new(GThread*,num_threads);
	for (n=0; n<num_threads; n++) {
		threads[n] = g_thread_create((GThreadFunc)process_queue,&conf,TRUE,NULL);
		g_async_queue_pop(conf.ready);
	}
	mysql_query(conn, "UNLOCK TABLES");
	
	/* XXX - need SHOW SLAVE STATUS and SHOW MASTER STATUS right around here */
	dump_database(conn, db, &conf);
	for (n=0; n<num_threads; n++) {
		struct job *j = g_new0(struct job,1);
		j->type = JOB_SHUTDOWN;
		g_async_queue_push(conf.queue,j);
	}
	
	for (n=0; n<num_threads; n++) {
		g_thread_join(threads[n]);
	}

	return (0);
}

/* Heuristic chunks building - based on estimates, produces list of ranges for datadumping 
   WORK IN PROGRESS
*/
GList * get_chunks_for_table(MYSQL *conn, char *database, char *table, struct configuration *conf) {
	
	GList *chunks = NULL;
	MYSQL_RES *indexes=NULL, *minmax=NULL, *total=NULL;
	MYSQL_ROW row;
	char *index = NULL, *field = NULL;
	
	/* first have to pick index, in future should be able to preset in configuration too */
	gchar *query = g_strdup_printf("SHOW INDEX FROM %s.%s",database,table);
	mysql_query(conn,query);
	g_free(query);
	indexes=mysql_store_result(conn);
	
	while ((row=mysql_fetch_row(indexes))) {
		if (!strcmp(row[2],"PRIMARY") && (!strcmp(row[3],"1"))) {
			/* Pick first column in PK, cardinality doesn't matter */
			field=row[4];
			index=row[2];
			break;
		}
	}

	/* If no PK found, try using first UNIQUE index */
	if (!field) {
		mysql_data_seek(indexes,0);
		while ((row=mysql_fetch_row(indexes))) {
			if(!strcmp(row[1],"0") && (!strcmp(row[3],"1"))) {
				/* Again, first column of any unique index */
				field=row[4];
				index=row[2];
				break;
			}
		}
	}
	
	/* Still unlucky? Pick any high-cardinality index */
	if (!field && conf->use_any_index) {
		guint64 max_cardinality=0;
		guint64 cardinality=0;
		
		mysql_data_seek(indexes,0);
		while ((row=mysql_fetch_row(indexes))) {
			if(!strcmp(row[3],"1")) {
				if (row[6])
					cardinality = strtoll(row[6],NULL,10);
				if (cardinality>max_cardinality) {
					field=row[4];
					max_cardinality=cardinality;
				}
			}
		}
	}
	/* Oh well, no chunks today - no suitable index */
	if (!field) goto cleanup;
	
	/* Get minimum/maximum */
	mysql_query(conn,query=g_strdup_printf("SELECT MIN(%s),MAX(%s) FROM %s.%s", field, field, database, table));
	g_free(query);
	minmax=mysql_store_result(conn);
	
	if (!minmax)
		goto cleanup;
	
	row=mysql_fetch_row(minmax);
	MYSQL_FIELD * fields=mysql_fetch_fields(minmax);
	char *min=row[0];
	char *max=row[1];
	
	/* Got total number of rows, skip chunk logic if estimates are low */
	guint64 rows = estimate_count(conn, database, table, field, NULL, NULL);
	if (rows <= rows_per_file)
		goto cleanup;

	/* This is estimate, not to use as guarantee! Every chunk would have eventual adjustments */
	guint64 estimated_chunks = rows / rows_per_file; 
	guint64 estimated_step, nmin, nmax, cutoff;
	int showed_nulls=0;
	
	/* Support just bigger INTs for now, very dumb, no verify approach */
	switch (fields[0].type) {
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_INT24:
			/* static stepping */
			nmin = strtoll(min,NULL,10);
			nmax = strtoll(max,NULL,10);
			estimated_step = (nmax-nmin)/estimated_chunks+1;
			cutoff = nmin;
			while(cutoff<=nmax) {
				chunks=g_list_append(chunks,g_strdup_printf("%s%s(%s >= %llu AND %s < %llu)", 
						!showed_nulls?field:"",
						!showed_nulls?" IS NULL OR ":"",
						field, cutoff, field, cutoff+estimated_step));
				cutoff+=estimated_step;
				showed_nulls=1;
			}
			
		default:
			goto cleanup;
	}

	
cleanup:	
	if (indexes) 
		mysql_free_result(indexes);
	if (minmax)
		mysql_free_result(minmax);
	if (total)
		mysql_free_result(total);
	return chunks;
}

/* Try to get EXPLAIN'ed estimates of row in resultset */
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field, char *from, char *to) {
	char *querybase, *query;

	g_assert(conn && database && table);
	
	querybase = g_strdup_printf("EXPLAIN SELECT `%s` FROM %s.%s", (field?field:"*"), database, table);
	if (from || to) {
		g_assert(field != NULL);
		char *fromclause=NULL, *toclause=NULL;
		char *escaped;
		if (from) {
			escaped=g_new(char,strlen(from)*2+1);
			mysql_real_escape_string(conn,escaped,from,strlen(from));
			fromclause = g_strdup_printf(" `%s` >= \"%s\" ", field, escaped);
			g_free(escaped);
		}
		if (to) {
			escaped=g_new(char,strlen(to)*2+1);
			mysql_real_escape_string(conn,escaped,from,strlen(from));
			toclause = g_strdup_printf( " `%s` <= \"%s\"", field, escaped);
			g_free(escaped);
		}
		query = g_strdup_printf("%s WHERE %s %s %s", querybase, (from?fromclause:""), ((from&&to)?"AND":""), (to?toclause:""));
		
		if (toclause) g_free(toclause);
		if (fromclause) g_free(fromclause);
		mysql_query(conn,query);
		g_free(querybase);
		g_free(query);
	} else {
		mysql_query(conn,querybase);
		g_free(querybase);
	}
	
	MYSQL_RES * result = mysql_store_result(conn);
	MYSQL_FIELD * fields = mysql_fetch_fields(result);

	int i;
	for (i=0; i<mysql_num_fields(result); i++)  {
		if (!strcmp(fields[i].name,"rows"))
			break;
	}

	MYSQL_ROW row = NULL;
	
	guint64 count=0;
	
	if (result)
		row = mysql_fetch_row(result);
	
	if (row && row[i])
		count=strtoll(row[i],NULL,10);
	
	if (result)
		mysql_free_result(result);
	
	return(count);
}

void create_backup_dir(char *directory) {
	if (g_mkdir(directory, 0700) == -1)
	{
		if (errno != EEXIST)
		{
			g_critical("Unable to create `%s': %s",
				directory,
				g_strerror(errno));
			exit(1);
		}
	}
}

void dump_database(MYSQL * conn, char *database, struct configuration *conf) {
	mysql_select_db(conn,database);
	if (mysql_query(conn, "SHOW /*!50000 FULL */ TABLES")) {
		g_critical("Error: DB: %s - Could not execute query: %s", database, mysql_error(conn));
		return; 
	}
	MYSQL_RES *result = mysql_store_result(conn);
	guint num_fields = mysql_num_fields(result);
	
	MYSQL_ROW row;
	while ((row = mysql_fetch_row(result))) {
		/* We no care about views! */
		if (num_fields>1 && strcmp(row[1],"BASE TABLE"))
			continue;
		dump_table(conn, database, row[0], conf);
	}
	mysql_free_result(result);
}

void dump_table_data_file(MYSQL *conn, char *database, char *table, char *where, char *filename, struct configuration *conf) {
	FILE *outfile;
	outfile = g_fopen(filename, "w");
	if (!outfile) {
		g_critical("Error: DB: %s TABLE: %s Could not create output file %s (%d)", database, table, filename, errno);
		return;
	}
	dump_table_data(conn, outfile, database, table, where, conf);
	fclose(outfile);

}

void dump_table(MYSQL *conn, char *database, char *table, struct configuration *conf) {

	GList * chunks = NULL; 
	/* This code for now does nothing, and is in development */
	if (rows_per_file)
		chunks = get_chunks_for_table(conn, database, table, conf); 

	
	if (chunks) {
		int nchunk=0;
		for (chunks = g_list_first(chunks); chunks; chunks=g_list_next(chunks)) {
			struct job *j = g_new0(struct job,1);
			j->database=g_strdup(database);
			j->table=g_strdup(table);
			j->conf=conf;
			j->type=JOB_DUMP;
			j->filename=g_strdup_printf("%s/%s.%s.%05d.sql", directory, database, table, nchunk);
			j->where=g_strdup((char*)chunks->data);
			g_async_queue_push(conf->queue,j);
			nchunk++;
		}
	} else {
		struct job *j = g_new0(struct job,1);
		j->database=g_strdup(database);
		j->table=g_strdup(table);
		j->conf=conf;
		j->type=JOB_DUMP;
		j->filename = g_strdup_printf("%s/%s.%s.sql", directory, database, table);
		g_async_queue_push(conf->queue,j);
		return;
	}
}

/* Do actual data chunk reading/writing magic */
void dump_table_data(MYSQL * conn, FILE *file, char *database, char *table, char *where, struct configuration *conf)
{
	guint i;
	gulong *allocated=NULL;
	gchar **escaped=NULL;
	guint num_fields = 0;
	MYSQL_RES *result = NULL;
	char *query = NULL;
	
	g_fprintf(file, (char *) "SET NAMES BINARY; \n");

	/* Poor man's database code */
 	query = g_strdup_printf("SELECT * FROM %s.%s %s %s", database, table, where?"WHERE":"",where?where:"");
	mysql_query(conn, query);

	result = mysql_use_result(conn);
	num_fields = mysql_num_fields(result);
	MYSQL_FIELD *fields = mysql_fetch_fields(result);

	/*
	 * This will hold information for how big data is the \escaped array
	 * allocated
	 */
	allocated = g_new0(gulong, num_fields);

	/* Array for actual escaped data */
	escaped = g_new0(gchar *, num_fields);

	MYSQL_ROW row;

	gulong cw = 0;				/* chunk written */

	/* Poor man's data dump code */
	while ((row = mysql_fetch_row(result))) {
		gulong *lengths = mysql_fetch_lengths(result);

		if (cw == 0)
			cw += g_fprintf(file, "INSERT INTO %s VALUES\n (", table);
		else
			cw += g_fprintf(file, ",\n (");

		for (i = 0; i < num_fields; i++) {
			/* Don't escape safe formats, saves some time */
			if (fields[i].flags & NUM_FLAG) {
				cw += g_fprintf(file, "\"%s\"", row[i]);
			} else {
				/* We reuse buffers for string escaping, growing is expensive just at the beginning */
				if (lengths[i] > allocated[i]) {
					escaped[i] = g_renew(gchar, escaped[i], lengths[i] * 2 + 1);
					allocated[i] = lengths[i];
				} else if (!escaped[i]) {
					escaped[i] = g_new(gchar, 1);
					allocated[i] = 0;
				}
				mysql_real_escape_string(conn, escaped[i], row[i], lengths[i]);
				cw += g_fprintf(file, "\"%s\"", escaped[i]);
			}
			if (i < num_fields - 1) {
				g_fprintf(file, ",");
			} else {
				/* INSERT statement is closed once over limit */
				if (cw > statement_size) {
					g_fprintf(file, ");\n");
					cw = 0;
				} else {
					cw += g_fprintf(file, ")");
				}
			}
		}
	}
	fprintf(file, ";\n");
	
// cleanup:
	g_free(query);
	
	if (allocated)
		g_free(allocated);
			
	if (escaped) {
		for (i=0; i < num_fields; i++) {
			if (escaped[i])
				g_free(escaped[i]);
		}
		g_free(escaped);
	}
	
	if (result) {
		mysql_free_result(result);
	}
}
