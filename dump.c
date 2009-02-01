#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <errno.h>
#include <glib/gstdio.h>

struct configuration {
	char *directory;
	guint statement_size;
};

void dump_table(MYSQL *conn, char *database, char *table, struct configuration *conf);
void dump_table_data(MYSQL *, FILE *, char *, char *, char *, struct configuration *conf);
void dump_database(MYSQL *, char *, struct configuration *conf);

int main(int ac, char **av)
{
	g_thread_init(NULL);
	MYSQL *conn;
	conn = mysql_init(NULL);
	mysql_real_connect(conn, "localhost", "root", "", NULL, 3306, NULL, 0);
	mysql_query(conn, "START TRANSACTION WITH CONSISTENT SNAPSHOT");
	mysql_query(conn, "SET NAMES binary");

	struct configuration conf = { "output", 1000000 };

	dump_database(conn, "test", &conf);
	return (0);
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


void dump_table(MYSQL *conn, char *database, char *table, struct configuration *conf) {
	/* Poor man's file code */
	char *filename = g_strdup_printf("%s/%s.%s.dumping", conf->directory, database, table);
	

	FILE *outfile = g_fopen(filename, "w");
	if (!outfile) {
		g_critical("Error: DB: %s TABLE: %s Could not create output file %s (%d)", database, table, filename, errno);
		goto cleanup;
	}
	
	g_fprintf(outfile, (char *) "SET NAMES BINARY; \n");
	
	dump_table_data(conn, outfile, database, table, NULL, conf);
	
	cleanup:
		if(outfile)
			fclose(outfile);
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
	


	/* Poor man's database code */
 	query = g_strdup_printf("SELECT * FROM %s %s %s", table, where?"WHERE":"",where?where:"");
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
			if (fields[i].flags & NUM_FLAG) {
				cw += g_fprintf(file, "\"%s\"", row[i]);
			} else {
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
				if (cw > conf->statement_size) {
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
