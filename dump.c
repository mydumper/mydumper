#include <mysql.h>
#include <stdio.h>
#include <glib.h>
#include <glib/gstdio.h>

struct configuration {
	char *directory;
	guint chunksize;
};

void dump_table(MYSQL *, char *, char *, char *, struct configuration *conf);

int main(int ac, char **av)
{
	MYSQL *conn;
	conn = mysql_init(NULL);
	mysql_real_connect(conn, "localhost", "root", "", NULL, 3306, NULL, 0);
	mysql_query(conn, "START TRANSACTION WITH CONSISTENT SNAPSHOT");

	struct configuration conf = { "output", 1000000 };

	dump_table(conn, "test", "categorylinks", NULL,  &conf);
	return (0);
}

void dump_table(MYSQL * conn, char *database, char *table, char *where, struct configuration *conf)
{
	guint i;

	/* Poor man's file code */
	char *filename = g_strdup_printf("%s/%s.dumping", conf->directory, table);
	mysql_query(conn, "SET NAMES binary");
	FILE *outfile = g_fopen(filename, "w");
	g_fprintf(outfile, (char *) "SET NAMES BINARY; \n");

	/* Poor man's database code */
	char *query = g_strdup_printf("SELECT * FROM %s.%s %s %s", database, table, where?"WHERE":"",where?where:"");
	mysql_query(conn, query);

	MYSQL_RES *result = mysql_use_result(conn);
	guint num_fields = mysql_num_fields(result);
	MYSQL_FIELD *fields = mysql_fetch_fields(result);

	/*
	 * This will hold information for how big data is the \escaped array
	 * allocated
	 */
	gulong *allocated = g_new0(gulong, num_fields);

	/* Array for actual escaped data */
	gchar **escaped = g_new0(gchar *, num_fields);

	MYSQL_ROW row;

	gulong cw = 0;				/* chunk written */

	/* Poor man's data dump code */
	while ((row = mysql_fetch_row(result))) {
		gulong *lengths = mysql_fetch_lengths(result);

		if (cw == 0)
			cw += g_fprintf(outfile, "INSERT INTO %s VALUES\n (", table);
		else
			cw += g_fprintf(outfile, ",\n (");

		for (i = 0; i < num_fields; i++) {
			if (fields[i].flags & NUM_FLAG) {
				cw += g_fprintf(outfile, "\"%s\"", row[i]);
			} else {
				if (lengths[i] > allocated[i]) {
					escaped[i] = g_renew(gchar, escaped[i], lengths[i] * 2 + 1);
					allocated[i] = lengths[i];
				} else if (!escaped[i]) {
					escaped[i] = g_new(gchar, 1);
					allocated[i] = 0;
				}
				mysql_real_escape_string(conn, escaped[i], row[i], lengths[i]);
				cw += g_fprintf(outfile, "\"%s\"", escaped[i]);
			}
			if (i < num_fields - 1) {
				g_fprintf(outfile, ",");
			} else {
				/* INSERT statement is closed once over limit */
				if (cw > conf->chunksize) {
					g_fprintf(outfile, ");\n");
					cw = 0;
				} else {
					cw += g_fprintf(outfile, ")");
				}
			}
		}
	}
	fprintf(outfile, ";\n");
	
	
// cleanup:
	if (outfile)
		fclose(outfile);
	g_free(filename);
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
}
