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

    Authors:        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
*/
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <mysql.h>
#include "common_options.h"
#include "config.h"
#include "common_options.h"
char *db = NULL;
char *defaults_file = NULL;
char *defaults_extra_file = NULL;
#ifdef WITH_SSL
//char *key = NULL;
//char *cert = NULL;
//char *ca = NULL;
//char *capath = NULL;
//char *cipher = NULL;
//char *tls_version = NULL;
//gchar *ssl_mode = NULL;
#endif

gchar *fifo_directory = NULL;
gboolean help =FALSE;
int detected_server = 0;
GString *set_session = NULL;
GString *set_global = NULL;
GString *set_global_back = NULL;
gchar *sql_mode= NULL;
MYSQL *main_connection = NULL;
gboolean no_schemas = FALSE;
gboolean no_data = FALSE;
GKeyFile *key_file = NULL;

guint num_threads = 4;
guint verbose = 2;
gboolean debug = FALSE;
//gboolean ssl = FALSE;
//gboolean compress_protocol = FALSE;
gboolean program_version = FALSE;

gchar *tables_list = NULL;
gchar *tables_skiplist_file = NULL;
char **tables = NULL;

//int (*m_write)(FILE * file, const char * buff, int len);

gboolean no_stream = FALSE;

gchar *set_names_str=NULL;
gchar *set_names_statement=NULL;

gchar identifier_quote_character=BACKTICK;
const char *identifier_quote_character_str= "`";

gboolean schema_sequence_fix = FALSE;


GOptionEntry common_entries[] = {
    {"threads", 't', 0, G_OPTION_ARG_INT, &num_threads,
     "Number of threads to use, default 4", NULL},
    {"version", 'V', 0, G_OPTION_ARG_NONE, &program_version,
     "Show the program version and exit", NULL},
    {"verbose", 'v', 0, G_OPTION_ARG_INT, &verbose,
     "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, "
     "default 2",
     NULL},
    {"debug", 0, 0, G_OPTION_ARG_NONE, &debug, "Turn on debugging output "
     "(automatically sets verbosity to 3)", NULL},
    {"defaults-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_file,
     "Use a specific defaults file. Default: /etc/mydumper.cnf", NULL},
    {"defaults-extra-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_extra_file,
     "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values", NULL},
    {"fifodir", 0, 0, G_OPTION_ARG_FILENAME, &fifo_directory,
     "Directory where the FIFO files will be created when needed. Default: Same as backup", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


GOptionEntry common_filter_entries[] = {
    {"omit-from-file", 'O', 0, G_OPTION_ARG_STRING, &tables_skiplist_file,
     "File containing a list of database.table entries to skip, one per line "
     "(skips before applying regex option)",
     NULL},
    {"tables-list", 'T', 0, G_OPTION_ARG_STRING, &tables_list,
     "Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};
