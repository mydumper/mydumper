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
#ifdef WITH_SSL
char *key = NULL;
char *cert = NULL;
char *ca = NULL;
char *capath = NULL;
char *cipher = NULL;
char *tls_version = NULL;
gchar *ssl_mode = NULL;
#endif

gboolean help =FALSE;
int (*m_close)(void *file) = NULL;
int detected_server = 0;
GString *set_session = NULL;
GString *set_global = NULL;
GString *set_global_back = NULL;
MYSQL *main_connection = NULL;
gboolean no_data = FALSE;
GKeyFile *key_file = NULL;
const gchar *compress_extension = NULL;

guint num_threads = 4;
guint verbose = 2;
gboolean debug = FALSE;
gboolean ssl = FALSE;
gboolean compress_protocol = FALSE;
gboolean program_version = FALSE;

gchar *tables_list = NULL;
gchar *tables_skiplist_file = NULL;
char **tables = NULL;

int (*m_write)(FILE * file, const char * buff, int len);

gboolean no_stream = FALSE;

gchar *set_names_str=NULL;
gchar *set_names_statement=NULL;

gchar identifier_quote_character=BACKTICK;
gchar * identifier_quote_character_str=NULL;

gboolean schema_sequence_fix = FALSE;

gboolean identifier_quote_character_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (strlen(option_name) == 28 && g_strstr_len(option_name,28,"--identifier-quote-character")){
    if (strlen(value) == 8 && g_strstr_len(value,8,"BACKTICK")){
      identifier_quote_character=BACKTICK;
      return TRUE;
    }
    if (strlen(value) == 12 && g_strstr_len(value,12,"DOUBLE_QUOTE")){
      identifier_quote_character=DOUBLE_QUOTE;
      return TRUE;
    }
  }
  return FALSE;
}



GOptionEntry common_entries[] = {
    {"threads", 't', 0, G_OPTION_ARG_INT, &num_threads,
     "Number of threads to use, default 4", NULL},
    {"version", 'V', 0, G_OPTION_ARG_NONE, &program_version,
     "Show the program version and exit", NULL},
    {"identifier-quote-character", 0, 0, G_OPTION_ARG_CALLBACK, &identifier_quote_character_arguments_callback, 
     "This set the identifier quote character that is used to INSERT statements only"
     "on mydumper and to split statement on myloader. Use SQL_MODE to change the"
     "CREATE TABLE statements"
     "Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK", NULL},
    {"verbose", 'v', 0, G_OPTION_ARG_INT, &verbose,
     "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, "
     "default 2",
     NULL},
#if GLIB_CHECK_VERSION(2,72,0)
    {"debug", 0, 0, G_OPTION_ARG_NONE, &debug, "Turn on debugging output "
     "(automatically sets verbosity to 3)", NULL},
#endif
    {"defaults-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_file,
     "Use a specific defaults file. Default: /etc/mydumper.cnf", NULL},
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

GOptionEntry common_connection_entries[] = {
    {"compress-protocol", 'C', 0, G_OPTION_ARG_NONE, &compress_protocol,
     "Use compression on the MySQL connection", NULL},
#ifdef WITH_SSL
    {"ssl", 0, 0, G_OPTION_ARG_NONE, &ssl, "Connect using SSL", NULL},
    {"ssl-mode", 0, 0, G_OPTION_ARG_STRING, &ssl_mode,
#ifdef LIBMARIADB
     "Desired security state of the connection to the server: REQUIRED, VERIFY_IDENTITY", NULL},
#else
     "Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY", NULL},
#endif
    {"key", 0, 0, G_OPTION_ARG_STRING, &key, "The path name to the key file",
     NULL},
    {"cert", 0, 0, G_OPTION_ARG_STRING, &cert,
     "The path name to the certificate file", NULL},
    {"ca", 0, 0, G_OPTION_ARG_STRING, &ca,
     "The path name to the certificate authority file", NULL},
    {"capath", 0, 0, G_OPTION_ARG_STRING, &capath,
     "The path name to a directory that contains trusted SSL CA certificates "
     "in PEM format",
     NULL},
    {"cipher", 0, 0, G_OPTION_ARG_STRING, &cipher,
     "A list of permissible ciphers to use for SSL encryption", NULL},
    {"tls-version", 0, 0, G_OPTION_ARG_STRING, &tls_version,
     "Which protocols the server permits for encrypted connections", NULL},
#endif
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


