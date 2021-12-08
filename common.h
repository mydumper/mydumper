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

    Authors:        Andrew Hutchings, SkySQL (andrew at skysql dot com)
*/
#ifndef _common_h
#define _common_h

char *hostname = NULL;
char *username = NULL;
char *password = NULL;
char *socket_path = NULL;
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
int detected_server = 0;
GString *set_session=NULL;
gboolean stream = FALSE;
gboolean no_delete = FALSE;
GAsyncQueue *stream_queue;
gchar *compress_extension = NULL;

FILE * (*m_open)(const char *filename, const char *);
int (*m_close)(void *file) = NULL;
int (*m_write)(FILE * file, const char * buff, int len);
void load_config_file(gchar * config_file, GOptionContext *context, const gchar * group);
void execute_gstring(MYSQL *conn, GString *ss);
gchar * identity_function(gchar ** r);
gchar *replace_escaped_strings(gchar *c);
void load_hash_from_key_file(GHashTable * set_session_hash, gchar * config_file, const gchar * group_variables);
void refresh_set_session_from_hash(GString *ss, GHashTable * set_session_hash);

gboolean askPassword = FALSE;
guint port = 0;
guint num_threads = 4;
guint verbose = 2;
gboolean ssl = FALSE;
gboolean compress_protocol = FALSE;
gboolean program_version = FALSE;

GOptionEntry common_entries[] = {
    {"host", 'h', 0, G_OPTION_ARG_STRING, &hostname, "The host to connect to",
     NULL},
    {"user", 'u', 0, G_OPTION_ARG_STRING, &username,
     "Username with the necessary privileges", NULL},
    {"password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL},
    {"ask-password", 'a', 0, G_OPTION_ARG_NONE, &askPassword,
     "Prompt For User password", NULL},
    {"port", 'P', 0, G_OPTION_ARG_INT, &port, "TCP/IP port to connect to",
     NULL},
    {"socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path,
     "UNIX domain socket file to use for connection", NULL},
    {"threads", 't', 0, G_OPTION_ARG_INT, &num_threads,
     "Number of threads to use, default 4", NULL},
    {"compress-protocol", 'C', 0, G_OPTION_ARG_NONE, &compress_protocol,
     "Use compression on the MySQL connection", NULL},
    {"version", 'V', 0, G_OPTION_ARG_NONE, &program_version,
     "Show the program version and exit", NULL},
    {"verbose", 'v', 0, G_OPTION_ARG_INT, &verbose,
     "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, "
     "default 2",
     NULL},
    {"defaults-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_file,
     "Use a specific defaults file", NULL},
#ifdef WITH_SSL
    {"ssl", 0, 0, G_OPTION_ARG_NONE, &ssl, "Connect using SSL", NULL},
    {"ssl-mode", 0, 0, G_OPTION_ARG_STRING, &ssl_mode,
     "Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY", NULL},
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
    {"stream", 0, 0, G_OPTION_ARG_NONE, &stream,
     "It will stream over STDOUT once the files has been written", NULL},
    {"no-delete", 0, 0, G_OPTION_ARG_NONE, &no_delete,
      "It will not delete the files after stream has been completed", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

#endif

char * checksum_table(MYSQL *conn, char *database, char *table, int *errn);
int write_file(FILE * file, char * buff, int len);
void create_backup_dir(char *new_directory) ;
guint strcount(gchar *text);
