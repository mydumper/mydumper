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

    Authors:        Aaron Brady, Shopify (insom)
*/

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <glib.h>
#include <string.h>
#include "config.h"
#include "connection.h"
#include "common.h"

char *hostname = NULL;
char *username = NULL;
char *password = NULL;
char *socket_path = NULL;
guint port = 0;
gboolean askPassword = FALSE;
char *protocol_str = NULL;
enum mysql_protocol_type protocol=MYSQL_PROTOCOL_DEFAULT;
static gint print_connection_details=1;
gboolean local_infile = FALSE;
gchar * default_connection_database=NULL;


#ifdef WITH_SSL
char *key=NULL;
char *cert=NULL;
char *ca=NULL;
char *capath=NULL;
char *cipher=NULL;
char *tls_version=NULL;
gboolean ssl=FALSE;
gchar *ssl_mode=NULL;
#endif

gboolean compress_protocol = FALSE;

gboolean connection_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (g_strstr_len(option_name,10,"--protocol")){
    if (g_strstr_len(value,3,"tcp")){
      protocol=MYSQL_PROTOCOL_TCP;
      return TRUE;
    }
    if (g_strstr_len(value,6,"socket")){
      protocol=MYSQL_PROTOCOL_SOCKET;
      return TRUE;
    }
  }
  return FALSE;
}

GOptionEntry connection_entries[] = {
    {"host", 'h', 0, G_OPTION_ARG_STRING, &hostname, "The host to connect to",
     NULL},
    {"user", 'u', 0, G_OPTION_ARG_STRING, &username,
     "Username with the necessary privileges", NULL},
    {"password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL},
    {"default-connection-database", 0, 0, G_OPTION_ARG_STRING, &default_connection_database, 
      "Set the database name to connect to. Default: INFORMATION_SCHEMA", NULL},
    {"ask-password", 'a', 0, G_OPTION_ARG_NONE, &askPassword,
     "Prompt For User password", NULL},
    {"port", 'P', 0, G_OPTION_ARG_INT, &port, "TCP/IP port to connect to",
     NULL},
    {"socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path,
     "UNIX domain socket file to use for connection", NULL},
    {"protocol", 0, 0, G_OPTION_ARG_CALLBACK, &connection_arguments_callback,
     "The protocol to use for connection (tcp, socket)", NULL},
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



gchar *connection_defaults_file=NULL;
const gchar *connection_default_file_group=NULL;
const gchar *program_name=NULL;

void set_connection_defaults_file_and_group(gchar *cdf, const gchar *group){
  connection_defaults_file=cdf;
  connection_default_file_group=group;
}


void initialize_connection(const gchar *app){
  program_name=app;
  set_names_statement=g_strdup_printf("/*!40101 SET NAMES %s*/",set_names_in_conn_by_default);
}


GOptionGroup * load_connection_entries(GOptionContext *context){
  GOptionGroup *connection_group =
      g_option_group_new("connectiongroup", "Connection Options", "connection", NULL, NULL);
  g_option_group_add_entries(connection_group, connection_entries);
  g_option_context_add_group(context, connection_group);
  return connection_group;
}

#ifdef WITH_SSL
static
void check_pem_exists(const char *filename, const char *option) {
  if (!filename)
    m_critical("SSL required option missing: %s\n", option);
  else if (!g_file_test(filename, G_FILE_TEST_EXISTS))
    m_critical("%s file does not exist: %s\n", option, filename);
}

static
void check_capath(const char *path) {
  if (!g_file_test(path, G_FILE_TEST_IS_DIR))
    m_critical("capath is not directory: %s\n", path);

}
#endif

void configure_connection(MYSQL *conn) {
  if (connection_defaults_file != NULL)
    mysql_options(conn, MYSQL_READ_DEFAULT_FILE, connection_defaults_file);
  if (connection_default_file_group != NULL)
    mysql_options(conn, MYSQL_READ_DEFAULT_GROUP, connection_default_file_group);
 
  if (local_infile)
    mysql_options(conn, MYSQL_OPT_LOCAL_INFILE, NULL);

  mysql_options4(conn, MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", program_name);
  gchar *version=g_strdup_printf("Release %s", VERSION);
  mysql_options4(conn, MYSQL_OPT_CONNECT_ATTR_ADD, "app_version", version);
  g_free(version);
  if (compress_protocol)
    mysql_options(conn, MYSQL_OPT_COMPRESS, NULL);

  if (protocol!=MYSQL_PROTOCOL_DEFAULT)
    mysql_options(conn, MYSQL_OPT_PROTOCOL, &protocol);

#ifdef WITH_SSL
#ifdef LIBMARIADB
  my_bool enable= ssl_mode != NULL;
  if (ssl_mode) {
      if (g_ascii_strncasecmp(ssl_mode, "REQUIRED", 16) == 0) {
        mysql_options(conn, MYSQL_OPT_SSL_ENFORCE, &enable);
        if (key || cert) {
          check_pem_exists(key, "key");
          check_pem_exists(cert, "cert");
        }
      }
      else if (g_ascii_strncasecmp(ssl_mode, "VERIFY_IDENTITY", 16) == 0) {
        mysql_options(conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &enable);
        if (capath)
          check_capath(capath);
        else
          check_pem_exists(ca, "ca");
      }
      else {
        m_critical("Unsupported ssl-mode specified: %s\n", ssl_mode);
      }
  }else{
    mysql_options(conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &enable);
  }
#else
  unsigned int i;
  if (ssl) {
    i = SSL_MODE_REQUIRED;
    mysql_options(conn, MYSQL_OPT_SSL_MODE, &i);
  } else {
    if (ssl_mode) {
      if (g_ascii_strncasecmp(ssl_mode, "DISABLED", 16) == 0) {
        i = SSL_MODE_DISABLED;
      }
      else if (g_ascii_strncasecmp(ssl_mode, "PREFERRED", 16) == 0) {
        i = SSL_MODE_PREFERRED;
        if (key || cert) {
          check_pem_exists(key, "key");
          check_pem_exists(cert, "cert");
        }
      }
      else if (g_ascii_strncasecmp(ssl_mode, "REQUIRED", 16) == 0) {
        i = SSL_MODE_REQUIRED;
        if (key || cert) {
          check_pem_exists(key, "key");
          check_pem_exists(cert, "cert");
        }
      }
      else if (g_ascii_strncasecmp(ssl_mode, "VERIFY_CA", 16) == 0) {
        i = SSL_MODE_VERIFY_CA;
        if (capath)
          check_capath(capath);
        else
          check_pem_exists(ca, "ca");
      }
      else if (g_ascii_strncasecmp(ssl_mode, "VERIFY_IDENTITY", 16) == 0) {
        i = SSL_MODE_VERIFY_IDENTITY;
        if (capath)
          check_capath(capath);
        else
          check_pem_exists(ca, "ca");
      }
      else {
        m_critical("Unsupported ssl-mode specified: %s\n", ssl_mode);
      }
      mysql_options(conn, MYSQL_OPT_SSL_MODE, &i);
    }
  }
#endif // LIBMARIADB
  if (key) {
    mysql_options(conn, MYSQL_OPT_SSL_KEY, key);
  }
  if (cert) {
    mysql_options(conn, MYSQL_OPT_SSL_CERT, cert);
  }
  if (ca) {
    mysql_options(conn, MYSQL_OPT_SSL_CA, ca);
  }
  if (capath) {
    mysql_options(conn, MYSQL_OPT_SSL_CAPATH, capath);
  }
  if (cipher) {
    mysql_options(conn, MYSQL_OPT_SSL_CIPHER, cipher);
  }
  if (tls_version) {
    mysql_options(conn, MYSQL_OPT_TLS_VERSION, tls_version);
  }
#endif

  if (!hostname)
    hostname= getenv("MYSQL_HOST");

  if (!port) {
    char *p= getenv("MYSQL_TCP_PORT");
    port= p ? atoi(p) : 0;
  }
}

void print_connection_details_once(){
  if (!g_atomic_int_dec_and_test(&print_connection_details)){
    return;
  }
  GString * print_head=g_string_sized_new(20);
  g_string_append(print_head,"Connection");
  switch (protocol){
    case MYSQL_PROTOCOL_DEFAULT:
      g_string_append_printf(print_head," via default library settings");
      break;
    case MYSQL_PROTOCOL_TCP:
      g_string_append_printf(print_head," via TCP/IP");
      break;
    case MYSQL_PROTOCOL_SOCKET:
      g_string_append_printf(print_head," via UNIX socket");
      break;
    default:
//      g_debug("Host: %s Port: %s User: %s ");
    ;
  }
  if (password || askPassword)
    g_string_append(print_head," using password");

  GString * print_body=g_string_sized_new(20);

  if (hostname)
    g_string_append_printf(print_body,"\n\tHost: %s", hostname);

  if (port)
    g_string_append_printf(print_body,"\n\tPort: %d", port);

  if (socket_path)
    g_string_append_printf(print_body,"\n\tSocket: %s", socket_path);

  if (username)
    g_string_append_printf(print_body,"\n\tUser: %s", username);

  if (print_body->len > 1){
    g_string_append(print_head, ":");
    g_string_append(print_head, print_body->str);
  }

  g_message("%s", print_head->str);
  g_string_free(print_head, TRUE);
  g_string_free(print_body,TRUE);
}


void m_connect(MYSQL *conn){
  configure_connection(conn);
  if (!mysql_real_connect(conn, hostname, username, password, default_connection_database?default_connection_database:"INFORMATION_SCHEMA", port,
                          socket_path, 0)) {
    m_critical("Error connection to database: %s", mysql_error(conn));
  }
  print_connection_details_once();

//  if (set_names_statement)
    m_query_warning(conn, set_names_statement, "Not able to execute SET NAMES statement", NULL);
}

void hide_password(int argc, char *argv[]){
  if (password != NULL){
    int i=1;
    for(i=1; i < argc; i++){
      gchar * p= g_strstr_len(argv[i],-1,password);
      if (p != NULL){
        strncpy(p, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", strlen(password));
      }
    }
  }
}

char *passwordPrompt(void) {
  char *p;
  p = getpass("Enter MySQL Password: ");

  return p;
}

void ask_password(){
  // prompt for password if it's NULL
  if (sizeof(password) == 0 || (password == NULL && askPassword)) {
    password = passwordPrompt();
  }
}

