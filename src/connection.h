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
#ifndef _connection_h
#define _connection_h
#include <mysql.h>

void initialize_connection(const gchar *app);
//void initialize_connection(gchar *cdf, const gchar *group, const gchar *app);
void set_connection_defaults_file_and_group(gchar *cdf, const gchar *group);
void m_connect(MYSQL *conn);
//void m_connect(MYSQL *conn, gchar *schema);
void hide_password(int argc, char *argv[]);
void ask_password();
GOptionGroup * load_connection_entries(GOptionContext *context);

extern char *hostname;
extern char *username;
extern char *password;
extern gboolean askPassword;
extern char *socket_path;
extern guint port;
extern char *tls_version; 
extern char *capath;
extern char *key;
extern char *cert;
extern char *ca;
extern gboolean ssl;
extern gchar *ssl_mode;
extern char *cipher;
extern gboolean compress_protocol;
extern char *protocol_str;
#endif
