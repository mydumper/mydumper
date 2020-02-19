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

#include <pcre.h>
#include <glib.h>
#include <string.h>
#include "config.h"
#include "connection.h"

extern char *defaults_file;
#ifdef WITH_SSL
extern char *key;
extern char *cert;
extern char *ca;
extern char *capath;
extern char *cipher;
extern gboolean ssl;
#endif
extern guint compress_protocol;

void configure_connection(MYSQL *conn, const char *name) {
  if (defaults_file != NULL) {
    mysql_options(conn, MYSQL_READ_DEFAULT_FILE, defaults_file);
  }
  mysql_options(conn, MYSQL_READ_DEFAULT_GROUP, name);

  if (compress_protocol)
    mysql_options(conn, MYSQL_OPT_COMPRESS, NULL);

#ifdef WITH_SSL
  unsigned int i;
  if (ssl) {
    i = SSL_MODE_REQUIRED;
  } else {
    i = SSL_MODE_DISABLED;
  }

  mysql_ssl_set(conn, key, cert, ca, capath, cipher);
  mysql_options(conn, MYSQL_OPT_SSL_MODE, &i);
#endif
}
