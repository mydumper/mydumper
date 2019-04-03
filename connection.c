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
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "connection.h"

extern GArray *resolve_ips;
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
		mysql_options(conn,MYSQL_READ_DEFAULT_FILE,defaults_file);
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

	mysql_ssl_set(conn,key,cert,ca,capath,cipher);
	mysql_options(conn,MYSQL_OPT_SSL_MODE,&i);
#endif
}

void pre_resolve_host(const char *host) {
	if (host == NULL) {
		return;
	}

	struct addrinfo hints, *res;
	int errcode;
	char addrstr[100];
	void *ptr;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	errcode = getaddrinfo(host, NULL, &hints, &res);
	if (errcode != 0) {
		g_critical("failed to resolve host %s\n", host);
		exit(EXIT_FAILURE);
	}

	while (res) {
		if (res->ai_family == AF_INET6) {
			ptr = &((struct sockaddr_in6 *) res->ai_addr)->sin6_addr;
		} else if (res->ai_family == AF_INET) {
			ptr = &((struct sockaddr_in *) res->ai_addr)->sin_addr;
		} else {
			g_warning("skip unsupported ai_family: %d", res->ai_family);
			continue;
		}
		inet_ntop(res->ai_family, ptr, addrstr, 100);
		char *addrptr = g_strndup(addrstr, strlen(addrstr));
		g_array_append_val(resolve_ips, addrptr);
		res = res->ai_next;
	}
}

MYSQL *mysql_connect_wrap(MYSQL *mysql, const char *user, const char *passwd,
                          const char *connect_db, unsigned int db_port,
                          const char *unix_socket, unsigned long client_flag) {

	if (resolve_ips == NULL || resolve_ips->len == 0) {
		return mysql_real_connect(mysql, NULL, user, passwd, connect_db, db_port, unix_socket, client_flag);
	}
	guint i = 0;
	for(; i < resolve_ips->len; ++i) {
		char *ip = g_array_index(resolve_ips, char*, i);
		if(!mysql_real_connect(mysql, ip, user, passwd, connect_db, db_port, unix_socket, client_flag)) {
			g_warning("Failed to connect to ip: %s error: %s", ip, mysql_error(mysql));
		} else {
			return mysql;
		}
	}
	return NULL;
}
