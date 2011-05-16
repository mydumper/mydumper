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

char *hostname=NULL;
char *username=NULL;
char *password=NULL;
char *socket_path=NULL;
char *db=NULL;
guint port=3306;
guint num_threads= 4;
guint verbose=2;
gboolean compress_protocol= FALSE;
gboolean program_version= FALSE;

GOptionEntry common_entries[] =
{
        { "host", 'h', 0, G_OPTION_ARG_STRING, &hostname, "The host to connect to", NULL },
        { "user", 'u', 0, G_OPTION_ARG_STRING, &username, "Username with privileges to run the dump", NULL },
        { "password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL },
        { "port", 'P', 0, G_OPTION_ARG_INT, &port, "TCP/IP port to connect to", NULL },
        { "socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path, "UNIX domain socket file to use for connection", NULL },
        { "threads", 't', 0, G_OPTION_ARG_INT, &num_threads, "Number of threads to use, default 4", NULL },
        { "compress-protocol", 'C', 0, G_OPTION_ARG_NONE, &compress_protocol, "Use compression on the MySQL connection", NULL },
	{ "version", 'V', 0, G_OPTION_ARG_NONE, &program_version, "Show the program version and exit", NULL },
	{ "verbose", 'v', 0, G_OPTION_ARG_INT, &verbose, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, default 2", NULL },
        { NULL, 0, 0, G_OPTION_ARG_NONE,   NULL, NULL, NULL }
};

#endif
