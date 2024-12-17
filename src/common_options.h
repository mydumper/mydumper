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




#define BACKTICK '`'
#define DOUBLE_QUOTE '"'
#define TRADITIONAL 0
#define AWS 1

extern char identifier_quote_character;
extern const char *identifier_quote_character_str;
extern guint max_threads_per_table;
extern gchar *set_names_str;
extern gchar *set_names_statement;
extern gboolean no_stream;
extern gboolean stream;
extern gboolean no_delete;
extern gchar *defaults_file;
extern char *defaults_extra_file;
extern GKeyFile * key_file;
extern guint num_threads;
extern MYSQL *main_connection;
extern GString *set_global_back;
extern gboolean no_sync;


