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

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/
#define DEFAULT_DELIMITER ";\n"

enum kind_of_statement { NOT_DEFINED, INSERT, OTHER, CLOSE};

struct statement{
  guint result;
  guint preline;
  GString *buffer;
  const gchar *filename;
  enum kind_of_statement kind_of_statement;
  gboolean is_schema;
  gchar *error;
  guint error_number;
//  struct thread_data *td;
};

void initialize_connection_pool();
void start_connection_pool(MYSQL *thrconn);

int restore_data_in_gstring(struct thread_data *td, GString *data, gboolean is_schema, struct database *use_database);
int restore_data_in_gstring_extended(struct thread_data *td, GString *data, gboolean is_schema, struct database *use_database, void log_fun(const char *, ...) , const char *fmt, ...);
int restore_data_from_mydumper_file(struct thread_data *td, const char *filename, gboolean is_schema, struct database *use_database);
void release_load_data_as_it_is_close( gchar * filename );
struct connection_data *close_restore_thread(gboolean return_connection);
void wait_restore_threads_to_close();
