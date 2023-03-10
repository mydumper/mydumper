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

void load_restore_entries(GOptionGroup *main_group);
int restore_data_from_file(struct thread_data *td, char *database, char *table,
                  const char *filename, gboolean is_schema);
int restore_data_in_gstring_by_statement(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter);
int restore_data_in_gstring(struct thread_data *td, GString *data, gboolean is_schema, guint *query_counter);
void release_load_data_as_it_is_close( gchar * filename );
