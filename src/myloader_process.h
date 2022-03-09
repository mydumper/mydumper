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


void process_database_filename(char * filename, const char *object);
void process_table_filename(char * filename);
void process_metadata_filename( GHashTable *table_hash, char * filename);
void process_schema_filename(gchar *filename, const char * object);
void process_data_filename(char * filename);
//struct job * new_job (enum job_type type, void *job_data, char *use_database);
struct db_table* append_new_db_table(char * filename, gchar * database, gchar *table, guint64 number_rows, GHashTable *table_hash, GString *alter_table_statement);
void initialize_process(struct configuration *c);
