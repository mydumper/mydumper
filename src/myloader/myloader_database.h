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
#ifndef _src_myloader_database_h
#define _src_myloader_database_h

struct database {
  gchar *source_database; // aka: the logical schema name, that could be different of the filename.

  gchar *target_database; // aka: the output schema name this can change when use -B.
  gchar *database_name_in_filename; // aka: the key of the schema. Useful if you have mydumper_ filenames.
  enum schema_status schema_state;
  GAsyncQueue *sequence_queue;
  GAsyncQueue *control_job_queue;
  GMutex * mutex; // TODO: use g_mutex_init() instead of g_mutex_new()
  gchar *schema_checksum;
  gchar *post_checksum;
  gchar *triggers_checksum;
  gchar *events_checksum;
};

void initialize_database();
struct database * get_database(gchar *k, gchar *v);
void execute_use_if_needs_to(struct connection_data *cd, struct database *database, const gchar * msg);
gboolean execute_use(struct connection_data *cd);
void start_database(struct thread_data *td);
gboolean has_been_defined_a_target_database();
#endif
