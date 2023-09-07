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
#define METADATA_PARTIAL_INTERVAL 2
void initialize_stream();
void wait_stream_to_finish();
void metadata_partial_push (struct db_table *dbt);
void stream_queue_push(struct db_table *dbt,gchar *filename);
guint get_stream_queue_length();
void send_initial_metadata();
