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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

void load_write_entries(GOptionGroup *main_group, GOptionContext *context);
void initialize_write();
guint64 write_table_data_into_file(MYSQL *conn, struct table_job *tj);
gboolean write_statement(FILE *load_data_file, float *filessize, GString *statement, struct db_table * dbt);
gboolean write_load_data_statement(struct table_job * tj, MYSQL_FIELD *fields, guint num_fields);
gboolean real_write_data(FILE *file, float *filesize, GString *data);
void initialize_sql_statement(GString *statement);
void message_dumping_data(struct thread_data *td, struct table_job *tj);
