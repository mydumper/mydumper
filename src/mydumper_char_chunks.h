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

void initialize_char_chunk();
//union chunk_step *new_char_step(MYSQL *conn, gchar *field, /*GList *list,*/ guint deep, guint number, MYSQL_ROW row, gulong *lengths);
struct chunk_step_item *new_char_step_item(MYSQL *conn, gchar *field, /*GList *list,*/ guint deep, guint number, MYSQL_ROW row, gulong *lengths);
void next_chunk_in_char_step(union chunk_step * cs);
void free_char_step(union chunk_step * cs);
struct chunk_step_item *get_next_char_chunk(struct db_table *dbt);
gchar * get_escaped_middle_char(MYSQL *conn, gchar *c1, guint c1len, gchar *c2, guint c2len, guint part);
gboolean get_new_minmax (struct thread_data *td, struct db_table *dbt, union chunk_step *cs);
gchar* update_cursor (MYSQL *conn, struct table_job *tj);
void process_char_chunk(struct table_job *tj);
gchar * update_char_where(union chunk_step * chunk_step);
