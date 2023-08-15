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

guint64 gint64_abs(gint64 a);
union chunk_step *new_integer_step(gchar *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, guint64 step, guint64 number, gboolean check_min, gboolean check_max);
void free_integer_step(union chunk_step * cs);
void common_to_chunk_step(struct db_table *dbt, union chunk_step * cs, union chunk_step * new_cs);
union chunk_step * split_unsigned_chunk_step(struct db_table *dbt, union chunk_step * cs);
union chunk_step * split_signed_chunk_step(struct db_table *dbt, union chunk_step * cs);
union chunk_step *get_next_integer_chunk(struct db_table *dbt);
void update_integer_min(MYSQL *conn, struct table_job *tj);
void update_integer_max(MYSQL *conn, struct table_job *tj);
void process_integer_chunk(struct thread_data *td, struct table_job *tj);
gchar * update_integer_where(struct thread_data *td, union chunk_step * chunk_step);