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
struct chunk_step_item *new_integer_step_item(gboolean include_null, GString *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, gboolean is_step_fixed_length, guint64 step, guint64 min_css, guint64 max_css, guint64 number, gboolean check_min, gboolean check_max, struct chunk_step_item * next, guint position);
void free_integer_step(union chunk_step * cs);
struct chunk_step_item *get_next_integer_chunk(struct db_table *dbt);
void process_integer_chunk(struct table_job *tj, struct chunk_step_item *csi);
gchar * get_integer_chunk_where(union chunk_step * chunk_step);
void update_integer_where_on_gstring(GString *where, gchar * field, gboolean is_unsigned, union type type, gboolean use_cursor);
