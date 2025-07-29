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
#define MAX_TIME_PER_QUERY 2


#if !defined(mydumper_mydumper_interger_chunks)
#define mydumper_mydumper_interger_chunks

#include "mydumper_chunks.h"

struct unsigned_int{
  guint64 initial_min;
  guint64 min;
  guint64 cursor;
  guint64 max;
};

struct signed_int{
  gint64 initial_min;
  gint64 min;
  gint64 cursor;
  gint64 max;
};

union type {
  struct unsigned_int unsign;
  struct signed_int   sign;
};

struct integer_step {
  gboolean is_unsigned;
  union type type;
  gboolean is_step_fixed_length;
  guint64 step;
  guint64 min_chunk_step_size;
  guint64 max_chunk_step_size;
  guint64 estimated_remaining_steps;
  gboolean check_max;
  gboolean check_min;
  guint64 rows_in_explain;
};
#endif 

guint64 gint64_abs(gint64 a);
struct chunk_step_item *new_integer_step_item(gboolean include_null, GString *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, gboolean is_step_fixed_length, guint64 step, guint64 min_css, guint64 max_css, guint64 number, gboolean check_min, gboolean check_max, struct chunk_step_item * next, guint position, gboolean multicolumn, guint64 rows_in_explain);
void free_integer_step(union chunk_step * cs);
struct chunk_step_item *get_next_integer_chunk(struct db_table *dbt);
void process_integer_chunk(struct table_job *tj, struct chunk_step_item *csi);
gchar * get_integer_chunk_where(union chunk_step * chunk_step);
void update_integer_where_on_gstring(GString *where, gboolean include_null, GString *prefix, gchar * field, gboolean is_unsigned, union type type, gboolean use_cursor);
