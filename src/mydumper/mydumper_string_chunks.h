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
#if !defined(mydumper_mydumper_string_chunks)
#define mydumper_mydumper_string_chunks

#include "mydumper_chunks.h"

struct string_step {
  gboolean is_step_fixed_length;
  guint64 step;
  guint64 estimated_remaining_steps;
  guint64 rows_in_explain;
  gboolean check_max;
  gboolean check_min;
};
#endif 

guint process_string_chunk_step(struct table_job *tj, struct chunk_step_item *csi);
void process_string_chunk(struct table_job *tj, struct chunk_step_item *csi);
struct chunk_step_item *get_next_string_chunk(struct db_table *dbt);
