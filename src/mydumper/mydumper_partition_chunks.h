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

#if !defined(mydumper_mydumper_partition_chunks)
#define mydumper_mydumper_partition_chunks

#include "mydumper_chunks.h"

struct partition_step{
  GList *list;
  gchar *current_partition;
};

#endif

union chunk_step *new_real_partition_step(GList *partition);
struct chunk_step_item *get_next_partition_chunk(struct db_table *dbt);
GList * get_partitions_for_table(MYSQL *conn, struct db_table *dbt);
void process_partition_chunk(struct table_job *tj, struct chunk_step_item *csi);
struct chunk_step_item *new_real_partition_step_item(GList *partition, guint deep, guint number);
