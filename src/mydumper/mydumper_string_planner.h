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
                    Andrew Hutchings, MariaDB Foundation (andrew dot mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/
#if !defined(mydumper_mydumper_string_planner)
#define mydumper_mydumper_string_planner

#include <glib.h>
#include <mysql.h>
#include "mydumper_string_planner_utils.h"

struct db_table;

gboolean string_pk_planner_enabled_for_table(guint64 rows);
void string_pk_planner_reset_for_table(struct db_table *dbt, guint64 rows);
gboolean string_pk_planner_budget_exhausted(struct db_table *dbt);
gboolean string_pk_planner_note_probe(struct db_table *dbt);
gboolean string_pk_plan_prefix_chunks(MYSQL *conn, struct db_table *dbt, guint64 rows);

#endif
