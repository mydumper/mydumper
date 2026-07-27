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
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot org)
                    Andrew Hutchings, MariaDB Foundation (andrew dot mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

#include "mydumper_string_planner_utils.h"

const gchar *string_pk_planner_strategy_name(enum string_pk_planner_strategy strategy){
  switch (strategy) {
    case STRING_PK_PLANNER_METADATA:
      return "metadata";
    case STRING_PK_PLANNER_RECURSIVE:
      return "recursive";
    case STRING_PK_PLANNER_AUTO:
    default:
      return "auto";
  }
}

gboolean string_pk_planner_strategy_from_string(const gchar *value, enum string_pk_planner_strategy *strategy){
  if (strategy == NULL) {
    return FALSE;
  }

  if (value == NULL || !g_ascii_strcasecmp(value, "auto")) {
    *strategy = STRING_PK_PLANNER_AUTO;
    return TRUE;
  }

  if (!g_ascii_strcasecmp(value, "metadata")) {
    *strategy = STRING_PK_PLANNER_METADATA;
    return TRUE;
  }

  if (!g_ascii_strcasecmp(value, "recursive")) {
    *strategy = STRING_PK_PLANNER_RECURSIVE;
    return TRUE;
  }

  return FALSE;
}

gboolean string_pk_planner_should_use_metadata_mode(enum string_pk_planner_strategy strategy, gboolean metadata_enabled, gboolean split_pk, guint64 rows, guint64 min_rows){
  if (!split_pk || !metadata_enabled) {
    return FALSE;
  }

  switch (strategy) {
    case STRING_PK_PLANNER_METADATA:
      return TRUE;
    case STRING_PK_PLANNER_RECURSIVE:
      return FALSE;
    case STRING_PK_PLANNER_AUTO:
    default:
      return rows >= min_rows;
  }
}

guint64 string_pk_planner_compute_root_step(guint64 rows, guint prefix_count, guint64 min_chunk_step_size_value){
  guint64 root_step = rows > 0 && prefix_count > 0 ? rows / prefix_count : 0;
  if (root_step == 0) {
    root_step = min_chunk_step_size_value > 0 ? min_chunk_step_size_value : 1;
  }
  if (root_step < min_chunk_step_size_value) {
    root_step = min_chunk_step_size_value;
  }
  return root_step;
}
