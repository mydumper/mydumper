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
#if !defined(mydumper_mydumper_string_planner_utils)
#define mydumper_mydumper_string_planner_utils

#include <glib.h>

enum string_pk_planner_strategy {
  STRING_PK_PLANNER_AUTO = 0,
  STRING_PK_PLANNER_METADATA,
  STRING_PK_PLANNER_RECURSIVE
};

gboolean string_pk_planner_strategy_from_string(const gchar *value, enum string_pk_planner_strategy *strategy);
const gchar *string_pk_planner_strategy_name(enum string_pk_planner_strategy strategy);
gboolean string_pk_planner_should_use_metadata_mode(enum string_pk_planner_strategy strategy, gboolean metadata_enabled, gboolean split_string_pk, guint64 rows, guint64 min_rows);
guint64 string_pk_planner_compute_root_step(guint64 rows, guint prefix_count, guint64 min_chunk_step_size);

/*
 * Resolves the effective per-prefix row target used by the metadata-assisted
 * planner.  When target_rows_per_prefix is greater than zero it is used
 * directly; otherwise the target is derived from rows / max_prefixes.  The
 * result is always at least 1 and never below min_chunk_step_size.
 */
guint64 string_pk_planner_compute_target(guint64 rows, guint64 target_rows_per_prefix, guint max_prefixes, guint64 min_chunk_step_size);

/*
 * Returns TRUE when a candidate level with candidate_count prefixes still fits
 * within the configured max_prefixes budget (0 means unbounded).
 */
gboolean string_pk_planner_level_fits_budget(guint candidate_count, guint max_prefixes);

#endif
