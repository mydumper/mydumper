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

#define MIN_CHUNK_STEP_SIZE 1000



union chunk_step;

#if !defined(mydumper_mydumper_chunks)
#define mydumper_mydumper_chunks

#include "mydumper_integer_chunks.h"
#include "mydumper_partition_chunks.h"

enum chunk_type{
  NONE,
  INTEGER,
  CHAR,
  PARTITION,
  MULTICOLUMN_INTEGER
};

enum chunk_states{
  UNASSIGNED,
  ASSIGNED,
  DUMPING_CHUNK,
  UNSPLITTABLE,
  COMPLETED
};

struct chunk_functions{
  void (*process)(struct table_job *tj, struct chunk_step_item *csi);
  struct chunk_step_item *(*get_next)(struct db_table *dbt);
};

union chunk_step {
  struct integer_step integer_step;
  struct partition_step partition_step;
};

struct chunk_step_item{
  union chunk_step *chunk_step;
  enum chunk_type chunk_type;
  struct chunk_step_item *next;
  struct chunk_functions chunk_functions;
  gboolean multicolumn;
  GString *where;
  gboolean include_null;
  GString *prefix;
  gchar *field;
  guint64 part;
  guint deep;
  guint position;
  GMutex *mutex;
  gboolean needs_refresh;
  enum chunk_states status;
};

#endif

void initialize_chunk();
void start_chunk_builder(struct configuration *conf);


guint64 gint64_abs(gint64 a);
void load_chunks_entries(GOptionContext *context);
GList *get_chunks_for_table(MYSQL *conn, struct db_table * dbt,
                            struct configuration *conf);
void set_chunk_strategy_for_dbt(MYSQL *conn, struct db_table *dbt);
void free_integer_step(union chunk_step * cs);
union chunk_step *get_next_chunk(struct db_table *dbt);
void *chunk_builder_thread(struct configuration *conf);
void finalize_chunk();
extern GAsyncQueue *give_me_another_transactional_chunk_step_queue;
extern GAsyncQueue *give_me_another_non_transactional_chunk_step_queue;
struct chunk_step_item * initialize_chunk_step_item (MYSQL *conn, struct db_table *dbt, guint position, guint64 rows, GString *local_where) ;
void build_where_clause_on_table_job(struct table_job *tj);
guint64 get_rows_from_explain(MYSQL * conn, struct db_table *dbt, GString *where, gchar *field);
guint64 get_rows_from_count(MYSQL * conn, struct db_table *dbt, GString *where);
