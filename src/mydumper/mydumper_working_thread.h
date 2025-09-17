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

#define INSERT_IGNORE "INSERT IGNORE"
#define INSERT "INSERT"
#define REPLACE "REPLACE"
#define UNLOCK_TABLES "UNLOCK TABLES"


#ifndef _src_mydumper_working_thread_h
#define _src_mydumper_working_thread_h

struct thread_data_buffers {
  GString *statement;
  GString *row;
  GString *escaped;
  GString *column;
};

struct thread_data {
  struct configuration *conf;
  guint thread_id;
  char *table_name;
  MYSQL *thrconn;
  gchar *binlog_snapshot_gtid_executed;
  GMutex *pause_resume_mutex;
  struct thread_data_buffers thread_data_buffers;
};

#endif


typedef gchar * (*fun_ptr2)(gchar **);

void initialize_working_thread();
void start_working_thread(struct configuration *conf );
void wait_working_thread_to_finish();
void finalize_working_thread();

void load_working_thread_entries(GOptionContext *context, GOptionGroup *extra_group, GOptionGroup * filter_group);
void dump_table(MYSQL *conn, struct db_table *dbt, struct configuration *conf, gboolean is_transactional);
void build_lock_tables_statement(struct configuration *conf);
void check_pause_resume( struct thread_data *td );
void update_estimated_remaining_chunks_on_dbt(struct db_table *dbt);
void free_db_table(struct db_table * dbt);
void get_binlog_position(MYSQL *conn, char **masterlog, char **masterpos, char **mastergtid);
