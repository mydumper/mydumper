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
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

struct schema_job {
  char *database;
  char *table;
  char *filename;
  char *checksum_filename;
  char *checksum_index_filename;
};

struct create_tablespace_job{
  char *filename;
};


struct create_database_job {
  char *database;
  char *filename;
  char *checksum_filename;
};

struct view_job {
  char *database;
  char *table;
  char *filename;
  char *filename2;
  char *checksum_filename;
};

struct schema_post_job {
  struct database *database;
  char *filename;
  char *checksum_filename;
};

struct table_checksum_job {
  char *database;
  char *table;
  char *filename;
//  char *schema_filename;
};

void initialize_dump_into_file();
void load_dump_into_file_entries(GOptionGroup *main_group);
void create_job_to_dump_tablespaces(MYSQL *conn, struct configuration *conf);
void create_job_to_dump_post(struct database *database, struct configuration *conf);
void create_job_to_dump_table_schema(struct db_table *dbt, struct configuration *conf, GAsyncQueue *queue);
void create_job_to_dump_view(struct db_table *dbt, struct configuration *conf);
void create_job_to_dump_checksum(struct db_table * dbt, struct configuration *conf);
void create_job_to_dump_database(struct database *database, struct configuration *conf, gboolean less_locking);
void create_job_to_dump_schema(char *database, struct configuration *conf);
void create_job_to_dump_triggers(MYSQL *conn, struct db_table *dbt, struct configuration *conf);
void create_job_to_dump_table(MYSQL *conn, struct db_table *dbt,
                    struct configuration *conf, gboolean is_innodb);
void create_jobs_for_non_innodb_table_list_in_less_locking_mode(MYSQL *conn, GList *noninnodb_tables_list,
                     struct configuration *conf);
void write_table_checksum_into_file(MYSQL *conn, char *database, char *table, char *filename);
void write_table_metadata_into_file(struct db_table * dbt);
void do_JOB_CREATE_DATABASE(struct thread_data *td, struct job *job);
void do_JOB_CREATE_TABLESPACE(struct thread_data *td, struct job *job);
void do_JOB_SCHEMA_POST(struct thread_data *td, struct job *job);
void do_JOB_VIEW(struct thread_data *td, struct job *job);
void do_JOB_SCHEMA(struct thread_data *td, struct job *job);
void do_JOB_TRIGGERS(struct thread_data *td, struct job *job);
void do_JOB_CHECKSUM(struct thread_data *td, struct job *job);

gchar *get_ref_table(gchar *k);
