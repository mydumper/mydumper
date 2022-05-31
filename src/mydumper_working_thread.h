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

#define INSERT_IGNORE "INSERT IGNORE"
#define INSERT "INSERT"
#define REPLACE "REPLACE"

typedef gchar * (*fun_ptr2)(gchar **);


void load_working_thread_entries(GOptionGroup *main_group);
void *working_thread(struct thread_data *td);
void dump_table(MYSQL *conn, struct db_table *dbt, struct configuration *conf, gboolean is_innodb);
void create_jobs_for_non_innodb_table_list_in_less_locking_mode(MYSQL *conn, GList *noninnodb_tables_list, struct configuration *conf);
void new_table_to_dump(MYSQL *conn, struct configuration *conf, gboolean is_view, struct database * database, char *table, char *datalength, gchar *ecol);
void initialize_working_thread();
