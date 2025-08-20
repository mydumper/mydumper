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

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/
#ifndef _src_myloader_common_h
#define _src_myloader_common_h

#include "myloader.h"
#include <stdio.h> 
//guint execute_use(struct thread_data *td, const gchar * msg);
//gboolean execute_use(struct thread_data *td);
gboolean execute_use(struct connection_data *cd);
//void execute_use_if_needs_to(struct thread_data *td, struct database *database, const gchar * msg);
void execute_use_if_needs_to(struct connection_data *cd, struct database *database, const gchar * msg);
enum file_type get_file_type (const char * filename);
struct database * get_db_hash(gchar *k, gchar *v);
//struct database * db_hash_insert(gchar *k, gchar *v);
//struct database * db_hash_lookup(gchar *database);
gboolean eval_table( char *db_name, char * table_name, GMutex * mutex);
//void load_schema(structconfiguration *conf, struct db_table *dbt, const gchar *filename);
void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table);
//int process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, struct db_table *dbt);
int process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, struct db_table *dbt, gboolean split_indexes);
void finish_alter_table(GString * alter_table_statement);
void initialize_common();
gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash);
void refresh_table_list(struct configuration *conf);
void refresh_table_list_without_table_hash_lock(struct configuration *conf, gboolean force);
void checksum_databases(struct thread_data *td);
void checksum_table_filename(const gchar *filename, MYSQL *conn);
//int execute_file_per_thread( const gchar *sql_fn, gchar *sql_fn3);
int execute_file_per_thread( const gchar *sql_fn, gchar *sql_fn3, gchar **exec);
gboolean has_compession_extension(const gchar *filename);
gboolean has_exec_per_thread_extension(const gchar *filename);
gboolean checksum_dbt(struct db_table *dbt,  MYSQL *conn) ;
gboolean checksum_database_template(gchar *_db, gchar *dbt_checksum,  MYSQL *conn,
                                const gchar *message, gchar* fun());
gchar *get_value(GKeyFile * kf,gchar *group, const gchar *key);
void change_master(GKeyFile * kf,gchar *group, struct replication_statements *replication_statements, struct replication_settings *rep_set);
gboolean get_command_and_basename(gchar *filename, gchar ***command, gchar **basename);
gboolean m_filename_has_suffix(gchar const *str, gchar const *suffix);
void initialize_thread_data(struct thread_data*td, struct configuration *conf, enum thread_states status, guint thread_id, struct db_table *dbt);
gboolean is_in_ignore_set_list(gchar *haystack);
void remove_ignore_set_session_from_hash();
void execute_replication_commands(MYSQL *conn, gchar *statement);
#endif
