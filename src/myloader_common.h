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

#define IS_INNODB_TABLE 2
#define INCLUDE_CONSTRAINT 4
#define IS_ALTER_TABLE_PRESENT 8

#include "myloader.h"
#include <stdio.h> 
#include "common.h"
//guint execute_use(struct thread_data *td, const gchar * msg);
gboolean execute_use(struct thread_data *td);
void execute_use_if_needs_to(struct thread_data *td, gchar *database, const gchar * msg);
enum file_type get_file_type (const char * filename);
struct database * get_db_hash(gchar *k, gchar *v);
//struct database * db_hash_insert(gchar *k, gchar *v);
//struct database * db_hash_lookup(gchar *database);
gboolean eval_table( char *db_name, char * table_name, GMutex * mutex);
//void load_schema(structconfiguration *conf, struct db_table *dbt, const gchar *filename);
void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table);
int process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, struct db_table *dbt);
void finish_alter_table(GString * alter_table_statement);
void initialize_common();
gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash);
void refresh_table_list(struct configuration *conf);
void refresh_table_list_without_table_hash_lock(struct configuration *conf);
void checksum_databases(struct thread_data *td);
void checksum_table_filename(const gchar *filename, MYSQL *conn);
int execute_file_per_thread( const gchar *sql_fn, gchar *sql_fn3);
void ml_open(FILE **infile, const gchar *filename);
gboolean has_compession_extension(const gchar *filename);
gboolean has_exec_per_thread_extension(const gchar *filename);
gchar *build_dbt_key(gchar *a, gchar *b);
gboolean m_query(  MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...);
void checksum_dbt(struct db_table *dbt,  MYSQL *conn) ;
void checksum_database_template(gchar *database, gchar *dbt_checksum,  MYSQL *conn, const gchar *message, gchar* fun()) ;
gchar *get_value(GKeyFile * kf,gchar *group, const gchar *key);
void change_master(GKeyFile * kf,gchar *group, GString *s);
#endif
