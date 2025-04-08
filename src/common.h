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

    Authors:        David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <mysql.h>
#include <stdio.h>
#include "common_options.h"
#define MYLOADER_MODE "myloader_mode"
#define IS_TRX_TABLE 2
#define INCLUDE_CONSTRAINT 4
#define IS_ALTER_TABLE_PRESENT 8
#define START_SLAVE "START SLAVE"
#define START_SLAVE_SQL_THREAD "START SLAVE SQL_THREAD"
#define CALL_START_REPLICATION "CALL mysql.rds_start_replication();"
#define STOP_SLAVE_SQL_THREAD "STOP SLAVE SQL_THREAD"
#define STOP_SLAVE "STOP SLAVE"
#define CALL_STOP_REPLICATION "CALL mysql.rds_stop_replication();"
#define RESET_SLAVE "RESET SLAVE"
#define CALL_RESET_EXTERNAL_MASTER "CALL mysql.rds_reset_external_master()"
#define SHOW_SLAVE_STATUS "SHOW SLAVE STATUS"
#define SHOW_ALL_SLAVES_STATUS "SHOW ALL SLAVES STATUS"
#define START_REPLICA "START REPLICA"
#define START_REPLICA_SQL_THREAD "START REPLICA SQL_THREAD"
#define STOP_REPLICA "STOP REPLICA"
#define STOP_REPLICA_SQL_THREAD "STOP REPLICA SQL_THREAD"
#define RESET_REPLICA "RESET REPLICA"
#define SHOW_REPLICA_STATUS "SHOW REPLICA STATUS"
#define SHOW_ALL_REPLICAS_STATUS "SHOW ALL REPLICAS STATUS"
#define SHOW_MASTER_STATUS "SHOW MASTER STATUS"
#define SHOW_BINLOG_STATUS "SHOW BINLOG STATUS"
#define SHOW_BINARY_LOG_STATUS "SHOW BINARY LOG STATUS"
#define CHANGE_MASTER "CHANGE MASTER"
#define CHANGE_REPLICATION_SOURCE "CHANGE REPLICATION SOURCE"
#define ZSTD_EXTENSION ".zst"
#define GZIP_EXTENSION ".gz"

extern GList *ignore_errors_list;
extern gchar zstd_paths[2][15];
extern gchar gzip_paths[2][15];
extern gchar **zstd_cmd;
extern gchar **gzip_cmd;
extern const gchar *start_replica;
extern const gchar *stop_replica;
extern const gchar *start_replica_sql_thread;
extern const gchar *stop_replica_sql_thread;
extern const gchar *reset_replica;
extern const gchar *show_replica_status;
extern const gchar *show_all_replicas_status;
extern const gchar *show_binary_log_status;
extern const gchar *change_replication_source;
extern guint source_control_command;
#ifndef _src_common_h
#define _src_common_h
void initialize_share_common();
void initialize_zstd_cmd();
void initialize_gzip_cmd();

struct object_to_export{
  gboolean no_data;
  gboolean no_schema;
  gboolean no_trigger;
};

struct configuration_per_table{
  GHashTable *all_anonymized_function;
  GHashTable *all_where_per_table;
  GHashTable *all_limit_per_table;
  GHashTable *all_num_threads_per_table;
  GHashTable *all_columns_on_select_per_table;
  GHashTable *all_columns_on_insert_per_table;
  GHashTable *all_object_to_export;
  GHashTable *all_partition_regex_per_table;
  GHashTable *all_rows_per_table;
};

struct M_ROW{
  MYSQL_RES *res;
  MYSQL_ROW row;
};

#define STREAM_BUFFER_SIZE 1000000
#define STREAM_BUFFER_SIZE_NO_STREAM 100
#define DEFAULTS_FILE "/etc/mydumper.cnf"
struct function_pointer;
typedef gchar * (*fun_ptr)(gchar **,gulong*, struct function_pointer*);

struct function_pointer{
  // use when writing
  fun_ptr function;
  gboolean is_pre;

  // Content after `column`=
  gchar *value;

  // Used inside the function
  GList *parse;
  GList *delimiters;
  GHashTable *memory;
  gboolean replace_null;
  guint max_length;
  guint null_max_length;
  GList *unique_list;
  gboolean unique;
};

gchar * remove_new_line(gchar *to);
char * checksum_table_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_table(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_process_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_trigger_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_view_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_database_defaults(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_table_indexes(MYSQL *conn, char *database, char *table, int *errn);
int write_file(FILE * file, char * buff, int len);
guint strcount(gchar *text);
void m_remove0(gchar * directory, const gchar * filename);
gboolean m_remove(gchar * directory, const gchar * filename);
GKeyFile * load_config_file(gchar * config_file);
void load_config_group(GKeyFile *kf, GOptionContext *context, const gchar * group);
void execute_gstring(MYSQL *conn, GString *ss);
gchar *replace_escaped_strings(gchar *c);
void escape_tab_with(gchar *to);
void load_hash_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar * group_variables);
//void load_anonymized_functions_from_key_file(GKeyFile *kf, GHashTable *all_anonymized_function, fun_ptr get_function_pointer_for());
//void load_per_table_info_from_key_file(GKeyFile *kf, struct configuration_per_table * conf_per_table, fun_ptr get_function_pointer_for());
void load_per_table_info_from_key_file(GKeyFile *kf, struct configuration_per_table * conf_per_table, struct function_pointer * init_function_pointer());
void refresh_set_session_from_hash(GString *ss, GHashTable * set_session_hash);
void refresh_set_global_from_hash(GString *ss, GString *sr, GHashTable * set_global_hash);
gboolean is_table_in_list(gchar *database, gchar *table, gchar **tl);
gboolean is_mysql_special_tables(gchar *database, gchar *table);
GHashTable * initialize_hash_of_session_variables();
void load_common_entries(GOptionGroup *main_group);
void free_hash(GHashTable * set_session_hash);
void initialize_common_options(GOptionContext *context, const gchar *group);
gchar **get_table_list(gchar *tables_list);
void free_hash_table(GHashTable * hash);
void remove_definer(GString * data);
void remove_definer_from_gchar(char * str);
void print_version(const gchar *program);
gboolean stream_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error);
void initialize_set_names();
void free_set_names();
gchar *filter_sequence_schemas(const gchar *create_table);
void set_session_hash_insert(GHashTable * set_session_hash, const gchar *key, gchar *value);
void parse_key_file_group(GKeyFile *kf, GOptionContext *context, const gchar * group);
#endif

/* using fewer than 2 threads can cause mydumper to hang */
#define MIN_THREAD_COUNT 2
void check_num_threads();

void m_error(const char *fmt, ...);
void m_critical(const char *fmt, ...);
void m_warning(const char *fmt, ...);
void m_message(const char *fmt, ...);
void load_hash_of_all_variables_perproduct_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar *str);
GRecMutex * g_rec_mutex_new();
gboolean read_data(FILE *file, GString *data, gboolean *eof, guint *line);
gchar *m_date_time_new_now_local();

void print_int(const char*_key, int val);
void print_string(const char*_key, const char *val);
void print_bool(const char*_key, gboolean val);
void print_list(const char*_key, GList *list);

gchar *get_zstd_cmd();
gchar *get_gzip_cmd();
char * double_quoute_protect(char *r);
char * backtick_protect(char *r);
char * newline_protect(char *r);
char * newline_unprotect(char *r);
void set_thread_name(const char *format, ...);
extern void trace(const char *format, ...);
#define message(...) \
  if (debug) \
    trace(__VA_ARGS__); \
  else \
    g_message(__VA_ARGS__);

#define array_elements(A) ((guint) (sizeof(A)/sizeof(A[0])))
#define key_strcmp ((int (*)(const void *, const void *)) &strcmp)

#if !GLIB_CHECK_VERSION(2, 68, 0)
extern guint
g_string_replace (GString     *string,
                  const gchar *find,
                  const gchar *replace,
                  guint        limit);
#endif

#if !GLIB_CHECK_VERSION(2, 36, 0)
extern guint g_get_num_processors (void);
#endif
char *show_warnings_if_possible(MYSQL *conn);
int global_process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, gchar *real_table, gboolean split_indexes);
void initialize_conf_per_table(struct configuration_per_table *cpt);
void parse_object_to_export(struct object_to_export *object_to_export,gchar *val);
gchar *build_dbt_key(gchar *a, gchar *b);

gboolean common_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error);
void discard_mysql_output(MYSQL *conn);
gboolean m_query(  MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...);
gboolean m_query_verbose(MYSQL *conn, const char *q, void log_fun(const char *, ...) , const char *fmt, ...);
gboolean m_query_warning(  MYSQL *conn, const gchar *query, const char *fmt, ...);
gboolean m_query_critical( MYSQL *conn, const gchar *query, const char *fmt, ...);
MYSQL_RES *m_store_result(MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...);
MYSQL_RES *m_store_result_critical(MYSQL *conn, const gchar *query, const char *fmt, ...);
MYSQL_RES *m_use_result(MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...);
struct M_ROW* m_store_result_row(MYSQL *conn, const gchar *query, void log_fun_1(const char *, ...), void log_fun_2(const char *, ...), const char *fmt, ...);
struct M_ROW* m_store_result_single_row(MYSQL *conn, const gchar *query, const char *fmt, ...);
void m_store_result_row_free(struct M_ROW* mr);
gboolean create_dir(gchar *directory);
gchar *build_tmp_dir_name();
GThread * m_thread_new(const gchar* title, GThreadFunc func, gpointer data, const gchar* error_text);
