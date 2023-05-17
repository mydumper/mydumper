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
#define MYLOADER_MODE "myloader_mode"

#ifndef _src_common_h
#define _src_common_h

enum data_file_type { COMMON, COMPRESSED, FIFO };

struct configuration_per_table{
  GHashTable *all_anonymized_function;
  GHashTable *all_where_per_table;
  GHashTable *all_limit_per_table;
  GHashTable *all_num_threads_per_table;
};

#define STREAM_BUFFER_SIZE 1000000
#define DEFAULTS_FILE "/etc/mydumper.cnf"
struct function_pointer;
typedef gchar * (*fun_ptr)(gchar **,gulong*, struct function_pointer*);

struct function_pointer{
  fun_ptr function;
  GHashTable *memory;
  gchar *value;
  GList *parse;
  GList *delimiters;
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
void create_backup_dir(char *new_directory) ;
guint strcount(gchar *text);
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
gboolean is_table_in_list(gchar *table_name, gchar **tl);
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
#endif

/* using fewer than 2 threads can cause mydumper to hang */
#define MIN_THREAD_COUNT 2
void check_num_threads();

void m_error(const char *fmt, ...);
void m_critical(const char *fmt, ...);
void m_warning(const char *fmt, ...);
void load_hash_of_all_variables_perproduct_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar *str);
GRecMutex * g_rec_mutex_new();
gboolean read_data(FILE *file, enum data_file_type dft, GString *data, gboolean *eof, guint *line);
