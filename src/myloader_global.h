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

    Authors:        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    David Ducos, Percona (david dot ducos at percona dot com)

*/

extern GOptionEntry common_filter_entries[];
extern GOptionEntry common_connection_entries[];
extern GOptionEntry common_entries[];
extern gboolean program_version;
extern guint verbose;
extern gboolean debug;

extern gboolean disable_redo_log;
extern gchar *purge_mode_str;
extern GString *set_global;
extern GString *set_global_back;
extern gchar *defaults_file;
extern GKeyFile * key_file;
extern gchar *input_directory;
extern gchar *tables_list;
extern gboolean help;
extern char **tables;
extern gboolean append_if_not_exist;
extern gboolean innodb_optimize_keys;
extern gboolean innodb_optimize_keys_all_tables;
extern gboolean innodb_optimize_keys_per_table;
extern gboolean intermediate_queue_ended;
extern gboolean no_data;
extern gboolean no_delete;
extern gboolean overwrite_tables;
extern gboolean resume;
extern gboolean serial_tbl_creation;
extern gboolean shutdown_triggered;
extern gboolean skip_definer;
extern gboolean skip_post;
extern gboolean skip_triggers;
extern gboolean stream;
extern gchar *compress_extension;
extern gchar *db;
extern gchar *directory;
extern gchar *pmm_path;
extern gchar *pmm_resolution ;
extern gchar *set_names_str;
extern gchar *source_db;
extern gchar *tables_skiplist_file;
extern GHashTable *db_hash;
extern GHashTable * load_data_list;
extern GHashTable *tbl_hash;
extern GMutex *load_data_list_mutex;
extern GString *set_session;
extern guint commit_count;
extern guint errors;
extern guint max_threads_for_index_creation;
extern guint max_threads_per_table;
extern guint num_threads;
extern guint rows;
extern unsigned long long int total_data_sql_files;
extern int detected_server;
extern int (*m_close)(void *file);
extern int (*m_write)(FILE * file, const char * buff, int len);
extern gchar identifier_quote_character;
extern gchar * identifier_quote_character_str;
extern GString *change_master_statement;
