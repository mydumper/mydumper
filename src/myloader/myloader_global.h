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


#define SEQUENCE "sequence"
#define TRIGGER "trigger"
#define POST "post"
#define TABLESPACE "tablespace"
#define CREATE_DATABASE "create database"
#define VIEW "view"
#define INDEXES "indexes"
#define CONSTRAINTS "constraints"

extern struct restore_errors detailed_errors;
extern GOptionEntry common_filter_entries[];
extern GOptionEntry common_connection_entries[];
extern GOptionEntry common_entries[];
extern gboolean program_version;
extern guint verbose;
extern gboolean debug;

enum checksum_modes {
  CHECKSUM_SKIP= 0,
  CHECKSUM_WARN,
  CHECKSUM_FAIL
};

extern gboolean disable_redo_log;
extern enum checksum_modes checksum_mode;
extern enum purge_mode purge_mode;
extern GString *set_global;
extern GString *set_global_back;
extern gchar *defaults_file;
extern char *defaults_extra_file;
extern GKeyFile * key_file;
extern gchar *input_directory;
extern gchar *fifo_directory;
extern gchar *tables_list;
extern gboolean help;
extern char **tables;
extern gboolean append_if_not_exist;
extern gboolean optimize_keys;
extern gboolean optimize_keys_all_tables;
extern gboolean optimize_keys_per_table;
extern gboolean intermediate_queue_ended;
extern gboolean no_data;
extern gboolean no_schemas;
extern gboolean no_delete;
extern gboolean overwrite_tables;
extern gboolean overwrite_unsafe;
extern gboolean resume;
extern gboolean serial_tbl_creation;
extern gboolean shutdown_triggered;
extern gboolean skip_definer;
extern gboolean skip_post;
extern gboolean skip_triggers;
extern gboolean skip_constraints;
extern gboolean skip_indexes;
extern gboolean stream;
extern gchar *compress_extension;
extern gchar *db;
extern gchar *directory;
extern gchar *pmm_path;
extern gchar *pmm_resolution ;
extern gchar *source_db;
extern gchar *tables_skiplist_file;
extern GHashTable *db_hash;
extern GHashTable * load_data_list;
extern GHashTable *tbl_hash;
extern GMutex *load_data_list_mutex;
extern GString *set_session;
extern guint commit_count;
extern guint errors;
extern guint max_errors;
extern guint max_threads_for_index_creation;
extern guint max_threads_for_post_creation;
extern guint max_threads_for_schema_creation;
extern guint max_threads_per_table;
extern guint retry_count;
extern guint num_threads;
extern guint rows;
extern guint sequences;
extern guint sequences_processed;
extern GMutex sequences_mutex;
extern unsigned long long int total_data_sql_files;
extern int (*m_close)(void *file);
extern GHashTable * set_session_hash;

extern gchar *exec_per_thread;
extern gchar *exec_per_thread_extension;
extern gchar **exec_per_thread_cmd;

//extern guint index_threads_counter;
extern GMutex *index_mutex;
extern struct database *database_db;
extern gchar *optimize_keys_str;
extern gchar *checksum_str;
extern gboolean no_stream;
extern gchar *ignore_errors;
extern gboolean kill_at_once;
extern struct configuration_per_table conf_per_table;
extern guint source_control_command;
extern struct replication_settings replica_data;
extern struct replication_settings source_data;
extern gboolean set_gtid_purge;
extern gboolean show_warnings;
extern gboolean enable_binlog;
extern guint refresh_table_list_interval;
extern GList *ignore_set_list;
extern gboolean skip_table_sorting;
extern gboolean mysqldump;
extern int (*restore_data_from_file) (struct thread_data *, const char *, gboolean , struct database *);
extern struct replication_statements *replication_statements;
extern gboolean drop_database;
