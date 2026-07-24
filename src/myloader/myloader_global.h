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

#ifndef _src_myloader_myloader_global_h
#define _src_myloader_myloader_global_h

#include <glib.h>

#include "myloader.h"
#include "../common_options.h"

extern gboolean debug;
extern gboolean disable_redo_log;
extern gboolean program_version;
extern gboolean help;
extern gboolean append_if_not_exist;
extern gboolean optimize_keys;
extern gboolean optimize_keys_all_tables;
extern gboolean optimize_keys_per_table;
extern gboolean no_data;
extern gboolean no_schemas;
extern gboolean no_delete;
extern gboolean overwrite_tables;
extern gboolean overwrite_unsafe;
extern gboolean resume;
extern gboolean shutdown_triggered;
extern gboolean skip_definer;
extern gboolean skip_post;
extern gboolean skip_create_table;
extern gboolean skip_create_database;
extern gboolean skip_triggers;
extern gboolean skip_constraints;
extern gboolean skip_indexes;
extern gboolean stream;
extern gboolean no_stream;
extern gboolean kill_at_once;
extern gboolean set_gtid_purge;
extern gboolean show_warnings;
extern gboolean enable_binlog;
extern gboolean skip_table_sorting;
extern gboolean mysqldump;
extern gboolean drop_database;

extern gchar  *exec_per_thread;
extern gchar  *exec_per_thread_extension;
extern gchar **exec_per_thread_cmd;
extern gchar  *optimize_keys_str;
extern gchar  *checksum_str;
extern gchar  *ignore_errors;
extern gchar  *compress_extension;
extern gchar  *target_db;
extern gchar  *directory;
extern gchar  *source_db;
extern gchar  *replace_definer;
extern gchar  *defaults_file;
extern gchar  *input_directory;
extern gchar  *fifo_directory;
extern gchar  *load_data_tmp_directory;
extern gchar  *tables_list;

extern char **tables;
extern char  *defaults_extra_file;

extern enum purge_mode purge_mode;

extern guint verbose;
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
extern guint num_sequences;
extern guint sequences;
extern guint sequences_processed;
extern guint refresh_table_list_interval;

extern GHashTable *database_hash;
extern GHashTable *tbl_hash;
extern GHashTable *conf_per_table;
extern GHashTable *set_session_hash;

extern GKeyFile *key_file;

extern GList *ignore_set_list;

extern GMutex *index_mutex;
extern GMutex  sequences_mutex;

extern GString *set_session;
extern GString *set_global;
extern GString *set_global_back;

extern int (*restore_data_from_file)(struct thread_data *, const char *, gboolean, struct database *);

extern struct replication_statements *replication_statements;
extern struct replication_settings    replica_data;
extern struct replication_settings    source_data;
extern struct restore_errors          detailed_errors;

extern unsigned long long int total_data_sql_files;

#endif
