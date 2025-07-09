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

#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>

#include "myloader.h"
#include "myloader_stream.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"
#include "myloader_arguments.h"
#include "myloader_global.h"

static GMutex *db_hash_mutex = NULL;
GHashTable *db_hash=NULL;
GHashTable *tbl_hash=NULL;
int (*m_close)(void *file) = NULL;
struct database *database_db=NULL;
guint refresh_table_list_interval=100;
guint refresh_table_list_counter=1;
gboolean skip_table_sorting = FALSE;
gchar ** zstd_decompress_cmd = NULL; 
gchar ** gzip_decompress_cmd = NULL;
guint max_number_tables_to_sort_in_table_list = 100000;

void initialize_common(){
  refresh_table_list_counter=refresh_table_list_interval;
  db_hash_mutex=g_mutex_new();
  tbl_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  db_hash=g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );
  if (db){
    database_db=get_db_hash(g_strdup(db), g_strdup(db));
  }

  if ((exec_per_thread_extension==NULL) && (exec_per_thread != NULL))
    m_critical("--exec-per-thread-extension needs to be set when --exec-per-thread (%s) is used", exec_per_thread);
  if ((exec_per_thread_extension!=NULL) && (exec_per_thread == NULL))
    m_critical("--exec-per-thread needs to be set when --exec-per-thread-extension (%s) is used", exec_per_thread_extension);

  gchar *tmpcmd=NULL;
  if (exec_per_thread!=NULL){
    exec_per_thread_cmd=g_strsplit(exec_per_thread, " ", 0);
    tmpcmd=g_find_program_in_path(exec_per_thread_cmd[0]);
    if (!tmpcmd)
      m_critical("%s was not found in PATH, use --exec-per-thread for non default locations",exec_per_thread_cmd[0]);
    exec_per_thread_cmd[0]=tmpcmd;
  }

  gchar *cmd=NULL;
  tmpcmd=g_find_program_in_path(ZSTD);
  if (!tmpcmd){
    m_warning("%s was not found in PATH, use --exec-per-thread for non default locations",ZSTD);
  }else{
    zstd_decompress_cmd = g_strsplit(cmd=g_strdup_printf("%s -c -d", tmpcmd)," ",0);
    g_free(tmpcmd);
    g_free(cmd);
  }

  tmpcmd=g_find_program_in_path(GZIP);
  if (!tmpcmd){
    m_warning("%s was not found in PATH, use --exec-per-thread for non default locations",GZIP);
  }else{
    gzip_decompress_cmd = g_strsplit( cmd=g_strdup_printf("%s -c -d", tmpcmd)," ",0);
    g_free(tmpcmd);
    g_free(cmd);
  }
}

gboolean is_in_list(gchar *haystack, GList *list){
  GList *l=list;
  while(l){
    if (!g_ascii_strcasecmp(haystack, l->data)){
      return TRUE;
    }
    l=l->next;
  }
  return FALSE;
}

gboolean is_in_ignore_set_list(gchar *haystack){
  return is_in_list(haystack,ignore_set_list);
}

void remove_ignore_set_session_from_hash(){
  GList *l=ignore_set_list;
  while (l){
    g_hash_table_remove(set_session_hash,l->data);
    l=l->next;
  }


}


gchar *get_value(GKeyFile * kf,gchar *group, const gchar *_key){
  GError *error=NULL;
  gchar * val=g_key_file_get_value(kf,group,_key,&error);
  if (error != NULL && error->code == G_KEY_FILE_ERROR_KEY_NOT_FOUND){
    g_error_free(error);
    return NULL;
  }
  return g_strdup(val);
}

//g_list_free_full(change_master_parameter_list, g_free);

void execute_replication_commands(MYSQL *conn, gchar *statement){
  m_query_warning(conn, "COMMIT", "COMMIT failed");
  guint i;
  gchar** line=g_strsplit(statement, ";\n", -1);
  for (i=0; i < g_strv_length(line);i++){
     if (strlen(line[i])>2){
       GString *str=g_string_new(line[i]);
       g_string_append_c(str,';');
       m_query_warning(conn, str->str, "Sending replication command: %s", str->str);
       g_string_free(str,TRUE);
     }
  }
  g_strfreev(line);
  m_query_warning(conn, "START TRANSACTION", "START TRANSACTION failed");
}

void change_master(GKeyFile * kf,gchar *group, struct replication_statements *rs){
  gchar *val=NULL;
  guint i=0;
  gsize len=0;
  GError *error = NULL;
  GString *traditional_change_source=g_string_new("");
  GString *aws_change_source=g_string_new("");

  g_string_append(traditional_change_source,change_replication_source);
  g_string_append(traditional_change_source," TO ");

  gchar** group_name= g_strsplit(group, ".", 2);
  gchar* channel_name=g_strv_length(group_name)>1? group_name[1]:NULL;
  gchar **keys=g_key_file_get_keys(kf,group, &len, &error);
  guint exec_change_source=0, exec_reset_replica=0, exec_start_replica=0, exec_start_replica_until=0;

  gboolean auto_position = FALSE;
  gboolean source_ssl = FALSE;
  gchar *source_host = NULL;
  guint source_port = 3306;
  gchar *source_user = NULL;
  gchar *source_password = NULL;
  gchar *source_log_file= NULL;
  guint64 source_log_pos=0;
  gboolean first=TRUE;
  gchar *source_gtid=NULL;
  for (i=0; i < len; i++){
    if (!(g_strcmp0(keys[i], "myloader_exec_reset_slave") && g_strcmp0(keys[i], "myloader_exec_reset_replica") )){
      exec_reset_replica=g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10);
    } else if (!(g_strcmp0(keys[i], "myloader_exec_change_master") && g_strcmp0(keys[i], "myloader_exec_change_source"))){
      if (g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10) == 1 )
        exec_change_source=1;
    } else if (!(g_strcmp0(keys[i], "myloader_exec_start_slave") && g_strcmp0(keys[i], "myloader_exec_start_replica"))){
      if (g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10) == 1 )
        exec_start_replica=1;
    } else if (!g_strcmp0(keys[i], "executed_gtid_set") ){
      source_gtid=g_key_file_get_value(kf,group,keys[i],&error);
    } else if(!g_ascii_strcasecmp(keys[i], "channel_name")){
      channel_name=g_key_file_get_value(kf,group,keys[i],&error);
    } else {
      if (first){
        first=FALSE;
      }else{
        g_string_append_printf(traditional_change_source,", ");
      }
      if (!g_ascii_strcasecmp(keys[i], "SOURCE_AUTO_POSITION")){
        auto_position=g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10)>0;
//        g_string_append_printf(traditional_change_source, "%s = %d", (gchar *) keys[i], auto_position);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_SSL")){
        source_ssl=g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10)>0;
//        g_string_append_printf(traditional_change_source, "%s = %d", (gchar *) keys[i], source_ssl);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_HOST")){
        source_host=g_key_file_get_value(kf,group,keys[i],&error);
        g_string_append_printf(traditional_change_source, "%s = %s", (gchar *) keys[i], source_host);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_PORT")){
        source_port=g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10);
        g_string_append_printf(traditional_change_source, "%s = %d", (gchar *) keys[i], source_port);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_USER")){
        source_user=g_key_file_get_value(kf,group,keys[i],&error);
        g_string_append_printf(traditional_change_source, "%s = %s", (gchar *) keys[i], source_user);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_PASSWORD")){
        source_password=g_key_file_get_value(kf,group,keys[i],&error);
        g_string_append_printf(traditional_change_source, "%s = %s", (gchar *) keys[i], source_password);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_LOG_FILE")){
        source_log_file=g_key_file_get_value(kf,group,keys[i],&error);
        g_string_append_printf(traditional_change_source, "%s = %s", (gchar *) keys[i], source_log_file);
      } else if (!g_ascii_strcasecmp(keys[i], "SOURCE_LOG_POS")){
        source_log_pos=g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10);
        g_string_append_printf(traditional_change_source, "%s = %"G_GINT64_FORMAT, (gchar *) keys[i], source_log_pos);
      } else {
        val=g_key_file_get_value(kf,group,keys[i],&error);
        if (val != NULL)
          g_string_append_printf(traditional_change_source, "%s = %s", (gchar *) keys[i], val);
      }
    }
  }


  if (source_data>=0){
    exec_reset_replica=((source_data) & (1<<(0)))>0;
    exec_change_source=((source_data) & (1<<(1)))>0;
    exec_start_replica=((source_data) & (1<<(2)))>0;
    source_ssl        =((source_data) & (1<<(3)))>0;
    auto_position     =((source_data) & (1<<(4)))>0;
    exec_start_replica_until=((source_data) & (1<<(5)))>0;
  }
  g_assert( ( exec_start_replica_until != 0 && (exec_reset_replica == 0 && exec_change_source == 0) )
         || ( exec_start_replica_until == 0 )
      );

  if (source_ssl)
    g_string_append_printf(traditional_change_source, "SOURCE_SSL = %d", source_ssl);

  if (auto_position){
    g_string_append_printf(traditional_change_source, "SOURCE_AUTO_POSITION = %d", auto_position);
    g_string_append(aws_change_source,"CALL mysql.rds_set_external_master_with_auto_position");
  }else
    g_string_append(aws_change_source,"CALL mysql.rds_set_external_master");
  
  g_string_append_printf(aws_change_source,"( %s, %d, %s, %s, ", source_host, source_port, source_user, source_password );

  if (!auto_position)
    g_string_append_printf(aws_change_source,"%s, %"G_GINT64_FORMAT", %d, );\n", source_log_file, source_log_pos, source_ssl);
  else
    g_string_append_printf(aws_change_source,"%d, 0);\n", source_ssl);

  g_strfreev(keys);
  g_string_append(traditional_change_source," FOR CHANNEL '");
  if (channel_name!=NULL)
    g_string_append(traditional_change_source,channel_name);
  g_string_append(traditional_change_source,"';\n");

  if (set_gtid_purge){
    if (! rs->gtid_purge)
      rs->gtid_purge=g_string_new("");
    if (source_control_command == TRADITIONAL)
      g_string_append_printf(rs->gtid_purge, "%s;\nSET GLOBAL gtid_purged=%s;\n", reset_replica, source_gtid);
    else
      g_string_append_printf(rs->gtid_purge, "CALL mysql.rds_gtid_purged (%s);\n", source_gtid);
  }
  if (exec_reset_replica){
    if (!rs->reset_replica)
      rs->reset_replica=g_string_new("");

    g_string_append(rs->reset_replica,stop_replica);
    g_string_append(rs->reset_replica,";\n");

    g_string_append(rs->reset_replica,reset_replica);
    if (source_control_command == TRADITIONAL){
      g_string_append(rs->reset_replica," ");
      if (exec_reset_replica>1)
        g_string_append(rs->reset_replica,"ALL ");
      if (channel_name!=NULL)
        g_string_append_printf(rs->reset_replica,"FOR CHANNEL '%s'", channel_name);
    }
    g_string_append(rs->reset_replica,";\n");
  }

  if (exec_start_replica_until){
    if (! rs->start_replica_until)
      rs->start_replica_until=g_string_new("");
    g_string_append(rs->start_replica_until,stop_replica_sql_thread);
    if (source_control_command == TRADITIONAL){
      g_string_append(rs->start_replica_until," ");
      if (channel_name!=NULL)
        g_string_append_printf(rs->start_replica_until,"FOR CHANNEL '%s'", channel_name);
    }
    g_string_append(rs->start_replica_until,";\n");



    g_string_append(rs->start_replica_until,start_replica);
    g_string_append(rs->start_replica_until," UNTIL ");
    if (source_gtid){
      g_string_append_printf(rs->start_replica_until,"SQL_AFTER_GTIDS = %s",source_gtid);
    }else{
      g_string_append_printf(rs->start_replica_until,"SOURCE_LOG_FILE = '%s', SOURCE_LOG_POS = %"G_GINT64_FORMAT, source_log_file, source_log_pos);
    }
    g_string_append(rs->start_replica_until," FOR CHANNEL '");
    if (channel_name!=NULL)
      g_string_append(rs->start_replica_until,channel_name);
    g_string_append(rs->start_replica_until,"';\n");
  }
// SQL_AFTER_GTIDS
  if (exec_change_source){
    if (! rs->change_replication_source)
      rs->change_replication_source=g_string_new("");
    if (source_control_command == TRADITIONAL){
      g_string_append(rs->change_replication_source,traditional_change_source->str);
    }else{
      g_string_append(rs->change_replication_source,aws_change_source->str);
    }
  }

  if (exec_start_replica){
    if (! rs->start_replica)
      rs->start_replica=g_string_new("");
    g_string_append(rs->start_replica,start_replica);
    g_string_append(rs->start_replica,";\n");
  }

  if (source_control_command == TRADITIONAL)
    g_message("Change master will be executed for channel: %s", channel_name!=NULL?channel_name:"default channel");
  
  g_string_free(traditional_change_source,TRUE);
}

gboolean m_filename_has_suffix(gchar const *str, gchar const *suffix){
  if (has_exec_per_thread_extension(str)){
    return g_strstr_len(&(str[strlen(str)-strlen(exec_per_thread_extension)-strlen(suffix)]), strlen(str)-strlen(exec_per_thread_extension),suffix) != NULL;
  }else if ( g_str_has_suffix(str, GZIP_EXTENSION) ){
    return g_strstr_len(&(str[strlen(str)-strlen(GZIP_EXTENSION)-strlen(suffix)]), strlen(str)-strlen(GZIP_EXTENSION),suffix) != NULL;
  }else if ( g_str_has_suffix(str, ZSTD_EXTENSION) ){
    return g_strstr_len(&(str[strlen(str)-strlen(ZSTD_EXTENSION)-strlen(suffix)]), strlen(str)-strlen(ZSTD_EXTENSION),suffix) != NULL;
  }

  return g_str_has_suffix(str,suffix);
}
struct database * new_database(gchar *database, gchar *filename){
  struct database * d = g_new(struct database, 1);
  d->name=database;
  d->real_database = g_strdup(db ? db : d->name);
  d->filename = filename;
  d->mutex=g_mutex_new();
  d->sequence_queue= g_async_queue_new();
  d->queue=g_async_queue_new();;
  d->schema_state=NOT_FOUND;
  d->schema_checksum=NULL;
  d->post_checksum=NULL;
  d->triggers_checksum=NULL;
  return d;
}

struct database * get_db_hash(gchar *filename, gchar *name){
  g_mutex_lock(db_hash_mutex);
  struct database * d=g_hash_table_lookup(db_hash, filename);
  if (d==NULL){
    d=new_database(g_strdup(name), filename);
    g_hash_table_insert(db_hash, filename, d);
    if (g_strcmp0(filename,name))
      g_hash_table_insert(db_hash, g_strdup(name), d);
    d=g_hash_table_lookup(db_hash, name);
  }else{
    if (filename != name){
      d->name=g_strdup(name);
      d->real_database = g_strdup(db ? db : d->name);
    }
  }
  g_mutex_unlock(db_hash_mutex);
  return d;
}

/*
struct database * db_hash_lookup(gchar *database){
  struct database *r=NULL;
  g_mutex_lock(db_hash_mutex);
  r=g_hash_table_lookup(db_hash,database);
  g_mutex_unlock(db_hash_mutex);
  return r;
}
*/
gboolean eval_table( char *db_name, char * table_name, GMutex * mutex){
  if (table_name == NULL)
    g_error("Table name is null on eval_table()");
  g_mutex_lock(mutex);
  if ( tables ){
    if (!is_table_in_list( db_name, table_name, tables)){
      g_mutex_unlock(mutex);
      return FALSE;
    }
  }
  if ( tables_skiplist_file && check_skiplist(db_name, table_name )){
    g_mutex_unlock(mutex);
    return FALSE;
  }
  g_mutex_unlock(mutex);
  return eval_regex(db_name, table_name);
}

gboolean execute_use(struct connection_data *cd){
  if (cd->current_database){
    gchar *query = g_strdup_printf("USE `%s`", cd->current_database->real_database);
    if (m_query_warning(cd->thrconn, query, "Thread %d: Error switching to database `%s`", cd->thread_id, cd->current_database)) {
      g_free(query);
      return TRUE;
    }
    g_free(query);
  }else{
    g_warning("Thread %ld with connection %ld: Not able to switch database",cd->thread_id, cd->connection_id);
  }
  return FALSE;
}

void execute_use_if_needs_to(struct connection_data *cd, struct database *database, const gchar * msg){
  if ( database != NULL && (db == NULL || cd->current_database==NULL)){
    if (cd->current_database==NULL || g_strcmp0(database->real_database, cd->current_database->real_database) != 0){
      cd->current_database=database;
      if (execute_use(cd)){
        m_critical("Thread %ld with connection %ld: Error switching to database `%s` %s: %s", cd->thread_id, cd->connection_id, cd->current_database->real_database, msg, mysql_error(cd->thrconn));
      }
    }
  }
}

enum file_type get_file_type (const char * filename){

  if ((!strcmp(filename,          "metadata") || 
       !strcmp(filename,          "metadata.header") ||
       g_str_has_prefix(filename, "metadata.partial")) 
      && 
      !( g_str_has_suffix(filename, ".sql") || 
         has_exec_per_thread_extension(filename)))
    return METADATA_GLOBAL;

  if (source_db && !(g_str_has_prefix(filename, source_db) && strlen(filename) > strlen(source_db) && (filename[strlen(source_db)] == '.' || filename[strlen(source_db)] == '-') ) && !g_str_has_prefix(filename, "mydumper_"))
    return IGNORED;  

  if (m_filename_has_suffix(filename, "-schema.sql")) 
    return SCHEMA_TABLE;

  if ( strcmp(filename, "all-schema-create-tablespace.sql") == 0 )
    return SCHEMA_TABLESPACE;

  if ( strcmp(filename, "resume") == 0 ){
    if (!resume){
      m_critical("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.");
    }
    return RESUME;
  }

  if ( strcmp(filename, "resume.partial") == 0 )
    m_critical("resume.partial file found. Remove it and restart process if you consider that it will be safe.");

  if (m_filename_has_suffix(filename, "-checksum"))
    return CHECKSUM;

  if (m_filename_has_suffix(filename, "-schema-view.sql") )
    return SCHEMA_VIEW;

  if (m_filename_has_suffix(filename, "-schema-sequence.sql") )
    return SCHEMA_SEQUENCE;

  if (m_filename_has_suffix(filename, "-schema-triggers.sql") )
    return SCHEMA_TRIGGER;

  if (m_filename_has_suffix(filename, "-schema-post.sql") )
    return SCHEMA_POST;

  if (m_filename_has_suffix(filename, "-schema-create.sql") )
    return SCHEMA_CREATE;

  if (m_filename_has_suffix(filename, ".sql") )
    return DATA;

  if (m_filename_has_suffix(filename, ".dat"))
    return LOAD_DATA;

  return IGNORED;
}

void get_database_table_from_file(const gchar *filename,const char *sufix,gchar **database,gchar **table){
  gchar **split_filename = g_strsplit(filename, sufix, 0);
  gchar **split = g_strsplit(split_filename[0],".",0);
  g_strfreev(split_filename);
  guint count=g_strv_length(split);
  if (count > 2){
    g_warning("We need to get the db and table name from the create table statement");
    return;
  }
  *table=g_strdup(split[1]);
  *database=g_strdup(split[0]);
  g_strfreev(split);
}

int process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, struct db_table *dbt, gboolean split_indexes){
  return global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt->real_table, split_indexes);
}

gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash){
  gchar *a_key=build_dbt_key(((struct db_table *)a)->database->real_database,((struct db_table *)a)->table);
  gchar *b_key=build_dbt_key(((struct db_table *)b)->database->real_database,((struct db_table *)b)->table);
  struct db_table * a_val=g_hash_table_lookup(table_hash,a_key);
  struct db_table * b_val=g_hash_table_lookup(table_hash,b_key);
  g_free(a_key);
  g_free(b_key);
  return a_val->rows < b_val->rows;
}

gint compare_dbt_short(gconstpointer a, gconstpointer b){
  return ((struct db_table *)a)->rows < ((struct db_table *)b)->rows;
}

void refresh_table_list_without_table_hash_lock(struct configuration *conf, gboolean force){
  if (force || g_atomic_int_dec_and_test(&refresh_table_list_counter)){
    GList * table_list=NULL;
    GHashTableIter iter;
    gchar * lkey;
    g_mutex_lock(conf->table_list_mutex);
    g_hash_table_iter_init ( &iter, conf->table_hash );
    struct db_table *dbt=NULL;
    while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
      if (skip_table_sorting || g_list_length(table_list) > max_number_tables_to_sort_in_table_list)
        table_list=g_list_prepend(table_list,dbt);
      else
        table_list=g_list_insert_sorted(table_list,dbt,&compare_dbt_short);
    }
    g_list_free(conf->table_list);
    conf->table_list=table_list;
    g_atomic_int_set(&refresh_table_list_counter,refresh_table_list_interval);
    g_mutex_unlock(conf->table_list_mutex);
  }
}

void refresh_table_list(struct configuration *conf){
  g_mutex_lock(conf->table_hash_mutex);
  refresh_table_list_without_table_hash_lock(conf, TRUE);
  g_mutex_unlock(conf->table_hash_mutex);
}

static inline gboolean
checksum_template(const char *dbt_checksum, const char *checksum, const char *err_templ,
                  const char *info_templ, const char *message, const char *_db, const char *_table)
{
  g_assert(checksum_mode != CHECKSUM_SKIP);
  if (g_strcmp0(dbt_checksum, checksum)) {
    if (_table) {
      if (checksum_mode == CHECKSUM_WARN)
        g_warning(err_templ, message, _db, _table, checksum, dbt_checksum);
      else
        g_critical(err_templ, message, _db, _table, checksum, dbt_checksum);
    } else {
      if (checksum_mode == CHECKSUM_WARN)
        g_warning(err_templ, message, _db, checksum, dbt_checksum);
      else
        g_critical(err_templ, message, _db, checksum, dbt_checksum);
    }
    return FALSE;
  } else {
    g_message(info_templ, message, _db, _table);
  }
  return TRUE;
}

gboolean checksum_dbt_template(struct db_table *dbt, gchar *dbt_checksum,  MYSQL *conn,
                           const gchar *message, gchar* fun(MYSQL *,gchar *,gchar *))
{
  const char *checksum= fun(conn, dbt->database->real_database, dbt->real_table);
  return checksum_template(dbt_checksum, checksum,
                    "%s mismatch found for %s.%s: got %s, expecting %s",
                    "%s confirmed for %s.%s", message, dbt->database->real_database, dbt->real_table);
}

gboolean checksum_database_template(gchar *_db, gchar *dbt_checksum,  MYSQL *conn,
                                const gchar *message, gchar* fun(MYSQL *,gchar *,gchar *))
{
  const char *checksum= fun(conn, _db, NULL);
  return checksum_template(dbt_checksum, checksum,
                    "%s mismatch found for %s: got %s, expecting %s",
                    "%s confirmed for %s", message, _db, NULL);
}

gboolean checksum_dbt(struct db_table *dbt,  MYSQL *conn)
{
  gboolean checksum_ok=TRUE;
  if (checksum_mode != CHECKSUM_SKIP){
    if (!no_schemas){
      if (dbt->schema_checksum!=NULL){
        if (dbt->is_view)
          checksum_ok&=checksum_dbt_template(dbt, dbt->schema_checksum, conn,
                                "View checksum", checksum_view_structure);
        else
          checksum_ok&=checksum_dbt_template(dbt, dbt->schema_checksum, conn,
                                "Structure checksum", checksum_table_structure);
      }
      if (dbt->indexes_checksum!=NULL)
        checksum_ok&=checksum_dbt_template(dbt, dbt->indexes_checksum, conn,
                              "Schema index checksum", checksum_table_indexes);
    }
    if (dbt->triggers_checksum!=NULL && !skip_triggers)
      checksum_ok&=checksum_dbt_template(dbt, dbt->triggers_checksum, conn,
                            "Trigger checksum", checksum_trigger_structure);

    if (dbt->data_checksum!=NULL && !no_data)
      checksum_ok&=checksum_dbt_template(dbt, dbt->data_checksum, conn,
                            "Data checksum", checksum_table);
  }
  return checksum_ok;
}

gboolean has_exec_per_thread_extension(const gchar *filename){
  return exec_per_thread_extension!=NULL && g_str_has_suffix(filename, exec_per_thread_extension);
}


int execute_file_per_thread( const gchar *sql_fn, gchar *sql_fn3, gchar **exec){
  int childpid=fork();
  if(!childpid){
    FILE *sql_file2 = g_fopen(sql_fn,"r");
    FILE *sql_file3 = g_fopen(sql_fn3,"w");
    dup2(fileno(sql_file2), STDIN_FILENO);
    dup2(fileno(sql_file3), STDOUT_FILENO);
//    close(fileno(sql_file2));
//    close(fileno(sql_file3));
    execv(exec[0],exec);
  }
  return childpid;
}

gboolean get_command_and_basename(gchar *filename, gchar ***command, gchar **basename){
  int len=0;
  if (has_exec_per_thread_extension(filename)) {
    *command=exec_per_thread_cmd;
    len=strlen(exec_per_thread_extension);
  }else if ( g_str_has_suffix(filename, ZSTD_EXTENSION) ){
    *command=zstd_decompress_cmd;
    len=strlen(ZSTD_EXTENSION);
  }else if (g_str_has_suffix(filename, GZIP_EXTENSION)){
    *command=gzip_decompress_cmd;
    len=strlen(GZIP_EXTENSION);
  }else{
    goto avoid_command_check;
  }

  if (!*command)
    m_critical("We don't have a command for extension on file %s",filename);

avoid_command_check:
  if (len!=0){
    gchar *dotpos=&(filename[strlen(filename)]) - len;
    *dotpos='\0';
    *basename=g_strdup(filename);
    *dotpos='.';
    return TRUE;
  }
  *basename=g_strdup(filename);
  return FALSE;
}

void initialize_thread_data(struct thread_data*td, struct configuration *conf, enum thread_states status, guint thread_id, struct db_table *dbt){
  td->conf=conf;
  td->status=status;
  td->thread_id=thread_id;
//  td->connection_data.current_database=NULL;
  td->granted_connections=0;
  td->dbt=dbt;
//  td->use_database=NULL;
}

char *show_warnings_if_possible(MYSQL *conn){
  if (!show_warnings)
    return NULL;
  MYSQL_RES *result = m_store_result(conn, "SHOW WARNINGS", m_critical, "Error on SHOW WARNINGS", NULL);
  if (!result)
    return NULL;
  GString *_error=g_string_new("");
  MYSQL_ROW row = mysql_fetch_row(result);
  while (row){
    g_string_append(_error,row[2]);
    g_string_append(_error,"\n");
    row = mysql_fetch_row(result);
  }
  return g_string_free(_error, FALSE);
}
