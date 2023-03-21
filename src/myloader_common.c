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
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include "common.h"
#include "myloader_stream.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_restore_job.h"
#include "myloader_control_job.h"

#include "connection.h"
#include "tables_skiplist.h"
#include "regex.h"
#include <errno.h>
#include "myloader_global.h"


static GMutex *db_hash_mutex = NULL;
GHashTable *db_hash=NULL;
GHashTable *tbl_hash=NULL;


void initialize_common(){
  db_hash_mutex=g_mutex_new();
  tbl_hash=g_hash_table_new ( g_str_hash, g_str_equal );
}


gchar *get_value(GKeyFile * kf,gchar *group, const gchar *key){
  GError *error=NULL;
  gchar * val=g_key_file_get_value(kf,group,key,&error);
  if (error != NULL && error->code == G_KEY_FILE_ERROR_KEY_NOT_FOUND){
    g_error_free(error);
    return NULL;
  }
  return g_strdup(val);
}

//g_list_free_full(change_master_parameter_list, g_free);

void change_master(GKeyFile * kf,gchar *group, GString *output_statement){
  gchar *val=NULL;
  guint i=0;
  gsize len=0;
  GError *error = NULL;
  GString *s=g_string_new("");
  gchar** group_name= g_strsplit(group, ".", 2);
  gchar* channel_name=g_strv_length(group_name)>1? group_name[1]:NULL;
  gchar **keys=g_key_file_get_keys(kf,group, &len, &error);
  guint exec_change_master=0, exec_reset_slave=0, exec_start_slave=0;
  g_string_append(s,"CHANGE MASTER TO ");
  for (i=0; i < len; i++){
    if (!g_strcmp0(keys[i], "myloader_exec_reset_slave")){
      exec_reset_slave=g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10);
    } else if (!g_strcmp0(keys[i], "myloader_exec_change_master")){
      if (g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10) == 1 )
        exec_change_master=1;
    } else if (!g_strcmp0(keys[i], "myloader_exec_start_slave")){
      if (g_ascii_strtoull(g_key_file_get_value(kf,group,keys[i],&error), NULL, 10) == 1 )
        exec_start_slave=1;
    } else if(!g_ascii_strcasecmp(keys[i], "channel_name")){
      channel_name=g_key_file_get_value(kf,group,keys[i],&error);
    } else {
      val=g_key_file_get_value(kf,group,keys[i],&error);
      if (val != NULL)
        g_string_append_printf(s, "%s = %s, ", (gchar *) keys[i], val);
    }
  }
  g_strfreev(keys);
  g_string_set_size(s, s->len-2);
  g_string_append_c(s,' ');
  g_string_append(s,"FOR CHANNEL ");
  if (channel_name==NULL){
    g_string_append(s,"''");
  }else{
    g_string_append(s,channel_name);
  }
  g_string_append(s,";\n");

  if (exec_change_master){
    if (exec_reset_slave){
      g_string_append(output_statement,"STOP SLAVE ;\nRESET SLAVE ");
      if (exec_reset_slave>1)
        g_string_append(output_statement,"ALL ");
      if (channel_name!=NULL)
        g_string_append_printf(output_statement,"FOR CHANNEL %s ;\n", channel_name);
      g_string_append(output_statement,";\n");
    }

    g_string_append(output_statement,s->str);

    if (exec_start_slave)
      g_string_append(output_statement,"START SLAVE;\n");
    g_message("Change master will be executed for channel: %s", channel_name!=NULL?channel_name:"default channel");
  }
}

gboolean m_filename_has_suffix(gchar const *str, gchar const *suffix){
  if (g_str_has_suffix(str, compress_extension)){
    return g_strstr_len(&(str[strlen(str)-strlen(compress_extension)-strlen(suffix)]), strlen(str)-strlen(compress_extension),suffix) != NULL; 
  }
  return g_str_has_suffix(str,suffix);
}
struct database * new_database(gchar *database){
  struct database * d = g_new(struct database, 1);
  d->name=database;
  d->real_database = g_strdup(db ? db : database);
  d->mutex=g_mutex_new();
  d->queue=g_async_queue_new();;
  d->schema_state=NOT_FOUND;
  d->schema_checksum=NULL;
  d->post_checksum=NULL;
  return d;
}

struct database * get_db_hash(gchar *k, gchar *v){
  g_mutex_lock(db_hash_mutex);
  struct database * d=g_hash_table_lookup(db_hash, k);
  if (d==NULL){
    d=new_database(g_strdup(v));
    g_hash_table_insert(db_hash, k, d);
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
    if ( ! is_table_in_list(table_name, tables) ){
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

/*struct restore_job * new_restore_job( char * filename, char * database, struct db_table * dbt, GString * statement, guint part, guint sub_part, enum restore_job_type type, const char *object){
  struct restore_job *rj = g_new(struct restore_job, 1);
  rj->filename  = filename;
  rj->database  = database;
  rj->dbt       = dbt;
  rj->statement = statement;
  rj->part      = part;
  rj->sub_part  = sub_part;
  rj->type      = type;
  rj->object    = object;
  return rj;
}

*/
gboolean execute_use(struct thread_data *td){
  gchar *query = g_strdup_printf("USE `%s`", td->current_database);
  if (mysql_query(td->thrconn, query)) {
//    g_critical("Thread %d: Error switching to database `%s` %s", td->thread_id, td->current_database, msg);
    g_free(query);
    return TRUE;
  }
  g_free(query);
  return FALSE;
}

void execute_use_if_needs_to(struct thread_data *td, gchar *database, const gchar * msg){
  if ( database != NULL && db == NULL ){
    if (td->current_database==NULL || g_strcmp0(database, td->current_database) != 0){
      td->current_database=database;
      if (execute_use(td)){
        m_critical("Thread %d: Error switching to database `%s` %s", td->thread_id, td->current_database, msg);
      }
    }
  }
}


gboolean m_query(  MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...){
  if (mysql_query(conn, query)){
    va_list    args;
    va_start(args, fmt);
    gchar *c=g_strdup_vprintf(fmt,args);
    log_fun("%s: %s",c, mysql_error(conn));
    g_free(c);
    return FALSE;
  }
  return TRUE;
}



enum file_type get_file_type (const char * filename){
  if (m_filename_has_suffix(filename, "-schema.sql")) {
    return SCHEMA_TABLE;
  } else if (m_filename_has_suffix(filename, "-metadata")) {
    return METADATA_TABLE;
  } else if ( strcmp(filename, "metadata") == 0 ){
    return METADATA_GLOBAL;
  } else if ( strcmp(filename, "all-schema-create-tablespace.sql") == 0 ){
    return SCHEMA_TABLESPACE;
  } else if ( strcmp(filename, "resume") == 0 ){
    if (!resume){
      m_critical("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.");
    }
    return RESUME;
  } else if ( strcmp(filename, "resume.partial") == 0 ){
    m_critical("resume.partial file found. Remove it and restart process if you consider that it will be safe.");
  } else if (m_filename_has_suffix(filename, "-checksum")) {
    return CHECKSUM;
  } else if (m_filename_has_suffix(filename, "-schema-view.sql") ){
    return SCHEMA_VIEW;
  } else if (m_filename_has_suffix(filename, "-schema-sequence.sql") ){
    return SCHEMA_SEQUENCE;
  } else if (m_filename_has_suffix(filename, "-schema-triggers.sql") ){
    return SCHEMA_TRIGGER;
  } else if (m_filename_has_suffix(filename, "-schema-post.sql") ){
    return SCHEMA_POST;
  } else if (m_filename_has_suffix(filename, "-schema-create.sql") ){
    return SCHEMA_CREATE;
  } else if (m_filename_has_suffix(filename, ".sql") ){
    return DATA;
  }else if (m_filename_has_suffix(filename, ".dat"))
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

void append_alter_table(GString * alter_table_statement, char *database, char *table){
  g_string_append(alter_table_statement,"ALTER TABLE `");
  g_string_append(alter_table_statement, database);
  g_string_append(alter_table_statement,"`.`");
  g_string_append(alter_table_statement,table);
  g_string_append(alter_table_statement,"` ");
}

void finish_alter_table(GString * alter_table_statement){
  gchar * str=g_strrstr_len(alter_table_statement->str,alter_table_statement->len,",");
  if ((str - alter_table_statement->str) > (long int)(alter_table_statement->len - 5)){
    *str=';';
    g_string_append_c(alter_table_statement,'\n');
  }else
    g_string_append(alter_table_statement,";\n");
}

int process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, struct db_table *dbt){
  int flag=0;
  gchar** split_file= g_strsplit(statement, "\n", -1);
  gchar *autoinc_column=NULL;
  append_alter_table(alter_table_statement, dbt->database->real_database,dbt->real_table);
  append_alter_table(alter_table_constraint_statement, dbt->database->real_database,dbt->real_table);
  int fulltext_counter=0;
  int i=0;
  for (i=0; i < (int)g_strv_length(split_file);i++){
    if ( g_strstr_len(split_file[i],5,"  KEY")
      || g_strstr_len(split_file[i],8,"  UNIQUE")
      || g_strstr_len(split_file[i],9,"  SPATIAL")
      || g_strstr_len(split_file[i],10,"  FULLTEXT")
      || g_strstr_len(split_file[i],7,"  INDEX")
      ){
      // Ignore if the first column of the index is the AUTO_INCREMENT column
      if ((autoinc_column != NULL) && (g_strrstr(split_file[i],autoinc_column))){
        g_string_append(create_table_statement, split_file[i]);
        g_string_append_c(create_table_statement,'\n');
      }else{
        flag|=IS_ALTER_TABLE_PRESENT;
        if (g_strrstr(split_file[i],"  FULLTEXT")) fulltext_counter++;
        if (fulltext_counter>1){
          fulltext_counter=1;
          finish_alter_table(alter_table_statement);
          append_alter_table(alter_table_statement,dbt->database->real_database,dbt->real_table);
        }
        g_string_append(alter_table_statement,"\n ADD");
        g_string_append(alter_table_statement, split_file[i]);
      }
    }else{
      if (g_strstr_len(split_file[i],12,"  CONSTRAINT")){
        flag|=INCLUDE_CONSTRAINT;
        g_string_append(alter_table_constraint_statement,"\n ADD");
        g_string_append(alter_table_constraint_statement, split_file[i]);
      }else{
        if (g_strrstr(split_file[i],"AUTO_INCREMENT")){
          gchar** autoinc_split=g_strsplit(split_file[i],"`",3);
          autoinc_column=g_strdup_printf("(`%s`", autoinc_split[1]);
        }
        g_string_append(create_table_statement, split_file[i]);
        g_string_append_c(create_table_statement,'\n');
      }
    }
    if (g_strrstr(split_file[i],"ENGINE=InnoDB")) flag|=IS_INNODB_TABLE;
  }
  return flag;
}

gchar *build_dbt_key(gchar *a, gchar *b){
  return g_strdup_printf("`%s`_`%s`", a, b);
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

void refresh_table_list_without_table_hash_lock(struct configuration *conf){
  GList * table_list=NULL;
  GHashTableIter iter;
  gchar * lkey;
  g_mutex_lock(conf->table_list_mutex);
  g_hash_table_iter_init ( &iter, conf->table_hash );
  struct db_table *dbt=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
 //   table_list=g_list_insert_sorted_with_data (table_list,dbt,&compare_dbt,conf->table_hash);
    table_list=g_list_insert_sorted(table_list,dbt,&compare_dbt_short);
  }
  g_list_free(conf->table_list);
  conf->table_list=table_list;
//  g_message("Table Order:");
//  guint i=0;
//  while(table_list!=NULL){
//    i++;
//    g_message("%d: %s",i,((struct db_table *)table_list->data)->table);
//    table_list=table_list->next;
//  }
  g_mutex_unlock(conf->table_list_mutex);
}

void refresh_table_list(struct configuration *conf){
  g_mutex_lock(conf->table_hash_mutex);
  refresh_table_list_without_table_hash_lock(conf);
  g_mutex_unlock(conf->table_hash_mutex);
}

void checksum_dbt_template(struct db_table *dbt, gchar *dbt_checksum,  MYSQL *conn, const gchar *message, gchar* fun()) {
  int errn=0;
  gchar *checksum=fun(conn, dbt->database->name, dbt->real_table, &errn);
  if (g_strcmp0(dbt_checksum,checksum)){
    g_warning("%s mismatch found for `%s`.`%s`. Got '%s', expecting '%s'", message,dbt->database->name, dbt->table, dbt_checksum, checksum);
  }else{
    g_message("%s confirmed for `%s`.`%s`", message, dbt->database->name, dbt->table);
  }
}

void checksum_database_template(gchar *database, gchar *dbt_checksum,  MYSQL *conn, const gchar *message, gchar* fun()) {
  int errn=0;
  gchar *checksum=fun(conn, database, NULL, &errn);
  if (g_strcmp0(dbt_checksum,checksum)){
    g_warning("%s mismatch found for `%s`. Got '%s', expecting '%s'", message, database, dbt_checksum, checksum);
  }else{
    g_message("%s confirmed for `%s`", message, database);
  }
}


void checksum_filename(const gchar *filename, MYSQL *conn, const gchar *suffix, const gchar *message, gchar* fun()) {
  gchar *database = NULL, *table = NULL;
  get_database_table_from_file(filename,suffix,&database,&table);
  struct database *real_database=get_db_hash(database,database);
  gchar *real_table=NULL;
  if (table != NULL ){
    real_table=g_hash_table_lookup(tbl_hash,table);
    g_free(table);
  }
  g_free(database);
  void *infile;
  char checksum[256];
  int errn=0;
  char * row=fun(conn, db ? db : real_database->name, real_table, &errn);
  if (row == NULL)
    row = g_strdup("0");

  gboolean is_compressed = FALSE;
  gchar *path = g_build_filename(directory, filename, NULL);

  if (!g_str_has_suffix(path, compress_extension)) {
    infile = g_fopen(path, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(path, "r");
    is_compressed=TRUE;
  }

  if (!infile) {
    g_critical("cannot open checksum file %s (%d)", filename, errno);
    errors++;
    return;
  }

  char * cs= !is_compressed ? fgets(checksum, 256, infile) :gzgets((gzFile)infile, checksum, 256);
  if (cs != NULL) {
    if (checksum[strlen(checksum)-1]=='\n')
      checksum[strlen(checksum)-1]='\0';
    if(g_strcasecmp(checksum, row) != 0) {
      if (real_table != NULL)
        g_warning("%s mismatch found for `%s`.`%s`. Got '%s', expecting '%s' in file: %s", message, db ? db : real_database->name, real_table, row, checksum, filename);
      else 
        g_warning("%s mismatch found for `%s`. Got '%s', expecting '%s' in file: %s", message, db ? db : real_database->name, row, checksum, filename);
      errors++;
    } else {
      if (real_table != NULL)
        g_message("%s confirmed for `%s`.`%s`", message, db ? db : real_database->name, real_table);
      else
        g_message("%s confirmed for `%s`", message, db ? db : real_database->name);
    }
    g_free(row);
  } else {
    g_critical("error reading file %s (%d)", filename, errno);
    errors++;
    return;
  }
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
}

// this can be moved to the table structure and executed before index creation.
void checksum_databases(struct thread_data *td) {
  g_message("Starting table checksum verification");

  gchar *filename = NULL;
  GList *e = td->conf->checksum_list;//, *p;
  while (e){
    filename=e->data;
    if (g_str_has_suffix(filename,"-schema-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-checksum", "Structure checksum", checksum_table_structure);
    }else if (g_str_has_suffix(filename,"-schema-post-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-post-checksum", "Post checksum", checksum_process_structure);
    }else if (g_str_has_suffix(filename,"-schema-triggers-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-triggers-checksum", "Trigger checksum", checksum_trigger_structure);
    }else if (g_str_has_suffix(filename,"-schema-view-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-view-checksum", "View checksum", checksum_view_structure);
    }else if (g_str_has_suffix(filename,"-schema-sequence-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-sequence-checksum", "Sequence checksum", checksum_table_structure);
    }else if (g_str_has_suffix(filename,"-schema-create-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-create-checksum", "Schema create checksum", checksum_database_defaults);
    }else if (g_str_has_suffix(filename,"-schema-indexes-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-indexes-checksum", "Schema index checksum", checksum_table_indexes);
    }else{
      checksum_filename(filename, td->thrconn, "-checksum", "Checksum", checksum_table);
    }
    e=e->next;
  }
}

void checksum_dbt(struct db_table *dbt,  MYSQL *conn) {
  if (dbt->schema_checksum!=NULL){
    if (dbt->is_view)
      checksum_dbt_template(dbt, dbt->schema_checksum, conn, "View checksum", checksum_view_structure);
    else
      checksum_dbt_template(dbt, dbt->schema_checksum, conn, "Structure checksum", checksum_table_structure);
  }
  if (dbt->triggers_checksum!=NULL)
    checksum_dbt_template(dbt, dbt->triggers_checksum, conn, "Trigger checksum", checksum_trigger_structure);

  if (dbt->indexes_checksum!=NULL)
    checksum_dbt_template(dbt, dbt->indexes_checksum, conn, "Schema index checksum", checksum_table_indexes);

  if (dbt->data_checksum!=NULL)
    checksum_dbt_template(dbt, dbt->data_checksum, conn, "Checksum", checksum_table);

}

gboolean has_compession_extension(const gchar *filename){
  return g_str_has_suffix(filename, compress_extension);
}

void ml_open(FILE **infile, const gchar *filename, gboolean *is_compressed){
  if (!has_compession_extension(filename)) {
    *infile = g_fopen(filename, "r");
    *is_compressed = FALSE;
  } else {
    *infile = (void *)gzopen(filename, "r");
    *is_compressed = TRUE;
  }
}
