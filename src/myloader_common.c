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

extern gchar *compress_extension;
extern gchar *db;
extern gboolean no_delete;
extern gboolean stream;
extern gboolean resume;
extern char **tables;
extern gchar *tables_skiplist_file;
extern guint errors;
extern gchar *directory;

static GMutex *db_hash_mutex = NULL;
GHashTable *db_hash=NULL;
GHashTable *tbl_hash=NULL;

void initialize_common(){
  db_hash_mutex=g_mutex_new();
  tbl_hash=g_hash_table_new ( g_str_hash, g_str_equal );
}

gboolean m_filename_has_suffix(gchar const *str, gchar const *suffix){
  if (g_str_has_suffix(str, compress_extension)){
    return g_strstr_len(&(str[strlen(str)-strlen(compress_extension)-strlen(suffix)]), strlen(str)-strlen(compress_extension),suffix) != NULL; 
  }
  return g_str_has_suffix(str,suffix);
}

void db_hash_insert(gchar *k, gchar *v){
  g_mutex_lock(db_hash_mutex);
  g_hash_table_insert(db_hash, k, v);
  g_mutex_unlock(db_hash_mutex);
}

char * db_hash_lookup(gchar *database){
  char *r=NULL;
  g_mutex_lock(db_hash_mutex);
  r=g_hash_table_lookup(db_hash,database);
  g_mutex_unlock(db_hash_mutex);
  return r;
}

gboolean eval_table( char *db_name, char * table_name){
  if ( tables ){
    if ( ! is_table_in_list(table_name, tables) ){
      return FALSE;
    }
  }
  if ( tables_skiplist_file && check_skiplist(db_name, table_name )){
    return FALSE;
  }
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
guint execute_use(struct thread_data *td, const gchar * msg){
  gchar *query = g_strdup_printf("USE `%s`", td->current_database);
  if (mysql_query(td->thrconn, query)) {
    g_critical("Error switching to database `%s` %s", td->current_database, msg);
    g_free(query);
    return 1;
  }
  g_free(query);
  return 0;
}

void execute_use_if_needs_to(struct thread_data *td, gchar *database, const gchar * msg){
  if ( database != NULL && db == NULL ){
    if (td->current_database==NULL || g_strcmp0(database, td->current_database) != 0){
      td->current_database=database;
      if (execute_use(td, msg)){
        exit(EXIT_FAILURE);
      }
    }
  }
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
      g_critical("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.");
      exit(EXIT_FAILURE);
    }
    return RESUME;
  } else if ( strcmp(filename, "resume.partial") == 0 ){
    g_critical("resume.partial file found. Remove it and restart process if you consider that it will be safe.");
    exit(EXIT_FAILURE);
  } else if (m_filename_has_suffix(filename, "-checksum")) {
    return CHECKSUM;
  } else if (m_filename_has_suffix(filename, "-schema-view.sql") ){
    return SCHEMA_VIEW;
  } else if (m_filename_has_suffix(filename, "-schema-triggers.sql") ){
    return SCHEMA_TRIGGER;
  } else if (m_filename_has_suffix(filename, "-schema-post.sql") ){
    return SCHEMA_POST;
  } else if (m_filename_has_suffix(filename, "-schema-create.sql") ){
    return SCHEMA_CREATE;
  } else if (m_filename_has_suffix(filename, ".sql") ){
    return DATA;
  }else if (g_str_has_suffix(filename, ".dat"))
    return LOAD_DATA;
  return IGNORED;
}


gboolean read_data(FILE *file, gboolean is_compressed, GString *data,
                   gboolean *eof, guint *line) {
  char buffer[256];

  do {
    if (!is_compressed) {
      if (fgets(buffer, 256, file) == NULL) {
        if (feof(file)) {
          *eof = TRUE;
          buffer[0] = '\0';
        } else {
          return FALSE;
        }
      }
    } else {
      if (!gzgets((gzFile)file, buffer, 256)) {
        if (gzeof((gzFile)file)) {
          *eof = TRUE;
          buffer[0] = '\0';
        } else {
          return FALSE;
        }
      }
    }
    g_string_append(data, buffer);
    if (strlen(buffer) != 256)
      (*line)++;
  } while ((buffer[strlen(buffer)] != '\0') && *eof == FALSE);

  return TRUE;
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
  append_alter_table(alter_table_statement, dbt->real_database,dbt->real_table);
  append_alter_table(alter_table_constraint_statement, dbt->real_database,dbt->real_table);
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
          append_alter_table(alter_table_statement,dbt->real_database,dbt->real_table);
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

gint compare_dbt(gconstpointer a, gconstpointer b, gpointer table_hash){
  gchar *a_key=g_strdup_printf("%s_%s",((struct db_table *)a)->database,((struct db_table *)a)->table);
  gchar *b_key=g_strdup_printf("%s_%s",((struct db_table *)b)->database,((struct db_table *)b)->table);
  struct db_table * a_val=g_hash_table_lookup(table_hash,a_key);
  struct db_table * b_val=g_hash_table_lookup(table_hash,b_key);
  g_free(a_key);
  g_free(b_key);
  return a_val->rows < b_val->rows;
}

void refresh_table_list(struct configuration *conf){
  GList * table_list=NULL;
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, conf->table_hash );
  struct db_table *dbt=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
    table_list=g_list_insert_sorted_with_data (table_list,dbt,&compare_dbt,conf->table_hash);
  }
  g_list_free(conf->table_list);
  conf->table_list=table_list;
}

void checksum_filename(const gchar *filename, MYSQL *conn, const gchar *suffix, const gchar *message, gchar* fun()) {
  gchar *database = NULL, *table = NULL;
  get_database_table_from_file(filename,suffix,&database,&table);
  gchar *real_database=db_hash_lookup(database);
  gchar *real_table=NULL;
  if (table != NULL ){
    real_table=g_hash_table_lookup(tbl_hash,table);
    g_free(table);
  }
  g_free(database);
  void *infile;
  char checksum[256];
  int errn=0;
  char * row=fun(conn, db ? db : real_database, real_table, &errn);
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
    if(g_strcasecmp(checksum, row) != 0) {
      if (real_table != NULL)
        g_warning("%s mismatch found for `%s`.`%s`. Got '%s', expecting '%s' in file: %s", message, db ? db : real_database, real_table, row, checksum, filename);
      else 
        g_warning("%s mismatch found for `%s`. Got '%s', expecting '%s' in file: %s", message, db ? db : real_database, row, checksum, filename);
      errors++;
    } else {
      if (real_table != NULL)
        g_message("%s confirmed for `%s`.`%s`", message, db ? db : real_database, real_table);
      else
        g_message("%s confirmed for `%s`", message, db ? db : real_database);
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
    }else{
    if (g_str_has_suffix(filename,"-schema-post-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-post-checksum", "Post checksum", checksum_process_structure);
    }else{
    if (g_str_has_suffix(filename,"-schema-triggers-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-triggers-checksum", "Trigger checksum", checksum_trigger_structure);
    }else{
    if (g_str_has_suffix(filename,"-schema-view-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-view-checksum", "View checksum", checksum_view_structure);
    }else{
    if (g_str_has_suffix(filename,"-schema-create-checksum")){
      checksum_filename(filename, td->thrconn, "-schema-create-checksum", "Schema create checksum", checksum_database_defaults);
    }else{
      checksum_filename(filename, td->thrconn, "-checksum", "Checksum", checksum_table);
    }}}}}
    e=e->next;
  }
}

void ml_open(FILE **infile, const gchar *filename, gboolean *is_compressed){
  if (!g_str_has_suffix(filename, compress_extension)) {
    *infile = g_fopen(filename, "r");
    *is_compressed = FALSE;
  } else {
    *infile = (void *)gzopen(filename, "r");
    *is_compressed = TRUE;
  }
}

void remove_definer(GString * data){
  char * from=g_strstr_len(data->str,50," DEFINER=");
  if (from){
    from++;
    char * to=g_strstr_len(from,30," ");
    if (to){
      while(from != to){
        from[0]=' ';
        from++;
      }
    }
  }
}

