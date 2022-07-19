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
#include <errno.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include "common.h"
#include "myloader_stream.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_jobs_manager.h"
#include "myloader_control_job.h"
#include "myloader_restore_job.h"

extern gchar *compress_extension;
extern gchar *db;
extern gboolean stream;
extern guint max_threads_per_table; 
extern gchar *directory;
extern guint errors;
extern gboolean no_delete;
extern GHashTable *tbl_hash;
extern gboolean innodb_optimize_keys;
extern gboolean append_if_not_exist;

struct configuration *conf;
GMutex *table_hash_mutex=NULL;
void initialize_process(struct configuration *c){
  conf=c;
  table_hash_mutex=g_mutex_new();
}

struct db_table* append_new_db_table(char * filename, gchar * database, gchar *table, guint64 number_rows, GHashTable *table_hash, GString *alter_table_statement){
  if ( database == NULL || table == NULL){
    g_critical("It was not possible to process file: %s, database: %s table: %s",filename, database, table);
    exit(EXIT_FAILURE);
  }
  char *real_db_name=db_hash_lookup(database);
  if (real_db_name == NULL){
    g_critical("It was not possible to process file: %s. %s was not found and real_db_name is null. Restore without schema-create files is not supported",filename,database);
    exit(EXIT_FAILURE);
  }
  gchar *lkey=g_strdup_printf("%s_%s",database, table);
  struct db_table * dbt=g_hash_table_lookup(table_hash,lkey);
  if (dbt == NULL){
    g_mutex_lock(table_hash_mutex);
//struct db_table * dbt=g_hash_table_lookup(table_hash,lkey);
    dbt=g_hash_table_lookup(table_hash,lkey);
    if (dbt == NULL){
      dbt=g_new(struct db_table,1);
//    dbt->filename=filename;
      dbt->database=database;
    // This should be the only place where we should use `db ? db : `
      dbt->real_database = g_strdup(db ? db : real_db_name);
      dbt->table=table;
      dbt->real_table=dbt->table;
      dbt->rows=number_rows;
      dbt->restore_job_list = NULL;
      dbt->queue=g_async_queue_new();
      dbt->current_threads=0;
      dbt->max_threads=max_threads_per_table;
      dbt->mutex=g_mutex_new();
      dbt->indexes=alter_table_statement;
      dbt->start_time=NULL;
      dbt->start_index_time=NULL;
      dbt->finish_time=NULL;
      dbt->schema_created=FALSE;
      dbt->constraints=NULL;
      dbt->count=0;
      g_hash_table_insert(table_hash, lkey, dbt);
    }else{
      g_free(table);
      g_free(database);
      g_free(lkey);
      if (number_rows>0) dbt->rows=number_rows;
      if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
//    if (real_table != NULL) dbt->real_table=g_strdup(real_table);
    }
    g_mutex_unlock(table_hash_mutex);
  }else{
      g_free(table);
      g_free(database);
      g_free(lkey);
      if (number_rows>0) dbt->rows=number_rows;
      if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
  }
  return dbt;
}

void free_dbt(struct db_table * dbt){
  g_free(dbt->database);
  g_free(dbt->real_database);
  g_free(dbt->table);
//  if (dbt->constraints!=NULL) g_string_free(dbt->constraints,TRUE);
  dbt->constraints = NULL; // It should be free after constraint is executed
  g_async_queue_unref(dbt->queue);
  g_mutex_clear(dbt->mutex); 
}

void free_table_hash(GHashTable *table_hash){
  g_mutex_lock(table_hash_mutex);
  GHashTableIter iter;
  gchar * lkey;
  if (table_hash){
    g_hash_table_iter_init ( &iter, table_hash );
    struct db_table *dbt=NULL;
    while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt ) ) {
      free_dbt(dbt);
      g_free((gchar*)lkey);
      g_free(dbt);
    }
  } 
  g_mutex_unlock(table_hash_mutex);
}

void load_schema(struct db_table *dbt, gchar *filename){
  void *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  GString *create_table_statement=g_string_sized_new(512);
  g_string_set_size(data,0);
  g_string_set_size(create_table_statement,0);
  guint line=0;
  if (!g_str_has_suffix(filename, compress_extension)) {
    infile = g_fopen(filename, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(filename, "r");
    is_compressed = TRUE;
  }
  if (!infile) {
    g_critical("cannot open schema file %s (%d)", filename, errno);
    errors++;
    return;
  }
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof,&line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_strstr_len(data->str,13,"CREATE TABLE ")){
          gchar** create_table= g_strsplit(data->str, "`", 3);
          dbt->real_table=g_strdup(create_table[1]);
          if ( g_str_has_prefix(dbt->table,"mydumper_")){
            g_hash_table_insert(tbl_hash, dbt->table, dbt->real_table);
          }else{
            g_hash_table_insert(tbl_hash, dbt->real_table, dbt->real_table);
          }
          g_strfreev(create_table);
          if (append_if_not_exist){
            if ((g_strstr_len(data->str,13,"CREATE TABLE ")) && !(g_strstr_len(data->str,15,"CREATE TABLE IF"))){
              GString *tmp_data=g_string_sized_new(data->len);
              g_string_append(tmp_data, "CREATE TABLE IF NOT EXISTS ");
              g_string_append(tmp_data, &(data->str[13]));
              g_string_free(data,TRUE);
              data=tmp_data;
            }
          }
        }
        if (innodb_optimize_keys && (dbt->rows == 0 || dbt->rows >= 1000000)){
          GString *alter_table_statement=g_string_sized_new(512);
          GString *alter_table_constraint_statement=g_string_sized_new(512);
          // Check if it is a /*!40  SET
          if (g_strrstr(data->str,"/*!40")){
            g_string_append(alter_table_statement,data->str);
            g_string_append(create_table_statement,data->str);
          }else{
            // Processing CREATE TABLE statement
            GString *new_create_table_statement=g_string_sized_new(512);
            int flag = process_create_table_statement(data->str, new_create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt);
            if (flag & IS_INNODB_TABLE){
              if (flag & IS_ALTER_TABLE_PRESENT){
                finish_alter_table(alter_table_statement);
                g_message("Fast index creation will be use for table: %s.%s",dbt->real_database,dbt->real_table);
              }else{
                g_string_free(alter_table_statement,TRUE);
                alter_table_statement=NULL;
              }
              g_string_append(create_table_statement,g_strjoinv("\n)",g_strsplit(new_create_table_statement->str,",\n)",-1)));
              dbt->indexes=alter_table_statement;
              if (stream){
                struct restore_job *rj = new_schema_restore_job(filename,JOB_RESTORE_STRING, dbt, dbt->real_database,dbt->indexes,"indexes");
                g_async_queue_push(conf->post_table_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
              }
              if (flag & INCLUDE_CONSTRAINT){
                struct restore_job *rj = new_schema_restore_job(strdup(filename),JOB_RESTORE_STRING,dbt, dbt->real_database, alter_table_constraint_statement, "constraint");
                g_async_queue_push(conf->post_table_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
                dbt->constraints=alter_table_constraint_statement;
              }else{
                 g_string_free(alter_table_constraint_statement,TRUE);
              }
              g_string_set_size(data, 0);
            }else{
              g_string_free(alter_table_statement,TRUE);
              g_string_free(alter_table_constraint_statement,TRUE);
              g_string_append(create_table_statement,data->str);
            }
          }
        }else{
          g_string_append(create_table_statement,data->str);
        }
        g_string_set_size(data, 0);
      }
    }
  }
  
  struct restore_job * rj = new_schema_restore_job(filename,JOB_RESTORE_SCHEMA_STRING, dbt, dbt->real_database, create_table_statement, "");
  g_async_queue_push(conf->table_queue, new_job(JOB_RESTORE,rj,dbt->real_database));
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
  if (stream && no_delete == FALSE){
    m_remove(NULL,filename);
  }
  g_string_free(data,TRUE);

}



void get_database_table_part_name_from_filename(const gchar *filename, gchar **database, gchar **table, guint *part, guint *sub_part){
  guint l = strlen(filename)-4;
  if (g_str_has_suffix(filename, compress_extension)) {
    l-=strlen(compress_extension);
  }
  gchar *f=g_strndup(filename, l);
  gchar **split_db_tbl = g_strsplit(f, ".", -1);
  g_free(f);
  if (g_strv_length(split_db_tbl)>=2) {
    (*database)=g_strdup(split_db_tbl[0]);
    (*table)=g_strdup(split_db_tbl[1]);
    if (g_strv_length(split_db_tbl)>=3) {
      *part=g_ascii_strtoull(split_db_tbl[2], NULL, 10);
    }else {
      *part=0;
    }
    if (g_strv_length(split_db_tbl)>3) *sub_part=g_ascii_strtoull(split_db_tbl[3], NULL, 10);
  }else {
    *database=NULL;
    *table=NULL;
    *part=0;
    *sub_part=0;
  }
  g_strfreev(split_db_tbl);
}

gchar * get_database_name_from_filename(const gchar *filename){
  gchar **split_file = g_strsplit(filename, "-schema-create.sql", 2);
  gchar *db_name=g_strdup(split_file[0]);
  g_strfreev(split_file);
  return db_name;
}

void get_database_table_name_from_filename(const gchar *filename, const gchar * suffix, gchar **database, gchar **table){
  gchar **split_file = g_strsplit(filename, suffix, 2);
  gchar **split_db_tbl = g_strsplit(split_file[0], ".", -1);
  g_strfreev(split_file);
  if (g_strv_length(split_db_tbl)==2){
    *database=g_strdup(split_db_tbl[0]);
    *table=g_strdup(split_db_tbl[1]);
  }else{
    *database=NULL;
    *table=NULL;
  }
  g_strfreev(split_db_tbl);
}

gchar * get_database_name_from_content(const gchar *filename){
  FILE *infile;
  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  ml_open(&infile,filename,&is_compressed);
/*  if (!g_str_has_suffix(filename, compress_extension)) {
    infile = g_fopen(filename, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(filename, "r");
    is_compressed = TRUE;
  }*/
  if (!infile) {
    g_critical("cannot open database schema file %s (%d)", filename, errno);
    errors++;
    return NULL;
  }
  gchar *real_database=NULL;
  guint line;
  while (eof == FALSE) {
    if (read_data(infile, is_compressed, data, &eof, &line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_str_has_prefix(data->str,"CREATE ")){
          gchar** create= g_strsplit(data->str, "`", 3);
          real_database=g_strdup(create[1]);
          g_strfreev(create);
          break;
        }
      }
    }
  }

  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
  return real_database;
}

void process_tablespace_filename(char * filename) {
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, NULL, NULL, "tablespace");
  g_async_queue_push(conf->database_queue, new_job(JOB_RESTORE,rj,NULL));
}


void process_database_filename(char * filename, const char *object) {
  gchar *db_kname,*db_vname;
  db_vname=db_kname=get_database_name_from_filename(filename);

  if (db_kname!=NULL){
    if (db)
      db_vname=db;
    else
      if (g_str_has_prefix(db_kname,"mydumper_"))
        db_vname=get_database_name_from_content(g_build_filename(directory,filename,NULL));
  }else{
    g_critical("It was not possible to process db file: %s",filename);
    exit(EXIT_FAILURE);
  }

  g_debug("Adding database: %s -> %s", db_kname, db_vname);
  db_hash_insert(db_kname, db_vname);

  if (!db){
    struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, db_vname, NULL, object);
    g_async_queue_push(conf->database_queue, new_job(JOB_RESTORE,rj,NULL));
  }
}

void process_table_filename(char * filename){
  gchar *db_name, *table_name;
  struct db_table *dbt=NULL;
  get_database_table_name_from_filename(filename,"-schema.sql",&db_name,&table_name);
  if (db_name == NULL || table_name == NULL){
      g_critical("It was not possible to process file: %s (1)",filename);
      exit(EXIT_FAILURE);
  }
  char *real_db_name=db_hash_lookup(db_name);
  if (real_db_name==NULL){
    g_critical("It was not possible to process file: %s (2) because real_db_name isn't found. Restore without schema-create files is not supported",filename);
    exit(EXIT_FAILURE);
  }
  if (!eval_table(real_db_name, table_name)){
    g_warning("Skiping table: `%s`.`%s`",real_db_name, table_name);
    return;
  }
  dbt=append_new_db_table(NULL, db_name, table_name,0,conf->table_hash,NULL);
  load_schema(dbt, g_build_filename(directory,filename,NULL));
  g_free(filename);
}

void process_metadata_filename(char * filename){
  gchar *db_name, *table_name;
  get_database_table_name_from_filename(filename,"-metadata",&db_name,&table_name);
  if (db_name == NULL || table_name == NULL){
      g_critical("It was not possible to process file: %s (1)",filename);
      exit(EXIT_FAILURE);
  }
  void *infile;
  gboolean is_compressed = FALSE;
  gchar *path = g_build_filename(directory, filename, NULL);
  char metadata_val[256];
  if (!g_str_has_suffix(path, compress_extension)) {
    infile = g_fopen(path, "r");
    is_compressed = FALSE;
  } else {
    infile = (void *)gzopen(path, "r");
    is_compressed = TRUE;
  }

  if (!infile) {
    g_critical("cannot open metadata file %s (%d)", path, errno);
    errors++;
    return;
  }

  char * cs= !is_compressed ? fgets(metadata_val, 256, infile) :gzgets((gzFile)infile, metadata_val, 256);
  append_new_db_table(NULL, db_name, table_name,g_ascii_strtoull(cs, NULL, 10),conf->table_hash,NULL);
  if (!is_compressed) {
    fclose(infile);
  } else {
    gzclose((gzFile)infile);
  }
}

void process_schema_filename(gchar *filename, const char * object) {
    gchar *database=NULL, *table_name=NULL, *real_db_name=NULL;
    get_database_table_from_file(filename,"-schema",&database,&table_name);
    if (database == NULL){
      g_critical("Database is null on: %s",filename);
    }
    real_db_name=db_hash_lookup(database);
    if (!eval_table(real_db_name, table_name)){
      g_warning("File %s has been filter out",filename);
      return;
    }
    struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, real_db_name, NULL, object);
    g_async_queue_push(conf->post_queue, new_job(JOB_RESTORE,rj,real_db_name));
}

void process_data_filename(char * filename){
  gchar *db_name, *table_name;
  // TODO: check if it is a data file
  // TODO: we need to count sections of the data file to determine if it is ok.
  guint part=0,sub_part=0;
  get_database_table_part_name_from_filename(filename,&db_name,&table_name,&part,&sub_part);
  if (db_name == NULL || table_name == NULL){
    g_critical("It was not possible to process file: %s (3)",filename);
    exit(EXIT_FAILURE);
  }
  char *real_db_name=db_hash_lookup(db_name);
  if (!eval_table(real_db_name, table_name)){
    g_warning("Skiping table: `%s`.`%s`",real_db_name, table_name);
    return;
  }
  struct db_table *dbt=append_new_db_table(filename, db_name, table_name,0,conf->table_hash,NULL);
  struct restore_job *rj = new_data_restore_job( g_strdup(filename), JOB_RESTORE_FILENAME, dbt, part, sub_part);
  g_mutex_lock(dbt->mutex);
  dbt->count++; 
//  dbt->restore_job_list=g_list_insert_sorted(dbt->restore_job_list,rj,&compare_filename_part);
  dbt->restore_job_list=g_list_append(dbt->restore_job_list,rj);
  g_mutex_unlock(dbt->mutex);
}



