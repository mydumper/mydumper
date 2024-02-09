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
#include "common.h"
#include "myloader_stream.h"
#include "myloader_common.h"
#include "myloader_process.h"
//#include "myloader_jobs_manager.h"
#include "myloader_control_job.h"
#include "myloader_restore_job.h"
#include "myloader_global.h"
#include <sys/wait.h>
#include <sys/stat.h>


GString *change_master_statement=NULL;
gboolean append_if_not_exist=FALSE;
GHashTable *fifo_hash=NULL;
GMutex *fifo_table_mutex=NULL;

struct configuration *conf;
extern gboolean schema_sequence_fix;
void initialize_process(struct configuration *c){
  conf=c;
  fifo_hash=g_hash_table_new(g_direct_hash,g_direct_equal);
  fifo_table_mutex = g_mutex_new();
}

struct db_table* append_new_db_table( struct database *real_db_name, gchar *table, guint64 number_rows, GString *alter_table_statement){
//  if ( database == NULL || table == NULL){
//    m_critical("It was not possible to process file: %s, database: %s table: %s",filename, database, table);
//  }
//  struct database *real_db_name=get_db_hash(database,database);
//  if (real_db_name == NULL){
//    m_error("It was not possible to process file: %s. %s was not found and real_db_name is null. Restore without schema-create files is not supported",filename,database);
//  }
  gchar *lkey=build_dbt_key(real_db_name->filename, table);
  struct db_table * dbt=g_hash_table_lookup(conf->table_hash,lkey);
  if (dbt == NULL){
//    g_message("Adding new table: `%s`.`%s`", real_db_name->name, table);
    g_mutex_lock(conf->table_hash_mutex);
//struct db_table * dbt=g_hash_table_lookup(table_hash,lkey);
    dbt=g_hash_table_lookup(conf->table_hash,lkey);
    if (dbt == NULL){
      dbt=g_new(struct db_table,1);
//    dbt->filename=filename;
      dbt->database=real_db_name;
    // This should be the only place where we should use `db ? db : `
 //     dbt->database->real_database = g_strdup(db ? db : real_db_name->name);
      dbt->table=table;
      dbt->real_table=dbt->table;
      dbt->rows=number_rows;
      dbt->restore_job_list = NULL;
//      dbt->queue=g_async_queue_new();
      dbt->current_threads=0;
      dbt->max_threads=max_threads_per_table>num_threads?num_threads:max_threads_per_table;
      dbt->retry_count= retry_count;
      dbt->mutex=g_mutex_new();
      dbt->indexes=alter_table_statement;
      dbt->start_data_time=NULL;
      dbt->finish_data_time=NULL;
      dbt->start_index_time=NULL;
      dbt->finish_time=NULL;
//      dbt->completed=FALSE;
      dbt->schema_state=NOT_FOUND;
//      dbt->schema_created=FALSE;
      dbt->index_enqueued=FALSE;
      dbt->remaining_jobs = 0;
      dbt->constraints=NULL;
      dbt->count=0;
      g_hash_table_insert(conf->table_hash, lkey, dbt);
      refresh_table_list_without_table_hash_lock(conf);
//      g_message("New db_table: %s", lkey);
      dbt->schema_checksum=NULL;
      dbt->triggers_checksum=NULL;
      dbt->indexes_checksum=NULL;
      dbt->data_checksum=NULL;
      dbt->is_view=FALSE;
      dbt->is_sequence=FALSE;
    }else{
//      g_message("Found db_table: %s", lkey);
      g_free(table);
      g_free(lkey);
      if (number_rows>0) dbt->rows=number_rows;
      if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
//    if (real_table != NULL) dbt->real_table=g_strdup(real_table);
    }
    g_mutex_unlock(conf->table_hash_mutex);
  }else{
//      g_message("Found db_table: %s", lkey);
      g_free(table);
      g_free(lkey);
      if (number_rows>0) dbt->rows=number_rows;
      if (alter_table_statement != NULL) dbt->indexes=alter_table_statement;
  }
  return dbt;
}

void free_dbt(struct db_table * dbt){
//  g_free(dbt->database);
//  g_free(dbt->database->real_database);
  g_free(dbt->table);
//  if (dbt->constraints!=NULL) g_string_free(dbt->constraints,TRUE);
  dbt->constraints = NULL; // It should be free after constraint is executed
//  g_async_queue_unref(dbt->queue);
  g_mutex_clear(dbt->mutex); 
  
}

void free_table_hash(GHashTable *table_hash){
  g_mutex_lock(conf->table_hash_mutex);
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
  g_mutex_unlock(conf->table_hash_mutex);
}

FILE * myl_open(char *filename, const char *type){
  FILE *file=NULL;
  gchar *basename=NULL, *fifoname=NULL;
  int child_proc;
  (void) child_proc;
  gchar **command=NULL;
  struct stat a;
  if (get_command_and_basename(filename, &command,&basename)){


    fifoname=basename;
    if (fifo_directory != NULL){
      gchar *basefilename=g_path_get_basename(basename);
      fifoname=g_strdup_printf("%s/%s", fifo_directory, basefilename);
      g_free(basename);
    }


    lstat(fifoname, &a);
    if ((a.st_mode & S_IFMT) == S_IFIFO){
      g_warning("FIFO file found %s, removing and continuing", fifoname);
      remove(fifoname);
    }

    if (mkfifo(fifoname,0666)){
      g_critical("cannot create named pipe %s (%d)", fifoname, errno); 
    }

    child_proc = execute_file_per_thread(filename, fifoname, command);
    file=g_fopen(fifoname,type);
    g_mutex_lock(fifo_table_mutex);
    struct fifo *f=g_hash_table_lookup(fifo_hash,file);
    if (f!=NULL){
      g_mutex_lock(f->mutex);
      g_mutex_unlock(fifo_table_mutex);
      f->pid = child_proc;
      f->filename=g_strdup(filename);
      f->stdout_filename=fifoname;
    }else{
      f=g_new0(struct fifo, 1);
      f->mutex=g_mutex_new();
      g_mutex_lock(f->mutex);
      f->pid = child_proc;
      f->filename=g_strdup(filename);
      f->stdout_filename=fifoname;
      g_hash_table_insert(fifo_hash,file,f);
      g_mutex_unlock(fifo_table_mutex);
    }

  }else{
    lstat(filename, &a);
    if ((a.st_mode & S_IFMT) == S_IFIFO){
      g_warning("FIFO file found %s. Skipping", filename);
      file=NULL;
    }else{
      file=g_fopen(filename, type);
    }
  }
  return file;
}

void myl_close(char *filename, FILE *file, gboolean rm){
  g_mutex_lock(fifo_table_mutex);
  struct fifo *f=g_hash_table_lookup(fifo_hash,file);
  g_mutex_unlock(fifo_table_mutex);
  fclose(file);

  if (f != NULL){
    int status=0;
    waitpid(f->pid, &status, 0);
    g_mutex_lock(fifo_table_mutex);
    g_mutex_unlock(f->mutex);
    g_mutex_unlock(fifo_table_mutex);

    remove(f->stdout_filename);
  }
  if (rm){
    m_remove(NULL,filename);
  }
}


struct control_job * load_schema(struct db_table *dbt, gchar *filename){
  void *infile;
//  gboolean is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  GString *create_table_statement=g_string_sized_new(512);
  g_string_set_size(data,0);
  g_string_set_size(create_table_statement,0);
  guint line=0;
  infile=myl_open(filename,"r");

  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return NULL;
  }

  while (eof == FALSE) {
    if (read_data(infile, data, &eof,&line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_strstr_len(data->str,13,"CREATE TABLE ")){
          // We consider that 30 is the max length to find the identifier
          // We considered that the CREATE TABLE could inlcude the IF NOT EXISTS clause
          if (!g_strstr_len(data->str,30,identifier_quote_character_str)){
            g_error("Identifier quote character (%s) not found on %s. Review file and configure --identifier-quote-character properly", identifier_quote_character_str, filename);
            return NULL;
          }
          {
            GError *err= NULL;
            GMatchInfo *match_info;
            char *expr= g_strdup_printf("CREATE\\s+TABLE\\s+[^%c]*%c(.+?)%c\\s*\\(", identifier_quote_character, identifier_quote_character, identifier_quote_character);
            /*
              G_REGEX_DOTALL: A dot metacharacter (".") in the pattern matches
              all characters, including newlines. Without it, newlines are excluded. This
              option can be changed within a pattern by a ("?s") option setting.

              G_REGEX_ANCHORED: The pattern is forced to be "anchored", that is,
              it is constrained to match only at the first matching point in the string that
              is being searched. This effect can also be achieved by appropriate constructs in
              the pattern itself such as the "^" metacharacter.

              G_REGEX_RAW: Usually strings must be valid UTF-8 strings, using
              this flag they are considered as a raw sequence of bytes.
            */
            const GRegexCompileFlags flags= G_REGEX_CASELESS|G_REGEX_MULTILINE|G_REGEX_DOTALL|G_REGEX_ANCHORED|G_REGEX_RAW;
            GRegex *regex= g_regex_new(expr, flags, 0, &err);
            if (!regex)
              goto regex_error;
            if (!g_regex_match(regex, data->str, 0, &match_info) ||
                !g_match_info_matches(match_info)) {
              g_regex_unref(regex);
regex_error:
              g_free(expr);
              g_error("Cannot parse real table name from CREATE TABLE statement:\n%s", data->str);
              return NULL;
            }
            dbt->real_table= g_match_info_fetch(match_info, 1);
            g_regex_unref(regex);
            if (!strlen(dbt->real_table))
              goto regex_error;
            g_free(expr);
          }
          if ( g_str_has_prefix(dbt->table,"mydumper_")){
            g_hash_table_insert(tbl_hash, dbt->table, dbt->real_table);
          }else{
            g_hash_table_insert(tbl_hash, dbt->real_table, dbt->real_table);
          }
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
        if (innodb_optimize_keys){
          GString *alter_table_statement=g_string_sized_new(512);
          GString *alter_table_constraint_statement=g_string_sized_new(512);
          // Check if it is a /*!40  SET
          if (g_strrstr(data->str,"/*!40")){
            g_string_append(alter_table_statement,data->str);
            g_string_append(create_table_statement,data->str);
          }else{
            // Processing CREATE TABLE statement
            GString *new_create_table_statement=g_string_sized_new(512);
            int flag = process_create_table_statement(data->str, new_create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt, (dbt->rows == 0 || dbt->rows >= 1000000));
            if (flag & IS_INNODB_TABLE){
              if (flag & IS_ALTER_TABLE_PRESENT){
                finish_alter_table(alter_table_statement);
                g_message("Fast index creation will be use for table: %s.%s",dbt->database->real_database,dbt->real_table);
              }else{
                g_string_free(alter_table_statement,TRUE);
                alter_table_statement=NULL;
              }
              g_string_append(create_table_statement,g_strjoinv("\n)",g_strsplit(new_create_table_statement->str,",\n)",-1)));
              dbt->indexes=alter_table_statement;
              if (flag & INCLUDE_CONSTRAINT){
                struct restore_job *rj = new_schema_restore_job(strdup(filename),JOB_RESTORE_STRING,dbt, dbt->database, alter_table_constraint_statement, CONSTRAINTS);
                g_async_queue_push(conf->post_table_queue, new_job(JOB_RESTORE,rj,dbt->database));
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

  if (schema_sequence_fix) {
    gchar *statement = filter_sequence_schemas(create_table_statement->str);
    g_string_assign(create_table_statement, statement);
    g_free(statement);
  }

  struct restore_job * rj = new_schema_restore_job(filename,JOB_TO_CREATE_TABLE, dbt, dbt->database, create_table_statement, "");
  struct control_job * cj = new_job(JOB_RESTORE,rj,dbt->database);
//  g_async_queue_push(conf->table_queue, new_job(JOB_RESTORE,rj,dbt->database->real_database));
  myl_close(filename,infile,FALSE);

  g_string_free(data,TRUE);

  return cj;
}



void get_database_table_part_name_from_filename(const gchar *filename, gchar **database, gchar **table, guint *part, guint *sub_part){
  gchar **split_db_tbl = g_strsplit(filename, ".", 4);
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

gchar * get_database_name_from_content(gchar *filename){
  FILE *infile;
//  enum data_file_type is_compressed = FALSE;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  infile=myl_open(filename,"r");
  if (!infile) {
    g_critical("cannot open database schema file %s (%d)", filename, errno);
    errors++;
    return NULL;
  }
  gchar *real_database=NULL;
  guint line;
  while (eof == FALSE) {
    if (read_data(infile, data, &eof, &line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_str_has_prefix(data->str,"CREATE ")){
          gchar** create= g_strsplit(data->str, identifier_quote_character_str, 3);
          real_database=g_strdup(create[1]);
          g_strfreev(create);
          break;
        }else{
          g_string_set_size(data,0);
        }
      }
    }
  }
  myl_close(filename, infile, FALSE);
  return real_database;
}

void process_tablespace_filename(char * filename) {
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, NULL, NULL, TABLESPACE);
  g_async_queue_push(conf->database_queue, new_job(JOB_RESTORE,rj,NULL));
}


void process_database_filename(char * filename) {
  gchar *db_kname,*db_vname;
  db_vname=db_kname=get_database_name_from_filename(filename);

  if (db_kname!=NULL){
    if (g_str_has_prefix(db_kname,"mydumper_"))
      db_vname=get_database_name_from_content(g_build_filename(directory,filename,NULL));
    if(!db_vname)
      m_critical("It was not possible to process db content in file: %s",filename);
  }else{
    m_critical("It was not possible to process db file: %s",filename);
  }

  trace("Adding database: %s -> %s", db_kname, db_vname);
  struct database *real_db_name = get_db_hash(db_kname, db_vname);
  if (!db){
    real_db_name->schema_state=NOT_CREATED;
    struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, real_db_name, NULL, CREATE_DATABASE);
    g_async_queue_push(conf->database_queue, new_job(JOB_RESTORE,rj,NULL));
  }else{
    real_db_name->schema_state=CREATED;
  }
}


/* @return TRUE to enqueue */
gboolean process_table_filename(char * filename){
  gchar *db_name, *table_name;
  struct db_table *dbt=NULL;
  get_database_table_name_from_filename(filename,"-schema.sql",&db_name,&table_name);
  if (db_name == NULL || table_name == NULL){
      m_critical("It was not possible to process file: %s (1)",filename);
  }

  struct database *real_db_name=get_db_hash(db_name,db_name);
  if (!eval_table(real_db_name->name, table_name, conf->table_list_mutex)){
    g_warning("Skiping table: `%s`.`%s`",real_db_name->name, table_name);
    return FALSE;
  }

  dbt=append_new_db_table(real_db_name, table_name,0,NULL);
  dbt->schema_state=NOT_CREATED;
  struct control_job * cj = load_schema(dbt, g_build_filename(directory,filename,NULL));
  if (!cj) {
    g_free(dbt);
    return FALSE;
  }
  g_mutex_lock(real_db_name->mutex);
  /*
    When processing is possible buffer queues from real_db_name requeued into
    object queue td->conf->table_queue (see set_db_schema_state_to_created()).
  */
  if (real_db_name->schema_state != CREATED || sequences_processed < sequences){
    g_async_queue_push(real_db_name->queue, cj);
    g_mutex_unlock(real_db_name->mutex);
    return FALSE;
  }else{
    if (cj) {
      trace("table_queue <- %s: %s", rjtype2str(cj->data.restore_job->type), filename);
      g_async_queue_push(conf->table_queue, cj);
    }
  }
  g_mutex_unlock(real_db_name->mutex);
  return TRUE;
//  g_free(filename);
}

void process_metadata_global(const char *file)
{
//  void *infile;
  gchar *path = g_build_filename(directory, file, NULL);
  GKeyFile * kf = load_config_file(path);
  if (kf==NULL)
    g_error("Global metadata file processing was not possible");

  set_thread_name("MDT");
  message("Reading metadata: %s", file);
  guint j=0;
  gchar *value=NULL;
  gchar *real_table_name;
  gsize length=0;
  gchar **groups=g_key_file_get_groups(kf, &length);
  gchar** database_table=NULL;
  struct db_table *dbt=NULL;
  change_master_statement=g_string_new("");
  const char *delim_bt= "`.`";
  const char *delim_dq= "\".\"";
  const char *delimiter=    identifier_quote_character == BACKTICK ? delim_bt : delim_dq;
  const char *wrong_quote=  identifier_quote_character == BACKTICK ? "\"" : "`";
  for (j= 0; j < length; j++) {
    gchar *group= newline_unprotect(groups[j]);
    if (g_str_has_prefix(group, "config")) {
      if (j > 0)
        m_critical("Wrong metadata: [config] group must be first");
      value= get_value(kf, group, "quote_character");
      if (value) {
        if (!strcmp(value, "BACKTICK")) {
          identifier_quote_character= BACKTICK;
          identifier_quote_character_str= "`";
          wrong_quote= "\"";
          delimiter= delim_bt;
        } else if (!strcmp(value, "DOUBLE_QUOTE")) {
          identifier_quote_character= DOUBLE_QUOTE;
          identifier_quote_character_str= "\"";
          delimiter= delim_dq;
          wrong_quote= "`";
        } else {
          m_critical("Wrong quote_character = %s in metadata", value);
        }
        trace("metadata: quote character is %c", identifier_quote_character);
      }
    } else if (g_str_has_prefix(group, wrong_quote))
      g_error("metadata is broken: group %s has wrong quoting: %s; must be: %c", group, wrong_quote, identifier_quote_character);
    else if (g_str_has_prefix(group, identifier_quote_character_str)) {
      database_table= g_strsplit(group+1, delimiter, 2);
      if (database_table[1] != NULL){
        database_table[1][strlen(database_table[1])-1]='\0';
        struct database *real_db_name=get_db_hash(database_table[0],database_table[0]);
        dbt=append_new_db_table(real_db_name, database_table[1],0,NULL);
        dbt->data_checksum=get_value(kf,group,"data_checksum");
        dbt->schema_checksum=get_value(kf,group,"schema_checksum");
        dbt->indexes_checksum= get_value(kf,group,"indexes_checksum");
        dbt->triggers_checksum=get_value(kf,group,"triggers_checksum");
        value=get_value(kf,group,"is_view");
        if (value != NULL && g_strcmp0(value,"1")==0){
          dbt->is_view=TRUE;
        }
        if (value) g_free(value);
        value=get_value(kf, group, "is_sequence");
        if (value != NULL && g_strcmp0(value, "1") == 0){
          dbt->is_sequence= TRUE;
          ++sequences;
        }
        if (value) g_free(value);
        if (get_value(kf,group,"rows")){
          dbt->rows=g_ascii_strtoull(get_value(kf,group,"rows"),NULL, 10);
        }
        value= get_value(kf, group, "real_table_name");
        if (value){
          real_table_name= newline_unprotect(value);
          g_free(value);
          if (g_strcmp0(dbt->real_table, real_table_name))
            dbt->real_table= real_table_name;
          else
            g_free(real_table_name);
        }
      } else {
        database_table[0][strlen(database_table[0])-1]='\0';
        struct database *database=get_db_hash(database_table[0],database_table[0]);
        database->schema_checksum=get_value(kf,group,"schema_checksum");
        database->post_checksum=get_value(kf,group,"post_checksum");
        database->triggers_checksum=get_value(kf,group,"triggers_checksum");
      }
    }else if (g_str_has_prefix(group,"replication")){
      change_master(kf, group, change_master_statement);
    }else if (g_strstr_len(group,6,"master")){
      change_master(kf, group, change_master_statement);
    }else if (g_strstr_len(group, 26,"myloader_session_variables")){
      load_hash_of_all_variables_perproduct_from_key_file(kf,set_session_hash,"myloader_session_variables");
      refresh_set_session_from_hash(set_session,set_session_hash);
    } else {
      trace("metadata: skipping group %s", group);
    }
    g_free(group);
  }
}


gboolean process_schema_view_filename(gchar *filename) {
  gchar *database=NULL, *table_name=NULL;
  struct database *real_db_name=NULL;
  get_database_table_from_file(filename,"-schema",&database,&table_name);
  if (database == NULL){
    g_critical("Database is null on: %s",filename);
  }
  real_db_name=get_db_hash(database,database);
  if (!eval_table(real_db_name->name, table_name, conf->table_list_mutex)){
    g_warning("File %s has been filter out(1)",filename);
    return FALSE;
  }
  struct db_table *dbt=append_new_db_table(real_db_name, table_name,0, NULL);
  dbt->is_view=TRUE;
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, real_db_name, NULL, VIEW);
  g_async_queue_push(conf->view_queue, new_job(JOB_RESTORE,rj,real_db_name));
  return TRUE;
}

gboolean process_schema_sequence_filename(gchar *filename) {
  gchar *database=NULL, *table_name=NULL;
  struct database *real_db_name=NULL;
  struct db_table *dbt;
  get_database_table_from_file(filename,"-schema-sequence",&database,&table_name);
  if (database == NULL){
    g_error("Database is null on: %s", filename);
    return FALSE;
  }
  real_db_name=get_db_hash(database,database);
  if (real_db_name==NULL){
    g_warning("It was not possible to process file: %s (3) because real_db_name isn't found. We might renqueue it, take into account that restores without schema-create files are not supported",filename);
    return FALSE;
  }
  if (!eval_table(real_db_name->name, table_name, conf->table_list_mutex)){
    g_warning("File %s has been filter out",filename);
    return TRUE;
  }
  dbt= append_new_db_table(real_db_name, table_name, 0, NULL);
  dbt->is_sequence= TRUE;
  dbt->schema_state= NOT_CREATED;
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, real_db_name, NULL, SEQUENCE );
  struct control_job *cj= new_job(JOB_RESTORE,rj,real_db_name);
  g_mutex_lock(real_db_name->mutex);
  if (real_db_name->schema_state != CREATED){
    trace("%s.sequence_queue <- %s: %s", database, rjtype2str(cj->data.restore_job->type), filename);
    trace("real_db_name: %p; sequence_queue: %p", real_db_name, real_db_name->sequence_queue);
    g_async_queue_push(real_db_name->sequence_queue, cj);
    g_mutex_unlock(real_db_name->mutex);
    return FALSE;
  }else{
    if (cj) {
      trace("table_queue <- %s: %s", rjtype2str(cj->data.restore_job->type), filename);
      g_async_queue_push(conf->table_queue, cj);
    }
  }
  g_mutex_unlock(real_db_name->mutex);
  return TRUE;
}


gboolean process_schema_filename(gchar *filename, const char * object) {
  gchar *database=NULL, *table_name=NULL;
  struct database *real_db_name=NULL;
  get_database_table_from_file(filename,"-schema",&database,&table_name);
  if (database == NULL){
    g_critical("Database is null on: %s",filename);
  }
  real_db_name=get_db_hash(database,database);
  if (table_name != NULL && !eval_table(real_db_name->name, table_name, conf->table_list_mutex)){
    g_warning("File %s has been filter out(1)",filename);
    return FALSE; 
  }
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, real_db_name, NULL, object);
  g_async_queue_push(conf->post_queue, new_job(JOB_RESTORE,rj,real_db_name));
  return TRUE; // SCHEMA_VIEW
}

gint cmp_restore_job(gconstpointer rj1, gconstpointer rj2){
  if (((struct restore_job *)rj1)->data.drj->part != ((struct restore_job *)rj2)->data.drj->part ){
    guint a=((struct restore_job *)rj1)->data.drj->part, b=((struct restore_job *)rj2)->data.drj->part;
    while ( a%2 == b%2 ){
      a=a>>1;
      b=b>>1;
    }
    return a%2 > b%2;
  }
  return ((struct restore_job *)rj1)->data.drj->sub_part > ((struct restore_job *)rj2)->data.drj->sub_part;
}

gboolean process_data_filename(char * filename){
  gchar *db_name, *table_name;
  // TODO: check if it is a data file
  // TODO: we need to count sections of the data file to determine if it is ok.
  guint part=0,sub_part=0;
  get_database_table_part_name_from_filename(filename,&db_name,&table_name,&part,&sub_part);
  if (db_name == NULL || table_name == NULL){
    m_critical("It was not possible to process file: %s (3)",filename);
  }

  struct database *real_db_name=get_db_hash(db_name,db_name);
  if (!eval_table(real_db_name->name, table_name, conf->table_list_mutex)){
    g_warning("Skiping table: `%s`.`%s`",real_db_name->name, table_name);
    return FALSE;
  }

  struct db_table *dbt=append_new_db_table(real_db_name, table_name,0,NULL);
  struct restore_job *rj = new_data_restore_job( g_strdup(filename), JOB_RESTORE_FILENAME, dbt, part, sub_part);
  g_mutex_lock(dbt->mutex);
  g_atomic_int_add(&(dbt->remaining_jobs), 1);
  dbt->count++; 
  dbt->restore_job_list=g_list_insert_sorted(dbt->restore_job_list,rj,&cmp_restore_job);
//  dbt->restore_job_list=g_list_append(dbt->restore_job_list,rj);
  g_mutex_unlock(dbt->mutex);
  return TRUE;
}

gboolean process_checksum_filename(char * filename){
  gchar *db_name, *table_name;
  // TODO: check if it is a data file
  // TODO: we need to count sections of the data file to determine if it is ok.
  get_database_table_from_file(filename,"-",&db_name,&table_name);
  if (db_name == NULL){
    m_critical("It was not possible to process file: %s (4)",filename);
  }
  if (table_name != NULL) {
    struct database *real_db_name=get_db_hash(db_name,db_name);
    if (!eval_table(real_db_name->name, table_name, conf->table_list_mutex)){
      return FALSE;
    }
  }
  return TRUE;
}
