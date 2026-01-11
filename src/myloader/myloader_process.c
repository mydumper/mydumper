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
#include <sys/wait.h>
#include <sys/stat.h>

#include "myloader.h"
#include "myloader_stream.h"
#include "myloader_common.h"
#include "myloader_process.h"
#include "myloader_control_job.h"
#include "myloader_restore_job.h"
#include "myloader_global.h"
#include "myloader_arguments.h"
#include "myloader_database.h"
#include "myloader_directory.h"
#include "myloader_worker_schema.h"


struct replication_statements *replication_statements=NULL;
gboolean append_if_not_exist=FALSE;
GHashTable *fifo_hash=NULL;
GMutex *fifo_table_mutex=NULL;
struct configuration *_conf;
extern gboolean schema_sequence_fix;
GAsyncQueue *partial_metadata_queue = NULL;

// Decompression throttle: limit concurrent decompressor processes
static GCond *decompress_cond = NULL;
static GMutex *decompress_mutex = NULL;
static guint active_decompressors = 0;
static guint max_decompressors = 0;

void initialize_process(struct configuration *c){
  partial_metadata_queue=g_async_queue_new();
  replication_statements=g_new(struct replication_statements,1);
  replication_statements->reset_replica=NULL;
  replication_statements->start_replica=NULL;
  replication_statements->change_replication_source=NULL;
  replication_statements->gtid_purge=NULL;
  replication_statements->start_replica_until=NULL;
  _conf=c;
  fifo_hash=g_hash_table_new(g_direct_hash,g_direct_equal);
  fifo_table_mutex = g_mutex_new();

  // Initialize decompression throttle
  decompress_cond = g_cond_new();
  decompress_mutex = g_mutex_new();
  // Limit concurrent decompressors to num_threads, capped at 32
  max_decompressors = num_threads;
  if (max_decompressors > 32) max_decompressors = 32;
  if (max_decompressors < 4) max_decompressors = 4;
}

// Release a decompressor slot
static void release_decompressor_slot(void){
  g_mutex_lock(decompress_mutex);
  active_decompressors--;
  g_cond_signal(decompress_cond);
  g_mutex_unlock(decompress_mutex);
}

FILE * myl_open(char *filename, const char *type){
  FILE *file=NULL;
  gchar *basename=NULL, *fifoname=NULL;
  int child_proc;
  (void) child_proc;
  gchar **command=NULL;
  struct stat a;
  if (get_command_and_basename(filename, &command, &basename)){
    // Acquire decompressor slot (throttle concurrent processes)
    g_mutex_lock(decompress_mutex);
    while (active_decompressors >= max_decompressors) {
      g_cond_wait(decompress_cond, decompress_mutex);
    }
    active_decompressors++;
    g_mutex_unlock(decompress_mutex);

    fifoname=basename;
    if (fifo_directory != NULL){
      gchar *basefilename=g_path_get_basename(basename);
      fifoname=g_strdup_printf("%s/%s", fifo_directory, basefilename);
      g_free(basename);
    }
// This 2 lines simulates if a common file or a fifo file is present in the fifo dir:
//    g_file_set_contents(fifoname, "   ",2,NULL );
//    mkfifo(fifoname,0666);
    if (g_file_test(fifoname, G_FILE_TEST_EXISTS)){
      lstat(fifoname, &a);
      g_message("FIFO file: %s", filename);
      if ((a.st_mode & S_IFMT) == S_IFIFO){
        g_warning("FIFO file found %s, removing and continuing", fifoname);
        remove(fifoname);
      }
    }
    if (mkfifo(fifoname,0666)){
      g_critical("cannot create named pipe %s (%d)", fifoname, errno);
    }

    child_proc = execute_file_per_thread(filename, fifoname, command);
    file=g_fopen(fifoname,type);

    // Issue #2075: Unlink FIFO immediately after both ends are connected.
    // Once fopen succeeds, both our process and the subprocess have the FIFO open.
    // The pipe remains usable through the open file descriptors.
    // This ensures automatic cleanup on crash (no stale FIFOs left behind).
    if (file != NULL) {
      g_unlink(fifoname);
//      m_remove0(fifo_directory,fifoname);
    }
    if (stream && !no_delete)
      g_unlink(filename);
/*    gchar *tmpbasename=g_path_get_basename(filename);
    m_remove(directory,tmpbasename);
    g_free(tmpbasename);
*/
    g_mutex_lock(fifo_table_mutex);
    struct fifo *f=g_hash_table_lookup(fifo_hash,file);
    if (f!=NULL){
      g_mutex_lock(f->mutex);
      g_mutex_unlock(fifo_table_mutex);
      f->pid = child_proc;
      f->filename=g_strdup(filename);
      f->stdout_filename=fifoname;
      f->uses_decompressor=TRUE;
    }else{
      f=g_new0(struct fifo, 1);
      f->mutex=g_mutex_new();
      g_mutex_lock(f->mutex);
      f->pid = child_proc;
      f->filename=g_strdup(filename);
      f->stdout_filename=fifoname;
      f->uses_decompressor=TRUE;
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
      if (stream && !no_delete)
        g_unlink(filename);
    }
  }
  return file;
}

void myl_close(const char *filename, FILE *file, gboolean rm){
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

    // Issue #2075: FIFO is already unlinked in myl_open() after both ends connect.
    // No need to remove here - the FIFO name no longer exists on filesystem.

    // Release decompressor slot
    if (f->uses_decompressor){
      release_decompressor_slot();
    }
  }
  (void) rm;
  (void) filename;
//  if (rm){
//    m_remove(NULL,filename);
//  }
}

void process_tablespace_filename(char * filename) {
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, NULL, NULL, TABLESPACE);
  g_async_queue_push(_conf->database_queue, new_control_job(JOB_RESTORE,rj,NULL));
}

static
gboolean parse_create_table_from_file(struct db_table *dbt, gchar *filename){
  void *infile;
  gboolean eof = FALSE;
  GString *data=g_string_sized_new(512);
  GString *create_table_statement=g_string_sized_new(512);
  g_string_set_size(data,0);
  g_string_set_size(create_table_statement,0);
  guint line=0;
  infile=myl_open(filename,"r");
  trace("parse_create_table_from_file starting on %s", filename);
  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return FALSE;
  }

  while (eof == FALSE) {
    if (read_data(infile, data, &eof,&line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_strstr_len(data->str,13,"CREATE TABLE ")){
          // We consider that 30 is the max length to find the identifier
          // We considered that the CREATE TABLE could inlcude the IF NOT EXISTS clause
          if (!g_strstr_len(data->str,30,identifier_quote_character_str)){
            g_error("Identifier quote character (%s) not found on %s. Review file and configure --identifier-quote-character properly", identifier_quote_character_str, filename);
            return FALSE;
          }
//          {
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
              return FALSE;
            }
            dbt->create_table_name= g_match_info_fetch(match_info, 1);
            g_regex_unref(regex);
            if (!strlen(dbt->create_table_name))
              goto regex_error;
            g_free(expr);
//          }
          if ( g_str_has_prefix(dbt->table_filename,"mydumper_") || !dbt->source_table_name){
            dbt->source_table_name=dbt->create_table_name;
//            g_hash_table_insert(tbl_hash, dbt->table_filename, dbt->source_table_name);
//          }else{
//            if (dbt->source_table_name)
//              g_hash_table_insert(tbl_hash, dbt->table_filename, dbt->source_table_name);
//            else
//              g_hash_table_insert(tbl_hash, dbt->table_filename, dbt->create_table_name);
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
        if (optimize_keys || skip_constraints || skip_indexes ){
          GString *alter_table_statement=g_string_sized_new(512);
          GString *alter_table_constraint_statement=g_string_sized_new(512);
          // Check if it is a /*!40  SET
          if (g_strrstr(data->str,"/*!40")){
            if (!should_ignore_set_statement(data)){
              g_string_append(alter_table_statement,data->str);
              g_string_append(create_table_statement,data->str);
            }
          }else{
            // Processing CREATE TABLE statement
            int flag = // process_create_table_statement(data->str, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt, (dbt->rows == 0 || dbt->rows >= 1000000 || skip_constraints || skip_indexes));
                      global_process_create_table_statement(data->str, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt->source_table_name?dbt->source_table_name:dbt->create_table_name, (dbt->rows == 0 || dbt->rows >= 10 || skip_constraints || skip_indexes));
            if (flag & IS_TRX_TABLE){
              if (flag & IS_ALTER_TABLE_PRESENT){
//                finish_alter_table(alter_table_statement);
                g_message("Fast index creation will be used for table: %s.%s",dbt->database->target_database,dbt->source_table_name);
              }else{
                g_string_free(alter_table_statement,TRUE);
                alter_table_statement=NULL;
              }
//              g_string_append(create_table_statement,g_strjoinv("\n)",g_strsplit(new_create_table_statement->str,",\n)",-1)));
              if (!skip_indexes){
                if (optimize_keys)
                  dbt->indexes=alter_table_statement;
                else if (alter_table_statement!=NULL)
                  g_string_append(create_table_statement,alter_table_statement->str);
              }
              if (!skip_constraints && (flag & INCLUDE_CONSTRAINT)){
                struct restore_job *rj = new_schema_restore_job(strdup(filename),JOB_RESTORE_STRING,dbt, dbt->database, alter_table_constraint_statement, CONSTRAINTS);
                g_async_queue_push(_conf->post_table_queue, new_control_job(JOB_RESTORE,rj,dbt->database));
                dbt->constraints=alter_table_constraint_statement;
              }else{
                 g_string_free(alter_table_constraint_statement,TRUE);
              }
              g_string_set_size(data, 0);
            }else{
              g_string_free(alter_table_statement,TRUE);
              g_string_free(alter_table_constraint_statement,TRUE);
              g_string_set_size(create_table_statement, 0);
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

  g_string_free(data,TRUE);
  myl_close(filename, infile, FALSE);  // Close file to release decompressor slot
  trace("parse_create_table_from_file ended on %s", filename);
  return schema_push( SCHEMA_TABLE_JOB, filename, JOB_TO_CREATE_TABLE, dbt, dbt->database, create_table_statement, CREATE_TABLE, dbt->database );

}

static
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

static
gchar * get_database_name_from_filename(const gchar *filename){
  gchar **split_file = g_strsplit(filename, "-schema-create.sql", 2);
  gchar *db_name=g_strdup(split_file[0]);
  g_strfreev(split_file);
  return db_name;
}

static
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
  gchar *target_database=NULL;
  guint line;
  while (eof == FALSE) {
    if (read_data(infile, data, &eof, &line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if (g_str_has_prefix(data->str,"CREATE ")){
          gchar** create= g_strsplit(data->str, identifier_quote_character_str, 3);
          target_database=g_strdup(create[1]);
          g_strfreev(create);
          break;
        }else{
          g_string_set_size(data,0);
        }
      }
    }
  }
  myl_close(filename, infile, FALSE);
  return target_database;
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
  struct database *_database = get_database(db_kname, db_vname);

  
  if (!eval_regex(_database->source_database, NULL)){
    g_warning("Skipping database: `%s`",_database->source_database);
    _database->schema_checksum = NULL;
    _database->post_checksum = NULL;
    _database->triggers_checksum = NULL;
    _database->events_checksum = NULL;
    return;
  }

  if (!has_been_defined_a_target_database()){
    _database->schema_state=NOT_CREATED;
//    struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, _database, NULL, CREATE_DATABASE);
    schema_push(SCHEMA_CREATE_JOB, filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, _database, NULL, CREATE_DATABASE, NULL );
//    schema_push( gchar * filename, enum restore_job_type type, struct db_table * dbt, struct database * database, GString * statement, enum restore_job_statement_type object, enum control_job_type type, struct database *use_database )
//    g_async_queue_push(conf->database_queue, new_control_job(JOB_RESTORE,rj,NULL));
  }else{
    _database->schema_state=CREATED;
  }
}

gboolean process_schema_sequence_filename(gchar *filename) {
  gchar *database=NULL, *table_name=NULL;
  struct database *_database=NULL;
  struct db_table *dbt;
  get_database_table_from_file(filename,"-schema-sequence",&database,&table_name);
  if (database == NULL){
    g_error("Database is null on: %s", filename);
    return FALSE;
  }
  _database=get_database(database,database);
  if (_database==NULL){
    g_warning("It was not possible to process file: %s (3) because _database isn't found. We might renqueue it, take into account that restores without schema-create files are not supported",filename);
    return FALSE;
  }
  if (!eval_table(_database->source_database, table_name, _conf->table_list_mutex)){
    g_warning("File %s has been filter out",filename);
    return FALSE;
  }
  append_new_db_table(&dbt, _database, NULL, table_name);//, 0, NULL);
  dbt->is_sequence= TRUE;
  dbt->schema_state= NOT_CREATED;
/*  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, _database, NULL, SEQUENCE );
  struct schema_job *sj= new_schema_job(JOB_RESTORE,rj,_database);
  g_mutex_lock(_database->mutex);
  if (_database->schema_state != CREATED){
    trace("%s.sequence_queue <- %s: %s", database, rjtype2str(cj->data.restore_job->type), filename);
    trace("_database: %p; sequence_queue: %p", _database, _database->sequence_queue);
    g_async_queue_push(_database->sequence_queue, sj);
    g_mutex_unlock(_database->mutex);
    return FALSE;
  }else{
//    if (cj) {
//      trace("table_queue <- %s: %s", rjtype2str(cj->data.restore_job->type), filename);
//      g_async_queue_push(conf->table_queue, cj);
      schema_push( SCHEMA_TABLE_JOB, filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, _database, NULL, JOB_RESTORE, _database );
//    }
  }
  g_mutex_unlock(_database->mutex);
  */
  return schema_push( SCHEMA_SEQUENCE_JOB, filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, _database, NULL, SEQUENCE, _database );
}

static
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

/* @return TRUE to enqueue */
gboolean process_table_filename(char * filename){
  gchar *db_name, *table_name;
  struct db_table *dbt=NULL;
  trace("Processing table filename: %s", filename);
  get_database_table_name_from_filename(filename,"-schema.sql",&db_name,&table_name);
  if (db_name == NULL || table_name == NULL){
      m_critical("It was not possible to process file: %s (1)",filename);
  }

  struct database *_database=get_database(db_name,db_name);
  if (!eval_table(_database->source_database, table_name, _conf->table_list_mutex)){
    g_warning("Skipping table: `%s`.`%s`",_database->source_database, table_name);
    dbt=get_table(_database->database_name_in_filename, table_name);
    if (dbt){
      dbt->schema_checksum=NULL;
      dbt->triggers_checksum=NULL;
      dbt->indexes_checksum=NULL;
      dbt->data_checksum=NULL;
    }
    return FALSE;
  }

  append_new_db_table(&dbt, _database, NULL, table_name);//,0,NULL);
  if (dbt->schema_state<NOT_CREATED){
    dbt->schema_state=NOT_CREATED;
  }else{
    // parsing was already done
    trace("Processing table filename: %s was already done", filename);
    return FALSE;
  }
  return parse_create_table_from_file(dbt, g_build_filename(directory,filename,NULL));
/*
  if (!cj) {
    g_free(dbt);
    return FALSE;
  }
  g_mutex_lock(_database->mutex);
//    When processing is possible buffer queues from _database requeued into
//    object queue td->conf->table_queue (see set_db_schema_state_to_created()).
  if (_database->schema_state != CREATED ){ // || sequences_processed < sequences){
    trace("Table schema %s.%s enqueue as database has not been created yet", _database->target_database, table_name);
    g_async_queue_push(_database->table_queue, cj);
    g_mutex_unlock(_database->mutex);
    return FALSE;
  }else{
//    trace("table_queue <- %s: %s", rjtype2str(cj->data.restore_job->type), filename);
//    g_async_queue_push(conf->table_queue, cj);
    schema_push( filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, _database, NULL, SEQUENCE, JOB_RESTORE, _database );
  }
  g_mutex_unlock(_database->mutex);
  return TRUE;
//  g_free(filename);
*/
}


gboolean first_metadata_processed=FALSE;

void process_metadata_global_filename(gchar *file, GOptionContext * local_context, gboolean is_global)
{
  set_thread_name("MDT");

  gchar *path = g_build_filename(directory, file, NULL);
  trace("Reading metadata: %s", path);
  GKeyFile * kf = load_config_file(path);
  g_free(path);
  if (kf==NULL)
    g_error("Global metadata file processing was not possible");

  guint j=0;
  gchar *value=NULL;
  gchar *real_table_name=NULL;
  gsize length=0;
  gchar **groups=g_key_file_get_groups(kf, &length);
  gchar** database_table=NULL;
  struct db_table *dbt=NULL;
  const char *delim_bt= "`.`";
  const char *delim_dq= "\".\"";
  const char *delimiter=    identifier_quote_character == BACKTICK ? delim_bt : delim_dq;
  const char *wrong_quote=  identifier_quote_character == BACKTICK ? "\"" : "`";

  if (is_global){
    m_remove(directory, file);

    if (g_key_file_has_group(kf, CONFIG)){
      gsize len=0;
      GError *error = NULL;
      gchar ** keys=g_key_file_get_keys(kf,CONFIG, &len, &error);
      gsize i=0;
      GSList *list = NULL;

      if (error != NULL){
        g_error("Loading configuration on section %s: %s", CONFIG, error->message);
      }else{
        // Transform the key-value pair to parameters option that the parsing will understand
        for (i=0; i < len; i++){
          list = g_slist_append(list, g_strdup_printf("--%s",keys[i]));
          value=g_key_file_get_value(kf,CONFIG,keys[i],&error);
          if ( value != NULL ) list=g_slist_append(list, value);
        }
        gint slen = g_slist_length(list) + 1;
        gchar ** gclist = g_new0(gchar *, slen);
        GSList *ilist=list;
        gint j2=0;
        for (j2=1; j2 < slen ; j2++){
          gclist[j2]=ilist->data;
          ilist=ilist->next;
        }
        g_slist_free(list);
        // Second parse over the options
        if (!g_option_context_parse(local_context, &slen, &gclist, &error)) {
          m_critical("option parsing failed: %s, try --help\n", error->message);
        }else{
          trace("Config file loaded");
        }
        g_strfreev(gclist);
      }
      g_strfreev(keys);

      if (identifier_quote_character==BACKTICK){
        wrong_quote= "\"";
        delimiter= delim_bt;
        identifier_quote_character_str= "`";
      }else if (identifier_quote_character==DOUBLE_QUOTE){
        delimiter= delim_dq;
        wrong_quote= "`";
        identifier_quote_character_str= "\"";
      }else{
        m_critical("Wrong quote_character in metadata");
      }
      trace("metadata: quote character is %c", identifier_quote_character);
      first_metadata_processed=TRUE;
    }else{
      m_error("Section [config] was not found on metadata file: %s", file);
    }
  }else{
    if (!first_metadata_processed){
      g_async_queue_push(partial_metadata_queue,file);
      return;
    }
    m_remove(directory, file);
  }

  g_free(file);

  if (g_key_file_has_group(kf, "myloader_session_variables")){
    g_message("myloader_session_variables found on metadata");
    load_hash_of_all_variables_perproduct_from_key_file(kf,set_session_hash,"myloader_session_variables");
    refresh_set_session_from_hash(set_session,set_session_hash);
  }

  if (!stream)
    release_directory_metadata_lock();

  for (j= 0; j < length; j++) {
    gchar *group= newline_unprotect(groups[j]);
    if (g_str_has_prefix(group, "config")) {
      trace("Ignoring [config] from metadata as it was already processed");
    }else if (g_strstr_len(group, 26,"myloader_session_variables")){
      trace("Ignoring [myloader_session_variables] from metadata as it was already processed");
    } else if (g_str_has_prefix(group, wrong_quote))
      g_error("metadata is broken: group %s has wrong quoting: %s; must be: %c", group, wrong_quote, identifier_quote_character);
    else if (g_str_has_prefix(group, identifier_quote_character_str)) {
      database_table= g_strsplit(groups[j]+1, delimiter, 2);
      if (database_table[1] != NULL){
        database_table[1][strlen(database_table[1])-1]='\0';
        if (!source_db || g_strcmp0(database_table[0],source_db)==0){
          struct database *_database=get_database(database_table[0],database_table[0]);
//          gchar *table_filename=g_strdup(database_table[1]);
         
          value= get_value(kf, groups[j], "real_table_name");
          if (value){
            trace("real_table_name= %s", value);
            real_table_name= newline_unprotect(value);
            g_free(value);
          }
          append_new_db_table(&dbt, _database, real_table_name, database_table[1]);//, real_table_name);//,0,NULL);
//          if (real_table_name) g_free(real_table_name);
  //        if (table_filename) g_free(table_filename);
          real_table_name=NULL;
          dbt->data_checksum=    dbt->object_to_export.no_data   ?NULL:get_value(kf,groups[j],"data_checksum");
          dbt->schema_checksum=  dbt->object_to_export.no_schema ?NULL:get_value(kf,groups[j],"schema_checksum");
          dbt->indexes_checksum= dbt->object_to_export.no_schema ?NULL:get_value(kf,groups[j],"indexes_checksum");
          dbt->triggers_checksum=dbt->object_to_export.no_trigger?NULL:get_value(kf,groups[j],"triggers_checksum");
          value=get_value(kf,groups[j],"is_view");
          if (value != NULL && g_strcmp0(value,"1")==0){
            dbt->is_view=TRUE;
          }
          if (value) g_free(value);
          value=get_value(kf, groups[j], "is_sequence");
          if (value != NULL && g_strcmp0(value, "1") == 0){
            dbt->is_sequence= TRUE;
            ++sequences;
          }
          if (value) g_free(value);
          if (get_value(kf,groups[j],"rows")){
            dbt->rows=g_ascii_strtoull(get_value(kf,groups[j],"rows"),NULL, 10);
          }
          if (value) g_free(value);
          if (get_value(kf,groups[j],"is_view")){
            dbt->is_view=g_ascii_strtoull(get_value(kf,groups[j],"is_view"),NULL, 10);
          }
        }
      } else {
        database_table[0][strlen(database_table[0])-1]='\0';
        if (!source_db || g_strcmp0(database_table[0],source_db)==0){
          struct database *database=get_database(database_table[0],database_table[0]);
          database->schema_checksum=  get_value(kf,groups[j],"schema_checksum");
          database->post_checksum=    get_value(kf,groups[j],"post_checksum");
          database->triggers_checksum=get_value(kf,groups[j],"triggers_checksum");
          database->events_checksum=  get_value(kf,groups[j],"events_checksum");
        }
      }
    }else if (g_strstr_len(group,6,"master") || g_strstr_len(group,6,"source")){
      change_master(kf, group, replication_statements, &source_data);
    }else if (g_str_has_prefix(group,"replication")){
      change_master(kf, group, replication_statements, &replica_data);
    } else {
      trace("metadata: skipping group %s", group);
    }
    g_free(group);
  }

  if (stream)
    metadata_has_been_processed();

  file=g_async_queue_try_pop(partial_metadata_queue);
  if (file)
    process_metadata_global_filename(file, local_context, FALSE);

}

gboolean process_schema_view_filename(gchar *filename) {
  gchar *database=NULL, *table_name=NULL;
  struct database *_database=NULL;
  get_database_table_from_file(filename,"-schema",&database,&table_name);
  if (database == NULL){
    g_critical("Database is null on: %s",filename);
  }
  _database=get_database(database,database);
  if (!eval_table(_database->source_database, table_name, _conf->table_list_mutex)){
    g_warning("File %s has been filter out(1)",filename);
    return FALSE;
  }
  struct db_table *dbt=NULL;
  append_new_db_table(&dbt,_database, NULL, table_name);//,0, NULL);
  dbt->is_view=TRUE;
  struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, _database, NULL, VIEW);
  g_async_queue_push(_conf->view_queue, new_control_job(JOB_RESTORE,rj,_database));
  return TRUE;
}

// TRIGGER or POST
gboolean process_schema_post_filename(gchar *filename, enum restore_job_statement_type object) {
  gchar *database=NULL, *table_name=NULL;
  struct database *_database=NULL;
	struct db_table *dbt=NULL;
  get_database_table_from_file(filename,"-schema",&database,&table_name);
  if (database == NULL){
    g_critical("Database is null on: %s",filename);
  }
  _database=get_database(database,database);
  if (table_name != NULL){ 
	  if (!eval_table(_database->source_database, table_name, _conf->table_list_mutex)){
      g_warning("File %s has been filter out(1)",filename);
      return FALSE; 
    }
		append_new_db_table(&dbt, _database, NULL, table_name);//, 0, NULL);
  }
  if ( object == TRIGGER || dbt==NULL || !dbt->object_to_export.no_trigger){
    struct restore_job *rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, NULL, _database, NULL, object); //TRIGGER or POST
    g_async_queue_push(_conf->post_queue, new_control_job(JOB_RESTORE,rj,_database));
	}
  return TRUE; // SCHEMA_VIEW
}

static
gint cmp_restore_job(gconstpointer rj1, gconstpointer rj2){
  guint a = ((struct restore_job *)rj1)->data.drj->part;
  guint b = ((struct restore_job *)rj2)->data.drj->part;
  if (a != b) {
    // Perf: Use __builtin_clz (single CPU instruction) instead of bit-shift loop
    // Find the highest differing bit between a and b using XOR
    guint diff = a ^ b;
    // Count leading zeros to find the position of the highest set bit in diff
    // Then compare that bit in a and b to determine ordering
    int bit_pos = (sizeof(guint) * 8 - 1) - __builtin_clz(diff);
    return ((a >> bit_pos) & 1) > ((b >> bit_pos) & 1) ? 1 : -1;
  }
  guint sub_a = ((struct restore_job *)rj1)->data.drj->sub_part;
  guint sub_b = ((struct restore_job *)rj2)->data.drj->sub_part;
  return (sub_a > sub_b) ? 1 : (sub_a < sub_b) ? -1 : 0;
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

  struct database *_database=get_database(db_name,db_name);
  if (!eval_table(_database->source_database, table_name, _conf->table_list_mutex)){
    g_warning("Skipping table: `%s`.`%s`",_database->source_database, table_name);
    return FALSE;
  }

  struct db_table *dbt=NULL;
  if (append_new_db_table(&dbt, _database, NULL, table_name)){
    if (!has_been_defined_a_target_database()){
      gchar *schema_filename=common_build_schema_table_filename(directory, _database->target_database, table_name, "schema");
      if (g_file_test(schema_filename,G_FILE_TEST_EXISTS)){
        schema_filename=common_build_schema_table_filename(NULL, _database->database_name_in_filename, table_name, "schema");
        trace("Filename %s detected and send to process", schema_filename);
        process_table_filename(schema_filename);
      }
    }
  }
	if (!dbt->object_to_export.no_data){
    struct restore_job *rj = new_data_restore_job( g_strdup(filename), JOB_RESTORE_FILENAME, dbt, part, sub_part);
    table_lock(dbt);
    g_atomic_int_add(&(dbt->remaining_jobs), 1);
    dbt->count++; 
    dbt->restore_job_list=g_list_insert_sorted(dbt->restore_job_list,rj,&cmp_restore_job);
//  dbt->restore_job_list=g_list_append(dbt->restore_job_list,rj);
    table_unlock(dbt);
	}else{
    g_warning("Ignoring file %s on `%s`.`%s`",filename, dbt->database->source_database, dbt->table_filename);
	}
  return TRUE;
}

