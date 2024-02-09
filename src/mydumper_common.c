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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include "string.h"
#include <stdlib.h>
#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <pcre.h>
#include "regex.h"
#include <errno.h>
#include "server_detect.h"
#include "mydumper_global.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include "mydumper_common.h"
//#include <sys/wait.h>
#include "mydumper_start_dump.h"
#include "mydumper_stream.h"
#include <sys/wait.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <sys/file.h>

gboolean compact = FALSE;
GAsyncQueue *close_file_queue=NULL;
GMutex *ref_table_mutex = NULL;
GHashTable *ref_table=NULL;
guint table_number=0;
GAsyncQueue *available_pids=NULL;
GHashTable *fifo_hash=NULL;
//GHashTable *fifo_hash_by_pid=NULL;
GMutex *fifo_table_mutex=NULL;
GMutex *pipe_creation=NULL;
GThread * cft = NULL;
guint open_pipe=0;
guint server_version= 0;

const char *routine_type[]= {"FUNCTION", "PROCEDURE", "PACKAGE", "PACKAGE BODY"};
guint nroutines= 4;

int (*m_close)(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt) = NULL;

void final_step_close_file(guint thread_id, gchar *filename, struct fifo *f, float size, struct db_table * dbt);

void * close_file_thread(void *data){
  (void)data;
  struct fifo *f=NULL;
  for (;;){
    f=g_async_queue_pop(close_file_queue);
    if (f->gpid == -10)
      break;
    g_mutex_lock(pipe_creation);
    close(f->pipe[1]);
    close(f->pipe[0]);
    g_mutex_unlock(pipe_creation);
    g_mutex_lock(f->out_mutex);
    if (f->error_number==EAGAIN){
      usleep(1000);
    }
    if (fsync(f->fdout))
      g_error("while syncing file %s (%d)",f->filename, errno);
    close(f->fdout);

    release_pid();
    final_step_close_file(0, f->filename, f, f->size, f->dbt);
    g_atomic_int_dec_and_test(&open_pipe);
 }
  return NULL;
}


void initialize_common(){
  available_pids = g_async_queue_new(); 
  close_file_queue=g_async_queue_new();
  guint i=0;
  for (i=0; i < (num_threads * 2); i++){
    release_pid();
  }
  ref_table_mutex = g_mutex_new();
  pipe_creation = g_mutex_new();
  ref_table=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free );
  fifo_hash=g_hash_table_new(g_str_hash, g_str_equal);  
  fifo_table_mutex = g_mutex_new();

  cft=g_thread_create((GThreadFunc)close_file_thread, NULL, TRUE, NULL);
}

void close_file_queue_push(struct fifo *f){
  g_async_queue_push(close_file_queue, f);
  if (f->child_pid>0){
  int status;
  int pid;
  gboolean b=TRUE;
  do {
    do {
      g_mutex_lock(pipe_creation);
      pid=waitpid(f->child_pid, &status, WNOHANG);
      g_mutex_unlock(pipe_creation);
      if (pid > 0){
        b=FALSE;
        break;
      }else if (pid == -1 && errno == ECHILD){
        b=FALSE;
        break;
      }
    } while (pid == -1 && errno == EINTR); 
  }while (b);
//g_message("close_file_queue_push:: %s child pid %d ended with %d and error: %d | EINTR=%d ECHILD=%d EINVAL=%d | b=%d",f->filename?f->filename:"NOFILENAME", f->child_pid, pid , errno, EINTR, ECHILD, EINVAL, b);
    f->error_number=errno;
    g_mutex_unlock(f->out_mutex);
  }
}

void wait_close_files(){
  struct fifo f;
  f.gpid=-10;
  f.child_pid=-10;
  f.filename=NULL;

  /*
  GHashTableIter iter;
  g_mutex_lock(fifo_table_mutex);
  struct fifo *ff=NULL;
  gchar * lkey=NULL;
  if (fifo_hash){
    g_hash_table_iter_init ( &iter, fifo_hash );
    while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &ff ) ) {
      if (ff->queue){
        g_async_queue_push(ff->queue, GINT_TO_POINTER(1));      
      }
    }
  }

  g_mutex_unlock(fifo_table_mutex);
  */
  close_file_queue_push(&f);
  g_thread_join(cft);
}

void free_common(){
  g_mutex_free(ref_table_mutex);
  ref_table_mutex=NULL;
  g_hash_table_destroy(ref_table);
  ref_table=NULL;
}


void release_pid(){
  g_async_queue_push(available_pids, GINT_TO_POINTER(1));
}

int execute_file_per_thread( int p_in[2], int out){
  int childpid=fork();
  if(!childpid){
    dup2(p_in[0], STDIN_FILENO);
    close(p_in[1]);
    dup2(out, STDOUT_FILENO);
    close(out);
    int fd=3;
    for (fd=3; fd<256; fd++) (void) close(fd);
    execv(exec_per_thread_cmd[0],exec_per_thread_cmd);
  }
  return childpid;
}

// filename must never use the compression extension. .fifo files should be deprecated
int m_open_pipe(gchar **filename, const char *type){
  (void)type;
  g_atomic_int_inc(&open_pipe);

  gchar *new_filename = g_strdup_printf("%s%s", *filename, exec_per_thread_extension);
  (void)type;
  struct fifo *f=NULL;

  g_mutex_lock(fifo_table_mutex);
  f=g_hash_table_lookup(fifo_hash,*filename);
  g_mutex_unlock(fifo_table_mutex);
  if (f){
    g_error("file already open: %s", *filename);
  }
  f=g_new0(struct fifo, 1);
  f->out_mutex=g_mutex_new();
  g_mutex_lock(f->out_mutex);
  f->fdout = open(new_filename, O_CREAT|O_WRONLY|O_TRUNC, 0660);
  if (!f->fdout){
    g_error("opening file: %s", new_filename);
  }
  g_async_queue_pop(available_pids);
  f->queue = g_async_queue_new();
  f->filename=g_strdup(*filename);
  f->stdout_filename=new_filename;
  guint e=0;
  g_mutex_lock(pipe_creation);
  gint status=pipe(f->pipe);
  if (status != 0){
    g_error("Not able to create pipe (%d)", e);
  }
  
  f->child_pid=execute_file_per_thread(f->pipe, f->fdout);

  g_mutex_unlock(pipe_creation);
  g_mutex_lock(fifo_table_mutex);
  g_hash_table_insert(fifo_hash,f->filename,f);
  g_mutex_unlock(fifo_table_mutex);
  return f->pipe[1];
}

void final_step_close_file(guint thread_id, gchar *filename, struct fifo *f, float size, struct db_table * dbt) {
  if (size > 0){
    if (stream) stream_queue_push(dbt,g_strdup(f->stdout_filename));
  }else if (!build_empty_files){
    if (remove(f->stdout_filename)) {
      g_warning("Thread %d: Failed to remove empty file : %s", thread_id, f->stdout_filename);
    }else{
      g_debug("Thread %d: File removed: %s", thread_id, filename);
    }
  }
}

int m_close_pipe(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt){
  release_pid();
  (void)file;
  (void)thread_id;
  g_mutex_lock(fifo_table_mutex);
  struct fifo *f=g_hash_table_lookup(fifo_hash,filename);
  g_mutex_unlock(fifo_table_mutex);
  if (f){
    f->size=size;
    f->dbt=dbt;
    close_file_queue_push(f);
    return 0;
  }else{
    g_warning("pipe %s not closed", filename);
  }
  return 1;
}

int m_close_file(guint thread_id, int file, gchar *filename, guint64 size, struct db_table * dbt){
  int r=close(file);
  if (size > 0){
    if (stream) stream_queue_push(dbt, g_strdup(filename));
  }else if (!build_empty_files){
    if (remove(filename)) {
      g_warning("Thread %d: Failed to remove empty file : %s", thread_id, filename);
    }else{
      g_debug("Thread %d: File removed: %s", thread_id, filename);
    }
  }
  return r;
}

char * determine_filename (char * table){
  // https://stackoverflow.com/questions/11794144/regular-expression-for-valid-filename
  // We might need to define a better filename alternatives
  if (check_filename_regex(table) && !g_strstr_len(table,-1,".") && !g_str_has_prefix(table,"mydumper_") )
    return g_strdup(table);
  else{
    char *r = g_strdup_printf("mydumper_%d",table_number);
    table_number++;
    return r;
  }
}

gchar *get_ref_table(gchar *k){
  g_mutex_lock(ref_table_mutex);
  gchar *val=g_hash_table_lookup(ref_table,k);
  if (val == NULL){
    char * t=g_strdup(k);
    val=determine_filename(t);
    g_hash_table_insert(ref_table, t, val);
  }
  g_mutex_unlock(ref_table_mutex);
  return val;
}


char * escape_string(MYSQL *conn, char *str){
  char * r=g_new(char, strlen(str) * 2 + 1);
  mysql_real_escape_string(conn, r, str, strlen(str));
  return r;
}

gchar * build_schema_table_filename(char *database, char *table, const char *suffix){
  GString *filename = g_string_sized_new(20);
  g_string_append_printf(filename, "%s.%s-%s.sql", database, table, suffix);
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

gchar * build_schema_filename(const char *database, const char *suffix){
  GString *filename = g_string_sized_new(20);
  g_string_append_printf(filename, "%s-%s.sql", database, suffix);
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

gchar * build_tablespace_filename(){
  return g_build_filename(dump_directory, "all-schema-create-tablespace.sql", NULL);;
}

gchar * build_meta_filename(char *database, char *table, const char *suffix){
  GString *filename = g_string_sized_new(20);
  if (table != NULL)
    g_string_append_printf(filename, "%s.%s-%s", database, table, suffix);
  else
    g_string_append_printf(filename, "%s-%s", database, suffix);
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

void set_charset(GString *statement, char *character_set,
                 char *collation_connection) {
  g_string_printf(statement,
                  "SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\n");
  g_string_append(statement,
                  "SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\n");
  g_string_append(statement,
                  "SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\n");

  g_string_append_printf(statement, "SET character_set_client = %s;\n",
                         character_set);
  g_string_append_printf(statement, "SET character_set_results = %s;\n",
                         character_set);
  g_string_append_printf(statement, "SET collation_connection = %s;\n",
                         collation_connection);
}

void restore_charset(GString *statement) {
  g_string_append(statement,
                  "SET character_set_client = @PREV_CHARACTER_SET_CLIENT;\n");
  g_string_append(statement,
                  "SET character_set_results = @PREV_CHARACTER_SET_RESULTS;\n");
  g_string_append(statement,
                  "SET collation_connection = @PREV_COLLATION_CONNECTION;\n");
}

void clear_dump_directory(gchar *directory) {
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory,
               error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    gchar *path = g_build_filename(directory, filename, NULL);
    if (g_unlink(path) == -1) {
      g_critical("error removing file %s (%d)\n", path, errno);
      errors++;
      return;
    }
    g_free(path);
  }

  g_dir_close(dir);
}

gboolean is_empty_dir(gchar *directory)
{
  GError *error = NULL;
  GDir *dir = g_dir_open(directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", directory,
               error->message);
    errors++;
    return FALSE;
  }

  const gchar *filename= g_dir_read_name(dir);
  g_dir_close(dir);

  return filename ? FALSE : TRUE;
}

void set_transaction_isolation_level_repeatable_read(MYSQL *conn){
  if (mysql_query(conn,
                  "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")) {
    g_critical("Failed to set isolation level: %s", mysql_error(conn));
    exit(EXIT_FAILURE);
  }
}

// Global Var used:
// - dump_directory
gchar * build_filename(char *database, char *table, guint64 part, guint sub_part, const gchar *extension, const gchar *second_extension){
  GString *filename = g_string_sized_new(20);
  sub_part == 0 ?
    g_string_append_printf(filename, "%s.%s.%05lu.%s%s%s", database, table, part, extension, second_extension!=NULL ?".":"",second_extension!=NULL ?second_extension:"" ):
    g_string_append_printf(filename, "%s.%s.%05lu.%05u.%s%s%s", database, table, part, sub_part, extension, second_extension!=NULL ?".":"",second_extension!=NULL ?second_extension:"");
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

gchar * build_data_filename(char *database, char *table, guint64 part, guint sub_part){
  return build_filename(database,table,part,sub_part,"sql",NULL);
}

gchar * build_fifo_filename(char *database, char *table, guint64 part, guint sub_part, const gchar *extension){
  return build_filename(database,table,part,sub_part, extension,"fifo");
}

gchar * build_stdout_filename(char *database, char *table, guint64 part, guint sub_part, const gchar *extension, gchar *second_extension){
  return build_filename(database,table,part,sub_part, extension, second_extension);
}

gchar * build_load_data_filename(char *database, char *table, guint64 part, guint sub_part){
  return build_filename(database, table, part, sub_part, "dat", NULL);
}

unsigned long m_real_escape_string(MYSQL *conn, char *to, const gchar *from, unsigned long length){
  (void) conn;
  (void) to;
  (void) from;
  guint to_length = 2*length+1;
  const char *to_start = to;
  const char *end, *to_end = to_start + (to_length ? to_length - 1 : 2 * length);;
  int tmp_length = 0;
  for (end = from + length; from < end; from++) {
    char escape = 0;
/*    if (use_mb_flag && (tmp_length = my_ismbchar(charset_info, from, end))) {
      if (to + tmp_length > to_end) {
        overflow = true;
        break;
      }
      while (tmp_length--) *to++ = *from++;
      from--;
      continue;
    }
*/
    /*
 *      If the next character appears to begin a multi-byte character, we
 *      escape that first byte of that apparent multi-byte character. (The
 *      character just looks like a multi-byte character -- if it were actually
 *      a multi-byte character, it would have been passed through in the test
 *      above.)
 *      Without this check, we can create a problem by converting an invalid
 *      multi-byte character into a valid one. For example, 0xbf27 is not
 *      a valid GBK character, but 0xbf5c is. (0x27 = ', 0x5c = \)
 *      */

//    tmp_length = use_mb_flag ? my_mbcharlen_ptr(charset_info, from, end) : 0;

    if (tmp_length > 1)
      escape = *from;
    else
      switch (*from) {
        case 0: /* Must be escaped for 'mysql' */
          escape = '0';
          break;
        case '\n': /* Must be escaped for logs */
          escape = 'n';
          break;
        case '\r':
          escape = 'r';
          break;
        case '\\':
          escape = '\\';
          break;
        case '\'':
          escape = '\'';
          break;
        case '"': /* Better safe than sorry */
          escape = '"';
          break;
        case '\032': /* This gives problems on Win32 */
          escape = 'Z';
          break;
      }
    if (escape) {
      if (to + 2 > to_end) {
//        overflow = true;
        break;
      }
      *to++ = *fields_escaped_by;
      *to++ = escape;
    } else {
      if (to + 1 > to_end) {
//        overflow = true;
        break;
      }
      *to++ = *from;
    }
  }
  *to = 0;

  return //overflow ? (size_t)-1 : 
         (size_t)(to - to_start);
}

void m_escape_char_with_char(gchar neddle, gchar repl, gchar *to, unsigned long length){
  gchar *from=g_new(char, length);
  memcpy(from, to, length);
  gchar *ffrom=from;
  const char *end = from + length;
  for (end = from + length; from < end; from++) {
    if ( *from == neddle ){
      *to = repl;
      to++;
    }
    *to=*from;
    to++;
  }
  g_free(ffrom);
}

void m_replace_char_with_char(gchar neddle, gchar repl, gchar *from, unsigned long length){
  const char *end = from + length;
  for (end = from + length; from < end; from++) {
    if ( *from == neddle ){
      *from = repl;
      from++;
    }
  }
}

void determine_show_table_status_columns(MYSQL_RES *result, guint *ecol, guint *ccol, guint *collcol, guint *rowscol){
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  guint i = 0;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcasecmp(fields[i].name, "Engine"))
      *ecol = i;
    else if (!strcasecmp(fields[i].name, "Comment"))
      *ccol = i;
    else if (!strcasecmp(fields[i].name, "Collation"))
      *collcol = i;
    else if (!strcasecmp(fields[i].name, "Rows"))
      *rowscol = i;
  }
  g_assert(*ecol > 0);
  g_assert(*ccol > 0);
  g_assert(*collcol > 0);
}

void determine_explain_columns(MYSQL_RES *result, guint *rowscol){
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  guint i = 0;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcasecmp(fields[i].name, "rows"))
      *rowscol = i;
  }
}


void initialize_sql_statement(GString *statement){
  g_string_set_size(statement, 0);
  if (is_mysql_like()) {
    if (set_names_statement)
      g_string_printf(statement,"%s;\n",set_names_statement);
    g_string_append(statement, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n");
    if (sql_mode && !compact)
      g_string_append_printf(statement, "/*!40101 SET SQL_MODE=%s*/;\n", sql_mode);
    if (!skip_tz) {
      g_string_append(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
    }
  } else if (detected_server == SERVER_TYPE_TIDB) {
    if (!skip_tz) {
      g_string_printf(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
    }
  } else {
    g_string_printf(statement, "SET FOREIGN_KEY_CHECKS=0;\n");
    if (sql_mode && !compact)
      g_string_append_printf(statement, "SET SQL_MODE=%s;\n", sql_mode);
  }
}

void set_tidb_snapshot(MYSQL *conn){
  gchar *query =
  g_strdup_printf("SET SESSION tidb_snapshot = '%s'", tidb_snapshot);
  if (mysql_query(conn, query)) {
    m_critical("Failed to set tidb_snapshot: %s.\nThis might be related to https://github.com/pingcap/tidb/issues/8887", mysql_error(conn));
  }
  g_free(query);
}

guint64 my_pow_two_plus_prev(guint64 prev, guint max){
  guint64 r=1;
  guint i=0;
  for (i=1;i<max;i++){
    r*=2;
  }
  return r+prev;
}

