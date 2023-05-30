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
#include "regex.h"
#include <errno.h>
#include "server_detect.h"
#include "mydumper_global.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include "mydumper_common.h"
#include <sys/wait.h>

GMutex *ref_table_mutex = NULL;
GHashTable *ref_table=NULL;
guint table_number=0;
GAsyncQueue *available_pids=NULL;
GHashTable *fifo_hash=NULL;
GMutex *fifo_table_mutex=NULL;

void initialize_common(){
  available_pids = g_async_queue_new(); 
  guint i=0;
  for (i=0; i < (num_threads * 2); i++){
    release_pid();
  }
  ref_table_mutex = g_mutex_new();
  ref_table=g_hash_table_new_full ( g_str_hash, g_str_equal, &g_free, &g_free );
  fifo_hash=g_hash_table_new(g_direct_hash,g_direct_equal);
  fifo_table_mutex = g_mutex_new();
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

int execute_file_per_thread( const gchar *sql_fn, const gchar *sql_fn3){
  g_async_queue_pop(available_pids);
  int childpid=fork();
  if(!childpid){
    FILE *sql_file2 = g_fopen(sql_fn,"r");
    FILE *sql_file3 = g_fopen(sql_fn3,"w");
    dup2(fileno(sql_file2), STDIN_FILENO);
    dup2(fileno(sql_file3), STDOUT_FILENO);
    close(fileno(sql_file2));
    close(fileno(sql_file3));
    execv(exec_per_thread_cmd[0],exec_per_thread_cmd);
  }
  return childpid;
}

// filename must never use the compression extension. .fifo files should be deprecated
FILE * m_open_pipe(const char *filename, const char *type){
  mkfifo(filename,0666);
  gchar *new_filename = g_strdup_printf("%s%s", filename, exec_per_thread_extension);
  int child_proc = execute_file_per_thread(filename,new_filename);
  FILE *file=g_fopen(filename,type);
  g_mutex_lock(fifo_table_mutex); 
  struct fifo *f=g_hash_table_lookup(fifo_hash,file);

  if (f!=NULL){
    g_mutex_unlock(fifo_table_mutex);
    g_mutex_lock(f->mutex);
    f->pid = child_proc;
    f->filename=g_strdup(filename);
  }else{
    f=g_new0(struct fifo, 1);
    f->mutex=g_mutex_new();
    g_mutex_lock(f->mutex);
    f->pid = child_proc;
    f->filename=g_strdup(filename);
    g_hash_table_insert(fifo_hash,file,f);
    g_mutex_unlock(fifo_table_mutex);
  }
  return file;
}

int m_close_pipe(void *file){
  release_pid();
  g_mutex_lock(fifo_table_mutex);
  struct fifo *f=g_hash_table_lookup(fifo_hash,file);
  g_mutex_unlock(fifo_table_mutex);
  int r=close(fileno(file));
  if (f != NULL){
    int status=0;
    waitpid(f->pid, &status, 0);
    g_mutex_lock(fifo_table_mutex);
    g_mutex_unlock(f->mutex);
    g_mutex_unlock(fifo_table_mutex);
    remove(f->filename);
  }else{
    g_mutex_unlock(fifo_table_mutex);
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

void determine_ecol_ccol(MYSQL_RES *result, guint *ecol, guint *ccol, guint *collcol){
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  guint i = 0;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcasecmp(fields[i].name, "Engine"))
      *ecol = i;
    else if (!strcasecmp(fields[i].name, "Comment"))
      *ccol = i;
    else if (!strcasecmp(fields[i].name, "Collation"))
      *collcol = i;
  }
}

void initialize_sql_statement(GString *statement){
  if ((detected_server == SERVER_TYPE_MYSQL) || (detected_server == SERVER_TYPE_MARIADB))  {
    if (set_names_statement)
      g_string_printf(statement,"%s;\n",set_names_statement);
    g_string_append(statement, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n");
    if (!skip_tz) {
      g_string_append(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
    }
  } else if (detected_server == SERVER_TYPE_TIDB) {
    if (!skip_tz) {
      g_string_printf(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
    }
  } else {
    g_string_printf(statement, "SET FOREIGN_KEY_CHECKS=0;\n");
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

