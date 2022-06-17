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
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/
#include "string.h"
#include <mysql.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include "regex.h"
#include <errno.h>

extern gchar *compress_extension;
extern gchar *dump_directory;
extern guint errors;

GMutex *ref_table_mutex = NULL;
GHashTable *ref_table=NULL;
guint table_number=0;

void initialize_common(){
  ref_table_mutex = g_mutex_new();
  ref_table=g_hash_table_new ( g_str_hash, g_str_equal );
}

char * determine_filename (char * table){
  // https://stackoverflow.com/questions/11794144/regular-expression-for-valid-filename
  // We might need to define a better filename alternatives
  if (check_filename_regex(table) && !g_strstr_len(table,-1,".") && !g_str_has_prefix(table,"mydumper_") )
    return table;
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
  g_string_append_printf(filename, "%s.%s-%s.sql%s", database, table, suffix, compress_extension);
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

gchar * build_schema_filename(const char *database, const char *suffix){
  GString *filename = g_string_sized_new(20);
  g_string_append_printf(filename, "%s-%s.sql%s", database, suffix, compress_extension);
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
// - compress_extension
gchar * build_filename(char *database, char *table, guint part, guint sub_part, const gchar *extension){
  GString *filename = g_string_sized_new(20);
  sub_part == 0 ?
    g_string_append_printf(filename, "%s.%s.%05d.%s%s", database, table, part, extension, compress_extension):
    g_string_append_printf(filename, "%s.%s.%05d.%05d.%s%s", database, table, part, sub_part, extension, compress_extension);
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

gchar * build_data_filename(char *database, char *table, guint part, guint sub_part){
  return build_filename(database,table,part,sub_part,"sql");
}



void determine_ecol_ccol(MYSQL_RES *result, guint *ecol, guint *ccol){
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  guint i = 0;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcasecmp(fields[i].name, "Engine"))
      *ecol = i;
    else if (!strcasecmp(fields[i].name, "Comment"))
      *ccol = i;
  }
}
