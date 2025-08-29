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
    Authors:        David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <mysql.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <glib.h>
#include <glib/gstdio.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <stdarg.h>
#include "regex.h"
#include "server_detect.h"
#include "common.h"
#include "config.h"
#include "connection.h"
#include "common_options.h"
//#include "mydumper_global.h"

extern gboolean help;
guint errors=0;
GList *ignore_errors_list=NULL;
GAsyncQueue *stream_queue = NULL;
gboolean use_defer= FALSE;
gboolean check_row_count= FALSE;
extern gchar **optimize_key_engines;
guint throttle_time=0;
guint throttle_max_usleep_limit=60000000;

gchar *set_names_in_conn_for_sct=NULL, *set_names_in_file_for_sct=NULL, *set_names_in_file_by_default=NULL;

gchar *get_zstd_cmd(){
  return g_find_program_in_path("zstd");
}

gchar *get_gzip_cmd(){
  return g_find_program_in_path("gzip");
}

GHashTable * initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  if (is_mysql_like()){
    set_session_hash_insert(set_session_hash, "WAIT_TIMEOUT", g_strdup("2147483"));
    set_session_hash_insert(set_session_hash, "NET_WRITE_TIMEOUT", g_strdup("2147483"));
  }
  return set_session_hash;
}

void initialize_set_names(){
  if (!set_names_in_conn_by_default)
    set_names_in_conn_by_default=g_strdup(BINARY_CHARSET);

  if (!set_names_in_conn_for_sct)
    set_names_in_conn_for_sct=g_strdup(AUTO_CHARSET);

  if (!set_names_in_file_by_default)
    set_names_in_file_by_default=g_strdup(BINARY_CHARSET);

  if (!set_names_in_file_for_sct)
    set_names_in_file_for_sct=set_names_in_file_by_default;
}

gchar *set_names_statement_template(gchar *_set_names){
  return g_strdup_printf("/*!40101 SET NAMES %s*/", _set_names);
}

void free_set_names(){
  g_free(set_names_in_conn_by_default);
  set_names_in_conn_by_default=NULL;
  g_free(set_names_statement);
}

char *generic_checksum(MYSQL *conn, char *database, char *table, const gchar *query_template, int column_number){
  char *query = g_strdup_printf(query_template, database, table);
  struct M_ROW *mr = m_store_result_single_row( conn, query, "Error dumping checksum (%s.%s)", database, table);
  g_free(query);
  char * r=NULL;
  if (mr->row)
    r=g_strdup_printf("%s",mr->row[column_number]);
  m_store_result_row_free(mr);
  return r;
}

char * checksum_table(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "CHECKSUM TABLE `%s`.`%s`", 1);
}


char * checksum_table_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 0);
}

char * checksum_process_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 0);
}

char * checksum_trigger_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';",0);
}

char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';",0);
}

char * checksum_view_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';",0);
}

char * checksum_database_defaults(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;",0);
}

char * checksum_table_indexes(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME", 0);
}

GKeyFile * load_config_file(gchar * config_file){
  GError *error = NULL;
  GKeyFile *kf = g_key_file_new ();
  gchar* contents=NULL;
  gsize length = 0;
  // Loads the config_file
  if (!g_file_get_contents (config_file, &contents, &length, &error)){
    g_error("Error while loading config file %s: %s", config_file, error->message);
  }
  gchar* current_contents=contents;
  GString * new_content=g_string_sized_new(length);
  gboolean equal_found=FALSE, new_line=TRUE;
  while ((unsigned int)(current_contents-contents) < length){
    if (current_contents[0] == '['){
      while (((unsigned int)(current_contents-contents) < length) && current_contents[0] != '\n'){
        g_string_append_c(new_content,current_contents[0]);
        current_contents++;
      }
    }else{
      if (current_contents[0] == '\n'){
        if (!equal_found && !new_line){
          g_string_append(new_content,"= 1");
        }
        new_line=TRUE;
        equal_found=FALSE;
      }else{ 
        if (current_contents[0] == '='){
          equal_found=TRUE;
        }
        new_line=FALSE;
      }
    }
    g_string_append_c(new_content,current_contents[0]);
    current_contents++;
  }

  if (!g_key_file_load_from_data (kf, new_content->str, new_content->len,
                                  G_KEY_FILE_KEEP_COMMENTS, &error)) {
    g_warning ("Failed to load config file %s: %s", config_file, error->message);
    return NULL;
  }
  return kf;
}

void parse_key_file_group(GKeyFile *kf, GOptionContext *context, const gchar * group){
  gsize len=0;
  GError *error = NULL;
  gchar ** keys=g_key_file_get_keys(kf,group, &len, &error);
  gsize i=0;
  GSList *list = NULL;
  if (error != NULL){
    g_error("Loading configuration on section %s: %s",group,error->message);
  }else{
    // Transform the key-value pair to parameters option that the parsing will understand
    for (i=0; i < len; i++){
      if (g_strcmp0("host",keys[i]) && g_strcmp0("user",keys[i]) && g_strcmp0("password",keys[i])){
        list = g_slist_append(list, g_strdup_printf("--%s",keys[i]));
        gchar *value=g_key_file_get_value(kf,group,keys[i],&error);
        if ( value != NULL ) list=g_slist_append(list, value);
      }
    }
    gint slen = g_slist_length(list) + 1;
    gchar ** gclist = g_new0(gchar *, slen);
    GSList *ilist=list;
    gint j=0;
    for (j=1; j < slen ; j++){
      gclist[j]=ilist->data;
      ilist=ilist->next;
    }
    g_slist_free(list);
    // Second parse over the options
    if (!g_option_context_parse(context, &slen, &gclist, &error)) {
      m_critical("option parsing failed: %s, try --help\n", error->message);
    }else{
      g_message("Config file loaded");
    }
    g_strfreev(gclist);
  }
  g_strfreev(keys);
}

void load_hash_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar * group_variables){
  guint i=0;
  GError *error = NULL;
  gchar *value=NULL;
  gsize len=0;
  gchar **keys=g_key_file_get_keys(kf,group_variables, &len, &error);
  for (i=0; i < len; i++){
    value=g_key_file_get_value(kf,group_variables,keys[i],&error);
    if (!error){
      set_session_hash_insert(set_session_hash, keys[i], g_strdup(value));
    }
  }
  g_strfreev(keys);
}

//struct function_pointer * init_function_pointer(gchar *value);
void load_per_table_info_from_key_file(GKeyFile *kf, struct configuration_per_table * cpt, struct function_pointer * init_function_pointer(gchar*))
{
  gsize len=0,len2=0;
  gchar **groups=g_key_file_get_groups(kf,&len);
  GHashTable *ht=NULL;
  GError *error = NULL;
  guint i=0,j=0;
  gchar *value=NULL;
  gchar **keys=NULL;
  for (i=0; i < len; i++){
    if (g_strstr_len(groups[i],strlen(groups[i]),"`.`") && g_str_has_prefix(groups[i],"`") && g_str_has_suffix(groups[i],"`")){
      ht=g_hash_table_new ( g_str_hash, g_str_equal );
      keys=g_key_file_get_keys(kf,groups[i], &len2, &error);
      for (j=0; j < len2; j++){
        if (keys[j][0]== '`' && keys[j][strlen(keys[j])-1]=='`'){
          if (init_function_pointer){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            struct function_pointer *fp = init_function_pointer(value);
            g_hash_table_insert(ht,g_strndup(keys[j]+1,strlen(keys[j])-2), fp);
					}
        }else{
          if (g_strcmp0(keys[j],"where") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_where_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"limit") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_limit_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"num_threads") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_num_threads_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"columns_on_select") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_columns_on_select_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"columns_on_insert") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_columns_on_insert_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"object_to_export") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_object_to_export, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"partition_regex") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            pcre2_code *r=NULL; 
            init_regex( &r, value);
            g_hash_table_insert(cpt->all_partition_regex_per_table, g_strdup(groups[i]), r);
          }
          if (g_strcmp0(keys[j],"rows") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(cpt->all_rows_per_table, g_strdup(groups[i]), g_strdup(value));
          }

        }
      }
      g_hash_table_insert(cpt->all_anonymized_function,g_strdup(groups[i]),ht);
    }
  }
  g_strfreev(groups);
}

void load_hash_of_all_variables_perproduct_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar *str){
  GString *s=g_string_new(str);
  load_hash_from_key_file(kf,set_session_hash,s->str);
  g_string_append_c(s,'_');
  g_string_append(s, g_utf8_strdown(get_product_name(), -1));
  load_hash_from_key_file(kf,set_session_hash,s->str);
  g_string_append_printf(s,"_%d",get_major());
  load_hash_from_key_file(kf,set_session_hash,s->str);
  g_string_append_printf(s,"_%d",get_secondary());
  load_hash_from_key_file(kf,set_session_hash,s->str);
  g_string_append_printf(s,"_%d",get_revision());
  load_hash_from_key_file(kf,set_session_hash,s->str);
}


void free_hash_table(GHashTable * hash){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, hash );
  gchar *e=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &e ) ) {
    g_free(lkey);
    g_free(e);
  }
}

void refresh_set_from_hash(GString *ss, const gchar * kind, GHashTable * set_hash){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, set_hash );
  gchar *e=NULL;
  gchar *c=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &e ) ) {
    c=g_strstr_len(e,strlen(e),"/*!");
    if (c!=NULL){
      c[0]='\0';
      c++;
      g_string_append_printf(ss,"/%s SET %s %s = %s */;\n", c, kind, lkey, e);
    }else
      g_string_append_printf(ss,"SET %s %s = %s ;\n", kind, lkey, e);
  }
}

void set_session_hash_insert(GHashTable * set_session_hash, const gchar *_key, gchar *value){
  g_hash_table_insert(set_session_hash, g_utf8_strup(_key, strlen(_key)), value);
}

void refresh_set_session_from_hash(GString *ss, GHashTable * set_session_hash){
  g_string_set_size(ss,0);

  if (!g_hash_table_contains(set_session_hash, "FOREIGN_KEY_CHECKS")) {
    set_session_hash_insert(set_session_hash, "FOREIGN_KEY_CHECKS", g_strdup("0"));
  }

  refresh_set_from_hash(ss, "SESSION", set_session_hash);
}

void set_global_rollback_from_hash(GString *ss, GString * sr, GHashTable * set_hash){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, set_hash );
  gchar *e=NULL;
//  gchar *c=NULL;
  if (g_hash_table_size(set_hash) > 0){
    GString *stmp=g_string_new(" INTO");
    g_string_append(ss, "SELECT ");
    g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &e );
    g_string_append_printf(stmp," @%s", lkey);
    g_string_append_printf(sr,"SET GLOBAL %s = @%s ;\n", lkey, lkey);
    g_string_append_printf(ss," @@%s", lkey);
    while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &e ) ) {
      g_string_append_printf(stmp,", @%s", lkey);
      g_string_append_printf(sr,"SET GLOBAL %s = @%s ;\n", lkey, lkey);
      g_string_append_printf(ss,", @@%s", lkey);
    }
    g_string_append_printf(ss,"%s ;\n",stmp->str);
  }
}

void refresh_set_global_from_hash(GString *ss, GString *sr, GHashTable * set_global_hash){
  set_global_rollback_from_hash(ss, sr, set_global_hash);
  refresh_set_from_hash(ss, "GLOBAL", set_global_hash);
}

void free_hash(GHashTable * set_session_hash){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, set_session_hash );
  gchar *e=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &e ) ) {
    g_free(e);
    g_free(lkey);
  }
}

void execute_gstring(MYSQL *conn, GString *ss)
{
  if (ss != NULL ){
    gchar** line=g_strsplit(ss->str, ";\n", -1);
    int i=0;
    for (i=0; i < (int)g_strv_length(line);i++){
       if (strlen(line[i]) > 3 && mysql_query(conn, line[i])){
         g_warning("Set session failed: %s",line[i]);
       }
    }
    g_strfreev(line);
  }
}

int write_file(FILE * file, char * buff, int len){
  return write(fileno(file), buff, len); 
}

gchar *replace_escaped_strings(gchar *c){
  guint i=0,j=0;

  while (c[i]!='\0'){
    if (c[i]=='\\') {
      switch (c[i+1]){
        case 'n':
          c[j]='\n';
          i=i+2;
          break;
        case 't':
          c[j]='\t';
          i=i+2;
          break;
        case 'r':
          c[j]='\r';
          i=i+2;
          break;
        case 'f':
          c[j]='\f';
          i=i+2;
          break;
        default:
          c[j]=c[i];
          i++;
      }
    }else{
      c[j]=c[i];
      i++;
    }
    j++;
  }
  c[j]=c[i];
  return c;
}

void escape_tab_with(gchar *to){
  gchar *from=g_strdup(to);
  guint i=0,j=0;
  while (from[i]!='\0'){
    if (from[i]=='\t'){
      to[j]='\\';
      j++;
      to[j]='t';
    }else
      to[j]=from[i];
    i++;
    j++;
  }
  to[j]=from[i];
  g_free(from);
//  return to;
}


gboolean create_dir(char *directory){
  if (!help){
    if (g_mkdir(directory, 0750) == -1) {
      if (errno != EEXIST) {
        m_critical("Unable to create `%s': %s", directory, g_strerror(errno));
      }
      return FALSE;
    }
  }
  return TRUE;
}

gchar *build_tmp_dir_name(){
  GError*error=NULL;
  return g_dir_make_tmp (NULL, &error);
}

guint strcount(gchar *text){
  gchar *t=text;
  guint i=0;
  while (t){
    t=g_strstr_len(t+1,strlen(t),"\n");
    i++;
  }
  return i;
}


gchar * remove_new_line(gchar *to){
  if (to==NULL)
    return to;
  gchar *from=to;
  guint i=0,j=0;
  while (from[j]!='\0'){
    if (from[j]!='\n'){
      to[i]=from[j];
      i++;
    }
    j++;
  }
  to[i]=from[j];
  return to;
}

void m_remove0(gchar * directory, const gchar * filename){
    gchar *path = g_build_filename(directory == NULL?"":directory, filename, NULL);
    g_message("Removing file: %s", path);
    if (remove(path) < 0)
      g_warning("Remove failed: %s (%s)", path, strerror(errno));
    g_free(path);
}

gboolean m_remove(gchar * directory, const gchar * filename){
  if (stream && no_delete == FALSE){
    m_remove0(directory,filename);
  }
  return TRUE;
}

gboolean matchText(const gchar *a, const gchar *b) {
  guint ai = 0;
  guint bi = 0;
  while ( a[ai] != '%' && a[ai] != '\0' && b[bi] != '\0') {
    if ((a[ai] == '_') || (a[ai] == b[bi])){
      ai++;
      bi++;
    } else if (a[ai] == '\\' && a[ai+1] == '_' && b[bi] == '_') {
      ai+=2;
      bi++;
    } else {
      return FALSE;
    }
  }
  if (a[ai] == '\0' ) {
    return b[bi] == '\0';
  }
  for (; a[ai] == '%' || a[ai] == '\0';ai++) {
    if (a[ai] == '\0') {
      return TRUE;
    }
  }
  for (; b[bi] != '\0'; bi++) {
    if (matchText(a + ai, b + bi)) {
      return TRUE;
    }
  }
  return FALSE;
}

gboolean is_table_in_list(gchar *database, gchar *table, gchar **tl){
  gchar * table_name_lower=g_ascii_strdown(g_strdup_printf("%s.%s", database, table),-1);
  gchar* tb_lower = NULL;
  gboolean match = FALSE;
  for (guint i = 0; tl[i] != NULL; i++) {
    // no need use match text
    if (g_strrstr(tl[i], "%") == NULL && g_strrstr(tl[i], "_") == NULL) {
       if (g_ascii_strcasecmp(tl[i], table_name_lower) == 0) {
         match = TRUE;
         break;
       }
    } else {
      tb_lower = g_ascii_strdown(tl[i], -1);
      if (matchText(tb_lower, table_name_lower)) {
        match = TRUE;
        break;
      }
    } 
  }
  g_free(table_name_lower);
  g_free(tb_lower);
  return match;
}

gboolean is_mysql_special_tables(gchar *database, gchar *table){
  return g_ascii_strcasecmp(database, "mysql") == 0 &&
        (g_ascii_strcasecmp(table, "general_log") == 0 ||
         g_ascii_strcasecmp(table, "slow_log") == 0 ||
         g_ascii_strcasecmp(table, "innodb_index_stats") == 0 ||
         g_ascii_strcasecmp(table, "innodb_table_stats") == 0);
}

void m_key_file_merge(GKeyFile *b, GKeyFile *a){
  gsize  group_len = 0, key_len=0;
  gchar **group = g_key_file_get_groups (a, &group_len), **_key=NULL;
  
  guint g=0, k=0;
  GError *error=NULL;
  for( g=0; g<group_len; g++ ){
    _key=g_key_file_get_keys(a, group[g], &key_len, &error );
    for(k=0; k<key_len; k++ ){
      g_key_file_set_value(b, group[g], _key[k], g_key_file_get_value(a, group[g], _key[k], &error));
    }
  }

}


void initialize_common_options(GOptionContext *context, const gchar *group){
  if (!optimize_key_engines)
    optimize_key_engines=g_strsplit("InnoDB,ROCKSDB", ",", 0);

  if (defaults_file == NULL ){ 
    if ( g_file_test(DEFAULTS_FILE, G_FILE_TEST_EXISTS) ){
      defaults_file=g_strdup(DEFAULTS_FILE);
    }
  }else{
    if (!g_file_test(defaults_file,G_FILE_TEST_EXISTS)){
      m_critical("Default file %s not found", defaults_file);
    }
  }

  if (defaults_extra_file != NULL ){
    if (!g_file_test(defaults_extra_file,G_FILE_TEST_EXISTS)){
      m_critical("Default extra file %s not found", defaults_extra_file);
    }
  }else{
    if (defaults_file == NULL){
      g_message("Using no configuration file");
      return;
    }
  }

  if (defaults_file == NULL){
    defaults_file=defaults_extra_file;
    defaults_extra_file=NULL;
  }

//  g_message("Using default file: %s %s", defaults_file, defaults_extra_file);

  gchar *new_defaults_file=NULL;
  if (!g_path_is_absolute(defaults_file)){
    new_defaults_file=g_build_filename(g_get_current_dir(),defaults_file,NULL);
    g_free(defaults_file);
    defaults_file=new_defaults_file;
  }

  key_file=load_config_file(defaults_file);

  if (key_file!=NULL){
    if (g_key_file_has_group(key_file, group )){
      parse_key_file_group(key_file, context, group);
      set_connection_defaults_file_and_group(defaults_file, group); 
    }
    if (g_key_file_has_group(key_file, "client" )){
      set_connection_defaults_file_and_group(defaults_file, NULL);
    }
  }else
    set_connection_defaults_file_and_group(defaults_file, NULL);

  if (defaults_extra_file == NULL)
    return;

  if (!g_path_is_absolute(defaults_extra_file)){
    new_defaults_file=g_build_filename(g_get_current_dir(),defaults_extra_file,NULL);
    g_free(defaults_extra_file);
    defaults_extra_file=new_defaults_file;
  }

  GKeyFile * extra_key_file=load_config_file(defaults_extra_file);

  if (extra_key_file!=NULL){ 
    if (g_key_file_has_group(extra_key_file, group )){
      g_message("Parsing extra key file");
      parse_key_file_group(extra_key_file, context, group);
      set_connection_defaults_file_and_group(defaults_extra_file, group);
    } 
    if (g_key_file_has_group(extra_key_file, "client" )){
      set_connection_defaults_file_and_group(defaults_extra_file, NULL);
    }
  }else
    set_connection_defaults_file_and_group(defaults_extra_file, NULL);

  g_message("Merging config files user: ");

  m_key_file_merge(key_file, extra_key_file);

//  g_key_file_free(extra_key_file);
}

gchar **get_table_list(gchar *_tables_list){
  gchar ** tl = g_strsplit(_tables_list, ",", 0);
  guint i=0;
  for(i=0; i < g_strv_length(tl); i++){
    if (g_strstr_len(tl[i],strlen(tl[i]),".") == NULL )
      m_error("Table name %s is not in DATABASE.TABLE format", tl[i]);
  }
  return tl;
}

void remove_definer_from_gchar(char * str){
  char * from = g_strstr_len(str,50," DEFINER=");
  if (from){
    from++;
    char * to=g_strstr_len(from,110," ");
    if (to){
      while(from != to){
        from[0]=' ';
        from++;
      }
    }
  }
}

void remove_definer(GString * data){
  remove_definer_from_gchar(data->str);
}

void print_version(const gchar *program){
    GString *str=g_string_new(program);
    g_string_append_printf(str, " v%s, built against %s %s", VERSION, DB_LIBRARY, MYSQL_VERSION_STR);
#ifdef WITH_SSL
    g_string_append(str," with SSL support");
#endif
    g_print("%s\n", str->str);
}

gboolean stream_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (g_strstr_len(option_name,8,"--stream")){
    stream = TRUE;
    use_defer= FALSE;

    if (value==NULL || !g_ascii_strcasecmp(value,"TRADITIONAL") || !g_ascii_strcasecmp(value,"0")){
      return TRUE;
    }

    guint64 val= strtol(value, NULL, 10);
    if (errno == ERANGE || val > 7 ){
      g_error("Value out of range on --stream");
      return FALSE;
    }

    if (!g_ascii_strcasecmp(value,"NO_DELETE")){
      no_delete=TRUE;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,"NO_STREAM_AND_NO_DELETE")){
      no_delete=TRUE;
      no_stream=TRUE;
      return TRUE;
    }
    if (!g_ascii_strcasecmp(value,"NO_STREAM")){
      no_stream=TRUE;
      return TRUE;
    }

    if (!val){
      return FALSE;
    }

    if ((val) & (1<<(2))) no_stream=TRUE;
    if ((val) & (1<<(1))) no_delete=TRUE;
    if ((val) & (1<<(0))) no_sync=TRUE;
    return TRUE;

  }
  return FALSE;
}

void check_num_threads()
{
  if (!num_threads) {
    num_threads= g_get_num_processors();
    g_assert(num_threads > 0);
  }

  if (num_threads < MIN_THREAD_COUNT) {
    g_warning("Invalid number of threads %d, setting to %d", num_threads, MIN_THREAD_COUNT);
    num_threads = MIN_THREAD_COUNT;
  }
}

void m_message(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  g_message("%s",c);
  g_free(c);
}

void m_warning(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  g_warning("%s",c);
  g_free(c);
}

void m_critical(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  execute_gstring(main_connection, set_global_back);
  g_critical("%s",c);
  exit(EXIT_FAILURE);
}

void m_error(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  execute_gstring(main_connection, set_global_back);
  g_error("%s", c); // g_error exits program
}


/* Function to work around a bug in MariaDB which outputs the explicit
 * scehma for a sequence in a SHOW CREATE TABLE even if it is local to the
 * current table
 */
gchar *filter_sequence_schemas(const gchar *create_table)
{
  pcre2_code *re = NULL;
  int error;
  PCRE2_SIZE erroroffset;
//  int ovector[12] = {0};
  gchar *out = g_strdup(create_table);

//  re = pcre_compile("(?:nextval|lastval)\\((`.*`\\.(`.*`))\\)",
//                    PCRE_CASELESS | PCRE_MULTILINE, &error, &erroroffset,
//                    NULL);

  re = pcre2_compile((PCRE2_SPTR)"(?:nextval|lastval)\\((`.*`\\.(`.*`))\\)", PCRE2_ZERO_TERMINATED, PCRE2_CASELESS | PCRE2_MULTILINE, &error,
                      &erroroffset, NULL);


  if (!re) {
    g_critical("Regular expression fail: %d", error);
    // We can safely continue here
  } else {
    int offset = 0;

  pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, NULL);

    while ((
          pcre2_match(re, (PCRE2_SPTR)out, strlen(out), offset, 0, match_data, NULL)
          //pcre_exec(re, NULL, out, strlen(out), offset, 0, ovector, 12)
          
          ) > 0) {
//      gchar* tmp = g_new(gchar, strlen(out));
//      size_t tmp_pos = 0;
      /* Positions generated:
       * ovector 0 - 1: nextval(`test`.`s`)
       * ovector 2 - 3: `test`.`s`
       * ovector 4 - 5: `s`
       */
/*      size_t write_len = ovector[2];

      memcpy(tmp, out, write_len);
      tmp_pos += write_len;
      write_len = ovector[5] - ovector[4];
      memcpy(tmp + tmp_pos, out + ovector[4], write_len);
      tmp_pos += write_len;
      write_len = strlen(out) - ovector[1] + 1;
      memcpy(tmp + tmp_pos, out + ovector[1] - 1, write_len);
      tmp[tmp_pos + write_len] = '\0';
      g_free(out);
      out = tmp;
      */
    }
  }
  return out;
}

GRecMutex * g_rec_mutex_new(){
  GRecMutex *r=g_new0(GRecMutex,1);
  g_rec_mutex_init(r);
  return r;

}

/*
  Read one line of data terminated by \n or maybe not terminated in case of EOF.

  FIXME: passing both *eof and *line here makes no sense. The caller may increment
  line by this condition:

  if (read_data(file, data, &eof) && !eof)
    ++line;
*/
gboolean read_data(FILE *file, GString *data,
                   gboolean *eof, guint *line) {
  char buffer[4096];
  size_t l;

  while (fgets(buffer, sizeof(buffer), file)) {
    l= strlen(buffer);
    //g_assert(l > 0 && l < sizeof(buffer));
    g_string_append(data, buffer);
    if (buffer[l - 1] == '\n') {
      (*line)++;
      *eof= FALSE;
      return TRUE;
    }
  }

  if (feof(file)) {
    *eof= TRUE;
    return TRUE;
  }

  return FALSE;
}

gchar *m_date_time_new_now_local(){
  GString *datetimestr=g_string_sized_new(26);
  GDateTime *datetime = g_date_time_new_now_local();
  g_string_append(datetimestr,g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S"));
  g_string_append_printf(datetimestr,".%d", g_date_time_get_microsecond(datetime));
  g_date_time_unref(datetime);
  return g_string_free(datetimestr,FALSE);
}

#if !GLIB_CHECK_VERSION(2, 68, 0)
guint
g_string_replace (GString     *_string,
                  const gchar *_find,
                  const gchar *_replace,
                  guint        _limit)
{
  gsize f_len, r_len, pos;
  gchar *cur, *next;
  guint n = 0;

  g_return_val_if_fail (_string != NULL, 0);
  g_return_val_if_fail (_find != NULL, 0);
  g_return_val_if_fail (_replace != NULL, 0);

  f_len = strlen (_find);
  r_len = strlen (_replace);
  cur = _string->str;

  while ((next = strstr (cur, _find)) != NULL)
    {
      pos = next - _string->str;
      g_string_erase (_string, pos, f_len);
      g_string_insert (_string, pos, _replace);
      cur = _string->str + pos + r_len;
      n++;
      /* Only match the empty string once at any given position, to
       * avoid infinite loops */
      if (f_len == 0)
        {
          if (cur[0] == '\0')
            break;
          else
            cur++;
        }
      if (n == _limit)
        break;
    }

  return n;
}
#endif

#if !GLIB_CHECK_VERSION(2, 36, 0)
/**
 * g_get_num_processors:
 *
 * Determine the approximate number of threads that the system will
 * schedule simultaneously for this process.  This is intended to be
 * used as a parameter to g_thread_pool_new() for CPU bound tasks and
 * similar cases.
 *
 * Returns: Number of schedulable threads, always greater than 0
 *
 * Since: 2.36
 */
guint
g_get_num_processors (void)
{
#ifdef G_OS_WIN32
  unsigned int count;
  SYSTEM_INFO sysinfo;
  DWORD_PTR process_cpus;
  DWORD_PTR system_cpus;

  /* This *never* fails, use it as fallback */
  GetNativeSystemInfo (&sysinfo);
  count = (int) sysinfo.dwNumberOfProcessors;

  if (GetProcessAffinityMask (GetCurrentProcess (),
                              &process_cpus, &system_cpus))
    {
      unsigned int af_count;

      for (af_count = 0; process_cpus != 0; process_cpus >>= 1)
        if (process_cpus & 1)
          af_count++;

      /* Prefer affinity-based result, if available */
      if (af_count > 0)
        count = af_count;
    }

  if (count > 0)
    return count;
#elif defined(_SC_NPROCESSORS_ONLN) && defined(THREADS_POSIX) && defined(HAVE_PTHREAD_GETAFFINITY_NP)
  {
    int idx;
    int ncores = MIN (sysconf (_SC_NPROCESSORS_ONLN), CPU_SETSIZE);
    cpu_set_t cpu_mask;
    CPU_ZERO (&cpu_mask);

    int af_count = 0;
    int err = pthread_getaffinity_np (pthread_self (), sizeof (cpu_mask), &cpu_mask);
    if (!err)
      for (idx = 0; idx < ncores && idx < CPU_SETSIZE; ++idx)
        af_count += CPU_ISSET (idx, &cpu_mask);

    int count = (af_count > 0) ? af_count : ncores;
    return count;
  }
#elif defined(_SC_NPROCESSORS_ONLN)
  {
    int count;

    count = sysconf (_SC_NPROCESSORS_ONLN);
    if (count > 0)
      return count;
  }
#elif defined HW_NCPU
  {
    int mib[2], count = 0;
    size_t len;

    mib[0] = CTL_HW;
    mib[1] = HW_NCPU;
    len = sizeof(count);

    if (sysctl (mib, 2, &count, &len, NULL, 0) == 0 && count > 0)
      return count;
  }
#endif

  return 1; /* Fallback */
}
#endif

char * double_quoute_protect(char *r) {
  GString *s= g_string_new_len(r, strlen(r) + 1);
  g_string_replace(s, "\"", "\"\"", 0);
  g_assert (s->str != r);
  r= g_string_free(s, FALSE);
  return r;
}

char * backtick_protect(char *r) {
  GString *s= g_string_new_len(r, strlen(r) + 1);
  g_string_replace(s, "`", "``", 0);
  g_assert (s->str != r);
  r= g_string_free(s, FALSE);
  return r;
}

char * newline_protect(char *r) {
  GString *s= g_string_new_len(r, strlen(r) + 1);
  g_string_replace(s, "\n", "\u10000", 0);
  g_assert (s->str != r);
  r= g_string_free(s, FALSE);
  return r;
}

char * newline_unprotect(char *r) {
  GString *s= g_string_new_len(r, strlen(r) + 1);
  g_string_replace(s, "\u10000", "\n", 0);
  g_assert (s->str != r);
  r= g_string_free(s, FALSE);
  return r;
}

extern gboolean debug;

static __thread char __name_buf[32];
static __thread char *__thread_name= NULL;

void set_thread_name(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  vsnprintf(__name_buf, sizeof(__name_buf), format, args);
  va_end(args);
  __thread_name= __name_buf;
}

void trace(const char *format, ...)
{
  if (!debug)
    return;
  char format2[1024];
  char msg[1024];
  if (__thread_name)
    snprintf(format2, sizeof(format2), "[%s] %s", __thread_name, format);
  else
    snprintf(format2, sizeof(format2), "[%p] %s", g_thread_self(), format);
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format2, args);
  va_end(args);
  g_debug("%s", msg);
}

#define WIDTH 40

void print_int(const char*_key, int val){
  printf("%s%*s= %d\n",_key, WIDTH-(int)(strlen(_key)),"", val);
}

void print_string(const char*_key, const char *val){
  if (val)
    printf("%s%*s= %s\n",_key, WIDTH-(int)(strlen(_key)),"", val);
  else
    printf("# %s%*s= ""\n",_key, WIDTH-(int)(strlen(_key)) -2,"");
}

void print_bool(const char*_key, gboolean val){
  if (val)
    printf("%s%*s= TRUE\n",_key, WIDTH-(int)(strlen(_key)),"");
  else
    printf("# %s%*s= FALSE\n",_key, WIDTH-(int)(strlen(_key)) - 2 ,"");
}

void print_list(const char*_key, GList *list){
  if (list){
    printf("%s%*s= \"%s\"", _key, WIDTH-(int)(strlen(_key)), "", (gchar *)(list->data));
    list=list->next;
    while (list){
      printf(",\"%s\"", (gchar*)(list->data));
      list=list->next;
    }
    printf("\n");
  }else{
    printf("# %s%*s= \"\"\n", _key, WIDTH-(int)(strlen(_key)) - 2, "");
  }
}

void append_alter_table(GString * alter_table_statement, char *table){
  g_string_append(alter_table_statement,"ALTER TABLE `");
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

int global_process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, gchar *real_table, gboolean split_indexes){
  int flag=0;
  gchar** split_file= g_strsplit(statement, "\n", -1);
  gchar *autoinc_column=NULL;
  append_alter_table(alter_table_statement, real_table);
  append_alter_table(alter_table_constraint_statement, real_table);
  int fulltext_counter=0;
  int i=0;
  gchar *engine_pos=NULL;
  for (i=0; i < (int)g_strv_length(split_file);i++){
    if (split_indexes &&( g_strstr_len(split_file[i],5,"  KEY")
      || g_strstr_len(split_file[i],8,"  UNIQUE")
      || g_strstr_len(split_file[i],9,"  SPATIAL")
      || g_strstr_len(split_file[i],10,"  FULLTEXT")
      || g_strstr_len(split_file[i],7,"  INDEX")
      )){
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
          append_alter_table(alter_table_statement,real_table);
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
    engine_pos=g_strrstr(split_file[i],"ENGINE=");
    if (engine_pos){
      engine_pos+=7;
      guint j=0;
      for( j=0; j<g_strv_length(optimize_key_engines); j++)
        if (g_str_has_prefix(engine_pos, optimize_key_engines[j]))
          flag|=IS_TRX_TABLE;
    }
  }
  g_string_replace(create_table_statement,",\n)","\n)", 0);
  finish_alter_table(alter_table_statement);
  finish_alter_table(alter_table_constraint_statement);
  return flag;
}

void initialize_conf_per_table(struct configuration_per_table *cpt){
  cpt->all_anonymized_function=g_hash_table_new ( g_str_hash, g_str_equal );
  cpt->all_where_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  cpt->all_limit_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  cpt->all_num_threads_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  cpt->all_columns_on_select_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
  cpt->all_columns_on_insert_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  cpt->all_object_to_export=g_hash_table_new ( g_str_hash, g_str_equal );

  cpt->all_partition_regex_per_table=g_hash_table_new ( g_str_hash, g_str_equal );

  cpt->all_rows_per_table=g_hash_table_new ( g_str_hash, g_str_equal );
}

gboolean str_list_has_str(gchar ** str_list, const gchar* str){
  guint i=0;
  for(i=0; i<g_strv_length(str_list); i++){
    if(g_strcmp0(str_list[i],str)==0){
      return TRUE;
    }
  }
  return FALSE;
}

void parse_object_to_export(struct object_to_export *object_to_export,gchar *val){
  if (!val){
    object_to_export->no_data=FALSE;
    object_to_export->no_schema=FALSE;
    object_to_export->no_trigger=FALSE;
    return;
  }
  gchar **split_option = g_strsplit(val, ",", 4);
  object_to_export->no_data=!str_list_has_str(split_option,"DATA");
  object_to_export->no_schema=!str_list_has_str(split_option,"SCHEMA");
  object_to_export->no_trigger=!str_list_has_str(split_option,"TRIGGER");
  if (str_list_has_str(split_option,"ALL")){
    object_to_export->no_data=FALSE;
    object_to_export->no_schema=FALSE;
    object_to_export->no_trigger=FALSE;
  }
  if (str_list_has_str(split_option,"NONE")){
    object_to_export->no_data=TRUE;
    object_to_export->no_schema=TRUE;
    object_to_export->no_trigger=TRUE;
  }
  g_strfreev(split_option);
}

gchar *build_dbt_key(gchar *a, gchar *b){
  return g_strdup_printf("%c%s%c.%c%s%c", identifier_quote_character, a, identifier_quote_character, identifier_quote_character, b, identifier_quote_character);
}
/*
gboolean common_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data;
  if (!strcmp(option_name,"--throttle")){
    if (value){
      gchar ** tp;
      gchar ** tq=g_strsplit(value, ":", 2);
      if (tq[1]){
        throttle_max_usleep_limit=atoi(tq[0]);
        tp=g_strsplit(tq[1], "=", 2);
      }else{
        tp=g_strsplit(value, "=", 2);
      }
      throttle_variable=g_strdup(tp[0]);
      throttle_value = atoi(tp[1]);
      g_strfreev(tq);
      g_strfreev(tp);
    }else{
      throttle_variable=g_strdup("Threads_running");
      throttle_value = 0;
    }
    return TRUE;
  }
  if (!strcmp(option_name, "--optimize-keys-engines")){
    if (value){
      optimize_key_engines = g_strsplit(value, ",", 0);
      return TRUE;
    }

  } else if (!strcmp(option_name, "--source-control-command")){
    if (!strcasecmp(value, "TRADITIONAL")) {
      source_control_command=TRADITIONAL;
      return TRUE;
    }
    if (!strcasecmp(value, "AWS")) {
      source_control_command=AWS;
      return TRUE;
    }
  } else if (!strcmp(option_name, "--ignore-errors")){
    guint n=0;
    gchar **tmp_ignore_errors_list = g_strsplit(value, ",", 0);
    while(tmp_ignore_errors_list[n]!=NULL){
      ignore_errors_list=g_list_append(ignore_errors_list,GINT_TO_POINTER(atoi(tmp_ignore_errors_list[n])));
      n++;
    }
    return TRUE;
  }
  return FALSE;
}
*/
void discard_mysql_output(MYSQL *conn){
  MYSQL_RES *result = NULL;
  MYSQL_ROW row = NULL;
  while( mysql_next_result(conn)){
    result = mysql_use_result(conn);
    if (!result)
      return;
    row = mysql_fetch_row(result);
    while (row){
      row = mysql_fetch_row(result);
    }
    mysql_free_result(result);
  }
}

static void m_log(MYSQL *conn, void log_fun_1(const char *, ...), void log_fun_2(const char *, ...), const char *fmt, va_list args){
  if (fmt && log_fun_1){
    gchar *c=g_strdup_vprintf(fmt,args);
    if (log_fun_2 && g_list_find(ignore_errors_list, GINT_TO_POINTER(mysql_errno(conn))))
      log_fun_2("%s - ERROR %d: %s",c, mysql_errno(conn), mysql_error(conn));
    else{
      if (mysql_errno(conn)){
        log_fun_1("%s - ERROR %d: %s",c, mysql_errno(conn), mysql_error(conn));
        if (log_fun_1 != m_message)
          errors++; 
      }else
        log_fun_1("%s",c);
    }
    g_free(c);
  }
}

static gboolean m_queryv(  MYSQL *conn, const gchar *query, void log_fun_1(const char *, ...), void log_fun_2(const char *, ...), const char *fmt, va_list args){
  if (mysql_query(conn, query)){
    m_log(conn, log_fun_1, log_fun_2, fmt, args);
    return TRUE;
  }
  return FALSE;
}


gboolean m_query(  MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_queryv(conn, query, log_fun, NULL, fmt,args);
}

// Executes the query, if there is an error it send critical stopping the process unless the error is ignored
gboolean m_query_warning(  MYSQL *conn, const gchar *query, const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_queryv(conn, query, m_warning, NULL, fmt,args);
}

// Executes the query, if there is an error it send critical stopping the process unless the error is ignored
gboolean m_query_critical(  MYSQL *conn, const gchar *query, const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_queryv(conn, query, m_critical, m_warning, fmt,args);
}


gboolean m_query_ext(  MYSQL *conn, const gchar *query, void log_fun_1(const char *, ...), void log_fun_2(const char *, ...), const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_queryv(conn, query, log_fun_1, log_fun_2, fmt,args);
}

gboolean m_query_verbose(MYSQL *conn, const char *q, void log_fun(const char *, ...) , const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  gboolean res= m_queryv(conn, q, log_fun, NULL, fmt, args);
  if (!res)
    g_message("%s: OK", q);
  return res;
}

MYSQL_RES *m_resultv(MYSQL_RES * m_result(MYSQL *), MYSQL *conn, const gchar *query, void log_fun_1(const char *, ...), void log_fun_2(const char *, ...), const char *fmt, va_list args){
  if (m_queryv(conn, query, log_fun_1, log_fun_2, fmt, args))
    return NULL;

  MYSQL_RES *res = m_result(conn);
  if (!res)
    m_log(conn, log_fun_1, log_fun_2, fmt, args);
  return res;
}

MYSQL_RES *m_store_result_critical(MYSQL *conn, const gchar *query, const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_resultv(mysql_store_result, conn, query, m_critical, m_warning, fmt, args);
}

MYSQL_RES *m_store_result(MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_resultv(mysql_store_result, conn, query, log_fun, NULL, fmt, args);
}

MYSQL_RES *m_use_result(MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_resultv(mysql_use_result, conn, query, log_fun, NULL, fmt, args);
}

struct M_ROW* m_store_result_row(MYSQL *conn, const gchar *query, void log_fun_1(const char *, ...), void log_fun_2(const char *, ...), const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  struct M_ROW *mr=g_new0(struct M_ROW,1);
  mr->row=NULL;
  mr->res = m_resultv(mysql_store_result, conn, query, log_fun_1, log_fun_2, fmt, args);
  if (mr->res)
    mr->row= mysql_fetch_row(mr->res);
  return mr;
}

struct M_ROW* m_store_result_single_row(MYSQL *conn, const gchar *query, const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  struct M_ROW *mr=g_new0(struct M_ROW,1);
  mr->row=NULL;
  mr->res = m_resultv(mysql_store_result, conn, query, m_critical, m_warning, fmt, args);
  if (mr->res){
    mr->row= mysql_fetch_row(mr->res);

    if (!mr->row)
      m_log(conn, m_critical, m_warning, fmt, args);
  }
  return mr;
}

void m_store_result_row_free(struct M_ROW* mr){
  mysql_free_result(mr->res);
  g_free(mr);
}

void execute_set_names(MYSQL *conn, gchar *_set_names){
  gchar *_set_names_statement=set_names_statement_template(_set_names); 
  m_query_warning(conn, _set_names_statement, "Not able to execute SET NAMES statement", NULL);
  g_free(_set_names_statement);
}

GThread * m_thread_new(const gchar* title, GThreadFunc func, gpointer data, const gchar* error_text){
  GThread * thread=g_thread_new(title, func, data);
  if (thread == NULL) {
    m_critical(error_text);
  }
  return thread;
}

void *monitor_throttling_thread (void *queue){
  (void)queue;
  guint current_value;
  gchar *query = g_strdup_printf("SHOW GLOBAL STATUS LIKE '%s'", throttle_variable);
  g_message("Query %s", query);
  struct M_ROW *mr;
  MYSQL *conn;
  conn = mysql_init(NULL);
  if (throttle_value==0){
    throttle_value=num_threads;
  }
  m_connect(conn);
  while (TRUE){
    mr = m_store_result_single_row (conn, query, "We were not able to check: '%s'", throttle_variable);

    if (mr->res && mr->row){
      current_value=atoi(mr->row[1]);

      if (current_value>throttle_value){
        if (throttle_time==0)
          throttle_time=10000;
        else
          throttle_time+=throttle_time;
        if (throttle_max_usleep_limit < throttle_time/1000000)
          throttle_time=throttle_max_usleep_limit*1000000;
        trace("Increasing throttle_time to: %d", throttle_time);
      }else if (current_value<throttle_value && throttle_time > 0){
        throttle_time=throttle_time/2;
        trace("Decreasing throttle_time to: %d", throttle_time);
      }
    }else{
      trace("Invalid query: %s", query);
    }
    m_store_result_row_free(mr);
    sleep(2);
  }

  return NULL;
}
