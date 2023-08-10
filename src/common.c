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
#include <pcre.h>
#include "regex.h"
#include "server_detect.h"
#include "common.h"
#include "config.h"
#include "connection.h"
#include <stdarg.h>
extern gboolean no_delete;
extern gboolean stream;
extern gchar *defaults_file;
extern gchar *defaults_extra_file;
extern GKeyFile * key_file;
extern gboolean no_stream;
extern gchar*set_names_str;
extern gchar*set_names_statement;
extern guint num_threads;
extern GString *set_global_back;
extern MYSQL *main_connection;
FILE * (*m_open)(char **filename, const char *);
GAsyncQueue *stream_queue = NULL;
extern int detected_server;

gchar zstd_paths[2][15] = { "/usr/bin/zstd", "/bin/zstd" };
gchar gzip_paths[2][15] = { "/usr/bin/gzip", "/bin/gzip" };


gchar *get_zstd_cmd(){
  guint i=0;
  for(i=0; i<2; i++){
    if (g_file_test( zstd_paths[i] , G_FILE_TEST_EXISTS))
      return zstd_paths[i];
  }
  return NULL;
}

gchar *get_gzip_cmd(){
  guint i=0;
  for(i=0; i<2; i++){
    if (g_file_test( gzip_paths[i] , G_FILE_TEST_EXISTS))
      return gzip_paths[i];
  }
  return NULL;
}


GHashTable * initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  if (detected_server == SERVER_TYPE_MYSQL || detected_server == SERVER_TYPE_MARIADB){
    g_hash_table_insert(set_session_hash,g_strdup("WAIT_TIMEOUT"),g_strdup("2147483"));
    g_hash_table_insert(set_session_hash,g_strdup("NET_WRITE_TIMEOUT"),g_strdup("2147483"));
  }
  return set_session_hash;
}

void initialize_set_names(){
  if (set_names_str){
    if (strlen(set_names_str)!=0){
      set_names_statement=g_strdup_printf("/*!40101 SET NAMES %s*/",set_names_str);
    }else
      set_names_str=NULL;
  } else {
    set_names_str=g_strdup("binary");
    set_names_statement=g_strdup("/*!40101 SET NAMES binary*/");
  }
}

void free_set_names(){
  g_free(set_names_str);
  g_free(set_names_statement);
}

char *generic_checksum(MYSQL *conn, char *database, char *table, int *errn,const gchar *query_template, int column_number){
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  *errn=0;
  char *query = g_strdup_printf(query_template, database, table);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    g_critical("Error dumping checksum (%s.%s): %s", database, table, mysql_error(conn));
    *errn=mysql_errno(conn);
    g_free(query);
    return NULL;
  }

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  char * r=NULL;
  if (row != NULL) r=g_strdup_printf("%s",row[column_number]);
  g_free(query);
  mysql_free_result(result);
  return r;
}

char * checksum_table(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn, "CHECKSUM TABLE `%s`.`%s`", 1);
}


char * checksum_table_structure(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 0);
}

char * checksum_process_structure(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 0);
}

char * checksum_trigger_structure(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';",0);
}

char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';",0);
}

char * checksum_view_structure(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';",0);
}

char * checksum_database_defaults(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;",0);
}

char * checksum_table_indexes(MYSQL *conn, char *database, char *table, int *errn){
  return generic_checksum(conn, database, table, errn,"SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME", 0);
}

GKeyFile * load_config_file(gchar * config_file){
  GError *error = NULL;
  GKeyFile *kf = g_key_file_new ();
  // Loads the config_file
  if (!g_key_file_load_from_file (kf, config_file,
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
    if (!error)
      g_hash_table_insert(set_session_hash, g_strdup(keys[i]), g_strdup(value));
  }
  g_strfreev(keys);
}

void load_per_table_info_from_key_file(GKeyFile *kf, struct configuration_per_table * conf_per_table, struct function_pointer * init_function_pointer(gchar*)){
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

          value = g_key_file_get_value(kf,groups[i],keys[j],&error);
          struct function_pointer *fp = init_function_pointer(value);
          g_hash_table_insert(ht,g_strndup(keys[j]+1,strlen(keys[j])-2), fp);
        }else{
          if (g_strcmp0(keys[j],"where") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(conf_per_table->all_where_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"limit") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(conf_per_table->all_limit_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"num_threads") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(conf_per_table->all_num_threads_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"columns_on_select") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(conf_per_table->all_columns_on_select_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"columns_on_insert") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(conf_per_table->all_columns_on_insert_per_table, g_strdup(groups[i]), g_strdup(value));
          }
          if (g_strcmp0(keys[j],"partition_regex") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            pcre *r=NULL; 
            init_regex( &r, value);
            g_hash_table_insert(conf_per_table->all_partition_regex_per_table, g_strdup(groups[i]), r);
          }
          if (g_strcmp0(keys[j],"rows") == 0){
            value = g_key_file_get_value(kf,groups[i],keys[j],&error);
            g_hash_table_insert(conf_per_table->all_rows_per_table, g_strdup(groups[i]), g_strdup(value));
          }

        }
      }
      g_hash_table_insert(conf_per_table->all_anonymized_function,g_strdup(groups[i]),ht);
    }
  }
  g_strfreev(groups);
}


void load_hash_of_all_variables_perproduct_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar *str){
  GString *s=g_string_new(str);
  load_hash_from_key_file(kf,set_session_hash,s->str);
  g_string_append_c(s,'_');
  g_string_append(s,get_product_name());
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

void refresh_set_session_from_hash(GString *ss, GHashTable * set_session_hash){
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


void create_fifo_dir(char *new_fifo_directory) {
  if (new_fifo_directory){
    if (g_mkdir(new_fifo_directory, 0750) == -1) {
      if (errno != EEXIST) {
        m_critical("Unable to create `%s': %s", new_fifo_directory, g_strerror(errno));
      }
    }
  }
}

void create_backup_dir(char *new_directory, char *new_fifo_directory) {
  if (g_mkdir(new_directory, 0750) == -1) {
    if (errno != EEXIST) {
      m_critical("Unable to create `%s': %s", new_directory, g_strerror(errno));
    }
  }
  create_fifo_dir(new_fifo_directory);
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

gboolean m_remove(gchar * directory, const gchar * filename){
  if (stream && no_delete == FALSE){
    gchar *path = g_build_filename(directory == NULL?"":directory, filename, NULL);
    g_message("Removing file: %s", path);
    remove(path);
    g_free(path);
  }
  return TRUE;
}

gboolean is_table_in_list(gchar *table_name, gchar **tl){
  guint i = 0;
  for (i = 0; tl[i] != NULL; i++)
    if (g_ascii_strcasecmp(tl[i], table_name) == 0)
      return TRUE;
  return FALSE;
}



void m_key_file_merge(GKeyFile *b, GKeyFile *a){
  gsize  group_len = 0, key_len=0;
  gchar **group = g_key_file_get_groups (a, &group_len), **key=NULL;
  
  guint g=0, k=0;
  GError *error=NULL;
  for( g=0; g<group_len; g++ ){
    key=g_key_file_get_keys(a, group[g], &key_len, &error );
    for(k=0; k<key_len; k++ ){
      g_key_file_set_value(b, group[g], key[k], g_key_file_get_value(a, group[g], key[k], &error));
    }
  }

}


void initialize_common_options(GOptionContext *context, const gchar *group){
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

  if (key_file!=NULL && g_key_file_has_group(key_file, group )){
    parse_key_file_group(key_file, context, group);
    set_connection_defaults_file_and_group(defaults_file, group); 
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

  if (extra_key_file!=NULL && g_key_file_has_group(extra_key_file, group )){
    g_message("Parsing extra key file");
    parse_key_file_group(extra_key_file, context, group);
    set_connection_defaults_file_and_group(defaults_extra_file, group);
  }else
    set_connection_defaults_file_and_group(defaults_extra_file, NULL);
  g_message("Merging config files user: ");

  m_key_file_merge(key_file, extra_key_file);

//  g_key_file_free(extra_key_file);
}

gchar **get_table_list(gchar *tables_list){
  gchar ** tl = g_strsplit(tables_list, ",", 0);
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
    if (value==NULL || g_strstr_len(value,11,"TRADITIONAL")){
      return TRUE;
    }
    if (g_strstr_len(value,9,"NO_DELETE")){
      no_delete=TRUE;
      return TRUE;
    }
    if (g_strstr_len(value,23,"NO_STREAM_AND_NO_DELETE")){
      no_delete=TRUE;
      no_stream=TRUE;
      return TRUE;
    }
  }
  return FALSE;
}

void check_num_threads(){
  if (num_threads < MIN_THREAD_COUNT) {
    g_warning("Invalid number of threads %d, setting to %d", num_threads, MIN_THREAD_COUNT);
    num_threads = MIN_THREAD_COUNT;
  }
}

void m_error(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  execute_gstring(main_connection, set_global_back); 
  g_error("%s", c);
}

void m_critical(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  execute_gstring(main_connection, set_global_back);
  g_critical("%s",c);
  exit(EXIT_FAILURE);
}


void m_warning(const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  gchar *c=g_strdup_vprintf(fmt,args);
  g_warning("%s",c);
  g_free(c);
}

/* Function to work around a bug in MariaDB which outputs the explicit
 * scehma for a sequence in a SHOW CREATE TABLE even if it is local to the
 * current table
 */
gchar *filter_sequence_schemas(const gchar *create_table)
{
  pcre *re = NULL;
  const char *error;
  int erroroffset;
  int ovector[12] = {0};
  gchar *out = g_strdup(create_table);

  re = pcre_compile("(?:nextval|lastval)\\((`.*`\\.(`.*`))\\)",
                    PCRE_CASELESS | PCRE_MULTILINE, &error, &erroroffset,
                    NULL);
  if (!re) {
    g_critical("Regular expression fail: %s", error);
    // We can safely continue here
  } else {
    int offset = 0;
    while ((pcre_exec(re, NULL, out, strlen(out), offset, 0, ovector, 12)) > 0) {
      gchar* tmp = g_new(gchar, strlen(out));
      size_t tmp_pos = 0;
      /* Positions generated:
       * ovector 0 - 1: nextval(`test`.`s`)
       * ovector 2 - 3: `test`.`s`
       * ovector 4 - 5: `s`
       */
      size_t write_len = ovector[2];

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
    }
  }
  return out;
}

GRecMutex * g_rec_mutex_new(){
  GRecMutex *r=g_new0(GRecMutex,1);
  g_rec_mutex_init(r);
  return r;

}

gboolean read_data(FILE *file, GString *data,
                   gboolean *eof, guint *line) {
  char buffer[256];

  do {
    if (fgets(buffer, 256, file) == NULL) {
      if (feof(file)) {
        *eof = TRUE;
        buffer[0] = '\0';
      } else {
        return FALSE;
      }
    }
    g_string_append(data, buffer);
    if (buffer[strlen(buffer)-1] == '\n')
      (*line)++;
  } while ((buffer[strlen(buffer)] != '\0') && *eof == FALSE);

  return TRUE;
}

gchar *m_date_time_new_now_local(){
  GString *datetimestr=g_string_sized_new(26);
  GDateTime *datetime = g_date_time_new_now_local();
  g_string_append(datetimestr,g_date_time_format(datetime,"\%Y-\%m-\%d \%H:\%M:\%S"));
  g_string_append_printf(datetimestr,".%d", g_date_time_get_microsecond(datetime));
  g_date_time_unref(datetime);
  gchar *r=datetimestr->str;
  g_string_free(datetimestr,FALSE);
  return r;
}

