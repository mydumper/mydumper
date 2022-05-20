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
#include <glib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <glib/gstdio.h>
#include "server_detect.h"
extern gboolean no_delete;
extern gboolean stream;

typedef gchar * (*fun_ptr)(gchar **);

FILE * (*m_open)(const char *filename, const char *);
GAsyncQueue *stream_queue = NULL;
extern int detected_server;

GHashTable * initialize_hash_of_session_variables(){
  GHashTable * set_session_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  if (detected_server == SERVER_TYPE_MYSQL){
    g_hash_table_insert(set_session_hash,g_strdup("WAIT_TIMEOUT"),g_strdup("2147483"));
    g_hash_table_insert(set_session_hash,g_strdup("NET_WRITE_TIMEOUT"),g_strdup("2147483"));
  }
  return set_session_hash;
}

char * checksum_table(MYSQL *conn, char *database, char *table, int *errn){
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  *errn=0;
  char *query = g_strdup_printf("CHECKSUM TABLE `%s`.`%s`", database, table);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    g_critical("Error dumping checksum (%s.%s): %s", database, table, mysql_error(conn));
    *errn=mysql_errno(conn);
    g_free(query);
    return NULL;
  }
  g_free(query);

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  char * r=g_strdup_printf("%s",row[1]);
  mysql_free_result(result);
  return r;
}


void load_config_file(gchar * config_file, GOptionContext *context, const gchar * group){
  GError *error = NULL;
  GKeyFile *kf = g_key_file_new ();
  // Loads the config_file
  if (!g_key_file_load_from_file (kf, config_file,
                                  G_KEY_FILE_KEEP_COMMENTS, &error)) {
    g_warning ("Failed to load config file %s: %s", config_file, error->message);
    return;
  }
  gsize len=0;
  gchar ** keys=g_key_file_get_keys(kf,group, &len, &error);
  gsize i=0;
  GSList *list = NULL;
  if (error != NULL){
    g_warning("loading %s: %s",group,error->message);
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
      g_print("option parsing failed: %s, try --help\n", error->message);
      exit(EXIT_FAILURE);
    }else{
      g_message("Config file loaded");
    }
  }
}

gchar * identity_function(gchar ** r){
  return *r;
}

gchar * random_int_function(gchar ** r){
  // TODO: This function is not near to be ok, it is just for testing.
  gchar * new_number=g_strdup_printf("%u",g_random_int());
  g_strlcpy(*r,new_number,strlen(*r)+1);
  return *r;
}

fun_ptr get_function_pointer_for (gchar *function_char){
  if (!g_strcmp0(function_char,"random_int"))
    return &random_int_function;
  // TODO: more functions needs to be added.
  if (!g_strcmp0(function_char,""))
    return &identity_function;
  if (!g_strcmp0(function_char,""))
    return &identity_function;     
  return &identity_function;
}


void load_hash_from_key_file(GHashTable * set_session_hash, GHashTable *all_anonymized_function, gchar * config_file, const gchar * group_variables){
  guint i=0,j=0;
  GError *error = NULL;
  gchar *value=NULL;
  gsize len=0,len2=0;
  GKeyFile *kf = g_key_file_new ();
  // Loads the config_file
  if (!g_key_file_load_from_file (kf, config_file,
                                  G_KEY_FILE_KEEP_COMMENTS, &error)) {
    g_warning ("Failed to load config file %s: %s", config_file, error->message);
    return;
  }
  gchar **keys=g_key_file_get_keys(kf,group_variables, &len, &error);
  for (i=0; i < len; i++){
    value=g_key_file_get_value(kf,group_variables,keys[i],&error);
    if (!error)
      g_hash_table_insert(set_session_hash, keys[i],value);
  }
  gchar **groups=g_key_file_get_groups(kf,&len);
  GHashTable *ht=NULL;
  for (i=0; i < len; i++){
    if (g_strstr_len(groups[i],strlen(groups[i]),"`.`") && g_str_has_prefix(groups[i],"`") && g_str_has_suffix(groups[i],"`")){
      ht=g_hash_table_new ( g_str_hash, g_str_equal );
      keys=g_key_file_get_keys(kf,groups[i], &len2, &error);
      for (j=0; j < len2; j++){
        value = g_key_file_get_value(kf,groups[i],keys[j],&error);
        g_hash_table_insert(ht,g_strdup(keys[j]),get_function_pointer_for(value));
      }
      g_hash_table_insert(all_anonymized_function,g_strdup(groups[i]),ht);
    }
  }

}


void refresh_set_session_from_hash(GString *ss, GHashTable * set_session_hash){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, set_session_hash );
  gchar *e=NULL;
  while ( g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &e ) ) {
    g_string_append_printf(ss,"SET SESSION %s = %s ;\n",lkey,e);
  }
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

void create_backup_dir(char *new_directory) {
  if (g_mkdir(new_directory, 0750) == -1) {
    if (errno != EEXIST) {
      g_critical("Unable to create `%s': %s", new_directory, g_strerror(errno));
      exit(EXIT_FAILURE);
    }
  }
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

gboolean m_remove(gchar * directory, const gchar * filename){
  if (stream && no_delete == FALSE){
    gchar *path = g_build_filename(directory == NULL?"":directory, filename, NULL);
    g_message("Removing file: %s", path);
    remove(path);
    g_free(path);
  }
  return TRUE;
}

gboolean is_table_in_list(gchar *table_name, gchar **table_list){
  guint i = 0;
  for (i = 0; table_list[i] != NULL; i++)
    if (g_ascii_strcasecmp(table_list[i], table_name) == 0)
      return TRUE;
  return FALSE;
}
