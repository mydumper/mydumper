

#include <mysql.h>
#include <glib.h>
#include <string.h>


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

void load_config_file(gchar * cf, GOptionContext *context, const gchar * group, GString *ss){
  GError *error = NULL;
  GKeyFile *kf = g_key_file_new ();
  if (!g_key_file_load_from_file (kf, cf,
                                  G_KEY_FILE_KEEP_COMMENTS, &error)) {
    g_warning ("Failed to load config file: %s", error->message);
    return;
  }
  gsize len=0;
  gchar ** keys=g_key_file_get_keys(kf,group, &len, &error);
  gsize i=0;
  GSList *list = NULL;
  for (i=0; i < len; i++){
    list = g_slist_append(list, g_strdup_printf("--%s",keys[i]));
    gchar *value=g_key_file_get_value(kf,group,keys[i],&error);
    if ( value != NULL ) list=g_slist_append(list, value);
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
  if (!g_option_context_parse(context, &slen, &gclist, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }else{
    g_message("Config file loaded");
  }
  gchar * group_variables=g_strdup_printf("%s_variables",group);
  keys=g_key_file_get_keys(kf,group_variables, &len, &error);
  for (i=0; i < len; i++){
    gchar *value=g_key_file_get_value(kf,group_variables,keys[i],&error);
    g_string_append_printf(ss,"SET SESSION %s = %s ;\n",keys[i],value); 
  }
}

void execute_gstring(MYSQL *conn, GString *ss)
{
  if (ss != NULL ){
    gchar** line=g_strsplit(ss->str, ";\n", -1);
    int i=0;
    for (i=0; i < (int)g_strv_length(line);i++){
       if (strlen(line[i]) > 3 && mysql_query(conn, line[i])){
         g_warning("Set session failed: %s | ",line[i]);
       }
    }
  }
}

