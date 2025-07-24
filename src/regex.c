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
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <glib.h>
#include "regex.h"
#include "common.h"
const char * filename_regex="^[\\w\\-_ ]+$";

GList *re_list = NULL;
static pcre2_code *filename_re = NULL;
static pcre2_code *partition_re = NULL;
GList *regex_list=NULL;

gboolean regex_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error){
  *error=NULL;
  (void) data; (void) option_name;
  regex_list=g_list_append(regex_list,g_strdup(value));
  return TRUE;
}

gboolean is_regex_being_used(){
  return regex_list!=NULL;
}

GOptionEntry regex_entries[] = {
    {"regex", 'x', 0, G_OPTION_ARG_CALLBACK, &regex_arguments_callback,
     "Regular expression for 'db.table' matching", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

GOptionGroup * load_regex_entries(GOptionContext *context){
  GOptionGroup *filter_group =
      g_option_group_new("filter", "Filter Options", "Filter Options", NULL, NULL);
  g_option_group_add_entries(filter_group, regex_entries);
  g_option_context_add_group(context, filter_group);
  return filter_group;
}

gboolean check_filename_regex(char *word) {
  /* This is not going to be used in threads */
  if (filename_re){
    pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(filename_re, NULL);
    int rc =pcre2_match(filename_re, (PCRE2_SPTR)word, strlen(word), 0, 0, match_data, NULL);
    return (rc > 0) ? TRUE : FALSE;
  }
  return TRUE;
}

void init_regex(pcre2_code **r, const char *str){
  int error;
  PCRE2_SIZE erroroffset;
  if (!*r) {
    *r = pcre2_compile((PCRE2_SPTR)str, PCRE2_ZERO_TERMINATED, PCRE2_CASELESS | PCRE2_MULTILINE, &error,
                      &erroroffset, NULL);
    if (!*r) {
      PCRE2_UCHAR buffer[1024];
      pcre2_get_error_message(error,buffer,1024);
      m_critical("Regular expression fail: %s (%d) %s", str, error, (gchar *)buffer);
    }
  }
}

void initialize_regex(gchar * partition_regex){
  init_regex(&filename_re,filename_regex);
  GList *l=NULL;
  pcre2_code *_re=NULL;
  l=regex_list;
  while (l){
    init_regex(&_re,l->data);
    re_list=g_list_append(re_list,_re);
    _re=NULL;
    l=l->next;
  }
  if (partition_regex)
    init_regex(&partition_re, partition_regex);
}

/* Check database.table string against regular expression */
gboolean check_regex(pcre2_code *tre, char *_database_name, char * _table_name) {
  /* This is not going to be used in threads */
  if (tre){
    char * p = g_strdup_printf("%s.%s", _database_name, _table_name);
    pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(tre, NULL);
    int rc = pcre2_match(tre, (PCRE2_SPTR)p, strlen(p), 0, 0, match_data, NULL);
  
    g_free(p);

    return (rc > 0) ? TRUE : FALSE;
  } return FALSE;
}

gboolean eval_regex(char * _database_name,char * _table_name){
  if (re_list){
    GList *l=re_list;
    gboolean r=FALSE;
    while (l && !r){
      r=check_regex(l->data, _database_name, _table_name);
      l=l->next;
    }
    return r;
  }
  return TRUE;
}

gboolean eval_pcre_regex(pcre2_code * re, char * word){
  if (re){
    pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, NULL);
    int rc = pcre2_match(re, (PCRE2_SPTR)word, strlen(word), 0, 0, match_data, NULL);
    return (rc > 0) ? TRUE : FALSE;
  }
  return TRUE;
}

gboolean eval_partition_regex(char * word){
  if (partition_re){
    return eval_pcre_regex(partition_re, word);
  }
  return TRUE;
}

void free_regex(){
  g_free(filename_re);
  filename_re=NULL;
}

