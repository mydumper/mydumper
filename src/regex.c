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
#include <pcre.h>
#include <glib.h>
#include "regex.h"
#include "common.h"
const char * filename_regex="^[\\w\\-_ ]+$";

static pcre *re = NULL;
static pcre *filename_re = NULL;

char *regex = NULL;


GOptionEntry regex_entries[] = {
    {"regex", 'x', 0, G_OPTION_ARG_STRING, &regex,
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
  int ovector[9] = {0};
  int rc = pcre_exec(filename_re, NULL, word, strlen(word), 0, 0, ovector, 9);
  return (rc > 0) ? TRUE : FALSE;
}

void init_regex(pcre **r, const char *str){
  const char *error;
  int erroroffset;
  if (!*r) {
    *r = pcre_compile(str, PCRE_CASELESS | PCRE_MULTILINE, &error,
                      &erroroffset, NULL);
    if (!*r) {
      m_critical("Regular expression fail: %s", error);
    }
  }
}

void initialize_regex(){
  if (regex)
    init_regex(&re,regex);
  init_regex(&filename_re,filename_regex);
}

/* Check database.table string against regular expression */
gboolean check_regex(pcre *tre, char *database, char *table) {
  /* This is not going to be used in threads */
  int rc;
  int ovector[9] = {0};

  char * p = g_strdup_printf("%s.%s", database, table);
  rc = pcre_exec(tre, NULL, p, strlen(p), 0, 0, ovector, 9);
  g_free(p);

  return (rc > 0) ? TRUE : FALSE;
}

gboolean eval_regex(char * a,char * b){

  if (re){
    return check_regex(re, a, b);
  }
  return TRUE;
}

void free_regex(){
  if (regex)
    g_free(re);
  g_free(filename_re);
}


