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
char *regexstring = NULL;
char *nregexstring = NULL;

GOptionEntry regex_entries[] = {
    {"regex", 'x', 0, G_OPTION_ARG_STRING, &regexstring,
     "Regular expression for 'db.table' matching", NULL},
    {"nregex", 'n', 0, G_OPTION_ARG_STRING, &nregexstring,
     "Regular expression for 'db.table' not matching", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};


const char * filename_regex="^[\\w\\-_ ]+$";

static pcre *re = NULL;
static pcre *nre = NULL;
static pcre *filename_re = NULL;

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
      g_critical("Regular expression fail: %s", error);
      exit(EXIT_FAILURE);
    }
  }
}

void initialize_regex(){
  if (regexstring){
    init_regex(&re,regexstring);
  }
  if (nregexstring){
    init_regex(&nre,nregexstring);
  }
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
    if (nre){
      return !check_regex(re, a, b) || check_regex(nre, a, b);
    }else{
      return !check_regex(re, a, b);
    }
  }else{
    if (nre){
      return check_regex(nre, a, b);
    }
  }
  return FALSE;
}
