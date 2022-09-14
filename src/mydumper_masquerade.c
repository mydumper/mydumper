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

#include <glib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <glib/gstdio.h>
#include <glib/gerror.h>
#include <gio/gio.h>
#include <mysql.h>
#include "mydumper_masquerade.h"

gchar * identity_function(gchar ** r, GHashTable * mem){
  (void) mem;
  return *r;
}

gchar * random_int_function(gchar ** r,GHashTable * mem){
  (void) mem;
  g_snprintf(*r, strlen(*r)+1, "%u", g_random_int());
  return *r;
}

gchar * random_int_function_with_mem(gchar ** r, GHashTable * mem){
  gchar *value=g_hash_table_lookup(mem,*r);
  if (value==NULL){
    value=g_strdup_printf("%u", g_random_int());
    g_hash_table_insert(mem,g_strdup(*r),value);
  }
  g_strlcpy(*r, value, strlen(*r)+1);
  return *r;
}

#ifndef WITH_GLIB_uuid_string_random
char *rand_string(char *str, size_t size)
{
    const char charset[] = "0123456789abcdef";
    if (size) {
        --size;
        size_t n;
        for (n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}
#endif

gchar * random_uuid_function(gchar ** r, GHashTable * mem){
  (void) mem;
#ifdef WITH_GLIB_uuid_string_random
  g_strlcpy(*r,g_uuid_string_random(), strlen(*r)+1);
#else
  rand_string(*r,strlen(*r));
#endif
  return *r;
}

gchar * random_uuid_function_with_mem(gchar ** r, GHashTable * mem){
  gchar *value=g_hash_table_lookup(mem,*r);
  if (value==NULL){
#ifdef WITH_GLIB_uuid_string_random
    value=g_strndup(g_uuid_string_random(),strlen(*r)+1);
#else
    rand_string(*r,strlen(*r));
#endif
    g_hash_table_insert(mem,g_strdup(*r),value);
  }
  g_strlcpy(*r, value, strlen(*r)+1);
  return *r;
}

fun_ptr get_function_pointer_for (gchar *function_char){
  if (!g_strcmp0(function_char,"random_int"))
    return &random_int_function;
  if (!g_strcmp0(function_char,"random_int_with_mem"))
    return &random_int_function_with_mem;

  if (!g_strcmp0(function_char,"random_uuid"))
    return &random_uuid_function;
  if (!g_strcmp0(function_char,"random_uuid_with_mem"))
    return &random_uuid_function_with_mem;

  // TODO: more functions needs to be added.
  if (!g_strcmp0(function_char,""))
    return &identity_function;
  if (!g_strcmp0(function_char,""))
    return &identity_function;
  return &identity_function;
}
