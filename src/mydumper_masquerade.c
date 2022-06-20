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
