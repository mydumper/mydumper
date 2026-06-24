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

#include "common.h"

extern gchar *tables_list;
extern gchar *tables_includelist_file;
extern char **tables;
extern gchar *pwd;
extern guint errors;


static
gchar **get_table_list_with_needle(gchar *haystack, const gchar *needle){
  gchar ** tl = g_strsplit(haystack, needle, -1);
  guint i=0;
  for(i=0; i < g_strv_length(tl); i++){
    if (g_strstr_len(tl[i],strlen(tl[i]),".") == NULL )
      m_error("Table name %s is not in DATABASE.TABLE format", tl[i]);
  }
  return tl;
}

gchar **get_table_list_from_parameter(gchar *_tables_list){
  return get_table_list_with_needle(_tables_list, ",");
}

gchar **get_table_list_from_file(gchar *_tables_list){
  return get_table_list_with_needle(_tables_list, "\n");
}



#if GLIB_CHECK_VERSION(2, 68, 0)

#if !GLIB_CHECK_VERSION(2, 70, 0)

void
my_strv_builder_addv(GStrvBuilder *builder, const gchar **strv) {
    if (!strv) return;

    for (gint i = 0; strv[i] != NULL; i++) {
        g_strv_builder_add(builder, strv[i]);
    }
}

#endif



void load_include_tables(){

  GStrvBuilder *builder = g_strv_builder_new();
  // Give ourselves an array of tables to dump
  if (tables_list){
    gchar ** _tables_list = get_table_list_from_parameter(tables_list);
    g_strv_builder_addv(builder, (const gchar**)_tables_list);
    g_strfreev(_tables_list);
  }
  if (tables_includelist_file){
    gchar* contents=NULL;
    gsize length=0;
    GError *error=NULL;

    gchar *_filename=tables_includelist_file;
    if (pwd && tables_includelist_file[0] != '/'){
      _filename=g_strdup_printf("%s/%s", pwd, tables_includelist_file);
    }
    g_file_get_contents(_filename, &contents, &length, &error);
    while (length > 0 && contents[length-1]=='\n'){
      contents[length-1]='\0';
      length--;
    }
    gchar ** tables_list_from_file= get_table_list_from_file(contents);
    g_strv_builder_addv(builder, (const gchar**)tables_list_from_file);
    g_strfreev(tables_list_from_file);
  }

  if (tables_list || tables_includelist_file)
    tables=g_strv_builder_end(builder);
  g_strv_builder_unref(builder);

#else

void load_include_tables(){

  // Give ourselves an array of tables to dump
  gchar ** _tables_list=NULL;

  if (tables_list){
    _tables_list = get_table_list_from_parameter(tables_list);
  }

  gchar ** tables_list_from_file=NULL;
  if (tables_includelist_file){
    gchar* contents=NULL;
    gsize length=0;
    GError *error=NULL;

    gchar *_filename=tables_includelist_file;
    if (pwd && tables_includelist_file[0] != '/'){
      _filename=g_strdup_printf("%s/%s", pwd, tables_includelist_file);
    }
    g_file_get_contents(_filename, &contents, &length, &error);
    while (length > 0 && contents[length-1]=='\n'){
      contents[length-1]='\0';
      length--;
    }
    tables_list_from_file= get_table_list_from_file(contents);
  }

  if (tables_list || tables_includelist_file){
    guint len1 = _tables_list?g_strv_length(_tables_list):0;
    guint len2 = tables_list_from_file?g_strv_length(tables_list_from_file):0;
    
    tables = g_new(gchar *, len1 + len2 + 1);
    
    guint i = 0, j = 0;
    
    for (j = 0; j < len1; j++) {
        tables[i++] = g_strdup(_tables_list[j]);
    }
    
    for (j = 0; j < len2; j++) {
        tables[i++] = g_strdup(tables_list_from_file[j]);
    }
  }
  if (_tables_list)
    g_strfreev(_tables_list);
  if (tables_list_from_file)
    g_strfreev(tables_list_from_file);

#endif
}

