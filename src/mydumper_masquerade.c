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
#include "mydumper_common.h"
#include "common.h"
struct function_pointer pp = {&identity_function, NULL, NULL, NULL, NULL};

GHashTable *file_hash = NULL;

void initialize_masquerade(){
  file_hash = g_hash_table_new_full( g_str_hash, g_str_equal,  &g_free, &g_free );
}

void finalize_masquerade(){
  g_hash_table_destroy(file_hash); 
}


GHashTable * load_file_content(gchar *filename){
  GHashTable * file_content=g_hash_table_new ( g_direct_hash, g_direct_equal );
  FILE *file = g_fopen(filename, "r");
  if (file == NULL){
    g_error("File not open: %s", filename);
  }
  GString *data=g_string_sized_new(256);
  gboolean eof = FALSE;
  guint line=0;
  GList *l=NULL;
  while (!eof){
    read_data(file, data, &eof, &line);
    while (data->str[data->len-1] == '\n' || data->str[data->len-1] == '\r')
      g_string_set_size(data, data->len - 1);
    if (data->len>0){
      l = (GList *) g_hash_table_lookup(file_content,(gpointer)GINT_TO_POINTER(data->len));
      l=g_list_prepend(l,g_strdup(data->str));
      g_hash_table_replace(file_content, (gpointer)GINT_TO_POINTER(data->len), l);
    }
    g_string_set_size(data, 0);
  }
  fclose(file);
  return file_content;
}

GHashTable * load_file_into_file_hash(gchar *filename){
  g_message("Loading content of %s",filename);
  GHashTable * file_content=g_hash_table_lookup(file_hash,filename);
  if (file_content==NULL){
    file_content=load_file_content(filename);
    g_hash_table_insert(file_hash,g_strdup(filename),file_content);
    file_content=g_hash_table_lookup(file_hash,filename);
  }
  return file_content;
}


gchar * identity_function(gchar ** r, gulong* length,  struct function_pointer *fp){
  (void) fp;
  (void) length;
  return *r;
}

gchar * random_int_function(gchar ** r, gulong* length, struct function_pointer *fp){
  (void) fp;
  gulong l;
  if (*length > 10)
    l=g_snprintf(*r, *length + 1, "%u%u", g_random_int(), g_random_int());
  else
    l=g_snprintf(*r, *length + 1, "%u", g_random_int());
  if (l<*length)
    *length=l;
  return *r;
}

gchar * random_int(gchar * r, gulong *length){
  return random_int_function(&r, length, NULL);
}

gchar * random_int_function_with_mem(gchar ** r, gulong* length, struct function_pointer *fp){
  (void) length;
  gchar *value=g_hash_table_lookup(fp->memory,*r);
  if (value==NULL){
    value=g_strdup_printf("%u", g_random_int());
    g_hash_table_insert(fp->memory,g_strdup(*r),value);
  }
  g_strlcpy(*r, value, strlen(*r)+1);
  return *r;
}


char *rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
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


#ifndef WITH_GLIB_uuid_string_random
char *rand_uuid(char *str, size_t size)
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

gchar * random_uuid_function(gchar ** r, gulong* length, struct function_pointer *fp){
 (void) length;
 (void) fp;
#ifdef WITH_GLIB_uuid_string_random
  g_strlcpy(*r,g_uuid_string_random(), strlen(*r)+1);
#else
  rand_uuid(*r,strlen(*r));
#endif
  return *r;
}

gchar * random_uuid_function_with_mem(gchar ** r, gulong* length, struct function_pointer *fp){
  (void) length;
  gchar *value=g_hash_table_lookup(fp->memory,*r);
  if (value==NULL){
    gchar *k=g_strdup(*r);
#ifdef WITH_GLIB_uuid_string_random
    value=g_strndup(g_uuid_string_random(),strlen(*r)+1);
#else
    rand_uuid(*r,strlen(*r));
#endif
    g_hash_table_insert(fp->memory,k,g_strdup(value));
  }
  g_strlcpy(*r, value, strlen(*r)+1);
  return *r;
}


gchar * random_string_function(gchar ** r, gulong* length, struct function_pointer *fp){
  (void) length; 
  (void) fp;
  rand_string(*r, strlen(*r)+1);
  return *r;
}

gchar * random_string_function_with_mem(gchar ** r, gulong* length, struct function_pointer *fp){
  (void) length;
  gchar *value=g_hash_table_lookup(fp->memory,*r);
  if (value==NULL){
    gchar *k=g_strdup(*r);
    value=rand_string(*r,strlen(*r)+1);
    g_hash_table_insert(fp->memory,k,g_strdup(value));
  }
  g_strlcpy(*r, value, strlen(*r)+1);

  return *r;
}



gchar *random_format_function(gchar ** r, gulong* max_len, struct function_pointer *fp){
  guint i=0;
  GList *l=fp->parse;
  GList *d=fp->delimiters;
  GList *fl=NULL;
  struct format_item *fi=NULL;
  gchar *p=*r;
  guint local_max_len=0;
  (void) local_max_len;
  gboolean cont=TRUE;
  struct format_item_file *fid = NULL;
  guint val;
  gulong local_len;
  while (l !=NULL && i < *max_len){
    if (d != NULL){
      //find delimiter position to determine size
      local_max_len=g_strstr_len(p, *max_len-i, (gchar*) (d->data)) - p;
    }else{
      local_max_len=*max_len-i;
    }
    cont=TRUE;
    while (l !=NULL && cont && i < *max_len){
      fi=l->data;
      switch (fi->type){
        case FORMAT_ITEM_FILE:
          fid = fi->data;
          if (fid->min < *max_len - i){
            val=g_random_int_range(fid->min, *max_len - i < fid->max ? *max_len - i : fid->max );
            fl = (GList *) g_hash_table_lookup((GHashTable *)fid->data,GINT_TO_POINTER(val));
            g_strlcpy(p, g_list_nth_data(fl, g_random_int_range(0,g_list_length(fl))) , (i+val > *max_len ? *max_len - i : val)+1);
            i+=val ;
            p+=val ;
          }
          break;
        case FORMAT_ITEM_CONFIG_FILE:
          break;
        case FORMAT_ITEM_CONSTANT:
          g_strlcpy(p, fi->data, (i+fi->len > *max_len? *max_len-i:fi->len )+1);
          i+=fi->len;
          p+=fi->len;
          break;
        case FORMAT_ITEM_DELIMITER:
          g_strlcpy(p, fi->data, strlen(fi->data)+1);
          i+=strlen(fi->data) ; 
          p+=strlen(fi->data) ;
          cont=FALSE;
          break;
        case FORMAT_ITEM_NUMBER:
          local_len=(i+fi->len > *max_len? *max_len-i : fi->len );
          random_int(p, &local_len );
          i+=local_len;
          p+=local_len;
          break;
        case FORMAT_ITEM_STRING:
          rand_string(p, (i+fi->len > *max_len? *max_len-i:fi->len )+1);
          i+=fi->len;
          p+=fi->len;
          break;
      }
      l=l->next; 
    }
    if (d != NULL){
      d=d->next;
    }

  } 
  if( i < *max_len){
    *max_len=i;
  }
  return *r;
}


fun_ptr get_function_pointer_for (gchar *function_char){
  if (g_str_has_prefix(function_char,"random_format")){
    return &random_format_function;
  }

  if (!g_strcmp0(function_char,"random_string"))
    return &random_string_function;
  if (!g_strcmp0(function_char,"random_string_with_mem"))
    return &random_string_function_with_mem;


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
  g_message("Function not found: Using default");
  return &identity_function;
}

gint comp(gconstpointer a, gconstpointer b){
  return GPOINTER_TO_INT(a) >= GPOINTER_TO_INT(b);
}


void parse_value(struct function_pointer * fp, gchar *val){
  char buffer[256];
  guint i=0;
  struct format_item *fi;
  GList *keys=NULL, *sorted=NULL;
  guint sum;
  while (*val != '\0'){
    if (*val == '\''){
      val++;
      i=0;
      while (*val != '\0' && *val!='\''){
        buffer[i]=*val;
        i++;
        val++;
      }
      if (*val!='\''){
        g_error("Parsing format failed missing quote (')");
      }
      buffer[i]='\0';
      fi=g_new0(struct format_item, 1);
      fi->type=FORMAT_ITEM_CONSTANT;
      fi->data = g_strdup(buffer);
      fi->len = i;
      fp->parse=g_list_append(fp->parse,fi);
      val++;
    }else if (*val == '<'){
      val++;
      i=0;
      while (*val != '\0' && *val!='>'){
        buffer[i]=*val;
        i++;
        val++;
      }
      if (*val!='>'){
        g_error("Parsing format failed missing close character (>)");
      }
      if (i>0){
        buffer[i]='\0';
        fi=g_new0(struct format_item, 1);

        if (g_str_has_prefix(buffer,"file ")){
          fi->type=FORMAT_ITEM_FILE;
          struct format_item_file *fid=g_new0(struct format_item_file, 1);
          fid->data = load_file_into_file_hash(&(buffer[5]));
          keys=g_hash_table_get_keys(fid->data);
          sorted=g_list_sort(keys,comp);
          fid->max = GPOINTER_TO_INT(g_list_last(sorted)->data);
          fid->min = GPOINTER_TO_INT(g_list_first(sorted)->data);
          sum=(fid->min+fid->max) * (fid->max-fid->min + 1)/2;
          while (sorted !=NULL){
            sum-=GPOINTER_TO_INT(sorted->data);
            sorted=sorted->next;
          }
          if (sum != 0 )
            g_error("The file %s shouldn't have gaps: %d | %d | %d", buffer, sum , fid->min , fid->max);
          fi->data = fid;
          fp->parse=g_list_append(fp->parse,fi);
          g_list_free(keys);
        }else if (g_str_has_prefix(buffer,"string ")){
          fi->type=FORMAT_ITEM_STRING;
          fi->len=g_ascii_strtoull(&(buffer[7]), NULL, 10);
          fp->parse=g_list_append(fp->parse,fi);
        }else if (g_str_has_prefix(buffer,"number ")){
          fi->type=FORMAT_ITEM_NUMBER;
          fi->len=g_ascii_strtoull(&(buffer[7]), NULL, 10);
          fp->parse=g_list_append(fp->parse,fi);
        }else
          g_error("Parsing format failed key inside <tag> not valid");
      }
      val++;
    }else{
      i=0;
      while (*val != '\0' && *val!='<' && *val!='\''){
        buffer[i]=*val;
        i++;
        val++;
      }
      buffer[i]='\0';
      fi=g_new0(struct format_item, 1);
      fi->type=FORMAT_ITEM_DELIMITER;
      fi->data = g_strdup(buffer);
      fp->parse=g_list_append(fp->parse,fi);
      fp->delimiters=g_list_append(fp->delimiters,fi);

    }
  }
}

struct function_pointer * init_function_pointer(gchar *value){
  struct function_pointer * fp= g_new0(struct function_pointer, 1);
  fp->function=get_function_pointer_for(value);
  fp->memory=g_hash_table_new ( g_str_hash, g_str_equal );
  fp->value=value;
  fp->parse=NULL;
  fp->delimiters=NULL;
  g_message("init_function_pointer: %s", value);
  if (g_str_has_prefix(value,"random_format")){
    g_message("random_format_function");
    parse_value(fp, g_strdup(&(fp->value[14])));
  }
  return fp;
}
