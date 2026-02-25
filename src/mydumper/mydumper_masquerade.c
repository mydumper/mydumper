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
#include "mydumper.h"
struct function_pointer identity_function_pointer = {&identity_function, FALSE, NULL, NULL, NULL, NULL, FALSE, 0, 0, NULL, FALSE};

GHashTable *file_hash = NULL;

void initialize_masquerade(){
  srand(clock());

  file_hash = g_hash_table_new_full( g_str_hash, g_str_equal,  &g_free, &g_free );
}

void finalize_masquerade(){
//  if (file_hash)
//    g_hash_table_unref(file_hash); 
}


// Masquerade Utils

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

gint comp(gconstpointer a, gconstpointer b){
  return GPOINTER_TO_INT(a) >= GPOINTER_TO_INT(b);
}

// Functions that will be executed

void identity_function(GString *str, gchar * row, gulong* length,  struct function_pointer *fp){
  (void) str; 
  (void) row;
  (void) fp;
  (void) length;
}

void random_basic_function(GString *str, gchar *row, gulong* length, struct function_pointer *fp, void (*random_funtion)(gchar *, guint) ){
  gchar *new_r=NULL;
  guint max_length=0;
  if (fp && fp->memory && row){
    new_r=g_hash_table_lookup(fp->memory, row);
    if (new_r){
      *length=strlen(new_r);
      g_string_assign(str,new_r);
      return;
    }
  }
  gchar*_key=NULL;

  if (row){
    if (fp && fp->memory)
      _key=g_strdup(row);

retry:
    max_length=fp->max_length>0 && *length>fp->max_length?fp->max_length:*length;
    g_string_set_size(str, max_length);
    random_funtion(str->str, max_length);

    if (fp && fp->unique){
      if (g_list_find_custom(fp->unique_list,str->str,(GCompareFunc)g_strcmp0)){
        goto retry;
      }
      fp->unique_list=g_list_prepend(fp->unique_list,g_strdup(str->str));
    }

    if (fp && fp->memory)
      g_hash_table_insert(fp->memory,_key,g_strdup(str->str));

    *length=str->len;

  }else{
    // NULL value
    if (fp && fp->replace_null){
retry2:
      g_string_set_size(str, fp->null_max_length + 1);
      random_funtion(str->str, fp->null_max_length );

      if (fp->unique){
        if (g_list_find_custom(fp->unique_list,str->str,(GCompareFunc)g_strcmp0)){
          goto retry2;
        }
        fp->unique_list=g_list_prepend(fp->unique_list,g_strdup(str->str));
      }

      *length=str->len;
    }
  }
}

void m_random_int(gchar *r, guint len){
  if (len > 8)
    g_snprintf(r, len + 1, "%u%u", g_random_int(), g_random_int());
  else
    g_snprintf(r, len + 1,   "%u", g_random_int());
}

void random_int_function(GString *str, gchar *row, gulong* length, struct function_pointer *fp){
  random_basic_function(str, row, length,fp,&m_random_int);
}

void m_random_string(char *str, guint size){
  const char charset[] = "abcdefghijklmnopqrstuvwxyz";
  if (size) {
//    --size;
    size_t n;
    for (n = 0; n < size; n++) {
      int _key = rand() % (int) (sizeof charset - 1);
      str[n] = charset[_key];
    }
    str[size] = '\0';
  }
}

void random_string_function(GString *str, gchar *row, gulong* length, struct function_pointer *fp){
  random_basic_function(str,row,length,fp,&m_random_string);
}

void m_random_uuid(char *str, guint size){
#ifndef WITH_GLIB_uuid_string_random
const char charset[] = "0123456789abcdef";
  if (size) {
//        --size;
    size_t n;
    for (n = 0; n < size; n++) {
      if (str[n]!='-'){
        if ( n==8 || n==13 || n==18 || n==23)
          str[n] = '-';
        else
          str[n] = charset[rand() % (int) (sizeof charset - 1)];
      }
    }
    str[size] = '\0';
 }
#else
  gchar *uuid=g_uuid_string_random();
  g_strlcpy(str, uuid, strlen(str)+1);
  g_free(uuid);
#endif
}

void random_uuid_function(GString *str, gchar *row,gulong* length, struct function_pointer *fp){
  random_basic_function(str,row,length,fp,&m_random_uuid);
}

gboolean apply_format_item(GString *str, gchar *row, struct format_item *fi){
  struct format_item_file *fid=NULL;
  gboolean cont=TRUE;
  gulong local_len;
  struct regex_item *ri=NULL;
  GString *new_r=NULL;

  switch (fi->type){
    case FORMAT_ITEM_FILE:
      fid = fi->data;
      guint random_length = (guint) g_random_int_range(fid->min, fid->max + 1);     
      GList *list_of_string_of_lenght = (GList *) g_hash_table_lookup((GHashTable *)fid->data,GINT_TO_POINTER(/*final_*/random_length));
      gchar *rnd_item_in_list=g_list_nth_data(list_of_string_of_lenght, g_random_int_range(0,g_list_length(list_of_string_of_lenght)));
      g_string_append(str, rnd_item_in_list);
      break;
    case FORMAT_ITEM_CONFIG_FILE:
      break;
    case FORMAT_ITEM_CONSTANT:
      g_string_append(str, fi->data);
      break;
    case FORMAT_ITEM_DELIMITER:
      g_string_append(str, fi->data);
      cont=FALSE;
      break;
    case FORMAT_ITEM_NUMBER:
      local_len=str->len;
      g_string_set_size(str, str->len+fi->len);
      m_random_int(&(str->str[local_len]), fi->len); 
      break;
    case FORMAT_ITEM_STRING:
      local_len=str->len;
      g_string_set_size(str, str->len+fi->len);
      m_random_string(&(str->str[local_len]), fi->len);
      break;
    case FORMAT_ITEM_REGEX:
      ri=(struct regex_item *)fi->data;
      GString * tmp_replacement=g_string_new_len("",1024);
      g_string_set_size(tmp_replacement, 0); 
      apply_format_item(tmp_replacement, tmp_replacement->str, ri->fi);
      PCRE2_UCHAR outputbuffer[REGEX_MAX_LEN];
      PCRE2_SIZE outlen=REGEX_MAX_LEN;
      if (!str->len)
        g_string_append(str,row);
      int rc = pcre2_substitute(*(ri->re), (PCRE2_SPTR)(str->str), str->len, 0, PCRE2_SUBSTITUTE_GLOBAL | PCRE2_SUBSTITUTE_EXTENDED, NULL, NULL, (PCRE2_SPTR)(tmp_replacement->str), tmp_replacement->len, outputbuffer, &outlen);
      if (rc < 0){
        g_critical("Error found on pcre2_substitute: %s | %s", new_r->str, tmp_replacement->str);
      }
      g_string_insert(str, 0, (gchar*)outputbuffer);
      g_string_set_size(str, outlen);
      break;
  }
  return cont;
}


void random_format_function(GString *str, gchar *row, gulong* max_len, struct function_pointer *fp){
  // max_len is ignored in this case
  (void) max_len;
  GList *l=fp->parse;
  GList *d=fp->delimiters;
  struct format_item *fi=NULL;
  gboolean cont=TRUE;
  while (l !=NULL ){ //&& pos_in_string < *max_len){
    // Delimiter could be implemented/simulated with regex.
    cont=TRUE;
    while (l !=NULL && cont ){ //&& pos_in_string < *max_len){
      fi=l->data;
      cont=apply_format_item(str, row, fi);
      l=l->next; 
    }
    if (d != NULL){
      d=d->next;
    }
  } 
}

void regex_function(GString *str, gchar *row, gulong* max_len, struct function_pointer *fp){
  pcre2_code *tre=NULL;
  GList *l=fp->parse;
  //GString *new_r= g_string_new(*r);
  pcre2_match_data *match_data = NULL;
  PCRE2_UCHAR outputbuffer[1024];
  PCRE2_SPTR replacement=NULL;
  size_t rlength=0;
  PCRE2_SIZE outlen=1024;
  while (l){
    tre=l->data;
    match_data=pcre2_match_data_create_from_pattern(tre, NULL);
    l=l->next;
    replacement=l->data;
    l=l->next;
    rlength = strlen((gchar *)replacement);
    int rc =pcre2_substitute(tre, (PCRE2_SPTR)row, strlen(row), 0, PCRE2_SUBSTITUTE_GLOBAL | PCRE2_SUBSTITUTE_EXTENDED, match_data, NULL, replacement, rlength, outputbuffer, &outlen);
    if (rc < 0){
      g_critical("Error found on pcre2_substitute: %s | %s", row, replacement);
    }
    g_string_printf(str, "%s", outputbuffer);
  }
  *max_len=str->len;
//  g_message("new_r->str: %s",new_r->str );
}

void apply_function(GString *str, gchar *row, gulong* max_len, struct function_pointer *fp){
  if (g_list_length(fp->parse)==2){
    g_string_printf(str,"%s%s%s",(gchar *) fp->parse->data, row, (gchar *) fp->parse->next->data);
  }else
    g_string_printf(str,"%s%s", (gchar *) fp->parse->data, row);
  *max_len=str->len;
}

void constant_function(GString *str, gchar *row, gulong* max_len, struct function_pointer *fp){
  (void)row;
  g_string_assign(str, fp->parse->data);
  *max_len=str->len;
}
//
// Function parsers
//

void parse_basic(struct function_pointer * fp, gchar *val){
  char buffer[256];
  guint i;
  while (*val != '\0'){
    while(*val == ' ')
      val++;
    i=0;
    while(*val != '\0' && *val != ' '){
      buffer[i]=*val;
      val++;
      i++;
    }
    buffer[i]='\0';
    if (g_str_has_prefix(buffer,"WITH_MEM")){
      fp->memory=g_hash_table_new ( g_str_hash, g_str_equal );
    }else if (g_str_has_prefix(buffer,"REPLACE_NULL")){
      fp->replace_null=TRUE;
      val++;
      i=0;
      while(*val != '\0' && *val != ' '){
        buffer[i]=*val;
        val++;
        i++;
      }
      buffer[i]='\0';
      fp->null_max_length=atoi(buffer);
    }else if (g_str_has_prefix(buffer,"UNIQUE")){
      fp->unique=TRUE;
    }else if (g_str_has_prefix(buffer,"MAX_LENGTH")){
      val++;
      i=0;
      while(*val != '\0' && *val != ' '){
        buffer[i]=*val;
        val++;
        i++;
      }
      buffer[i]='\0';
      fp->max_length=atoi(buffer);
    }
  }
}

void parse_regex_function(struct function_pointer * fp, gchar *val){
  char buffer[256];
  guint i=0;
  pcre2_code **re = NULL;
  gboolean even=TRUE;
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
      if (even){
        re=g_new0(pcre2_code *,1);
        init_regex(re,buffer);
        fp->parse=g_list_append(fp->parse,*re);
        even=FALSE;
      }else{
        fp->parse=g_list_append(fp->parse,g_strdup(buffer));
        even=TRUE;
      }

    }
    val++;
    while(*val == ' ')
      val++;
  }

  if (g_list_length(fp->parse)%2 != 0)
    g_error("Parsing regex function failed. Elements found: %d but even amount of elements are allowed", g_list_length(fp->parse));

}

void parse_apply_function(struct function_pointer * fp, gchar *val){
  char buffer[256];
  guint i=0;
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
      fp->parse=g_list_append(fp->parse,g_strdup(buffer));
    }
    val++;
    while(*val == ' ')
      val++;
  }

  if (g_list_length(fp->parse)>2 || g_list_length(fp->parse)==0)
    g_error("Parsing apply function failed. Elements found: %d but only 1 or 2 are allowed", g_list_length(fp->parse));

}

void parse_constant_function(struct function_pointer * fp, gchar *val){
  fp->parse=g_list_append(fp->parse,g_strdup(val));
}

void parse_random_format(struct function_pointer * fp, gchar *val){
  char buffer[256];
  guint i=0;
  struct format_item *fi=NULL,*regex_fi=NULL;
  GList *keys=NULL, *sorted=NULL;
  guint sum;
  GString *regex_content=g_string_sized_new(100);
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

      if (regex_fi){
        ((struct regex_item *)regex_fi->data)->fi=fi;
        fp->parse=g_list_append(fp->parse,regex_fi);
        regex_fi=NULL;
      }else
        fp->parse=g_list_append(fp->parse,fi);

      val++;
    }else if (*val == '<'){
      val++;
      i=0;
      while (*val != '\0' && *val!='>' && *val!=' '){ 
        buffer[i]=*val;
        i++;
        val++;
      }
      if (*val == ' '){
        buffer[i]=*val;
        i++;
        val++;
        while (*val == ' '){
          val++;
        }
        if (g_str_has_prefix(buffer,"regex ")){
          if ( *val == '\''){
            val++;
            while (( ( *(val-1) == '\\' && *val == '\'' ) || *val != '\'' ) && *val != '\0'){

              g_string_append_c(regex_content,*val);
              val++;
            }
            if (*val == '\0')
              g_error("Incorrect format, EOF found");
          }else
            g_error("Missing initial quote (') on regex");
        }

        while (*val != '\0' && *val!='>'){
          buffer[i]=*val;
          i++;
          val++;
        }
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
          if (regex_fi){
            fp->parse=g_list_append(fp->parse,regex_fi);
            regex_fi->data=fi;
            regex_fi=NULL;
          }else
            fp->parse=g_list_append(fp->parse,fi);
          g_list_free(keys);
        }else if (g_str_has_prefix(buffer,"string ")){
          fi->type=FORMAT_ITEM_STRING;
          fi->len=g_ascii_strtoull(&(buffer[7]), NULL, 10);
          if (regex_fi){
            ((struct regex_item *)regex_fi->data)->fi=fi;
            fp->parse=g_list_append(fp->parse,regex_fi);
            regex_fi=NULL;
          }else
            fp->parse=g_list_append(fp->parse,fi);
        }else if (g_str_has_prefix(buffer,"number ")){
          fi->type=FORMAT_ITEM_NUMBER;
          fi->len=g_ascii_strtoull(&(buffer[7]), NULL, 10);
          if (regex_fi){
            ((struct regex_item *)regex_fi->data)->fi=fi;
            fp->parse=g_list_append(fp->parse,regex_fi);
            regex_fi=NULL;
          }else
            fp->parse=g_list_append(fp->parse,fi);
        }else if (g_str_has_prefix(buffer,"regex ")){
          fi->type=FORMAT_ITEM_REGEX;
          if (regex_fi){
            g_critical("2 consectutive regex was found. It is not possible.");
          }

          pcre2_code **re=g_new0(pcre2_code *,1);
          init_regex(re,regex_content->str);
          g_string_set_size(regex_content, 0);
          struct regex_item *ri=g_new0(struct regex_item, 1);
          ri->re=re;
          fi->data=ri;
          regex_fi=fi;

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

// Function initializer

fun_ptr get_function_pointer_for (gchar *function_char){
  if (g_str_has_prefix(function_char,"random_format"))
    return &random_format_function;

  if (g_str_has_prefix(function_char,"random_string"))
    return &random_string_function;

  if (g_str_has_prefix(function_char,"random_int"))
    return &random_int_function;

  if (g_str_has_prefix(function_char,"random_uuid"))
    return &random_uuid_function;

  if (g_str_has_prefix(function_char,"apply"))
    return &apply_function;

  if (g_str_has_prefix(function_char,"constant"))
    return &constant_function;

  if (g_str_has_prefix(function_char,"regex"))
    return &regex_function;

  // TODO: more functions needs to be added.
  if (!g_strcmp0(function_char,""))
    return &identity_function;
  if (!g_strcmp0(function_char,""))
    return &identity_function;
  g_message("Function not found: Using default");
  return &identity_function;
}

struct function_pointer * init_function_pointer(gchar *value){
  struct function_pointer * fp= g_new0(struct function_pointer, 1);
  fp->function=get_function_pointer_for(value);
  fp->memory=NULL;
  fp->replace_null=FALSE;
  fp->value=value;
  fp->parse=NULL;
  fp->max_length=0;
  fp->null_max_length=2;
  fp->delimiters=NULL;
  fp->is_pre=FALSE;
  fp->unique=FALSE;
  fp->unique_list=NULL;
  g_debug("init_function_pointer: %s", value);
  if (g_str_has_prefix(value,"random_format")){
    parse_random_format(fp, g_strdup(&(fp->value[14])));
  }else
  if (g_str_has_prefix(value,"apply")){
    fp->is_pre=TRUE;
    parse_apply_function(fp, g_strdup(&(fp->value[6])));
  }else
  if (g_str_has_prefix(value,"constant")){
    fp->is_pre=TRUE;
    parse_constant_function(fp, g_strdup(&(fp->value[9])));
  }else
  if (g_str_has_prefix(value,"regex")){
//    fp->is_pre=TRUE;
    parse_regex_function(fp, g_strdup(&(fp->value[6])));
  }else{
    // Perf: Use strchr instead of g_strstr_len for single character search (SIMD optimized)
    gchar *space = strchr(fp->value, ' ');
    if (space)
      parse_basic(fp, g_strdup(space));
  }
  return fp;
}

