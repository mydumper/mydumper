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
#include "mydumper.h"

#define REGEX_MAX_LEN 2048

enum format_item_type {
  FORMAT_ITEM_FILE,
  FORMAT_ITEM_CONFIG_FILE,
  FORMAT_ITEM_STRING,
  FORMAT_ITEM_NUMBER,
  FORMAT_ITEM_DELIMITER,
  FORMAT_ITEM_CONSTANT,
  FORMAT_ITEM_REGEX
};

struct format_item_delimiter{
  

};


struct format_item_file{
  GHashTable * data;
  guint min;
  guint max;

};

struct format_item{
  enum format_item_type type;
  guint len;
  void * data;  // if type is FORMAT_ITEM_FILE then it is format_item_file
                // if type is FORMAT_ITEM_STRING then it is gchar* 
                // if type is FORMAT_ITEM_DELIMITER the string to match
                // if type is FORMAT_ITEM_REGEX is the next format_item
};



struct regex_item{
  pcre2_code **re;
  struct format_item *fi;  
};

void initialize_masquerade();
gchar * identity_function(gchar ** r, gulong* length, struct function_pointer *fp);
//fun_ptr get_function_pointer_for (gchar *function_char);
void finalize_masquerade();
struct function_pointer * init_function_pointer(gchar *value);
