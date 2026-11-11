#include <glib.h>
#include <string.h>

#include "myloader_encryption_utils.h"

gboolean remove_encryption_option_from_create_table(GString *data)
{
  static const gchar option[] = " ENCRYPTION='N'";
  const gsize        option_len = sizeof(option) - 1;

  if (!data || !g_str_has_prefix(data->str, "CREATE TABLE "))
    return FALSE;

  // Ignore trailing whitespace and an optional statement terminator: myloader
  // splits schema files on ";\n", so statements usually arrive without ';'.
  gsize end = data->len;
  while (end > 0 && (data->str[end - 1] == '\n' || data->str[end - 1] == '\r' || data->str[end - 1] == ' '))
    end--;
  if (end > 0 && data->str[end - 1] == ';')
    end--;
  if (end < option_len || memcmp(data->str + end - option_len, option, option_len) != 0)
    return FALSE;

  g_string_erase(data, end - option_len, option_len);
  return TRUE;
}
