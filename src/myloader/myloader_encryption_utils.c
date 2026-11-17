#include <glib.h>
#include <string.h>

#include "myloader_encryption_utils.h"

gboolean parse_remove_encryption_option_mode(const gchar *value, enum remove_encryption_option_mode *mode)
{
  if (value == NULL || !g_strcmp0(value, "") || !g_strcmp0(value, "1") || !g_ascii_strcasecmp(value, "ALWAYS") || !g_ascii_strcasecmp(value, "ON"))
  {
    *mode = REMOVE_ENCRYPTION_OPTION_ALWAYS;
    return TRUE;
  }
  if (!g_strcmp0(value, "0") || !g_ascii_strcasecmp(value, "NEVER") || !g_ascii_strcasecmp(value, "OFF"))
  {
    *mode = REMOVE_ENCRYPTION_OPTION_NEVER;
    return TRUE;
  }
  if (!g_ascii_strcasecmp(value, "AUTO"))
  {
    *mode = REMOVE_ENCRYPTION_OPTION_AUTO;
    return TRUE;
  }
  return FALSE;
}

const gchar *remove_encryption_option_mode_to_str(enum remove_encryption_option_mode mode)
{
  switch (mode)
  {
    case REMOVE_ENCRYPTION_OPTION_ALWAYS:
      return "ALWAYS";
    case REMOVE_ENCRYPTION_OPTION_NEVER:
      return "NEVER";
    case REMOVE_ENCRYPTION_OPTION_AUTO:
    default:
      return "AUTO";
  }
}

gboolean should_remove_encryption_option(enum remove_encryption_option_mode mode, gboolean target_is_mariadb)
{
  switch (mode)
  {
    case REMOVE_ENCRYPTION_OPTION_ALWAYS:
      return TRUE;
    case REMOVE_ENCRYPTION_OPTION_NEVER:
      return FALSE;
    case REMOVE_ENCRYPTION_OPTION_AUTO:
    default:
      return target_is_mariadb;
  }
}

gboolean remove_encryption_option_from_create_table(GString *data)
{
  static const gchar option[] = " ENCRYPTION='N'";
  const gsize        option_len = sizeof(option) - 1;

  if (!data || !g_strstr_len(data->str, data->len, "CREATE TABLE "))
    return FALSE;

  // Ignore trailing whitespace and an optional statement terminator: myloader
  // splits schema files on ";\n", so statements may arrive without ';'.
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
