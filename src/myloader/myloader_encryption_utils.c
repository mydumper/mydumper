#include <glib.h>
#include <string.h>

#include "myloader_encryption_utils.h"

gboolean parse_encryption_option_mode(const gchar *value, enum encryption_option_mode *mode)
{
  if (!g_ascii_strcasecmp(value ? value : "", "CONVERT"))
  {
    *mode = ENCRYPTION_OPTION_CONVERT;
    return TRUE;
  }
  if (!g_ascii_strcasecmp(value ? value : "", "REMOVE"))
  {
    *mode = ENCRYPTION_OPTION_REMOVE;
    return TRUE;
  }
  if (!g_ascii_strcasecmp(value ? value : "", "KEEP"))
  {
    *mode = ENCRYPTION_OPTION_KEEP;
    return TRUE;
  }
  return FALSE;
}

const gchar *encryption_option_mode_to_str(enum encryption_option_mode mode)
{
  switch (mode)
  {
    case ENCRYPTION_OPTION_REMOVE:
      return "REMOVE";
    case ENCRYPTION_OPTION_KEEP:
      return "KEEP";
    case ENCRYPTION_OPTION_CONVERT:
    default:
      return "CONVERT";
  }
}

table_option_modifier_func get_table_encryption_modifier(enum encryption_option_mode mode, gboolean target_is_mariadb)
{
  switch (mode)
  {
    case ENCRYPTION_OPTION_REMOVE:
      return &remove_table_encryption_options;
    case ENCRYPTION_OPTION_KEEP:
      return NULL;
    case ENCRYPTION_OPTION_CONVERT:
    default:
      return target_is_mariadb ? &convert_table_encryption_options_to_mariadb : &convert_table_encryption_options_to_mysql;
  }
}

table_option_modifier_func get_database_encryption_modifier(enum encryption_option_mode mode, gboolean target_is_mariadb)
{
  switch (mode)
  {
    case ENCRYPTION_OPTION_REMOVE:
      return &remove_database_encryption_option;
    case ENCRYPTION_OPTION_KEEP:
      return NULL;
    case ENCRYPTION_OPTION_CONVERT:
    default:
      // MariaDB has no database encryption option
      return target_is_mariadb ? &remove_database_encryption_option : NULL;
  }
}

enum table_option_name
{
  TABLE_OPTION_OTHER,
  TABLE_OPTION_ENGINE,
  TABLE_OPTION_ENCRYPTION,         // MySQL/Percona
  TABLE_OPTION_ENCRYPTED,          // MariaDB
  TABLE_OPTION_ENCRYPTION_KEY_ID,  // MariaDB
};

struct table_option
{
  enum table_option_name name;
  gsize                  start;  // the space before the option
  gsize                  end;    // after the value
  gchar                  value[16];
};

static enum table_option_name table_option_name(const gchar *name, gsize len)
{
  static const struct
  {
    const gchar           *name;
    enum table_option_name id;
  } names[] = {
      {"ENGINE", TABLE_OPTION_ENGINE},
      {"ENCRYPTION", TABLE_OPTION_ENCRYPTION},
      {"ENCRYPTED", TABLE_OPTION_ENCRYPTED},
      {"ENCRYPTION_KEY_ID", TABLE_OPTION_ENCRYPTION_KEY_ID},
  };
  for (guint n = 0; n < G_N_ELEMENTS(names); n++)
    if (strlen(names[n].name) == len && !g_ascii_strncasecmp(name, names[n].name, len))
      return names[n].id;
  return TABLE_OPTION_OTHER;
}

static gboolean is_word_char(gchar c)
{
  return g_ascii_isalnum(c) || c == '_';
}

/*
  Parses a `NAME`=value or NAME = 'value' table option starting at the space
  data->str[pos]. Returns FALSE if there is no option there.
*/
static gboolean parse_table_option(GString *data, gsize pos, struct table_option *opt)
{
  gsize i = pos;
  while (i < data->len && data->str[i] == ' ')
    i++;
  gboolean backtick = i < data->len && data->str[i] == '`';
  if (backtick)
    i++;
  gsize name_start = i;
  while (i < data->len && is_word_char(data->str[i]))
    i++;
  if (i == name_start)
    return FALSE;
  gsize name_len = i - name_start;
  if (backtick)
  {
    if (i >= data->len || data->str[i] != '`')
      return FALSE;
    i++;
  }
  while (i < data->len && data->str[i] == ' ')
    i++;
  if (i >= data->len || data->str[i] != '=')
    return FALSE;
  i++;
  while (i < data->len && data->str[i] == ' ')
    i++;
  gsize value_start, value_end;
  if (i < data->len && (data->str[i] == '\'' || data->str[i] == '"'))
  {
    // quoted value, with '' or \' escaping
    gchar quote = data->str[i];
    value_start = ++i;
    for (; i < data->len; i++)
    {
      if (data->str[i] == '\\')
        i++;
      else if (data->str[i] == quote)
      {
        if (i + 1 < data->len && data->str[i + 1] == quote)
          i++;
        else
          break;
      }
    }
    if (i >= data->len)
      return FALSE;
    value_end = i++;
  }
  else
  {
    value_start = i;
    while (i < data->len && is_word_char(data->str[i]))
      i++;
    if (i == value_start)
      return FALSE;
    value_end = i;
  }
  // the option has to end here, otherwise it is something else
  if (i < data->len && !(data->str[i] == ' ' || data->str[i] == ';' || data->str[i] == '\n' || data->str[i] == '\r'))
    return FALSE;

  opt->name = table_option_name(data->str + name_start, name_len);
  opt->start = pos;
  opt->end = i;
  g_strlcpy(opt->value, data->str + value_start, MIN(sizeof(opt->value), value_end - value_start + 1));
  return TRUE;
}

/*
  Finds the next table option from *pos to the end of the table options line.
  Quoted strings outside the options (e.g. inside versioned comments) are
  skipped.
*/
static gboolean next_table_option(GString *data, gsize *pos, struct table_option *opt)
{
  gchar quote = 0;
  for (gsize i = *pos; i < data->len; i++)
  {
    gchar c = data->str[i];
    if (quote)
    {
      if (c == '\\')
        i++;
      else if (c == quote)
        quote = 0;
      continue;
    }
    if (c == '\'' || c == '"')
      quote = c;
    // partitioning or the next statement starts on a new line
    else if (c == '\n' || c == ';')
      return FALSE;
    else if (c == ' ' && parse_table_option(data, i, opt))
    {
      *pos = opt->end;
      return TRUE;
    }
  }
  return FALSE;
}

/* The table options are on the line that closes the column definitions. */
static gboolean find_table_options(GString *data, gsize *start)
{
  if (!data)
    return FALSE;
  const gchar *create = g_strstr_len(data->str, data->len, "CREATE TABLE ");
  if (!create)
    return FALSE;
  const gchar *close = g_strstr_len(create, data->len - (create - data->str), "\n)");
  if (!close)
    return FALSE;
  *start = close - data->str + 2;
  return TRUE;
}

enum encryption_action
{
  ENCRYPTION_ACTION_REMOVE,
  ENCRYPTION_ACTION_TO_MARIADB,
  ENCRYPTION_ACTION_TO_MYSQL
};

static gboolean rewrite_table_encryption_options(GString *data, enum encryption_action action)
{
  gsize start;
  if (!find_table_options(data, &start))
    return FALSE;

  struct table_option opt;
  gsize               pos = start;
  // Without ENGINE the table uses the default engine, which is InnoDB
  gboolean innodb = TRUE;
  while (next_table_option(data, &pos, &opt))
    if (opt.name == TABLE_OPTION_ENGINE)
    {
      innodb = !g_ascii_strcasecmp(opt.value, "InnoDB");
      break;
    }

  gboolean changed = FALSE;
  pos = start;
  while (next_table_option(data, &pos, &opt))
  {
    gboolean     yes = !g_ascii_strcasecmp(opt.value, "Y") || !g_ascii_strcasecmp(opt.value, "YES");
    gboolean     no = !g_ascii_strcasecmp(opt.value, "N") || !g_ascii_strcasecmp(opt.value, "NO");
    gboolean     rewrite = FALSE;
    const gchar *replacement = NULL;
    switch (opt.name)
    {
      case TABLE_OPTION_ENCRYPTION:
        if (action == ENCRYPTION_ACTION_REMOVE)
          rewrite = TRUE;
        else if (action == ENCRYPTION_ACTION_TO_MARIADB)
        {
          // MariaDB only knows ENCRYPTED on InnoDB, e.g. ENCRYPTION='N' of the
          // MEMORY view dependency tables is removed
          rewrite = TRUE;
          if (innodb)
            replacement = yes ? " `ENCRYPTED`=YES" : (no ? " `ENCRYPTED`=NO" : NULL);
        }
        break;
      case TABLE_OPTION_ENCRYPTED:
        if (action == ENCRYPTION_ACTION_REMOVE)
          rewrite = TRUE;
        else if (action == ENCRYPTION_ACTION_TO_MYSQL)
        {
          // `ENCRYPTED`=DEFAULT has no equivalent: the default is used
          rewrite = TRUE;
          replacement = yes && innodb ? " ENCRYPTION='Y'" : (no ? " ENCRYPTION='N'" : NULL);
        }
        break;
      case TABLE_OPTION_ENCRYPTION_KEY_ID:
        if (action != ENCRYPTION_ACTION_TO_MARIADB)
        {
          rewrite = TRUE;
          if (action == ENCRYPTION_ACTION_TO_MYSQL)
          {
            static gint warned = 0;
            if (g_atomic_int_compare_and_exchange(&warned, 0, 1))
              g_warning("ENCRYPTION_KEY_ID is not supported by the target server, it was removed: encrypted tables will use its master key");
          }
        }
        break;
      default:
        break;
    }
    if (rewrite)
    {
      g_string_erase(data, opt.start, opt.end - opt.start);
      pos = opt.start;
      if (replacement)
      {
        g_string_insert(data, opt.start, replacement);
        pos += strlen(replacement);
      }
      changed = TRUE;
    }
  }
  return changed;
}

gboolean remove_table_encryption_options(GString *data)
{
  return rewrite_table_encryption_options(data, ENCRYPTION_ACTION_REMOVE);
}

gboolean convert_table_encryption_options_to_mariadb(GString *data)
{
  return rewrite_table_encryption_options(data, ENCRYPTION_ACTION_TO_MARIADB);
}

gboolean convert_table_encryption_options_to_mysql(GString *data)
{
  return rewrite_table_encryption_options(data, ENCRYPTION_ACTION_TO_MYSQL);
}

gboolean remove_database_encryption_option(GString *data)
{
  static GRegex *regex = NULL;
  if (g_once_init_enter(&regex))
  {
    // /*!80016 DEFAULT ENCRYPTION='N' */ as written by SHOW CREATE DATABASE,
    // or unversioned [DEFAULT] ENCRYPTION [=] 'Y'
    GRegex *r = g_regex_new(
        "[ \\t]*/\\*!\\d+\\s+(DEFAULT\\s+)?ENCRYPTION\\s*=?\\s*'[YN]'\\s*\\*/"
        "|\\s+(DEFAULT\\s+)?ENCRYPTION\\s*=?\\s*'[YN]'",
        G_REGEX_CASELESS | G_REGEX_RAW | G_REGEX_OPTIMIZE, 0, NULL);
    g_once_init_leave(&regex, r);
  }
  if (!data)
    return FALSE;
  const gchar *create = g_strstr_len(data->str, data->len, "CREATE DATABASE ");
  if (!create)
    create = g_strstr_len(data->str, data->len, "CREATE SCHEMA ");
  if (!create)
    return FALSE;
  gsize  offset = create - data->str;
  gchar *rewritten = g_regex_replace_literal(regex, data->str + offset, data->len - offset, 0, "", 0, NULL);
  if (!rewritten || strlen(rewritten) == data->len - offset)
  {
    g_free(rewritten);
    return FALSE;
  }
  g_string_truncate(data, offset);
  g_string_append(data, rewritten);
  g_free(rewritten);
  return TRUE;
}
