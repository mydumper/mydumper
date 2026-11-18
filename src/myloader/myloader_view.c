/* Rewrite schema identifiers in view DDL without changing literals or aliases. */
#include <string.h>

#include "myloader_view.h"

struct view_token
{
  gsize  start, end;
  gchar *name;
  gchar  punctuation, quote;
};

struct view_context
{
  gboolean query, from, relation;
};

static gboolean word_byte(guchar c)
{
  return g_ascii_isalnum(c) || c == '_' || c == '$' || c >= 128;
}

static gboolean keyword(struct view_token *token, const gchar *word)
{
  return token->name && !token->quote && !g_ascii_strcasecmp(token->name, word);
}

static GArray *tokenize_view(const gchar *sql, gchar quote, gboolean no_backslash_escapes)
{
  GArray *tokens = g_array_new(FALSE, FALSE, sizeof(struct view_token));
  gsize   i = 0;
  while (sql[i])
  {
    if (g_ascii_isspace(sql[i]))
    {
      i++;
      continue;
    }
    if (sql[i] == '#' || (sql[i] == '-' && sql[i + 1] == '-' && g_ascii_isspace(sql[i + 2])))
    {
      while (sql[i] && sql[i] != '\n')
        i++;
      continue;
    }
    if (sql[i] == '/' && sql[i + 1] == '*')
    {
      i += 2;
      if (sql[i] == '!' || (sql[i] == 'M' && sql[i + 1] == '!'))
      {
        i += sql[i] == '!' ? 1 : 2;
        while (g_ascii_isdigit(sql[i]))
          i++;
      }
      else
      {
        while (sql[i] && !(sql[i] == '*' && sql[i + 1] == '/'))
          i++;
        if (sql[i])
          i += 2;
      }
      continue;
    }
    if (sql[i] == '*' && sql[i + 1] == '/')
    {
      i += 2;
      continue;
    }
    struct view_token token = {0};
    token.start = i;
    if (sql[i] == '`' || sql[i] == '\'' || sql[i] == '"')
    {
      gchar    delimiter = sql[i++];
      gboolean identifier = delimiter == '`' || (delimiter == '"' && quote == '"');
      GString *name = g_string_new(NULL);
      while (sql[i])
      {
        if (sql[i] == delimiter)
        {
          if (sql[i + 1] != delimiter)
          {
            i++;
            break;
          }
          i++;
        }
        else if (!identifier && !no_backslash_escapes && sql[i] == '\\' && sql[i + 1])
        {
          i++;
        }
        g_string_append_c(name, sql[i++]);
      }
      if (identifier)
      {
        token.name = g_string_free(name, FALSE);
        token.quote = delimiter;
      }
      else
        g_string_free(name, TRUE);
    }
    else if (word_byte(sql[i]))
    {
      while (word_byte(sql[i]))
        i++;
      token.name = g_strndup(sql + token.start, i - token.start);
    }
    else
      token.punctuation = sql[i++];
    token.end = i;
    g_array_append_val(tokens, token);
  }
  return tokens;
}

void rewrite_view_database_references(GString *sql, const gchar *source, const gchar *target, GHashTable *mapping, gchar quote, gboolean no_backslash_escapes)
{
  GArray             *tokens = tokenize_view(sql->str, quote, no_backslash_escapes);
  GArray             *stack = g_array_new(FALSE, FALSE, sizeof(struct view_context));
  struct view_context context = {0};
  g_array_append_val(stack, context);
  GString *output = g_string_sized_new(sql->len);
  gsize    copied = 0;
  gboolean view = FALSE, view_name = FALSE;
  for (guint i = 0; i < tokens->len; i++)
  {
    struct view_token *t = &g_array_index(tokens, struct view_token, i);
    if (!view)
    {
      if (keyword(t, "VIEW"))
        view = view_name = TRUE;
      continue;
    }
    struct view_context *current = &g_array_index(stack, struct view_context, stack->len - 1);
    if (t->punctuation == '(')
    {
      context = (struct view_context){FALSE, current->relation, current->relation};
      current->relation = FALSE;
      g_array_append_val(stack, context);
      continue;
    }
    if (t->punctuation == ')')
    {
      if (stack->len > 1)
        g_array_set_size(stack, stack->len - 1);
      continue;
    }
    if (keyword(t, "SELECT"))
    {
      *current = (struct view_context){TRUE, FALSE, FALSE};
      continue;
    }
    if ((keyword(t, "FROM") && current->query) ||
        ((keyword(t, "JOIN") || keyword(t, "STRAIGHT_JOIN")) && current->from))
    {
      current->from = current->relation = TRUE;
      continue;
    }
    if (keyword(t, "WHERE") || keyword(t, "GROUP") || keyword(t, "HAVING") ||
        keyword(t, "ORDER") || keyword(t, "LIMIT") || keyword(t, "UNION") ||
        keyword(t, "EXCEPT") || keyword(t, "INTERSECT") || keyword(t, "WINDOW"))
      current->from = current->relation = FALSE;
    if (t->punctuation == ',' && current->from)
      current->relation = TRUE;
    if (!t->name || keyword(t, "LATERAL") ||
        (view_name && (keyword(t, "IF") || keyword(t, "NOT") || keyword(t, "EXISTS"))))
      continue;
    if (i + 2 < tokens->len && g_array_index(tokens, struct view_token, i + 1).punctuation == '.' &&
        g_array_index(tokens, struct view_token, i + 2).name &&
        (!i || g_array_index(tokens, struct view_token, i - 1).punctuation != '.'))
    {
      gchar following = i + 3 < tokens->len ? g_array_index(tokens, struct view_token, i + 3).punctuation : 0;
      /* Two-part column names can be aliases, even when the alias equals the source schema. */
      if (view_name || current->relation || following == '.' || following == '(')
      {
        const gchar *replacement = mapping ? g_hash_table_lookup(mapping, t->name) : NULL;
        if (!replacement && !g_strcmp0(t->name, source))
          replacement = target;
        if (replacement && strcmp(replacement, t->name))
        {
          gchar delimiter = t->quote ? t->quote : quote;
          g_string_append_len(output, sql->str + copied, t->start - copied);
          g_string_append_c(output, delimiter);
          for (const gchar *p = replacement; *p; p++)
          {
            g_string_append_c(output, *p);
            if (*p == delimiter)
              g_string_append_c(output, *p);
          }
          g_string_append_c(output, delimiter);
          copied = t->end;
        }
      }
    }
    view_name = FALSE;
    current->relation = FALSE;
  }
  if (copied)
  {
    g_string_append_len(output, sql->str + copied, sql->len - copied);
    g_string_assign(sql, output->str);
  }
  g_string_free(output, TRUE);
  for (guint i = 0; i < tokens->len; i++)
    g_free(g_array_index(tokens, struct view_token, i).name);
  g_array_free(tokens, TRUE);
  g_array_free(stack, TRUE);
}
