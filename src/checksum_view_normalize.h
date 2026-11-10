/* Canonical expression trees for server-generated VIEW_DEFINITION text. */
#ifndef CHECKSUM_VIEW_NORMALIZE_H
#define CHECKSUM_VIEW_NORMALIZE_H

/* Length framing keeps tokens, literals and tree boundaries unambiguous. */
static gchar *view_node(const gchar *kind, const gchar *a, const gchar *b, const gchar *c)
{
  GString     *s = g_string_new(kind);
  const gchar *parts[] = {a, b, c};
  for (guint i = 0; i < G_N_ELEMENTS(parts); i++)
    if (parts[i])
      g_string_append_printf(s, "%" G_GSIZE_FORMAT ":%s", strlen(parts[i]), parts[i]);
  return g_string_free(s, FALSE);
}

typedef struct
{
  GPtrArray *tokens;
  guint      depth;
  guint      budget;
  gboolean   exhausted;
} ViewParser;

static const gchar *view_token(ViewParser *p, guint i)
{
  return i < p->tokens->len ? g_ptr_array_index(p->tokens, i) : "";
}

static gboolean view_is(ViewParser *p, guint i, const gchar *s)
{
  return !g_ascii_strcasecmp(view_token(p, i), s);
}

static gboolean view_word_in(const gchar *word, const gchar *words)
{
  gchar   *needle = g_strdup_printf("|%s|", word);
  gchar   *lower = g_ascii_strdown(needle, -1);
  gboolean found = strstr(words, lower) != NULL;
  g_free(lower);
  g_free(needle);
  return found;
}

static guint view_close(ViewParser *p, guint start, guint end)
{
  guint depth = 0;
  for (guint i = start; i < end; i++)
  {
    if (view_is(p, i, "("))
      depth++;
    if (view_is(p, i, ")") && !--depth)
      return i;
  }
  return end;
}

static gint view_precedence(const gchar *op)
{
  if (view_word_in(op, "|or|"))
    return 1;
  if (view_word_in(op, "|xor|"))
    return 2;
  if (view_word_in(op, "|and|"))
    return 3;
  if (view_word_in(op, "|between|not between|"))
    return 5;
  if (view_word_in(op, "|=|<=>|>=|>|<=|<|<>|!=|is|is not|"))
    return 4;
  if (view_word_in(op, "|like|not like|regexp|not regexp|in|not in|"))
    return 6;
  if (!strcmp(op, "|"))
    return 7;
  if (!strcmp(op, "&"))
    return 8;
  if (!strcmp(op, "<<") || !strcmp(op, ">>"))
    return 9;
  if (!strcmp(op, "+") || !strcmp(op, "-"))
    return 10;
  if (view_word_in(op, "|*|/|%|div|mod|"))
    return 11;
  if (!strcmp(op, "^"))
    return 12;
  if (!strcmp(op, "collate"))
    return 14;
  return 0;
}

static gchar *view_sequence(ViewParser *p, guint start, guint end);
static gchar *view_expression(ViewParser *p, guint *pos, guint end, gint minimum);

static gchar *view_atom(ViewParser *p, guint *pos, guint end)
{
  guint i = *pos;
  if (i >= end)
    return NULL;
  const gchar *token = view_token(p, i);
  gchar       *node = NULL;
  if (view_is(p, i, "exists") && view_is(p, i + 1, "("))
  {
    guint close = view_close(p, i + 1, end), query_end = close;
    if (close == end || !view_is(p, i + 2, "select"))
      return NULL;
    /* LIMIT 1 cannot change existence; OFFSET and LIMIT 0 can. */
    if (close >= i + 5 && view_is(p, close - 2, "limit") && view_is(p, close - 1, "1"))
      query_end -= 2;
    gchar *query = view_sequence(p, i + 2, query_end);
    node = view_node("exists", query, NULL, NULL);
    g_free(query);
    i = close + 1;
  }
  else if (view_is(p, i, "case"))
  {
    guint close = i + 1, depth = 1;
    for (; close < end; close++)
    {
      if (view_is(p, close, "case"))
        depth++;
      if (view_is(p, close, "end") && !--depth)
        break;
    }
    if (close == end)
      return NULL;
    gchar *body = view_sequence(p, i + 1, close);
    node = view_node("case", body, NULL, NULL);
    g_free(body);
    i = close + 1;
  }
  else if (view_is(p, i, "("))
  {
    guint close = view_close(p, i, end), inner = i + 1;
    if (close == end)
      return NULL;
    gboolean structural = FALSE;
    guint    nesting = 0;
    for (guint j = inner; j < close; j++)
    {
      if (view_is(p, j, "("))
        nesting++;
      else if (view_is(p, j, ")"))
        nesting--;
      else if (!nesting && view_word_in(view_token(p, j), "|select|join|straight_join|union|"))
        structural = TRUE;
    }
    /* Parse nested JOIN/query groups once, not as failed expression trees first. */
    if (!structural)
      node = view_expression(p, &inner, close, 1);
    if (!node || inner != close)
    {
      g_free(node);
      gchar *body = view_sequence(p, i + 1, close);
      node = view_node("group", body, NULL, NULL);
      g_free(body);
    }
    i = close + 1;
  }
  else if (view_is(p, i, "not") || view_is(p, i, "+") || view_is(p, i, "-") || view_is(p, i, "~"))
  {
    i++;
    gchar *operand = view_expression(p, &i, end, view_is(p, *pos, "not") ? 4 : 13);
    if (!operand)
      return NULL;
    node = view_node("unary", token, operand, NULL);
    g_free(operand);
  }
  else
  {
    if (!(g_ascii_isalnum(*token) || *token == '_' || *token == '$' || *token == '`' || *token == '\'' || *token == '"' || (guchar)*token >= 128))
      return NULL;
    if (view_word_in(token, "|select|from|where|as|on|join|left|right|inner|outer|cross|natural|straight_join|using|when|then|else|end|group|by|order|having|limit|offset|union|all|distinct|asc|desc|and|or|xor|between|in|is|like|regexp|div|mod|collate|escape|over|partition|rows|range|interval|"))
      return NULL;
    node = view_node("token", token, NULL, NULL);
    i++;
    while (i + 1 < end && view_is(p, i, "."))
    {
      gchar *next = view_node("qualified", node, view_token(p, i + 1), NULL);
      g_free(node);
      node = next;
      i += 2;
    }
    if (i < end && view_is(p, i, "("))
    {
      guint close = view_close(p, i, end);
      if (close == end)
      {
        g_free(node);
        return NULL;
      }
      gchar *args = view_sequence(p, i + 1, close);
      gchar *call = view_node("call", node, args, NULL);
      g_free(args);
      g_free(node);
      node = call;
      i = close + 1;
    }
  }
  *pos = i;
  return node;
}

static gchar *view_expression(ViewParser *p, guint *pos, guint end, gint minimum)
{
  if (p->exhausted)
    return NULL;
  if (++p->depth > 128 || !p->budget--)
  {
    p->exhausted = TRUE;
    p->depth--;
    return NULL;
  }
  gchar *left = view_atom(p, pos, end);
  while (left && *pos < end)
  {
    guint  i = *pos, width = 1;
    gchar *op = g_ascii_strdown(view_token(p, i), -1);
    if ((view_is(p, i, "not") && view_word_in(view_token(p, i + 1), "|in|like|regexp|between|")) ||
        (view_is(p, i, "is") && view_is(p, i + 1, "not")))
    {
      g_free(op);
      op = g_strdup_printf("%s %s", view_token(p, i), view_token(p, i + 1));
      gchar *lower = g_ascii_strdown(op, -1);
      g_free(op);
      op = lower;
      width = 2;
    }
    gint precedence = view_precedence(op);
    if (!precedence || precedence < minimum)
    {
      g_free(op);
      break;
    }
    i += width;
    gchar *right = view_expression(p, &i, end, precedence + 1);
    if (!right)
    {
      g_free(op);
      break;
    }
    if (view_word_in(op, "|between|not between|"))
    {
      if (!view_is(p, i, "and"))
      {
        g_free(right);
        g_free(op);
        break;
      }
      i++;
      gchar *upper = view_expression(p, &i, end, precedence);
      if (!upper)
      {
        g_free(right);
        g_free(op);
        break;
      }
      gchar *bounds = view_node("bounds", right, upper, NULL);
      g_free(right);
      g_free(upper);
      right = bounds;
    }
    gchar *next = view_node("binary", op, left, right);
    g_free(left);
    g_free(right);
    g_free(op);
    left = next;
    *pos = i;
  }
  p->depth--;
  return left;
}

static gchar *view_sequence(ViewParser *p, guint start, guint end)
{
  GString *out = g_string_new(NULL);
  for (guint i = start; i < end;)
  {
    gchar *node = view_expression(p, &i, end, 1);
    if (!node)
      node = view_node("raw", view_token(p, i++), NULL, NULL);
    g_string_append_printf(out, "%" G_GSIZE_FORMAT ":%s", strlen(node), node);
    g_free(node);
    if (p->exhausted)
      break;
  }
  return g_string_free(out, FALSE);
}

static gchar *normalize_view_expressions(const gchar *sql, gboolean no_backslash_escapes, gboolean ansi_quotes, gboolean special_precedence)
{
  ViewParser   parser = {g_ptr_array_new_with_free_func(g_free), 0, 0, FALSE};
  const gchar *p = sql;
  gboolean     supported = !special_precedence;
  while (*p && supported)
  {
    if (g_ascii_isspace(*p))
    {
      p++;
      continue;
    }
    const gchar *start = p;
    if (*p == '#' || (p[0] == '-' && (p[1] == '-' || p[1] == '>')) || (p[0] == '/' && p[1] == '*'))
    {
      supported = FALSE;
      break;
    }
    if (*p == '\'' || *p == '"' || *p == '`')
    {
      gchar    quote = *p++;
      gboolean string = quote == '\'' || (quote == '"' && !ansi_quotes), closed = FALSE;
      while (*p)
      {
        if (*p == '\\' && string && !no_backslash_escapes && p[1])
          p += 2;
        else if (*p++ == quote)
        {
          if (*p != quote)
          {
            closed = TRUE;
            break;
          }
          p++;
        }
      }
      if (!closed)
        supported = FALSE;
    }
    else if (g_ascii_isalnum(*p) || *p == '_' || *p == '$' || (guchar)*p >= 128)
    {
      while (g_ascii_isalnum(*p) || *p == '_' || *p == '$' || (guchar)*p >= 128)
        p++;
      gchar *word = g_strndup(start, p - start);
      /* shortcut: retain special SQL grammars verbatim until each has parser tests. */
      if (view_word_in(word, "|interval|binary|sounds|escape|member|over|match|against|with|window|extract|trim|position|"))
        supported = FALSE;
      g_free(word);
    }
    else
    {
      p++;
      if ((start[0] == '<' && (*p == '=' || *p == '>' || *p == '<')) ||
          (start[0] == '>' && (*p == '=' || *p == '>')) ||
          (start[0] == '!' && *p == '='))
        p++;
      if (p - start == 2 && start[0] == '<' && start[1] == '=' && *p == '>')
        p++;
      /* shortcut: retain unsupported operators verbatim until their SQL-mode rules have tests. */
      if (start[0] == '!' && p - start == 1)
        supported = FALSE;
      if ((start[0] == '|' && *p == '|') || (start[0] == '&' && *p == '&'))
        supported = FALSE;
    }
    g_ptr_array_add(parser.tokens, g_strndup(start, p - start));
  }
  parser.budget = parser.tokens->len * 64 + 1;
  gchar *result = supported ? view_sequence(&parser, 0, parser.tokens->len) : NULL;
  if (!supported || parser.exhausted)
  {
    g_free(result);
    result = view_node("verbatim", sql, NULL, NULL);
  }
  g_ptr_array_free(parser.tokens, TRUE);
  return result;
}
#endif
