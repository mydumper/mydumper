/* Lexical normalization tests; no server, dump, or restore is required. */
#include "../../src/checksum.c"

struct M_ROW *m_store_result_single_row(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)conn;
  (void)query;
  (void)fmt;
  g_assert_not_reached();
}

void m_store_result_row_free(struct M_ROW *mr) { (void)mr; }
void trace(const char *fmt, ...) { (void)fmt; }
void print_bool(const char *key, gboolean val)
{
  (void)key;
  (void)val;
}
int  get_product(void) { return 0; }
int  get_major(void) { return 0; }
int  get_secondary(void) { return 0; }
void m_error(const char *fmt, ...)
{
  (void)fmt;
  g_assert_not_reached();
}
gboolean machine_log_json_enabled(void) { return FALSE; }
void     machine_log_event(const gchar *domain, GLogLevelFlags level, ...)
{
  (void)domain;
  (void)level;
}

static void check(const gchar *sql, const gchar *database, gboolean no_backslash, gboolean ansi, const gchar *expected)
{
  gchar *actual = normalize_view_definition(sql, database, no_backslash, ansi);
  g_assert_cmpstr(actual, ==, expected);
  g_free(actual);
}

static void pair(const gchar *left, const gchar *right, gboolean equal)
{
  gchar *a = normalize_view_expressions(left, FALSE, FALSE, FALSE);
  gchar *b = normalize_view_expressions(right, FALSE, FALSE, FALSE);
  if (equal)
    g_assert_cmpstr(a, ==, b);
  else
    g_assert_cmpstr(a, !=, b);
  g_free(a);
  g_free(b);
}

int main(void)
{
  check("select 'db', `db`.`t`.`c` from `db`.`t`", "db", FALSE, FALSE,
      "select 'db', `t`.`c` from `t`");
  check("select '`db`.`t`', 'a''db', 'a\\'db' from `db`.`t`", "db", FALSE, FALSE,
      "select '`db`.`t`', 'a''db', 'a\\'db' from `t`");
  check("select 'db\\', `db`.`t`.`c`", "db", TRUE, FALSE,
      "select 'db\\', `t`.`c`");
  check("select \"db\" from `db`.`t`", "db", FALSE, FALSE,
      "select \"db\" from `t`");
  check("select \"db\".\"t\".\"c\" from \"db\".\"t\"", "db", FALSE, TRUE,
      "select \"t\".\"c\" from \"t\"");
  check("select `db_copy`.`t`.`c`, `db_column` from `db`.`db_table`", "db", FALSE, FALSE,
      "select `db_copy`.`t`.`c`, `db_column` from `db_table`");
  check("select 'a`b' from `a``b`.`t`", "a`b", FALSE, FALSE,
      "select 'a`b' from `t`");
  check("select db.t.c from db.t", "db", FALSE, FALSE,
      "select t.c from t");
  check("select * from `db` . `t`", "db", FALSE, FALSE,
      "select * from  `t`");
  check("select 'db' /* `db`.`t` */ from `db`.`t` -- `db`.`t`\n# db.t\n", "db", FALSE, FALSE,
      "select 'db' /* `db`.`t` */ from `t` -- `db`.`t`\n# db.t\n");
  check("select 'db' from `other`.`t`", "db", FALSE, FALSE,
      "select 'db' from `other`.`t`");
  check("select '`db`.`t`", "db", FALSE, FALSE, "select '`db`.`t`");
  check("", "db", FALSE, FALSE, "");
  check("select `other`.`db`.`a` from `other`.`db`", "db", FALSE, FALSE,
      "select `other`.`db`.`a` from `other`.`db`");
  check("select `db`.`db`.`a` from `db`.`db`", "db", FALSE, FALSE,
      "select `db`.`a` from `db`");

  pair("select ((a+b)*c) from t", "select (a+b)*c from t", TRUE);
  pair("select ((a=1) and (b=2))", "select a=1 and b=2", TRUE);
  pair("select (case when (a between 1 and 5) then (b+c) else a end)",
      "select case when a between 1 and 5 then b+c else a end", TRUE);
  pair("select cast(((a+b)*c) as decimal(20,6))", "select cast((a+b)*c as decimal(20,6))", TRUE);
  pair("select if((a=1), greatest((b+c),d), (a+b))", "select if(a=1,greatest(b+c,d),a+b)", TRUE);
  pair("select a from t where exists(select 1 from u)",
      "select a from t where exists(select 1 from u limit 1)", TRUE);
  pair("select a from t where not exists(select 1 from u)",
      "select a from t where not exists(select 1 from u limit 1)", TRUE);
  pair("select (not (a between 1 and 5))", "select not a between 1 and 5", TRUE);
  pair("select a from (t left join u on ((t.a=u.a)))", "select a from (t left join u on t.a=u.a)", TRUE);
  pair("select a from t where a not in (1,2)", "select a from t where (a not in (1,2))", TRUE);
  pair("select (a+b)*c", "select a+b*c", FALSE);
  pair("select a-(b-c)", "select (a-b)-c", FALSE);
  pair("select a+(b+c)", "select (a+b)+c", FALSE);
  pair("select (a or b) and c", "select a or (b and c)", FALSE);
  pair("select not (a and b)", "select (not a) and b", FALSE);
  pair("select (not a) between 1 and 5", "select not (a between 1 and 5)", FALSE);
  pair("select (a between 1 and 5) and b", "select a between 1 and (5 and b)", FALSE);
  pair("select a from t where exists(select 1 from u limit 0)", "select a from t where exists(select 1 from u)", FALSE);
  pair("select a from t where exists(select 1 from u limit 1 offset 1)", "select a from t where exists(select 1 from u)", FALSE);
  pair("select a from t where exists(select 1 from u limit 1,1)", "select a from t where exists(select 1 from u)", FALSE);
  pair("select a from t where exists(select 1 from (select a from u limit 1) x)",
      "select a from t where exists(select 1 from (select a from u) x)", FALSE);
  pair("select a from t limit 1", "select a from t", FALSE);
  pair("select a from t left join (u join w on u.a=w.a) on t.a=u.a",
      "select a from (t left join u on t.a=u.a) join w on u.a=w.a", FALSE);
  pair("select 'a b'", "select 'ab'", FALSE);
  pair("select '(a+b)'", "select 'a+b'", FALSE);
  pair("select 'limit 1'", "select ''", FALSE);
  pair("select 'a\\\'b'", "select 'ab'", FALSE);
  pair("select 'a''b'", "select 'a' 'b'", FALSE);
  pair("select `a.b`", "select `a`.`b`", FALSE);
  pair("select a from external1.t", "select a from external2.t", FALSE);
  pair("select a collate utf8mb4_bin", "select a collate utf8mb4_unicode_ci", FALSE);
  pair("select a,b", "select b,a", FALSE);
  pair("select null", "select 'null'", FALSE);
  pair("select 1.25", "select 1.26", FALSE);
  pair("select a in (1,2)", "select a in (1,3)", FALSE);
  pair("select a /* hint */", "select a /* other */", FALSE);
  pair("select a || (b+c)", "select (a || b)+c", FALSE);
  pair("select !a+b", "select !(a+b)", FALSE);
  pair("select (a collate utf8mb4_bin)", "select a collate utf8mb4_bin", TRUE);
  pair("select (a+b) collate utf8mb4_bin", "select a+b collate utf8mb4_bin", FALSE);
  pair("select (a=b) between 0 and 1", "select a=(b between 0 and 1)", FALSE);
  pair("select a=b between 0 and 1", "select a=(b between 0 and 1)", TRUE);
  pair("select a between b and (c between 0 and 1)", "select a between b and c between 0 and 1", TRUE);
  pair("select a between b and (c between 0 and 1)", "select (a between b and c) between 0 and 1", FALSE);
  pair("select a + interval 1 day", "select (a + interval 1) day", FALSE);
  gchar *special = normalize_view_expressions("select (not a)=b", FALSE, FALSE, TRUE);
  g_assert_true(g_str_has_prefix(special, "verbatim"));
  g_free(special);
  gchar *quoted = normalize_view_expressions("select 'a\\', (b+c)", TRUE, FALSE, FALSE);
  g_assert_false(g_str_has_prefix(quoted, "verbatim"));
  g_free(quoted);
  GString *deep = g_string_new("select ");
  for (guint i = 0; i < 200; i++)
    g_string_append_c(deep, '(');
  g_string_append(deep, "a");
  for (guint i = 0; i < 200; i++)
    g_string_append_c(deep, ')');
  gchar *limited = normalize_view_expressions(deep->str, FALSE, FALSE, FALSE);
  g_assert_true(g_str_has_prefix(limited, "verbatim"));
  g_free(limited);
  g_string_free(deep, TRUE);
  g_print("15 qualifier cases, 45 expression pairs and 3 fallback/quote cases passed.\n");
  return 0;
}
