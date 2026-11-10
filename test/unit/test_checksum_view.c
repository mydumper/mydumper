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
void print_bool(const char *key, gboolean val) { (void)key; (void)val; }
int get_product(void) { return 0; }
int get_major(void) { return 0; }
int get_secondary(void) { return 0; }
void m_error(const char *fmt, ...) { (void)fmt; g_assert_not_reached(); }
gboolean machine_log_json_enabled(void) { return FALSE; }
void machine_log_event(const gchar *domain, GLogLevelFlags level, ...) { (void)domain; (void)level; }

static void check(const gchar *sql, const gchar *database, gboolean no_backslash, gboolean ansi, const gchar *expected)
{
  gchar *actual = normalize_view_definition(sql, database, no_backslash, ansi);
  g_assert_cmpstr(actual, ==, expected);
  g_free(actual);
}

int main(void)
{
  check("select 'db', `db`.`t`.`c` from `db`.`t`", "db", FALSE, FALSE,
      "select 'db', ``.`t`.`c` from ``.`t`");
  check("select '`db`.`t`', 'a''db', 'a\\'db' from `db`.`t`", "db", FALSE, FALSE,
      "select '`db`.`t`', 'a''db', 'a\\'db' from ``.`t`");
  check("select 'db\\', `db`.`t`.`c`", "db", TRUE, FALSE,
      "select 'db\\', ``.`t`.`c`");
  check("select \"db\" from `db`.`t`", "db", FALSE, FALSE,
      "select \"db\" from ``.`t`");
  check("select \"db\".\"t\".\"c\" from \"db\".\"t\"", "db", FALSE, TRUE,
      "select \"\".\"t\".\"c\" from \"\".\"t\"");
  check("select `db_copy`.`t`.`c`, `db_column` from `db`.`db_table`", "db", FALSE, FALSE,
      "select `db_copy`.`t`.`c`, `db_column` from ``.`db_table`");
  check("select 'a`b' from `a``b`.`t`", "a`b", FALSE, FALSE,
      "select 'a`b' from ``.`t`");
  check("select db.t.c from db.t", "db", FALSE, FALSE,
      "select .t.c from .t");
  check("select * from `db` . `t`", "db", FALSE, FALSE,
      "select * from `` . `t`");
  check("select 'db' /* `db`.`t` */ from `db`.`t` -- `db`.`t`\n# db.t\n", "db", FALSE, FALSE,
      "select 'db' /* `db`.`t` */ from ``.`t` -- `db`.`t`\n# db.t\n");
  check("select 'db' from `other`.`t`", "db", FALSE, FALSE,
      "select 'db' from `other`.`t`");
  check("select '`db`.`t`", "db", FALSE, FALSE, "select '`db`.`t`");
  check("", "db", FALSE, FALSE, "");
  g_print("13 view normalization cases passed.\n");
  return 0;
}
