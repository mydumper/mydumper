/* Exercise the real checksum query against disposable integration servers. */
#include "../../src/checksum.c"

struct M_ROW *m_store_result_single_row(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)fmt;
  if (mysql_query(conn, query))
    g_error("%s", mysql_error(conn));
  struct M_ROW *row = g_new0(struct M_ROW, 1);
  row->res = mysql_store_result(conn);
  if (!row->res)
    g_error("%s", mysql_error(conn));
  row->row = mysql_fetch_row(row->res);
  return row;
}
void m_store_result_row_free(struct M_ROW *row)
{
  mysql_free_result(row->res);
  g_free(row);
}
int  get_product(void) { return 0; }
int  get_major(void) { return 0; }
int  get_secondary(void) { return 0; }
void trace(const char *fmt, ...) { (void)fmt; }
void print_bool(const char *key, gboolean value)
{
  (void)key;
  (void)value;
}
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

int main(int argc, char **argv)
{
  if (argc != 3)
    return 2;
  MYSQL *conn = mysql_init(NULL);
  if (!mysql_real_connect(conn, argv[1], "root", "", argv[2], 3306, NULL, 0))
    g_error("%s", mysql_error(conn));
  if (mysql_query(conn, "SET SESSION sql_mode='', sql_quote_show_create=1"))
    g_error("%s", mysql_error(conn));
  gchar *query = g_strdup_printf("SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA='%s' ORDER BY TABLE_NAME", argv[2]);
  if (mysql_query(conn, query))
    g_error("%s", mysql_error(conn));
  g_free(query);
  MYSQL_RES *views = mysql_store_result(conn);
  MYSQL_ROW  view;
  while ((view = mysql_fetch_row(views)))
  {
    gchar *checksum = checksum_view_structure(conn, argv[2], view[0]);
    if (!checksum)
      return 1;
    g_print("%s\t%s\n", view[0], checksum);
    g_free(checksum);
  }
  mysql_free_result(views);
  mysql_close(conn);
  return 0;
}
