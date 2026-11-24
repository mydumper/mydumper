/* Exercise the real checksum verification code without a database connection. */
#include <glib.h>
#include <stdarg.h>
#include <string.h>
#include "common.h"
#include "checksum.h"
#include "logging.h"

MYSQL_RES *m_store_result_critical(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)conn; (void)query; (void)fmt;
  g_assert_not_reached();
}
MYSQL_ROW STDCALL mysql_fetch_row(MYSQL_RES *result) { (void)result; g_assert_not_reached(); }
void STDCALL mysql_free_result(MYSQL_RES *result) { (void)result; g_assert_not_reached(); }

enum result_kind { NO_RESULT, NO_ROW, SQL_NULL, MATCH, MISMATCH };
static enum result_kind result_kind;
static guint queries, confirmed, warnings, criticals, finished, failed, missing;

struct M_ROW *m_store_result_single_row(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)conn; (void)query; (void)fmt;
  queries++;
  if (result_kind == NO_RESULT)
    return NULL;
  struct M_ROW *mr = g_new0(struct M_ROW, 1);
  if (result_kind != NO_ROW)
  {
    mr->row = g_new0(char *, 1);
    if (result_kind != SQL_NULL)
      mr->row[0] = g_strdup(result_kind == MATCH ? "ABC123" : "bad");
  }
  return mr;
}

void m_store_result_row_free(struct M_ROW *mr)
{
  if (mr->row)
  {
    g_free(mr->row[0]);
    g_free(mr->row);
  }
  g_free(mr);
}

void trace(const char *fmt, ...) { (void)fmt; }
void print_bool(const char *key, gboolean val) { (void)key; (void)val; }
int get_product(void) { return 0; }
int get_major(void) { return 0; }
int get_secondary(void) { return 0; }
void m_error(const char *fmt, ...) { (void)fmt; g_assert_not_reached(); }
gboolean machine_log_json_enabled(void) { return TRUE; }

void machine_log_event(const gchar *domain, GLogLevelFlags level, ...)
{
  (void)domain; (void)level;
  va_list ap;
  va_start(ap, level);
  const char *key;
  while ((key = va_arg(ap, const char *)) != NULL)
  {
    const char *value = va_arg(ap, const char *);
    if (!strcmp(key, "STATUS"))
    {
      finished += !strcmp(value, "finished");
      failed += !strcmp(value, "failed");
    }
  }
  va_end(ap);
}

static void capture_log(const gchar *domain, GLogLevelFlags level, const gchar *message, gpointer data)
{
  (void)domain; (void)data;
  confirmed += strstr(message, "confirmed") != NULL;
  missing += strstr(message, "could not be computed") != NULL;
  warnings += (level & G_LOG_LEVEL_WARNING) != 0;
  criticals += (level & G_LOG_LEVEL_CRITICAL) != 0;
}

static void check(gboolean database, enum result_kind kind, enum checksum_modes mode, gboolean expected_present)
{
  queries = confirmed = warnings = criticals = finished = failed = missing = 0;
  result_kind = kind;
  checksum_mode = mode;
  gchar expected[] = "abc123";
  gchar database_name[] = "test";
  gchar table_name[] = "table";
  struct database_level_checksum db = {0};
  struct table_level_checksum table = {0};
  db.schema = table.schema = expected_present ? expected : NULL;
  gboolean ok = database ? checksum_database(database_name, &db, NULL)
                         : checksum_dbt(database_name, table_name, FALSE, &table, NULL);
  gboolean checked = expected_present && mode != CHECKSUM_SKIP;
  gboolean valid = !checked || kind == MATCH;
  g_assert_cmpint(ok, ==, valid);
  g_assert_cmpuint(queries, ==, checked ? 1 : 0);
  g_assert_cmpuint(confirmed, ==, checked && valid ? 1 : 0);
  g_assert_cmpuint(finished, ==, checked && valid ? 1 : 0);
  g_assert_cmpuint(failed, ==, checked && !valid ? 1 : 0);
  g_assert_cmpuint(missing, ==, checked && kind <= SQL_NULL ? 1 : 0);
  g_assert_cmpuint(warnings, ==, checked && !valid && mode == CHECKSUM_WARN ? 1 : 0);
  g_assert_cmpuint(criticals, ==, checked && !valid && mode == CHECKSUM_FAIL ? 1 : 0);
}

int main(void)
{
  g_log_set_always_fatal(G_LOG_FATAL_MASK);
  g_log_set_handler(NULL, G_LOG_LEVEL_MASK, capture_log, NULL);
  for (int database = 0; database <= 1; database++)
    for (int mode = CHECKSUM_WARN; mode <= CHECKSUM_FAIL; mode++)
    {
      for (int kind = NO_RESULT; kind <= MISMATCH; kind++)
        check(database, kind, mode, TRUE);
      check(database, NO_RESULT, mode, FALSE);
    }
  check(FALSE, NO_RESULT, CHECKSUM_SKIP, TRUE);
  g_print("25 checksum cases passed (table/database, WARN/FAIL, missing/matching/mismatching results).\n");
  return 0;
}
