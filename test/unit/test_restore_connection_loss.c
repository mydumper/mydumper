/* Exercise the real statement error path without a database or restore files. */
#include <glib.h>
#include <stdlib.h>
#include <unistd.h>
static void test_exit(int status) G_GNUC_NORETURN G_GNUC_UNUSED;
#define exit test_exit
#include "../../src/myloader/myloader_restore.c"
#undef exit

gboolean              dry_run = FALSE;
guint                 commit_count = 500, errors;
struct restore_errors detailed_errors;
GString              *set_session, *aws_session_commands;
static guint          error_number, connects, queries;
static gboolean       json_mode;
static const gchar   *error_text = "";
static const gchar   *sqlstate = "00000";
static void           reset_error(void)
{
  error_number = 0;
  error_text = "";
  sqlstate = "00000";
}
int STDCALL mysql_real_query(MYSQL *conn, const char *query, unsigned long length)
{
  (void)conn;
  (void)query;
  (void)length;
  queries++;
  if (commit_count == 1 && queries == 2)
  {
    reset_error();
    return 0;
  }
  error_number = 2013;
  error_text = "original transport failure";
  sqlstate = "HY000";
  return 1;
}
unsigned int STDCALL mysql_errno(MYSQL *conn)
{
  (void)conn;
  return error_number;
}
const char *STDCALL mysql_error(MYSQL *conn)
{
  (void)conn;
  return error_text;
}
const char *STDCALL mysql_sqlstate(MYSQL *conn)
{
  (void)conn;
  return sqlstate;
}
int STDCALL mysql_ping(MYSQL *conn)
{
  (void)conn;
  error_number = 2006;
  error_text = "ping failure";
  sqlstate = "08003";
  return 1;
}
void STDCALL   mysql_close(MYSQL *conn) { (void)conn; }
MYSQL *STDCALL mysql_init(MYSQL *conn)
{
  (void)conn;
  return (MYSQL *)3;
}
unsigned long STDCALL mysql_thread_id(MYSQL *conn)
{
  (void)conn;
  return 164;
}
void m_connect(MYSQL *conn)
{
  (void)conn;
  connects++;
  reset_error();
}
gboolean execute_use(struct connection_data *cd)
{
  (void)cd;
  return FALSE;
}
void execute_gstring(MYSQL *conn, GString *statements)
{
  (void)statements;
  (void)conn;
}
void     execute_aws_session_setup(MYSQL *conn) { (void)conn; }
gboolean should_ignore_error_code(guint code)
{
  (void)code;
  return FALSE;
}
gboolean machine_log_json_enabled(void) { return json_mode; }
void     machine_log_event(const gchar *domain, GLogLevelFlags level, ...)
{
  (void)domain;
  (void)level;
  va_list args;
  va_start(args, level);
  const gchar *key;
  while ((key = va_arg(args, const gchar *)))
  {
    const gchar *value = va_arg(args, const gchar *);
    if (!strcmp(key, "CONNECTION_ID"))
      g_assert_cmpstr(value, ==, "160");
    if (!strcmp(key, "MYSQL_ERRNO"))
      g_assert_cmpstr(value, ==, "2013");
    if (!strcmp(key, "SQLSTATE"))
      g_assert_cmpstr(value, ==, "HY000");
  }
  va_end(args);
}
static void test_exit(int status)
{
  g_assert_cmpint(status, ==, EXIT_FAILURE);
  g_assert_cmpuint(connects, ==, 0);
  g_assert_cmpuint(queries, ==, 1);
  _Exit(0);
}
static void lost_connection(gconstpointer mode)
{
  if (g_test_subprocess())
  {
    json_mode = !strcmp(mode, "json");
    g_log_set_always_fatal(!strcmp(mode, "diagnostics") ? G_LOG_LEVEL_ERROR : G_LOG_LEVEL_ERROR | G_LOG_LEVEL_CRITICAL);
    struct connection_data cd = {0};
    cd.thrconn = (MYSQL *)1;
    cd.thread_id = 3;
    cd.connection_id = 160;
    cd.transaction = TRUE;
    GString *data = g_string_new("INSERT INTO t VALUES(1)");
    guint    counter = 4;
    restore_data_in_gstring_by_statement(&cd, data, FALSE, &counter, 12, 14);
    _Exit(0);
  }
  g_test_trap_subprocess(NULL, 2000000, 0);
  g_test_trap_assert_passed();
  g_test_trap_assert_stderr("*connection 160*ERROR 2013*original transport failure*Stopping restore*");
  g_test_trap_assert_stderr_unmatched("*ERROR 0:*");
}
static void autocommit_retry(void)
{
  commit_count = 1;
  queries = connects = 0;
  json_mode = FALSE;
  struct connection_data cd = {0};
  cd.thrconn = (MYSQL *)1;
  GString *data = g_string_new("INSERT INTO t VALUES(1)");
  guint    counter = 0;
  g_test_expect_message(NULL, G_LOG_LEVEL_WARNING, "*ERROR 2013*");
  g_assert_cmpint(restore_data_in_gstring_by_statement(&cd, data, FALSE, &counter, 1, 1), ==, 0);
  g_test_assert_expected_messages();
  g_assert_cmpuint(queries, ==, 2);
  g_assert_cmpuint(connects, ==, 1);
  g_assert_cmpuint(counter, ==, 1);
  g_string_free(data, TRUE);
}

int main(int argc, char **argv)
{
  g_test_init(&argc, &argv, NULL);
  g_test_add_data_func("/restore-loss/original-diagnostics", "diagnostics", lost_connection);
  g_test_add_data_func("/restore-loss/no-core-with-fatal-criticals", "fatal", lost_connection);
  g_test_add_data_func("/restore-loss/json-original-diagnostics", "json", lost_connection);
  g_test_add_func("/restore-loss/autocommit-retry-unchanged", autocommit_retry);
  return g_test_run();
}
