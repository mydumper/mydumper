/* Test the actual cancellation path without a database or dump files. */
#include <glib.h>
#include <stdlib.h>
#include <unistd.h>
static void cancel_test_exit(int status) G_GNUC_NORETURN;
#define exit cancel_test_exit
#include "../../src/mydumper/mydumper_start_dump.c"
#undef exit

GString                    *set_global_back;
enum source_control_command source_control_command;
const gchar                *start_replica_sql_thread = "START REPLICA SQL_THREAD";
static gchar                calls[32];
static guint                count;
static gboolean             restart_fails;
static MYSQL               *primary = (MYSQL *)1, *secondary = (MYSQL *)2;
static const gchar         *expected;
static void                 record(gchar c)
{
  calls[count++] = c;
  calls[count] = 0;
}
static void unlock_global(MYSQL *conn)
{
  g_assert_true(conn == primary);
  record('L');
}
static void unlock_ddl(MYSQL *conn)
{
  g_assert_true(conn == secondary);
  record('D');
}
gboolean is_mysql_like(void) { return TRUE; }
gboolean m_query_warning(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)fmt;
  g_assert_true(conn == primary);
  g_assert_cmpstr(query, ==, start_replica_sql_thread);
  record('R');
  return restart_fails;
}
void discard_mysql_output(MYSQL *conn)
{
  g_assert_true(conn == primary);
  record('A');
}
void execute_gstring(MYSQL *conn, GString *statements)
{
  g_assert_true(conn == primary);
  g_assert_true(statements == set_global_back);
  record('G');
}
void STDCALL mysql_close(MYSQL *conn) { record(conn == primary ? 'C' : 'S'); }
static void  cancel_test_exit(int status)
{
  g_assert_cmpint(status, ==, EXIT_FAILURE);
  g_assert_cmpstr(calls, ==, expected);
  if (!restart_fails)
    g_assert_false(replica_stopped);
  _Exit(0);
}
static void run_cancel(gconstpointer data)
{
  if (g_test_subprocess())
  {
    g_log_set_always_fatal(G_LOG_LEVEL_ERROR);
    const gchar *mode = data;
    gboolean     distinct = strcmp(mode, "same") != 0;
    replica_stopped = strcmp(mode, "untouched") != 0;
    restart_fails = strcmp(mode, "failed") == 0;
    source_control_command = strcmp(mode, "aws") == 0 ? AWS : TRADITIONAL;
    release_global_write_lock_function = unlock_global;
    release_ddl_lock_function = distinct ? unlock_ddl : NULL;
    set_global_back = g_string_new("SET GLOBAL test_variable=1;");
    expected = !distinct ? "LRGC" : !replica_stopped            ? "LDGSC"
                                : source_control_command == AWS ? "LDRAGSC"
                                                                : "LDRGSC";
    cancel_dump(primary, distinct ? secondary : primary);
  }
  g_test_trap_subprocess(NULL, 2000000, 0);
  g_test_trap_assert_passed();
}
int main(int argc, char **argv)
{
  g_test_init(&argc, &argv, NULL);
  g_test_add_data_func("/cancel/separate-connections", "normal", run_cancel);
  g_test_add_data_func("/cancel/shared-connection", "same", run_cancel);
  g_test_add_data_func("/cancel/replica-not-stopped", "untouched", run_cancel);
  g_test_add_data_func("/cancel/aws-result-draining", "aws", run_cancel);
  g_test_add_data_func("/cancel/restart-failure-still-restores-globals", "failed", run_cancel);
  return g_test_run();
}
