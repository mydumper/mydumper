/* Issue #1925: a transaction rolled back by the server (deadlock on a statement
 * or a failed COMMIT) must be replayed as a whole, not only its last statement.
 * The MySQL client API is replaced by a scripted fake server. */
#include <glib.h>
#include <stdlib.h>
#include <unistd.h>
static void test_exit(int status) G_GNUC_NORETURN G_GNUC_UNUSED;
#define exit test_exit
#include "../../src/myloader/myloader_restore.c"
#undef exit

gboolean              dry_run = FALSE;
guint                 commit_count = 1000, errors;
guint                 retry_count = 10, retry_wait = 0;
struct restore_errors detailed_errors;
GString              *set_session, *aws_session_commands;

static GPtrArray     *executed;     // every statement sent to the fake server
static guint          fail_at[4];   // 1-based statement numbers that fail...
static guint          fail_errno[4];  // ...with this error
static guint          error_number;

static void test_exit(int status)
{
  (void)status;
  g_assert_not_reached();
}
static void reset_server(void)
{
  if (executed)
    g_ptr_array_free(executed, TRUE);
  executed = g_ptr_array_new_with_free_func(g_free);
  memset(fail_at, 0, sizeof(fail_at));
  memset(fail_errno, 0, sizeof(fail_errno));
  error_number = 0;
  errors = 0;
  detailed_errors.retries = 0;
  retry_count = 10;
  retry_wait = 0;
  max_transaction_size = DEFAULT_MAX_TRANSACTION_SIZE;
}
static int run(const char *query)
{
  g_ptr_array_add(executed, g_strdup(query));
  error_number = 0;
  for (guint i = 0; i < G_N_ELEMENTS(fail_at); i++)
    if (fail_at[i] == executed->len)
      error_number = fail_errno[i];
  return error_number != 0;
}
int STDCALL mysql_real_query(MYSQL *conn, const char *query, unsigned long length)
{
  (void)conn;
  gchar *q = g_strndup(query, length);
  int    r = run(q);
  g_free(q);
  return r;
}
int STDCALL mysql_query(MYSQL *conn, const char *query)
{
  (void)conn;
  return run(query);
}
gboolean m_query_warning(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)fmt;
  return mysql_query(conn, query);
}
unsigned int STDCALL mysql_errno(MYSQL *conn)
{
  (void)conn;
  return error_number;
}
const char *STDCALL mysql_error(MYSQL *conn)
{
  (void)conn;
  return error_number ? "scripted failure" : "";
}
const char *STDCALL mysql_sqlstate(MYSQL *conn)
{
  (void)conn;
  return error_number ? "40001" : "00000";
}
int STDCALL mysql_ping(MYSQL *conn)
{
  (void)conn;
  return 0;
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
  return 160;
}
void m_connect(MYSQL *conn) { (void)conn; }
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
gboolean machine_log_json_enabled(void) { return FALSE; }
void     machine_log_event(const gchar *domain, GLogLevelFlags level, ...)
{
  (void)domain;
  (void)level;
}

static struct connection_data *new_transaction(void)
{
  struct connection_data *cd = g_new0(struct connection_data, 1);
  cd->thrconn = (MYSQL *)1;
  cd->thread_id = 3;
  cd->connection_id = 160;
  cd->transaction = TRUE;
  cd->replay = g_ptr_array_new_with_free_func(free_replay_statement);
  return cd;
}
static int insert(struct connection_data *cd, const gchar *statement, guint *counter)
{
  GString *data = g_string_new(statement);
  int      r = restore_data_in_gstring_by_statement(cd, data, FALSE, counter, 1, 1);
  g_string_free(data, TRUE);
  return r;
}
static void assert_executed(const gchar *const *expected)
{
  guint n = g_strv_length((gchar **)expected);
  for (guint i = 0; i < n && i < executed->len; i++)
    g_assert_cmpstr(g_ptr_array_index(executed, i), ==, expected[i]);
  g_assert_cmpuint(executed->len, ==, n);
}

/* The deadlock hits the 3rd statement: the 2 earlier ones were rolled back too. */
static void statement_deadlock(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  fail_at[0] = 3;
  fail_errno[0] = ER_LOCK_DEADLOCK;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpint(insert(cd, "INSERT 2", &counter), ==, 0);
  g_assert_cmpint(insert(cd, "INSERT 3", &counter), ==, 0);
  g_assert_cmpint(m_commit(cd), ==, 0);
  const gchar *expected[] = {"INSERT 1", "INSERT 2", "INSERT 3",
      "START TRANSACTION", "INSERT 1", "INSERT 2", "INSERT 3", "COMMIT", NULL};
  assert_executed(expected);
  g_assert_cmpuint(counter, ==, 3);
  g_assert_cmpuint(errors, ==, 0);
  g_assert_cmpuint(detailed_errors.retries, ==, 1);
}

/* COMMIT failed with a deadlock (Galera/Group Replication): nothing was committed. */
static void commit_deadlock(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  fail_at[0] = 3;
  fail_errno[0] = ER_LOCK_DEADLOCK;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpint(insert(cd, "INSERT 2", &counter), ==, 0);
  g_assert_cmpint(m_commit(cd), ==, 0);
  const gchar *expected[] = {"INSERT 1", "INSERT 2", "COMMIT",
      "START TRANSACTION", "INSERT 1", "INSERT 2", "COMMIT", NULL};
  assert_executed(expected);
  g_assert_cmpuint(errors, ==, 0);
  g_assert_cmpuint(cd->replay->len, ==, 0);
}

/* The replay itself deadlocks once: it is replayed again. */
static void replay_deadlocks_again(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  fail_at[0] = 2;
  fail_errno[0] = ER_LOCK_DEADLOCK;
  fail_at[1] = 5;
  fail_errno[1] = ER_LOCK_DEADLOCK;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpint(insert(cd, "INSERT 2", &counter), ==, 0);
  const gchar *expected[] = {"INSERT 1", "INSERT 2",
      "START TRANSACTION", "INSERT 1", "INSERT 2",
      "START TRANSACTION", "INSERT 1", "INSERT 2", NULL};
  assert_executed(expected);
  g_assert_cmpuint(errors, ==, 0);
  g_assert_cmpuint(detailed_errors.retries, ==, 2);
}

/* Lost connection on COMMIT: it may have been applied, so no replay, and it is an error. */
static void commit_connection_lost(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  fail_at[0] = 2;
  fail_errno[0] = CR_SERVER_LOST;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpint(m_commit(cd), ==, 2);
  const gchar *expected[] = {"INSERT 1", "COMMIT", NULL};
  assert_executed(expected);
  g_assert_cmpuint(errors, ==, 1);
}

/* A LOAD DATA can not be replayed: it commits right away. */
static void load_data_commits(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpint(insert(cd, "LOAD DATA LOCAL INFILE 'f' INTO TABLE t", &counter), ==, 0);
  const gchar *expected[] = {"INSERT 1", "LOAD DATA LOCAL INFILE 'f' INTO TABLE t",
      "COMMIT", "START TRANSACTION", NULL};
  assert_executed(expected);
  g_assert_cmpuint(cd->replay->len, ==, 0);
  g_assert_cmpuint(counter, ==, 0);
}

/* --max-transaction-size: commit before the statement that would exceed it. */
static void commit_on_transaction_size(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  max_transaction_size = 1;  // MB
  GString *big = g_string_new("INSERT ");
  while (big->len < 600 * 1024)
    g_string_append_c(big, 'x');
  g_assert_cmpint(insert(cd, big->str, &counter), ==, 0);
  g_assert_cmpint(insert(cd, big->str, &counter), ==, 0);
  g_assert_cmpuint(executed->len, ==, 4);  // INSERT, COMMIT, START TRANSACTION, INSERT
  g_assert_cmpstr(g_ptr_array_index(executed, 1), ==, "COMMIT");
  g_assert_cmpstr(g_ptr_array_index(executed, 2), ==, "START TRANSACTION");
  g_assert_cmpuint(counter, ==, 1);
  g_assert_cmpuint(cd->replay->len, ==, 1);  // only the open transaction is kept
  g_assert_cmpuint(cd->replay_size, ==, big->len);
  g_string_free(big, TRUE);
}

/* --retry-count=0 disables the replay: the restore fails instead of losing rows. */
static void retry_count_zero_fails(void)
{
  if (g_test_subprocess())
  {
    g_log_set_always_fatal(G_LOG_LEVEL_ERROR);
    reset_server();
    retry_count = 0;
    struct connection_data *cd = new_transaction();
    guint                   counter = 0;
    fail_at[0] = 2;
    fail_errno[0] = ER_LOCK_DEADLOCK;
    g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
    g_assert_cmpint(insert(cd, "INSERT 2", &counter), ==, 1);
    g_assert_cmpuint(executed->len, ==, 2);
    g_assert_cmpuint(cd->replay->len, ==, 0);
    g_assert_cmpuint(errors, ==, 1);
    _Exit(0);
  }
  g_test_trap_subprocess(NULL, 2000000, 0);
  g_test_trap_assert_passed();
  g_test_trap_assert_stderr("*rolled back and could not be replayed*");
}

/* --retry-wait: attempt number times the wait, in milliseconds. */
static void retry_wait_between_attempts(void)
{
  reset_server();
  retry_wait = 50;
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  fail_at[0] = 2;
  fail_errno[0] = ER_LOCK_DEADLOCK;
  fail_at[1] = 4;  // first replay fails: waits 50ms, then 100ms after the second
  fail_errno[1] = ER_LOCK_DEADLOCK;
  fail_at[2] = 7;
  fail_errno[2] = ER_LOCK_DEADLOCK;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  gint64 start = g_get_monotonic_time();
  g_assert_cmpint(insert(cd, "INSERT 2", &counter), ==, 0);
  gint64 elapsed_ms = (g_get_monotonic_time() - start) / 1000;
  g_assert_cmpint(elapsed_ms, >=, 150);
  g_assert_cmpuint(detailed_errors.retries, ==, 3);
}

/* With --retry-count=0 nothing is replayed, so a LOAD DATA does not need its own commit. */
static void load_data_without_retries_does_not_commit(void)
{
  reset_server();
  retry_count = 0;
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpint(insert(cd, "LOAD DATA LOCAL INFILE 'f' INTO TABLE t", &counter), ==, 0);
  const gchar *expected[] = {"INSERT 1", "LOAD DATA LOCAL INFILE 'f' INTO TABLE t", NULL};
  assert_executed(expected);
  g_assert_cmpuint(cd->replay->len, ==, 0);
  g_assert_cmpuint(counter, ==, 2);
}

/* Without an explicit transaction (--queries-per-transaction=1) nothing is kept. */
static void autocommit_keeps_nothing(void)
{
  reset_server();
  struct connection_data *cd = new_transaction();
  guint                   counter = 0;
  cd->transaction = FALSE;
  g_assert_cmpint(insert(cd, "INSERT 1", &counter), ==, 0);
  g_assert_cmpuint(cd->replay->len, ==, 0);
}

int main(int argc, char **argv)
{
  g_test_init(&argc, &argv, NULL);
  // The replay logs warnings on purpose; only errors are fatal here.
  g_log_set_always_fatal(G_LOG_LEVEL_ERROR);
  g_test_add_func("/transaction-replay/statement-deadlock", statement_deadlock);
  g_test_add_func("/transaction-replay/commit-deadlock", commit_deadlock);
  g_test_add_func("/transaction-replay/replay-deadlocks-again", replay_deadlocks_again);
  g_test_add_func("/transaction-replay/commit-connection-lost", commit_connection_lost);
  g_test_add_func("/transaction-replay/load-data-commits", load_data_commits);
  g_test_add_func("/transaction-replay/load-data-without-retries-does-not-commit", load_data_without_retries_does_not_commit);
  g_test_add_func("/transaction-replay/autocommit-keeps-nothing", autocommit_keeps_nothing);
  g_test_add_func("/transaction-replay/commit-on-transaction-size", commit_on_transaction_size);
  g_test_add_func("/transaction-replay/retry-count-zero-fails", retry_count_zero_fails);
  g_test_add_func("/transaction-replay/retry-wait-between-attempts", retry_wait_between_attempts);
  return g_test_run();
}
