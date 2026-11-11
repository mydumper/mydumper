/* Exercise index shutdown against the real shared connection pool. */
#include <glib.h>

#include "../../src/myloader/myloader_restore.c"
#include "../../src/myloader/myloader_worker_index.c"

static guint closed_connections;
static MYSQL handles[4];
guint        max_threads_for_index_creation = 0;
gboolean     shutdown_triggered = FALSE;

void         trace(const char *format, ...) { (void)format; }
void STDCALL mysql_close(MYSQL *conn)
{
  g_assert_nonnull(conn);
  closed_connections++;
}
gboolean process_job(struct thread_data *td, struct control_job *job, gboolean *retry)
{
  (void)td;
  (void)job;
  (void)retry;
  g_assert_not_reached();
  return FALSE;
}
void table_lock(struct db_table *dbt) { (void)dbt; }
void table_unlock(struct db_table *dbt) { (void)dbt; }

static void idle_sessions_at_index_shutdown(void)
{
  struct connection_data slots[3] = {{0}};
  struct thread_data     td = {0};
  struct control_job     shutdown = {.type = JOB_SHUTDOWN};
  connection_pool = g_async_queue_new();
  index_queue = g_async_queue_new();
  closed_connections = 0;
  struct connection_data active = {.thrconn = &handles[3], .connection_id = 4};
  for (guint i = 0; i < G_N_ELEMENTS(slots); i++)
  {
    if (i != 1)
    {
      slots[i].thrconn = &handles[i];
      slots[i].connection_id = i + 1;
      slots[i].current_database = (struct database *)&handles[i];
    }
    g_async_queue_push(connection_pool, &slots[i]);
  }
  g_async_queue_push(index_queue, &shutdown);
  g_assert_false(process_index(&td));
  g_assert_cmpuint(closed_connections, ==, 2);
  g_assert_true(active.thrconn == &handles[3]);
  g_assert_cmpuint(active.connection_id, ==, 4);
  g_assert_cmpint(g_async_queue_length(connection_pool), ==, 3);
  for (guint i = 0; i < G_N_ELEMENTS(slots); i++)
  {
    g_assert_null(slots[i].thrconn);
    g_assert_cmpuint(slots[i].connection_id, ==, 0);
    g_assert_null(slots[i].current_database);
  }
  set_connection_pool_idle_release(TRUE);
  g_assert_cmpuint(closed_connections, ==, 2);
  set_connection_pool_idle_release(FALSE);
  g_async_queue_unref(index_queue);
  g_async_queue_unref(connection_pool);
}

static void completed_connection_during_tail(void)
{
  struct connection_data active = {.thrconn = &handles[0], .connection_id = 1};
  connection_pool = g_async_queue_new();
  closed_connections = 0;
  set_connection_pool_idle_release(TRUE);
  g_assert_cmpuint(closed_connections, ==, 0);
  return_connection_to_pool(&active);
  g_assert_cmpuint(closed_connections, ==, 1);
  g_assert_null(active.thrconn);
  g_assert_true(g_async_queue_try_pop(connection_pool) == &active);
  set_connection_pool_idle_release(FALSE);
  g_async_queue_unref(connection_pool);
}

static gpointer concurrent_return(gpointer data)
{
  return_connection_to_pool(data);
  return NULL;
}

static void shutdown_racing_with_return(void)
{
  connection_pool = g_async_queue_new();
  for (guint i = 0; i < 64; i++)
  {
    struct connection_data active = {.thrconn = &handles[0]};
    closed_connections = 0;
    set_connection_pool_idle_release(FALSE);
    GThread *worker = g_thread_new("return", concurrent_return, &active);
    set_connection_pool_idle_release(TRUE);
    g_thread_join(worker);
    g_assert_cmpuint(closed_connections, ==, 1);
    g_assert_null(active.thrconn);
    g_assert_true(g_async_queue_try_pop(connection_pool) == &active);
  }
  set_connection_pool_idle_release(FALSE);
  g_async_queue_unref(connection_pool);
}

static void post_import_keeps_connections(void)
{
  struct connection_data cd = {.thrconn = &handles[0]};
  connection_pool = g_async_queue_new();
  closed_connections = 0;
  return_connection_to_pool(&cd);
  g_assert_cmpuint(closed_connections, ==, 0);
  set_connection_pool_idle_release(TRUE);
  g_assert_cmpuint(closed_connections, ==, 1);
  g_assert_true(g_async_queue_try_pop(connection_pool) == &cd);
  wait_index_worker_to_finish();
  cd.thrconn = &handles[1];
  return_connection_to_pool(&cd);
  g_assert_cmpuint(closed_connections, ==, 1);
  g_assert_true(cd.thrconn == &handles[1]);
  g_async_queue_unref(connection_pool);
}

static void cancellation_does_not_start_tail(void)
{
  struct connection_data cd = {.thrconn = &handles[0]};
  struct thread_data     td = {0};
  struct control_job     shutdown = {.type = JOB_SHUTDOWN};
  connection_pool = g_async_queue_new();
  index_queue = g_async_queue_new();
  closed_connections = 0;
  shutdown_triggered = TRUE;
  return_connection_to_pool(&cd);
  g_async_queue_push(index_queue, &shutdown);
  g_assert_false(process_index(&td));
  g_assert_cmpuint(closed_connections, ==, 0);
  g_assert_true(cd.thrconn == &handles[0]);
  shutdown_triggered = FALSE;
  g_async_queue_unref(index_queue);
  g_async_queue_unref(connection_pool);
}

int main(int argc, char **argv)
{
  g_test_init(&argc, &argv, NULL);
  g_test_add_func("/index-tail/idle-sessions-at-shutdown", idle_sessions_at_index_shutdown);
  g_test_add_func("/index-tail/completed-connection", completed_connection_during_tail);
  g_test_add_func("/index-tail/concurrent-return", shutdown_racing_with_return);
  g_test_add_func("/index-tail/post-import-reuse", post_import_keeps_connections);
  g_test_add_func("/index-tail/cancellation-unchanged", cancellation_does_not_start_tail);
  return g_test_run();
}
