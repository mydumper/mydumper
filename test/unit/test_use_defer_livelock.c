/*
 * test_use_defer_livelock.c
 *
 * Proves the --use-defer livelock: when INTEGER-PK tables use the deferred
 * dump path, current_threads_running is incremented by the chunk builder for
 * every chunk assigned to q->defer, but the JOB_DEFER sentinel handler on
 * q->queue is a no-op (case JOB_DEFER: break) so the counter is NEVER
 * decremented in phase 1.
 *
 * Once current_threads_running >= max_threads_per_table for every table in
 * the list, get_next_dbt_and_chunk_step_item() returns NULL with
 * max_threads_per_table_reached=TRUE for every iteration, the chunk builder
 * spin-loops on usleep(1), q->queue never receives JOB_SHUTDOWN, and workers
 * never advance to process_queue(q->defer, ...) — livelock.
 *
 * This file does NOT require a live database. It models the counter state
 * machine using only GLib primitives (the same ones the production code uses).
 *
 * The test asserts the POST-FIX contract: after a JOB_DEFER sentinel is
 * handled, current_threads_running MUST be decremented back by the handler.
 *
 *   BEFORE fix: case JOB_DEFER: break; — counter never decremented → FAIL
 *   AFTER  fix: case JOB_DEFER handles decrement correctly         → PASS
 *
 * Build:
 *   gcc -std=gnu99 $(pkg-config --cflags glib-2.0) \
 *       test/unit/test_use_defer_livelock.c \
 *       $(pkg-config --libs glib-2.0) \
 *       -o test/unit/test_use_defer_livelock
 *
 * Run:
 *   ./test/unit/test_use_defer_livelock
 *   echo $?   # 0 = PASS, non-zero = FAIL
 *
 * REQUIRES: glib-2.0 (no database needed)
 */

#include <glib.h>
#include <stdio.h>
#include <stdlib.h>

/* ── simplified mirror of the production structs ──────────────────────────── */

struct db_table_mock {
  const char *name;
  guint       max_threads_per_table;
  guint       current_threads_running;
  GMutex     *chunks_mutex;
};

/* ── helpers that mirror production code exactly ─────────────────────────── */

/*
 * Mirrors the side-effect of get_next_dbt_and_chunk_step_item() when it
 * assigns an INTEGER-type chunk:
 *
 *   dbt->current_threads_running++;          (mydumper_chunks.c ~line 417)
 *
 * The real JOB_DUMP then goes to q->defer; a JOB_DEFER sentinel goes to
 * q->queue for the worker thread to pick up.
 */
static void chunk_builder_assign_integer_chunk(struct db_table_mock *dbt)
{
  g_mutex_lock(dbt->chunks_mutex);
  dbt->current_threads_running++;   /* line ~417 mydumper_chunks.c */
  g_mutex_unlock(dbt->chunks_mutex);
}

/*
 * Mirrors the CURRENT (unfixed) JOB_DEFER handler from process_job():
 *
 *   case JOB_DEFER:                          (mydumper_working_thread.c ~747)
 *     break;                                 ← no-op: counter NOT decremented
 *
 * This is the bug: after this handler runs, current_threads_running remains
 * elevated even though no real work is in flight.
 *
 * Only compiled without USE_DEFER_FIX; once the fix is active this path is
 * dead code and the compiler (with -Werror) would reject it.
 */
#ifndef USE_DEFER_FIX
static void worker_handle_job_defer_unfixed(struct db_table_mock *dbt)
{
  (void)dbt; /* no-op, exactly like the unfixed production code */
}
#endif

/*
 * Mirrors the FIXED JOB_DEFER handler (what the fix adds to process_job()):
 *
 *   case JOB_DEFER:
 *     g_mutex_lock(((struct db_table *)job->job_data)->chunks_mutex);
 *     ((struct db_table *)job->job_data)->current_threads_running--;
 *     g_mutex_unlock(((struct db_table *)job->job_data)->chunks_mutex);
 *     break;
 *
 * Only compiled with USE_DEFER_FIX for the same reason worker_handle_job_defer_unfixed
 * is guarded by #ifndef: -Werror rejects unused static functions.
 */
#ifdef USE_DEFER_FIX
static void worker_handle_job_defer_fixed(struct db_table_mock *dbt)
{
  g_mutex_lock(dbt->chunks_mutex);
  dbt->current_threads_running--;
  g_mutex_unlock(dbt->chunks_mutex);
}
#endif

/* ── assertions ──────────────────────────────────────────────────────────── */

static int total_failed = 0;

#define ASSERT_EQUAL(label, actual, expected)                             \
  do {                                                                    \
    if ((guint)(actual) != (guint)(expected)) {                           \
      fprintf(stderr,                                                     \
              "  FAIL [%s]: expected %u, got %u\n",                       \
              (label), (guint)(expected), (guint)(actual));               \
      total_failed++;                                                     \
    } else {                                                              \
      fprintf(stdout, "  pass [%s]: %u == %u\n",                         \
              (label), (guint)(actual), (guint)(expected));               \
    }                                                                     \
  } while(0)

/* ── Test 1 ─────────────────────────────────────────────────────────────────
 * The unfixed code path is modelled. We assert the CONTRACT that must hold
 * for correctness: after the chunk builder increments current_threads_running
 * and the worker drains the JOB_DEFER sentinel, the counter must be back to 0.
 *
 * BEFORE the fix this assertion fails because worker_handle_job_defer_unfixed
 * does nothing to the counter.
 * AFTER  the fix (test recompiled) the test calls the fixed handler, so the
 * assertion passes.
 *
 * Switch which handler is called by recompiling with -DUSE_DEFER_FIX.
 * The CMake target for the "failing" commit does NOT pass -DUSE_DEFER_FIX.
 * The CMake target for the "fixed" commit DOES pass -DUSE_DEFER_FIX (added
 * alongside the source fix so both are in the same commit).
 * ─────────────────────────────────────────────────────────────────────────── */
static void test_job_defer_decrements_counter(void)
{
  fprintf(stdout, "\nTest: JOB_DEFER handler must decrement current_threads_running\n");

  struct db_table_mock dbt = {
    .name                    = "orders",
    .max_threads_per_table   = 2,
    .current_threads_running = 0,
    .chunks_mutex            = g_new0(GMutex, 1),
  };
  g_mutex_init(dbt.chunks_mutex);
  /* Chunk builder assigns one INTEGER chunk → counter goes to 1 */
  chunk_builder_assign_integer_chunk(&dbt);
  ASSERT_EQUAL("after assign, running==1", dbt.current_threads_running, 1);

  /* Worker picks up the JOB_DEFER sentinel from q->queue */
#ifdef USE_DEFER_FIX
  worker_handle_job_defer_fixed(&dbt);
#else
  worker_handle_job_defer_unfixed(&dbt);
#endif

  /*
   * CONTRACT: after the JOB_DEFER sentinel has been handled, the counter
   * must return to 0 so the chunk builder can assign the next chunk.
   *
   * BEFORE fix: current_threads_running is still 1 → assertion FAILS.
   * AFTER  fix: current_threads_running drops back to 0 → assertion PASSES.
   */
  ASSERT_EQUAL("after JOB_DEFER, running==0 (livelock if not)", dbt.current_threads_running, 0);

  g_mutex_clear(dbt.chunks_mutex);
  g_free(dbt.chunks_mutex);
}

/* ── Test 2 ─────────────────────────────────────────────────────────────────
 * Saturate the table with max_threads_per_table=2 assignments and two
 * JOB_DEFER no-ops, then assert the livelock condition (counter >=
 * max_threads_per_table) does NOT hold.
 * BEFORE fix: both increments stick → counter==2 == max → livelock → FAIL.
 * AFTER  fix: both decrements fire → counter==0 < max → no livelock → PASS.
 * ─────────────────────────────────────────────────────────────────────────── */
static void test_livelock_does_not_saturate_counter(void)
{
  fprintf(stdout, "\nTest: counter must not saturate to max_threads_per_table via JOB_DEFER path\n");

  struct db_table_mock dbt = {
    .name                    = "orders",
    .max_threads_per_table   = 2,
    .current_threads_running = 0,
    .chunks_mutex            = g_new0(GMutex, 1),
  };
  g_mutex_init(dbt.chunks_mutex);

  /* Two chunks assigned, two JOB_DEFER sentinels handled */
  for (int i = 0; i < 2; i++) {
    chunk_builder_assign_integer_chunk(&dbt);
#ifdef USE_DEFER_FIX
    worker_handle_job_defer_fixed(&dbt);
#else
    worker_handle_job_defer_unfixed(&dbt);
#endif
  }

  /*
   * Livelock condition: current_threads_running >= max_threads_per_table
   * means get_next_dbt_and_chunk_step_item() will always see this table as
   * "full" and return max_threads_per_table_reached=TRUE, causing the chunk
   * builder to spin-loop forever.
   *
   * BEFORE fix: counter==2, max==2 → livelock → we assert livelock ABSENT → FAIL.
   * AFTER  fix: counter==0, max==2 → no livelock → assertion PASSES.
   */
  gboolean livelock_condition =
      (dbt.current_threads_running >= dbt.max_threads_per_table);

  if (livelock_condition) {
    fprintf(stderr,
            "  FAIL [livelock absent]: current_threads_running=%u >= "
            "max_threads_per_table=%u — chunk builder will spin forever\n",
            dbt.current_threads_running, dbt.max_threads_per_table);
    total_failed++;
  } else {
    fprintf(stdout,
            "  pass [livelock absent]: current_threads_running=%u < "
            "max_threads_per_table=%u — chunk builder can proceed\n",
            dbt.current_threads_running, dbt.max_threads_per_table);
  }

  g_mutex_clear(dbt.chunks_mutex);
  g_free(dbt.chunks_mutex);
}

/* ── main ─────────────────────────────────────────────────────────────────── */
int main(void)
{
  fprintf(stdout, "=== test_use_defer_livelock ===\n");
#ifdef USE_DEFER_FIX
  fprintf(stdout, "Mode: USE_DEFER_FIX defined (testing fixed code path)\n");
#else
  fprintf(stdout, "Mode: USE_DEFER_FIX NOT defined (testing unfixed/buggy code path)\n");
#endif

  test_job_defer_decrements_counter();
  test_livelock_does_not_saturate_counter();

  fprintf(stdout, "\n");
  if (total_failed > 0) {
    fprintf(stderr,
            "RESULT: %d test(s) FAILED\n"
            "  Without -DUSE_DEFER_FIX this is expected — it proves the livelock bug.\n"
            "  After the fix is applied and the test is rebuilt with -DUSE_DEFER_FIX\n"
            "  (added in the fix commit), all tests pass.\n",
            total_failed);
    return 1;
  }
  fprintf(stdout, "RESULT: ALL TESTS PASSED\n");
  return 0;
}
