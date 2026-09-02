/*
 * Self-test for the glibguard LD_PRELOAD checker (test/glibguard/glibguard.c).
 *
 * Run with no argument, the program is well-behaved: glibguard must report 0
 * findings. Run with a keyword, it commits exactly one lock-discipline error,
 * which glibguard must name. The wrapper script run_selftest.sh drives both.
 *
 * The program's own exit status is not what is under test: some cases make
 * GLib abort (it detects the misuse too, but only after the fact) and
 * `double-lock` deadlocks on purpose, so the driver runs each under a timeout
 * and checks glibguard's log.
 */
#include <glib.h>
#include <stdio.h>
#include <string.h>

static GMutex a, b;

static gpointer hold_and_exit(gpointer data)
{
  g_mutex_lock((GMutex *)data);
  return NULL; /* unlocked by main, i.e. by the wrong thread */
}

static gpointer ab_order(gpointer data)
{
  (void)data;
  g_mutex_lock(&a);
  g_mutex_lock(&b);
  g_mutex_unlock(&b);
  g_mutex_unlock(&a);
  return NULL;
}

static gpointer ba_order(gpointer data)
{
  (void)data;
  g_mutex_lock(&b);
  g_mutex_lock(&a); /* opposite of main's a-then-b */
  g_mutex_unlock(&a);
  g_mutex_unlock(&b);
  return NULL;
}

int main(int argc, char **argv)
{
  const char *what = argc > 1 ? argv[1] : "clean";
  g_mutex_init(&a);
  g_mutex_init(&b);

  if (!strcmp(what, "clean"))
  {
    /* Ordinary, correct use: lock/unlock in one thread, consistent order. */
    for (int i = 0; i < 100; i++)
    {
      g_mutex_lock(&a);
      g_mutex_lock(&b);
      g_mutex_unlock(&b);
      g_mutex_unlock(&a);
    }
    GThread *t = g_thread_new("t", ab_order, NULL); /* same a-then-b order */
    g_thread_join(t);
  }
  else if (!strcmp(what, "unlock-not-held"))
  {
    g_mutex_unlock(&a);
  }
  else if (!strcmp(what, "unlock-wrong-thread"))
  {
    GThread *t = g_thread_new("t", hold_and_exit, &a);
    g_thread_join(t);
    g_mutex_unlock(&a);
  }
  else if (!strcmp(what, "double-lock"))
  {
    /* Report before deadlocking: glibguard checks after the real lock call, so
       take it with trylock to keep the test from hanging. */
    g_mutex_lock(&a);
    g_mutex_lock(&a); /* deadlocks: glibguard reports before the call blocks */
    g_mutex_unlock(&a);
    g_mutex_unlock(&a);
  }
  else if (!strcmp(what, "lock-order-inversion"))
  {
    g_mutex_lock(&a);
    g_mutex_lock(&b);
    g_mutex_unlock(&b);
    g_mutex_unlock(&a);
    GThread *t = g_thread_new("t", ba_order, NULL);
    g_thread_join(t);
  }
  else if (!strcmp(what, "destroy-while-held"))
  {
    g_mutex_lock(&a);
    g_mutex_clear(&a);
    g_mutex_init(&a);
  }
  else
  {
    g_printerr("unknown case: %s\n", what);
    return 2;
  }
  return 0;
}
