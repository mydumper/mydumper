/*
 * glibguard - an LD_PRELOAD lock-discipline checker for GLib synchronisation.
 *
 * GLib's GMutex, GRecMutex, GCond and GAsyncQueue are implemented on raw
 * futexes on Linux, so neither ThreadSanitizer nor Helgrind can see them: every
 * access they protect is reported as a race, and every misuse of the locks
 * themselves goes unreported. This library interposes the GLib entry points and
 * checks how they are used, so a test can assert lock discipline instead of
 * hoping a bad interleaving shows up as a crash.
 *
 * Checks (each printed once per distinct call site):
 *   unlock-not-held      unlocking a mutex nobody holds
 *   unlock-wrong-thread   unlocking a mutex held by another thread
 *   destroy-while-held    g_mutex_clear/g_mutex_free on a held mutex
 *   double-lock           re-locking a non-recursive GMutex already held by
 *                         this thread (deadlock)
 *   lock-order-inversion  A-then-B here, B-then-A somewhere else (deadlock risk
 *                         even if this run did not deadlock)
 *   held-at-exit          thread returned still holding a mutex
 *
 * Build:
 *   gcc -shared -fPIC -O2 -g glibguard.c $(pkg-config --cflags glib-2.0) \
 *       -o libglibguard.so -ldl -lpthread
 * Use:
 *   GLIBGUARD_LOG=out.txt LD_PRELOAD=./libglibguard.so ./myloader ...
 *   ./resolve.sh out.txt ./myloader        # turn module+offset into file:line
 *
 * Environment:
 *   GLIBGUARD_LOG=<path>    write findings there instead of stderr
 *   GLIBGUARD_ABORT=1       abort() on the first finding (for a core dump)
 *   GLIBGUARD_JITTER_US=<n> sleep 0..n us inside lock/unlock to widen races
 *   GLIBGUARD_QUIET=1       only print the summary line
 *
 * Exit status of the traced program is untouched; the summary line
 * "glibguard: N finding(s)" is what a test should assert on.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <glib.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define MAX_LOCKS 2048
#define MAX_HELD 64
#define MAX_SITES 4096

struct lock_rec
{
  const void *addr;      /* GMutex* / GRecMutex*, NULL when the slot is free */
  pid_t       owner;     /* tid holding it, 0 when unheld */
  int         depth;     /* recursion depth (GRecMutex) */
  int         recursive; /* 1 for GRecMutex */
  const void *site;      /* return address of the acquiring call */
};

static struct lock_rec locks[MAX_LOCKS];
static uint8_t         order[MAX_LOCKS][MAX_LOCKS / 8]; /* order[a][b]: a was held when b was taken */
static pthread_mutex_t guard = PTHREAD_MUTEX_INITIALIZER;
static __thread int    held[MAX_HELD];
static __thread int    nheld;
static __thread int    in_guard; /* re-entrancy: our own code must not recurse */
static const void     *seen_sites[MAX_SITES];
static int             nseen;
static int             findings;
static int             used;     /* this process actually used GLib locks */
static const char     *log_path; /* opened lazily: LD_PRELOAD also applies
                                    to wrappers like timeout(1)/sh, which
                                    must not truncate the log or add a
                                    summary line of their own */
static int  log_fd = -1;
static int  opt_abort, opt_quiet;
static long opt_jitter;

#define FNS(X)           \
  X(g_mutex_lock)        \
  X(g_mutex_trylock)     \
  X(g_mutex_unlock)      \
  X(g_mutex_clear)       \
  X(g_mutex_free)        \
  X(g_rec_mutex_lock)    \
  X(g_rec_mutex_trylock) \
  X(g_rec_mutex_unlock)  \
  X(g_rec_mutex_clear)   \
  X(g_cond_wait)         \
  X(g_cond_wait_until)

#define DECL(n) static typeof(n) *real_##n;
#define LOAD(n) real_##n = dlsym(RTLD_NEXT, #n);
FNS(DECL)
G_GNUC_END_IGNORE_DEPRECATIONS

static pid_t tid(void) { return (pid_t)syscall(SYS_gettid); }

__attribute__((constructor)) static void glibguard_init(void)
{
  FNS(LOAD)
  const char *e = getenv("GLIBGUARD_LOG");
  if (e && *e)
    log_path = e;
  opt_abort = getenv("GLIBGUARD_ABORT") != NULL;
  opt_quiet = getenv("GLIBGUARD_QUIET") != NULL;
  e = getenv("GLIBGUARD_JITTER_US");
  opt_jitter = e ? atol(e) : 0;
}

static void out(const char *fmt, ...)
{
  if (log_fd < 0)
  {
    log_fd = 2;
    if (log_path)
    {
      int fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd >= 0)
        log_fd = fd;
    }
  }
  char    buf[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof buf, fmt, ap);
  va_end(ap);
  if (n > 0)
  {
    ssize_t w = write(log_fd, buf, (size_t)n < sizeof buf ? (size_t)n : sizeof buf - 1);
    (void)w;
  }
}

/* Print an address as module+offset; addr2line on the module resolves it. */
static void fmt_site(const void *pc, char *buf, size_t len)
{
  Dl_info info;
  if (pc && dladdr(pc, &info) && info.dli_fbase && info.dli_fname)
  {
    const char *base = strrchr(info.dli_fname, '/');
    snprintf(buf, len, "%s+0x%lx", base ? base + 1 : info.dli_fname,
        (unsigned long)((const char *)pc - (const char *)info.dli_fbase));
  }
  else
    snprintf(buf, len, "%p", pc);
}

/* Report each finding once per call site, so a hot loop cannot flood the log. */
static int first_time(const void *site)
{
  for (int i = 0; i < nseen; i++)
    if (seen_sites[i] == site)
      return 0;
  if (nseen < MAX_SITES)
    seen_sites[nseen++] = site;
  return 1;
}

static void finding(const char *kind, const void *obj, const void *site, const char *extra, const void *site2)
{
  findings++;
  if (opt_quiet || !first_time(site))
    goto done;
  char s1[256], s2[256];
  fmt_site(site, s1, sizeof s1);
  if (site2)
  {
    fmt_site(site2, s2, sizeof s2);
    out("glibguard: %s mutex=%p tid=%d at %s (%s at %s)\n", kind, obj, (int)tid(), s1, extra, s2);
  }
  else
    out("glibguard: %s mutex=%p tid=%d at %s%s\n", kind, obj, (int)tid(), s1, extra ? extra : "");
done:
  if (opt_abort)
    abort();
}

__attribute__((destructor)) static void glibguard_fini(void)
{
  if (used || findings)
    out("glibguard: %d finding(s)\n", findings);
}

static void jitter(void)
{
  if (!opt_jitter)
    return;
  struct timespec ts = {0, (rand() % opt_jitter) * 1000L};
  nanosleep(&ts, NULL);
}

/* --- registry, all called with `guard` held ------------------------------ */

static int slot_of(const void *m, int create, int recursive)
{
  int free_slot = -1;
  for (int i = 0; i < MAX_LOCKS; i++)
  {
    if (locks[i].addr == m)
      return i;
    if (free_slot < 0 && locks[i].addr == NULL)
      free_slot = i;
  }
  if (create && free_slot >= 0)
  {
    locks[free_slot].addr = m;
    locks[free_slot].recursive = recursive;
    return free_slot;
  }
  return -1;
}

static void order_set(int a, int b) { order[a][b / 8] |= (uint8_t)(1u << (b % 8)); }
static int  order_get(int a, int b) { return (order[a][b / 8] >> (b % 8)) & 1; }

static void slot_clear(int i)
{
  memset(&locks[i], 0, sizeof locks[i]);
  memset(order[i], 0, sizeof order[i]);
  for (int j = 0; j < MAX_LOCKS; j++)
    order[j][i / 8] &= (uint8_t)~(1u << (i % 8));
}

/* --- interposed entry points --------------------------------------------- */

static void note_acquired(const void *m, const void *site, int recursive)
{
  if (in_guard)
    return;
  in_guard = 1;
  used = 1;
  pthread_mutex_lock(&guard);
  int i = slot_of(m, 1, recursive);
  if (i >= 0)
  {
    /* Record and check acquisition order against every lock already held. */
    for (int h = 0; h < nheld; h++)
    {
      int a = held[h];
      if (a == i)
        continue;
      if (order_get(i, a) && !order_get(a, i))
        finding("lock-order-inversion", m, site, "opposite order taken", locks[a].site);
      order_set(a, i);
    }
    locks[i].owner = tid();
    locks[i].depth++;
    locks[i].site = site;
    if (nheld < MAX_HELD)
      held[nheld++] = i;
  }
  pthread_mutex_unlock(&guard);
  in_guard = 0;
}

/* Called before the real lock: a non-recursive GMutex this thread already
   holds is about to deadlock, and after the call we would never report it. */
static void note_locking(const void *m, const void *site, int recursive)
{
  if (in_guard || recursive)
    return;
  in_guard = 1;
  used = 1;
  pthread_mutex_lock(&guard);
  int i = slot_of(m, 0, recursive);
  if (i >= 0 && locks[i].depth > 0 && locks[i].owner == tid())
    finding("double-lock", m, site, "already held by this thread", locks[i].site);
  pthread_mutex_unlock(&guard);
  in_guard = 0;
}

static void note_releasing(const void *m, const void *site, int recursive)
{
  if (in_guard)
    return;
  in_guard = 1;
  used = 1;
  pthread_mutex_lock(&guard);
  int i = slot_of(m, 0, recursive);
  if (i < 0 || locks[i].depth == 0)
    finding("unlock-not-held", m, site, " (no thread holds this mutex)", NULL);
  else if (locks[i].owner != tid())
    finding("unlock-wrong-thread", m, site, "locked", locks[i].site);
  else
  {
    if (--locks[i].depth == 0)
      locks[i].owner = 0;
    for (int h = nheld - 1; h >= 0; h--)
      if (held[h] == i)
      {
        memmove(&held[h], &held[h + 1], (size_t)(nheld - h - 1) * sizeof held[0]);
        nheld--;
        break;
      }
  }
  pthread_mutex_unlock(&guard);
  in_guard = 0;
}

void g_mutex_lock(GMutex *m)
{
  note_locking(m, __builtin_return_address(0), 0);
  jitter();
  real_g_mutex_lock(m);
  note_acquired(m, __builtin_return_address(0), 0);
}

gboolean g_mutex_trylock(GMutex *m)
{
  gboolean r = real_g_mutex_trylock(m);
  if (r)
    note_acquired(m, __builtin_return_address(0), 0);
  return r;
}

void g_mutex_unlock(GMutex *m)
{
  note_releasing(m, __builtin_return_address(0), 0);
  jitter();
  real_g_mutex_unlock(m);
}

void g_rec_mutex_lock(GRecMutex *m)
{
  jitter();
  real_g_rec_mutex_lock(m);
  note_acquired(m, __builtin_return_address(0), 1);
}

gboolean g_rec_mutex_trylock(GRecMutex *m)
{
  gboolean r = real_g_rec_mutex_trylock(m);
  if (r)
    note_acquired(m, __builtin_return_address(0), 1);
  return r;
}

void g_rec_mutex_unlock(GRecMutex *m)
{
  note_releasing(m, __builtin_return_address(0), 1);
  jitter();
  real_g_rec_mutex_unlock(m);
}

static void note_destroy(const void *m, const void *site)
{
  in_guard = 1;
  used = 1;
  pthread_mutex_lock(&guard);
  int i = slot_of(m, 0, 0);
  if (i >= 0)
  {
    if (locks[i].depth > 0)
      finding("destroy-while-held", m, site, "locked", locks[i].site);
    slot_clear(i);
  }
  pthread_mutex_unlock(&guard);
  in_guard = 0;
}

void g_mutex_clear(GMutex *m)
{
  note_destroy(m, __builtin_return_address(0));
  real_g_mutex_clear(m);
}

void g_mutex_free(GMutex *m)
{
  note_destroy(m, __builtin_return_address(0));
  real_g_mutex_free(m);
}

void g_rec_mutex_clear(GRecMutex *m)
{
  note_destroy(m, __builtin_return_address(0));
  real_g_rec_mutex_clear(m);
}

/* g_cond_wait drops and reacquires the mutex. */
void g_cond_wait(GCond *c, GMutex *m)
{
  const void *site = __builtin_return_address(0);
  note_releasing(m, site, 0);
  real_g_cond_wait(c, m);
  note_acquired(m, site, 0);
}

gboolean g_cond_wait_until(GCond *c, GMutex *m, gint64 t)
{
  const void *site = __builtin_return_address(0);
  note_releasing(m, site, 0);
  gboolean r = real_g_cond_wait_until(c, m, t);
  note_acquired(m, site, 0);
  return r;
}
