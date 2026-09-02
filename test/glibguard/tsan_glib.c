// LD_PRELOAD shim: tell ThreadSanitizer about GLib's futex-based locks and queues,
// which it cannot intercept (GMutex/GCond/GAsyncQueue bypass pthread on Linux).
#define _GNU_SOURCE
#include <dlfcn.h>
#include <glib.h>
#include <sanitizer/tsan_interface.h>
#define FNS(X)                      \
  X(g_mutex_lock)                   \
  X(g_mutex_trylock)                \
  X(g_mutex_unlock)                 \
  X(g_rec_mutex_lock)               \
  X(g_rec_mutex_unlock)             \
  X(g_cond_wait)                    \
  X(g_cond_wait_until)              \
  X(g_async_queue_push)             \
  X(g_async_queue_push_unlocked)    \
  X(g_async_queue_pop)              \
  X(g_async_queue_try_pop)          \
  X(g_async_queue_timeout_pop)      \
  X(g_async_queue_pop_unlocked)     \
  X(g_async_queue_lock)             \
  X(g_async_queue_unlock)           \
  X(g_async_queue_push_sorted)      \
  X(g_async_queue_push_front)       \
  X(g_async_queue_try_pop_unlocked) \
  X(g_async_queue_timeout_pop_unlocked)

#define DECL(n) static typeof(n) *real_##n;
#define LOAD(n) real_##n = dlsym(RTLD_NEXT, #n);
FNS(DECL)
__attribute__((constructor)) static void init(void) { FNS(LOAD) }
void                                     g_mutex_lock(GMutex *m)
{
  real_g_mutex_lock(m);
  __tsan_acquire(m);
}
gboolean g_mutex_trylock(GMutex *m)
{
  gboolean r = real_g_mutex_trylock(m);
  if (r)
    __tsan_acquire(m);
  return r;
}
void g_mutex_unlock(GMutex *m)
{
  __tsan_release(m);
  real_g_mutex_unlock(m);
}
void g_rec_mutex_lock(GRecMutex *m)
{
  real_g_rec_mutex_lock(m);
  __tsan_acquire(m);
}
void g_rec_mutex_unlock(GRecMutex *m)
{
  __tsan_release(m);
  real_g_rec_mutex_unlock(m);
}
void g_cond_wait(GCond *c, GMutex *m)
{
  __tsan_release(m);
  real_g_cond_wait(c, m);
  __tsan_acquire(m);
}
gboolean g_cond_wait_until(GCond *c, GMutex *m, gint64 t)
{
  __tsan_release(m);
  gboolean r = real_g_cond_wait_until(c, m, t);
  __tsan_acquire(m);
  return r;
}
void g_async_queue_push(GAsyncQueue *q, gpointer d)
{
  __tsan_release(q);
  real_g_async_queue_push(q, d);
}
void g_async_queue_push_unlocked(GAsyncQueue *q, gpointer d)
{
  __tsan_release(q);
  real_g_async_queue_push_unlocked(q, d);
}
gpointer g_async_queue_pop(GAsyncQueue *q)
{
  gpointer r = real_g_async_queue_pop(q);
  __tsan_acquire(q);
  return r;
}
gpointer g_async_queue_try_pop(GAsyncQueue *q)
{
  gpointer r = real_g_async_queue_try_pop(q);
  if (r)
    __tsan_acquire(q);
  return r;
}
gpointer g_async_queue_timeout_pop(GAsyncQueue *q, guint64 t)
{
  gpointer r = real_g_async_queue_timeout_pop(q, t);
  if (r)
    __tsan_acquire(q);
  return r;
}
gpointer g_async_queue_pop_unlocked(GAsyncQueue *q)
{
  gpointer r = real_g_async_queue_pop_unlocked(q);
  __tsan_acquire(q);
  return r;
}
void g_async_queue_lock(GAsyncQueue *q)
{
  real_g_async_queue_lock(q);
  __tsan_acquire(q);
}
void g_async_queue_unlock(GAsyncQueue *q)
{
  __tsan_release(q);
  real_g_async_queue_unlock(q);
}
void g_async_queue_push_sorted(GAsyncQueue *q, gpointer d, GCompareDataFunc f, gpointer u)
{
  __tsan_release(q);
  real_g_async_queue_push_sorted(q, d, f, u);
}
void g_async_queue_push_front(GAsyncQueue *q, gpointer d)
{
  __tsan_release(q);
  real_g_async_queue_push_front(q, d);
}
gpointer g_async_queue_try_pop_unlocked(GAsyncQueue *q)
{
  gpointer r = real_g_async_queue_try_pop_unlocked(q);
  if (r)
    __tsan_acquire(q);
  return r;
}
gpointer g_async_queue_timeout_pop_unlocked(GAsyncQueue *q, guint64 t)
{
  gpointer r = real_g_async_queue_timeout_pop_unlocked(q, t);
  if (r)
    __tsan_acquire(q);
  return r;
}
