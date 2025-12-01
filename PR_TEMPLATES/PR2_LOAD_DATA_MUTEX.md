# PR: Fix LOAD DATA streaming synchronization with proper condition variables

## Problem

The `load_data_mutex_locate()` function used an unconventional mutex pattern to synchronize `.sql` file processing with `.dat` file arrival in streaming mode. This pattern relied on undefined behavior:

1. **Double-lock**: Same thread locked the same mutex twice
2. **Cross-thread unlock**: One thread unlocked a mutex locked by another thread

```
STREAMING MODE SYNCHRONIZATION
==============================

When using mydumper with --stream --load-data, files arrive in this order:
  1. table-schema.sql    (CREATE TABLE)
  2. table.sql           (LOAD DATA 'table.dat' statement)
  3. table.dat.zst       (compressed row data)

The .sql file references the .dat file, but .dat may not have arrived yet.
The code must WAIT for .dat before executing LOAD DATA.

ORIGINAL IMPLEMENTATION (undefined behavior)
============================================

Thread A (.sql processor):              Thread B (.dat processor):
------------------------                ------------------------
load_data_mutex_locate()
  |
  +-> create mutex
  +-> g_mutex_lock(mutex)  [LOCK #1]
  +-> insert to hash
  +-> return TRUE

if (TRUE) g_mutex_lock(mutex) [LOCK #2]
  |                                     release_load_data_as_it_is_close()
  +-> BLOCKS (mutex already locked)       |
      waiting for unlock...               +-> lookup mutex in hash
                                          +-> g_mutex_unlock(mutex) [UNLOCK]
  +-> unblocks, continues                     (unlocking mutex locked
                                               by different thread!)

PROBLEMS:
---------
1. Double-lock on non-recursive mutex = undefined behavior (POSIX)
2. Cross-thread unlock = undefined behavior (POSIX)
3. Works on glibc by accident, may fail on other implementations
```

## Investigation

### Initial Fix Attempt: Remove Internal Lock

Our first attempt removed the internal lock, expecting the caller to handle locking:

```c
// WRONG FIX - created race condition
*mutex = g_mutex_new();
// g_mutex_lock(*mutex);  // REMOVED - let caller lock
g_hash_table_insert(load_data_list, filename, *mutex);
return TRUE;
```

**Result**: CI failures with checksum mismatch on `sakila.rental`

### Root Cause Analysis

```
RACE CONDITION WITH REMOVED INTERNAL LOCK
=========================================

Thread A (.sql):                Thread B (.dat):
---------------                 ----------------
1. lock(global_mutex)
2. mutex = g_mutex_new()
3. // NO LOCK (removed)
4. hash_insert(file, mutex)
5. unlock(global_mutex)
                                1. lock(global_mutex)
   <-- RACE WINDOW              2. mutex = hash_lookup(file)
                                3. g_mutex_unlock(mutex)
                                      |
                                      +-> UNLOCKING MUTEX THAT
                                          WAS NEVER LOCKED!
6. g_mutex_lock(mutex)          4. unlock(global_mutex)
      |
      +-> too late, .dat already "signaled"

RESULT: Unlock of never-locked mutex = undefined behavior
```

The internal lock must happen BEFORE the mutex is visible in the hash table.

### Second Fix Attempt: Keep Internal Lock, Remove Caller Lock

```c
// Still wrong - removes the wait mechanism
load_data_mutex_locate(filename, &mutex);
// if (...) g_mutex_lock(mutex);  // REMOVED
```

**Result**: CI failures - the second lock WAS the wait mechanism!

The original code's "double-lock" was intentional: the second lock BLOCKS until another thread unlocks. This is the synchronization mechanism.

## Solution

Replace the mutex hack with proper condition variable synchronization:

```
NEW IMPLEMENTATION (correct synchronization)
============================================

struct load_data_sync {
    GMutex *mutex;
    GCond  *cond;
    gboolean ready;  // TRUE when .dat file received
};

CASE 1: .sql arrives FIRST (must wait for .dat)
-----------------------------------------------

Thread A (.sql):                Thread B (.dat):
---------------                 ----------------
load_data_mutex_locate()
  |
  +-> sync = g_new(load_data_sync)
  +-> sync->ready = FALSE
  +-> hash_insert(file, sync)
  |
  +-> g_mutex_lock(sync->mutex)
  +-> while (!sync->ready)
  |     g_cond_wait(cond, mutex)   <-- BLOCKS
  |                                 release_load_data_as_it_is_close()
  |                                   |
  |                                   +-> sync = hash_lookup(file)
  |                                   +-> g_mutex_lock(sync->mutex)
  |                                   +-> sync->ready = TRUE
  +-> wakes up <-------------------   +-> g_cond_broadcast(cond)
  +-> g_mutex_unlock(sync->mutex)     +-> g_mutex_unlock(sync->mutex)
  |
  +-> continues with LOAD DATA


CASE 2: .dat arrives FIRST (no wait needed)
-------------------------------------------

Thread B (.dat):                Thread A (.sql):
----------------                ---------------
release_load_data_as_it_is_close()
  |
  +-> sync = g_new(load_data_sync)
  +-> sync->ready = TRUE  // Already ready!
  +-> hash_insert(file, sync)
                                load_data_mutex_locate()
                                  |
                                  +-> sync = hash_lookup(file)
                                  +-> sync->ready == TRUE
                                  +-> return immediately (no wait)
                                  |
                                  +-> continues with LOAD DATA
```

## Changes

```diff
 // New synchronization structure
+struct load_data_sync {
+    GMutex *mutex;
+    GCond *cond;
+    gboolean ready;
+};

 // Simplified function signature - no longer returns mutex
-gboolean load_data_mutex_locate(gchar *filename, GMutex **mutex)
+void load_data_mutex_locate(gchar *filename)

 // Wait using condition variable instead of mutex hack
-if (load_data_mutex_locate(filename, &mutex))
-    g_mutex_lock(mutex);
+load_data_mutex_locate(filename);
```

## Files Changed

```
src/myloader/myloader_restore.c  | 69 insertions(+), 26 deletions(-)
```

## Testing

- `test_451`: Streaming + LOAD DATA + checksums (existing test)
- Manual testing with high concurrency (16+ threads)

## Compatibility

- Preserves exact same synchronization semantics
- No change to command-line interface or file formats
- Replaces undefined behavior with well-defined POSIX primitives
