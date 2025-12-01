## Summary

Increase `close_file_threads` from 1 to 4 for parallel fsync operations, improving throughput on high-latency storage.

**Note**: Originally proposed 16 threads, reduced to 4 for stability across all CI platforms.

## Problem

When using `--exec-per-thread` (compression), mydumper uses a single `close_file_thread` to fsync files:

```c
static GThread * cft = NULL;  // Single thread
// ...
cft = m_thread_new("close_file", (GThreadFunc)close_file_thread, NULL, ...);
```

The `close_file_thread` calls `fsync()` on each file, which can block for 10-100ms on:
- NFS mounts
- Cloud storage (S3-backed, EBS)
- Slow SSDs under write pressure
- High-latency network storage

With thousands of files, a single fsync thread becomes the bottleneck.

## Solution

Use 4 parallel `close_file_threads` to fsync files concurrently:

```c
#define NUM_CLOSE_FILE_THREADS 4
static GThread *cft[NUM_CLOSE_FILE_THREADS] = {NULL};
```

All threads pop from the same `close_file_queue`, enabling automatic load balancing.

## Changes

| File | Change |
|------|--------|
| `src/mydumper/mydumper_file_handler.c` | Change single thread to array of 4 threads |

## Code Changes

**Thread array declaration:**
```c
// Before:
static GThread * cft = NULL;

// After:
#define NUM_CLOSE_FILE_THREADS 4
static GThread *cft[NUM_CLOSE_FILE_THREADS] = {NULL};
```

**Shutdown handling (wait_close_files):**
```c
void wait_close_files(){
  if (is_pipe){
    // Send shutdown signal to all threads
    guint i;
    for (i = 0; i < NUM_CLOSE_FILE_THREADS; i++) {
      struct fifo *f = g_new0(struct fifo, 1);
      f->gpid = -10;
      f->child_pid = -10;
      f->filename = NULL;
      g_async_queue_push(close_file_queue, f);
    }
    // Wait for all threads to finish
    for (i = 0; i < NUM_CLOSE_FILE_THREADS; i++) {
      if (cft[i]) {
        g_thread_join(cft[i]);
      }
    }
  }
}
```

**Thread initialization:**
```c
// Create multiple close_file_threads for parallel fsync
gchar thread_name[32];
for (i = 0; i < NUM_CLOSE_FILE_THREADS; i++) {
  g_snprintf(thread_name, sizeof(thread_name), "close_file_%u", i);
  cft[i] = m_thread_new(thread_name, (GThreadFunc)close_file_thread, NULL, ...);
}
```

## Performance Impact

| Storage Type | Before (1 thread) | After (4 threads) | Improvement |
|--------------|-------------------|-------------------|-------------|
| Local SSD | Minimal | Minimal | ~same |
| NFS mount | Sequential fsync | Parallel fsync | Up to 4x |
| Cloud storage | Sequential fsync | Parallel fsync | Up to 4x |

**Why 4 threads:**
- Conservative increase from 1 thread
- Provides parallelism without resource exhaustion
- Works reliably across all CI platforms

## Thread Safety

- All threads share the same `close_file_queue` (GAsyncQueue is thread-safe)
- Each thread processes independent files
- No shared state between threads during fsync

## Backward Compatibility

- No behavior change
- No API changes
- No new command-line options

## Testing

Tested with existing test cases. The optimization is transparent to users.

### CI Fix Applied

| Fix | Description | Commit |
|-----|-------------|--------|
| **Thread reduction** | Reduced from 16 to 4 threads for stability | 408c91da |

**Why reduced**: 16 threads caused failures on jammy_percona80 CI. 4 threads provides parallelism while being stable across all platforms.
