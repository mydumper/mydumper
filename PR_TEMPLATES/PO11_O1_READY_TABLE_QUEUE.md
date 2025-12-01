# PR: O(1) ready table queue for job dispatch

## Summary

Add an O(1) ready table queue to eliminate O(n) table list scans during job dispatch, significantly improving restore performance for databases with many tables.

## Problem

The current job dispatch in `give_me_next_data_job_conf()` does an O(n) scan of `loading_table_list` to find a table with pending jobs:

```c
GList *iter = conf->loading_table_list;
while (iter != NULL) {
  dbt = iter->data;
  // Check if this table has jobs...
  iter = iter->next;
}
```

For a database with 10,000+ tables, this scan happens on every job dispatch, resulting in millions of iterations during a restore.

## Solution

Add a `ready_table_queue` that holds tables known to have pending jobs:

1. **O(1) dispatch**: Try `ready_table_queue` first before falling back to O(n) scan
2. **Self-populating**: When a job is dispatched, if the table has more jobs, re-enqueue it
3. **Fallback**: O(n) scan still works as before, ensuring correctness

```c
// Try O(1) dispatch first
while ((dbt = g_async_queue_try_pop(conf->ready_table_queue)) != NULL) {
  table_lock(dbt);
  dbt->in_ready_queue = FALSE;

  // Re-validate and dispatch
  if (ready_conditions_met) {
    dispatch_job();
    enqueue_table_if_ready_locked(conf, dbt);  // Re-enqueue if more jobs
    return FALSE;  // Success
  }
  table_unlock(dbt);
}

// Fallback to O(n) scan
```

## Performance Impact

| Tables | Before (O(n) per dispatch) | After (O(1) queue) | Improvement |
|--------|---------------------------|-------------------|-------------|
| 1,000  | 1M+ iterations/restore    | ~1,000 iterations | ~1000x |
| 10,000 | 100M+ iterations/restore  | ~10,000 iterations | ~10,000x |
| 100,000| 10B+ iterations/restore   | ~100,000 iterations | ~100,000x |

## Changes

```diff
 src/myloader/myloader.h                   | +2 (ready_table_queue field)
 src/myloader/myloader_table.h             | +2 (in_ready_queue field)
 src/myloader/myloader.c                   | +1 (queue initialization)
 src/myloader/myloader_table.c             | +1 (flag initialization)
 src/myloader/myloader_worker_loader_main.c| +45 (O(1) dispatch logic)
```

## Files Changed

- `src/myloader/myloader.h`: Add `GAsyncQueue *ready_table_queue` to configuration
- `src/myloader/myloader_table.h`: Add `gboolean in_ready_queue` to db_table
- `src/myloader/myloader.c`: Initialize the queue
- `src/myloader/myloader_table.c`: Initialize the flag to FALSE
- `src/myloader/myloader_worker_loader_main.c`: Add O(1) dispatch logic

## How It Works

1. **Queue population**: When a job is dispatched from any table, if more jobs remain, the table is enqueued
2. **O(1) dispatch**: Next dispatch tries the queue first
3. **Re-validation**: Tables are re-checked when popped (state may have changed)
4. **Fallback safety**: If queue is empty, O(n) scan still works

## Testing

The optimization is transparent - same behavior, just faster dispatch.

Tested with:
- Single table restore (no change)
- Multi-table restore (significant improvement)
- Large-scale restore (6000+ tables)

## Risk

**Low**:
- Fallback to O(n) scan ensures correctness
- No change to external behavior
- Uses proven GLib async queue
- `in_ready_queue` flag prevents duplicate enqueuing
