# PR: Fix --no-data mode deadlock with index threads

## Summary

Fix deadlock when using `myloader --no-data` where index threads never receive shutdown signal.

## Problem

When using `--no-data` flag (schema-only restore), the index worker threads are started but never receive `JOB_SHUTDOWN`. They wait forever on `conf->index_queue`, causing myloader to hang.

```
Normal flow (with data):
1. Schema workers finish
2. Data workers finish
3. Data workers send JOB_SHUTDOWN to index queue
4. Index workers exit

--no-data flow (broken):
1. Schema workers finish
2. No data workers exist
3. Nobody sends JOB_SHUTDOWN to index queue
4. Index workers wait forever ← DEADLOCK
```

## Solution

In `wait_schema_worker_to_finish()`, send `JOB_SHUTDOWN` to index threads when in `--no-data` mode:

```c
void wait_schema_worker_to_finish(struct configuration *conf){
  // FIX: In --no-data mode, send JOB_SHUTDOWN to index threads
  if (no_data && conf->index_queue != NULL) {
    guint num_index_threads = max_threads_for_index_creation > 0
                            ? max_threads_for_index_creation
                            : num_threads;
    for (guint i = 0; i < num_index_threads; i++) {
      g_async_queue_push(conf->index_queue, new_control_job(JOB_SHUTDOWN, NULL, NULL));
    }
  }

  // ... rest of function
}
```

## Changes

```diff
 void wait_schema_worker_to_finish(struct configuration *conf){
+  // FIX: In --no-data mode, send JOB_SHUTDOWN to index threads
+  if (no_data && conf->index_queue != NULL) {
+    guint num_index_threads = max_threads_for_index_creation > 0
+                            ? max_threads_for_index_creation
+                            : num_threads;
+    for (guint i = 0; i < num_index_threads; i++) {
+      g_async_queue_push(conf->index_queue, new_control_job(JOB_SHUTDOWN, NULL, NULL));
+    }
+  }
+
   guint n=0;
   trace("Waiting schema worker to finish");
```

## Files Changed

```
src/myloader/myloader_worker_schema.c | 10 ++++++++++
```

## Testing

```bash
# This command should complete without hanging
myloader --no-data -d /path/to/dump --threads 32
```

## Risk

**Low**: Only affects `--no-data` mode. Normal restores unchanged.
