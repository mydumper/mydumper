# PR: Fix silent schema job loss when pushing to retry queue

## Summary

Fix a bug where failed schema jobs are silently lost instead of being retried, causing tables to never be created during restore.

## Problem

In `process_schema()`, when a schema job fails and needs to be retried, the code pushes the wrong variable to `retry_queue`:

```c
gboolean process_schema(struct thread_data * td){
  struct control_job *job = NULL;  // Declared but NEVER assigned
  struct schema_job * schema_job = g_async_queue_pop(schema_job_queue);

  switch (schema_job->type){
    case SCHEMA_TABLE_JOB:
      if (process_restore_job(td, schema_job->restore_job)){
        g_async_queue_push(retry_queue, job);  // BUG: pushes NULL!
      }
      break;
  }
}
```

**Impact**:
- Failed schema jobs are silently lost (NULL is pushed, actual job is discarded)
- Tables that fail creation on first attempt are never retried
- In high-concurrency restores (32 threads, 6000+ tables), this can result in hundreds of tables silently missing
- myloader exits with success (exit code 0) despite missing tables

## Solution

Push `schema_job` instead of the unused `job` variable:

```c
gboolean process_schema(struct thread_data * td){
  struct database * _database = NULL;
  // Removed: struct control_job *job = NULL;

  struct schema_job * schema_job = g_async_queue_pop(schema_job_queue);

  switch (schema_job->type){
    case SCHEMA_TABLE_JOB:
      if (process_restore_job(td, schema_job->restore_job)){
        g_async_queue_push(retry_queue, schema_job);  // FIX: push correct variable
      }
      break;
  }
}
```

## Changes

```diff
 gboolean process_schema(struct thread_data * td){
   struct database * _database = NULL;
-  struct control_job *job = NULL;

   struct schema_job * schema_job = g_async_queue_pop(schema_job_queue);
   ...
     case SCHEMA_TABLE_JOB:
       if (process_restore_job(td, schema_job->restore_job)){
-        trace("retry_queue <- ");
-        g_async_queue_push(retry_queue, job);
+        trace("retry_queue <- %s", schema_job_type2str(schema_job->type));
+        g_async_queue_push(retry_queue, schema_job);
       }
```

## Files Changed

```
src/myloader/myloader_worker_schema.c | 5 ++---
 1 file changed, 2 insertions(+), 3 deletions(-)
```

## Testing

- Existing CI test suite exercises schema creation paths
- Tests with multiple tables and threads will trigger the retry mechanism
- Any test that had intermittent "missing tables" failures should be more reliable

## Risk Assessment

**Very Low**:
- This is an obvious bug fix (pushing wrong variable)
- No behavioral change for success cases
- Only affects the error recovery path
- Removes dead code (unused variable)
