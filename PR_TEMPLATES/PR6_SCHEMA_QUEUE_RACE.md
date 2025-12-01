# PR: Fix schema job queue race condition with database mutex

## Summary

Fix race condition where schema jobs are lost when `SCHEMA_PROCESS_ENDED` handler drains database queues without holding the mutex.

## Problem

When `SCHEMA_PROCESS_ENDED` is received, `set_db_schema_created()` is called for all databases to drain their `table_queue` buffers into the main `schema_job_queue`. However, this happens WITHOUT holding `_database->mutex`, creating a race with `schema_push()`:

```
RACE CONDITION
==============

Thread A (file_type_worker):           Thread B (schema_worker):
---------------------------            -------------------------
schema_push(tableX)
  |
  +-> g_mutex_lock(_database->mutex)
  +-> check schema_state (NOT_CREATED)
                                       SCHEMA_PROCESS_ENDED received
                                         |
                                         +-> for each database:
                                         |     set_db_schema_created()
                                         |       sets schema_state = CREATED
                                         |       drains table_queue to schema_job_queue
                                         |
  +-> push to _database->table_queue   ← AFTER drain completed!
  +-> g_mutex_unlock(_database->mutex)

RESULT: Job for tableX is stuck in table_queue forever
        Table never gets created, no error logged
```

## Solution

Hold `_database->mutex` when calling `set_db_schema_created()`:

```c
case SCHEMA_PROCESS_ENDED:
  GHashTableIter iter;
  gpointer _key;
  g_hash_table_iter_init(&iter, database_hash);
  while (g_hash_table_iter_next(&iter, &_key, (gpointer)&_database)){
    g_mutex_lock(_database->mutex);      // FIX: Hold mutex
    set_db_schema_created(_database);
    g_mutex_unlock(_database->mutex);    // FIX: Release mutex
  }
  break;
```

## Changes

```diff
     case SCHEMA_PROCESS_ENDED:
       GHashTableIter iter;
       gpointer _key;
       g_hash_table_iter_init(&iter, database_hash);
       while (g_hash_table_iter_next(&iter, &_key, (gpointer)&_database)){
+        g_mutex_lock(_database->mutex);
         set_db_schema_created(_database);
+        g_mutex_unlock(_database->mutex);
       }
       break;
```

## Files Changed

```
src/myloader/myloader_worker_schema.c | 4 +++-
```

## Testing

Includes test case `test_811`:
- Creates 100 tables in `test_811` database
- Dumps with 8 threads, restores with 8 threads + 4 schema creation threads
- Stresses the `SCHEMA_PROCESS_ENDED` code path where the race occurs
- All 100 tables should be created successfully

```bash
./test_mydumper.sh -c 811
```

## Risk

**Low**: Adds proper mutex protection. May slightly increase lock contention, but correctness is more important than the microseconds saved.
