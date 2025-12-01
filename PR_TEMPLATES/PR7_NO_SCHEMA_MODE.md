# PR: Fix --no-schema mode data loading failure

## Summary

Fix `myloader --no-schema` mode where data loading fails because database schema_state is never marked as CREATED.

## Problem

When using `--no-schema` (Phase 2 of two-phase loading), data loading completely fails because `database->schema_state` is never set to `CREATED`.

```c
// In JOB_RESTORE_SCHEMA_FILENAME case:
if (!no_schemas && (...rj->data.srj->object == CREATE_DATABASE...)) {
    // ... execute CREATE DATABASE SQL ...
    if (rj->data.srj->object == CREATE_DATABASE)
        rj->data.srj->database->schema_state = CREATED;
}
// BUG: In --no-schema mode, the above block is skipped entirely
//      database->schema_state remains NOT_CREATED
```

**Result with --no-schema:**
1. `database->schema_state` stays as `NOT_CREATED`
2. Data dispatcher checks `dbt->database->schema_state == NOT_FOUND` and skips all tables
3. Zero data jobs are dispatched
4. myloader sits idle with no progress

## Solution

Add else clause to mark database as CREATED even when skipping SQL execution:

```c
if (!no_schemas && (...rj->data.srj->object == CREATE_DATABASE...)) {
    // ... execute CREATE DATABASE SQL ...
    if (rj->data.srj->object == CREATE_DATABASE)
        rj->data.srj->database->schema_state = CREATED;
} else if (no_schemas && rj->data.srj->object == CREATE_DATABASE) {
    // FIX: In --no-schema mode, skip SQL but still mark as CREATED
    rj->data.srj->database->schema_state = CREATED;
}
```

## Use Case

Two-phase loading for large restores:

```bash
# Phase 1: Create schemas (handles FK ordering, indexes)
myloader --no-data -d /dump --threads 32

# Phase 2: Load data only (currently broken without this fix)
myloader --no-schema -d /dump --threads 32
```

## Changes

```diff
     if (!no_schemas && (...rj->data.srj->object == CREATE_DATABASE...)) {
         // ... execute CREATE DATABASE SQL ...
         if (rj->data.srj->object == CREATE_DATABASE)
             rj->data.srj->database->schema_state = CREATED;
+    } else if (no_schemas && rj->data.srj->object == CREATE_DATABASE) {
+        rj->data.srj->database->schema_state = CREATED;
     }
```

## Files Changed

```
src/myloader/myloader_restore_job.c | 4 ++++
```

## Testing

Includes test case `test_812`:
- Phase 1: `pre_myloader.sh` uses `myloader --no-data --drop-table` to create empty schemas
- Phase 2: `myloader --no-schema` loads data only

```bash
./test_mydumper.sh -c 812
```

Manual two-phase loading test:
```bash
myloader --no-data --drop-table -d /dump --threads 32  # Phase 1: Create empty schemas
myloader --no-schema -d /dump --threads 32              # Phase 2: Load data only
```

### CI Fixes Applied

| Fix | Description | Commit |
|-----|-------------|--------|
| Initial test | Added test_812 with manual schema creation | 3774b330 |
| TRUNCATE fix | Added TRUNCATE to pre_myloader.sh | a163f716 |
| Phase 1 fix | Use `myloader --no-data` for Phase 1 schema creation | 6d260e36 |
| TRUNCATE approach | Add explicit TRUNCATE for --no-schema mode | 7bf929fd |
| **Final fix** | Simplify: remove overwrite-tables, use drop-table | 62813fc6 |

**Key insight**: `--overwrite-tables` doesn't work with `--no-schema` because TRUNCATE happens during schema processing (which is skipped). The clean solution is to use `--drop-table` in Phase 1 to create empty schemas, then `--no-schema` in Phase 2 loads into empty tables without needing TRUNCATE.

## Risk

**Low**: Only affects `--no-schema` mode. Fixes broken functionality.
