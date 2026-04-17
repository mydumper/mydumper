## Summary

Fix a race condition in myloader where `--stream` mode deletes a schema-create file before the schema worker thread can open it, causing `CRITICAL: cannot open file mydumper_0-schema-create.sql (2)`.

Cache the `CREATE DATABASE` statement in memory during the first read so the schema worker never needs to re-open the file.

## Bug

When all three conditions are met:

1. `--stream` mode (causes `myl_open()` to delete files after opening)
2. Database name triggers `mydumper_` filename renaming (name contains special characters, e.g. a dot)
3. No `--database` flag on myloader (so it must execute `CREATE DATABASE` from the file)

...then `process_database_filename()` opens and deletes the schema-create file via `get_database_name_from_content()` → `myl_open()`, then queues a schema job referencing that same file. When the schema worker later tries to open it via `restore_data_from_file()`, the file is already gone.

```
process_database_filename()                  schema worker thread
        |                                            |
  get_database_name_from_content()                   |
        |→ myl_open() → fopen + unlink  ← DELETED   |
        |→ reads CREATE DATABASE statement           |
        |→ myl_close()                               |
  schema_push(filename, statement=NULL)              |
        |                                            |
      (done)                              queue pop → restore_data_from_file()
                                                    → myl_open(filename)
                                                    → FILE NOT FOUND (errno 2)
```

## Fix

`get_database_name_from_content()` already reads the full `CREATE DATABASE` statement into a `GString` (`data`). This change captures that content and passes it through the existing `schema_restore_job.statement` field, so the schema worker can execute from memory via `restore_data_in_gstring()` instead of re-reading from disk.

This reuses existing infrastructure — no new structs, no new functions, no new fields.

### Changed files

**`src/myloader/myloader_process.c`** (+12 -4)

- `get_database_name_from_content()` — add `GString **out_content` parameter. When non-NULL, copies the parsed statement into a new `GString` before returning. The `FILE*` from `myl_open()` is still valid after `unlink` (Unix keeps the inode alive until the last fd closes), so the read succeeds.

- `process_database_filename()` — pass `&db_content` to `get_database_name_from_content()` for `mydumper_`-prefixed databases. Pass the captured `GString` to `schema_push()` as the `statement` argument. Free `db_content` on early-return paths where it isn't transferred to the job.

**`src/myloader/myloader_restore_job.c`** (+3 -1)

- `JOB_RESTORE_SCHEMA_FILENAME` handler — when `rj->data.srj->statement` is non-NULL, call `restore_data_in_gstring()` (execute from memory) instead of `restore_data_from_file()` (which would fail on the deleted file). Both return `int` with the same semantics.

**`test/clean_databases.sql`** (+1)

- Add `DROP DATABASE IF EXISTS \`db.dot\`` for test cleanup.

### What doesn't change

- Non-`mydumper_` databases are unaffected: `get_database_name_from_content()` is never called.

## Test

New integration test case `test/specific_32`:

- Creates a database named `` `db.dot` `` — the dot forces `determine_filename()` to rename it to `mydumper_0`, triggering the bug path.
- Dumps with `stream=1`, restores with `stream=1` and no `--database` flag.
- Without the fix: myloader aborts with `CRITICAL: cannot open file mydumper_0-schema-create.sql (2)`.
- With the fix: myloader exits 0 and all 3 rows are restored.

Verified in Docker (Debian bookworm + MySQL 8.4):

```
# Without fix
(myloader:5872): GLib-CRITICAL: cannot open file mydumper_0-schema-create.sql (2)
exit: 133 (SIGTRAP from G_DEBUG=fatal-criticals)

# With fix
Thread 5: restoring create database on `db.dot` from mydumper_0-schema-create.sql
exit: 0
SELECT COUNT(*) FROM `db.dot`.t1 → 3
```

## Build

Clean build, zero warnings (Debian bookworm):

```
cmake /mydumper && make -j$(nproc)
[100%] Built target myloader
[100%] Built target mydumper
```
