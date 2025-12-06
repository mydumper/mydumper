## Summary

Increases I/O buffer sizes to reduce syscall overhead and memory reallocations during data processing.

## Problem

Small default buffer sizes cause:
1. More `fgets()` calls for large INSERT statements
2. Frequent `g_string_maybe_expand()` calls as GString buffers grow
3. Increased syscall overhead when reading data files

## Changes

| File | Location | Before | After | Rationale |
|------|----------|--------|-------|-----------|
| `src/common.c` | `read_data()` | 4KB | 64KB | Fewer fgets calls for large data |
| `src/common.c` | `common_build_schema_table_filename()` | 20 bytes | 128 bytes | Typical filenames are longer |
| `src/myloader/myloader_restore.c` | `restore_data_in_gstring_by_statement()` | prefix length | prefix + 64KB | INSERT statements often exceed prefix |
| `src/myloader/myloader_restore.c` | `restore_data_from_mysqldump_file()` | 256 bytes | 4KB (schema) / 64KB (data) | Data files are much larger |
| `src/myloader/myloader_restore.c` | `restore_data_from_mydumper_file()` | 256 bytes | 4KB (schema) / 64KB (data) | Data files are much larger |

## Performance Impact

| Metric | Improvement |
|--------|-------------|
| fgets calls per MB | ~16x fewer (64KB vs 4KB buffer) |
| GString reallocations | Significantly reduced |
| Syscall overhead | Lower |

## Backward Compatibility

- No behavior change
- No new CLI options
- Schema files still use smaller buffers (4KB) since they're typically smaller

## Testing

Tested with existing test cases. Purely internal buffer sizing change.
