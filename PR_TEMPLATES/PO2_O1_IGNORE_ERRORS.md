## Summary

Optimizes `--ignore-errors` lookup from O(n) to O(1) using a GHashTable alongside the existing GList.

## Problem

The `--ignore-errors` option stores error codes in a GList. Each error check uses `g_list_find()` which is O(n) - it scans the entire list for every error occurrence. With many ignored errors or high error rates, this becomes a bottleneck.

## Solution

Build a GHashTable (`ignore_errors_set`) alongside the GList when parsing `--ignore-errors`. Use `g_hash_table_contains()` for O(1) lookups instead of `g_list_find()`.

## Changes

| File | Change |
|------|--------|
| `src/common.h` | Add `extern GHashTable *ignore_errors_set` and `should_ignore_error_code()` declaration |
| `src/common.c` | Add `ignore_errors_set` definition and `should_ignore_error_code()` function |
| `src/common_options.c` | Build hash set when parsing `--ignore-errors` option |
| `src/myloader/myloader_restore.c` | Use `should_ignore_error_code()` instead of `g_list_find()` |

## Performance Impact

| Scenario | Before | After |
|----------|--------|-------|
| 50 ignored error codes, 1M errors | 50M comparisons | 1M hash lookups |
| Lookup complexity | O(n) | O(1) |

## Backward Compatibility

- No behavior change
- No new dependencies
- GList is kept for any code that may iterate over the list

## Testing

Tested with existing test cases. The optimization is transparent to users.
