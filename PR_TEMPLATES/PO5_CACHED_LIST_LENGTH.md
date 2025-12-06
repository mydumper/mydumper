## Summary

Optimizes list length queries from O(n) to O(1) by adding a cached count field to the MList struct.

## Problem

The `message_dumping_data_short()` and `message_dumping_data_long()` functions in `mydumper_write.c` call `g_list_length()` on `transactional_table->list` and `non_transactional_table->list` for every progress message. With 250K tables, each `g_list_length()` call traverses up to 250K elements (O(n)).

These functions are called frequently during dump operations, making this a hot path performance issue.

## Solution

Add a `count` field to the `MList` struct and maintain it during list operations:
- Initialize to 0 when MList is created
- Increment when items are prepended to the list
- Use `->count` instead of `g_list_length()` for O(1) access

## Changes

| File | Change |
|------|--------|
| `src/mydumper/mydumper_start_dump.h` | Add `guint count` field to `struct MList` |
| `src/mydumper/mydumper_working_thread.c` | Initialize count=0 and increment on list prepend |
| `src/mydumper/mydumper_write.c` | Use `->count` instead of `g_list_length()` |
| `src/mydumper/mydumper_pmm.c` | Use `->count` instead of `g_list_length()` |

## Performance Impact

| Scenario | Before | After |
|----------|--------|-------|
| 250K tables, progress messages | O(n) = 250K traversals per call | O(1) = single field read |
| Progress logging overhead | Significant at scale | Negligible |

## Backward Compatibility

- No behavior change
- No new dependencies
- No API changes

## Thread Safety Note

The `count` field is accessed from multiple threads:
- Working threads increment count when adding items
- PMM (Percona Monitoring and Management) thread reads count for metrics

**CI Fix Applied**: Added mutex protection in `mydumper_pmm.c` when reading the count field:

```c
// Use cached count for O(1) access with proper mutex protection
g_mutex_lock(transactional_table->mutex);
guint trans_count = transactional_table->count;
g_mutex_unlock(transactional_table->mutex);
```

| Fix | Description | Commit |
|-----|-------------|--------|
| **Mutex protection** | Added lock/unlock around count access in PMM | 9c719893 |

## Testing

Tested with existing test cases. The optimization is transparent to users - only affects internal performance.
