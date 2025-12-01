# PR: Auto-scale schema/index threads based on CPU count

## Summary

Automatically scale `max_threads_for_schema_creation` and `max_threads_for_index_creation` based on available CPU cores, improving restore performance on modern multi-core systems.

## Problem

The default values for schema and index creation threads are hardcoded to 4:
- `max_threads_for_schema_creation = 4`
- `max_threads_for_index_creation = 4`

On modern systems with 8+ cores, these defaults leave significant parallelism potential unused during schema creation and index building phases.

## Solution

Add automatic thread scaling that detects CPU count and adjusts thread limits:

```c
guint cpu_count = g_get_num_processors();
if (max_threads_for_schema_creation == 4 && cpu_count > 4) {
  guint auto_schema_threads = cpu_count > 8 ? 8 : cpu_count;
  max_threads_for_schema_creation = auto_schema_threads;
}
if (max_threads_for_index_creation == 4 && cpu_count > 4) {
  guint auto_index_threads = cpu_count > 8 ? 8 : cpu_count;
  max_threads_for_index_creation = auto_index_threads;
}
```

**Behavior**:
- Only auto-scales if user hasn't explicitly set these values (checks for default of 4)
- Scales up to CPU count, capped at 8 threads maximum
- No change on systems with 4 or fewer cores

| CPU Cores | Schema Threads | Index Threads |
|-----------|----------------|---------------|
| 1-4       | 4 (default)    | 4 (default)   |
| 5-7       | CPU count      | CPU count     |
| 8+        | 8              | 8             |

## Changes

```diff
 src/myloader/myloader.c | +12 lines
```

## Testing

Tested on:
- 4-core system: No change in behavior
- 8-core system: Schema/index threads scaled to 8
- 16-core system: Schema/index threads capped at 8

User can still override with explicit flags:
```bash
myloader --max-threads-for-schema-creation=2 --max-threads-for-index-creation=2 ...
```

## Risk

**Very Low**:
- Only affects thread count defaults
- Respects explicit user configuration
- Capped at reasonable maximum (8)
- Uses proven GLib API (`g_get_num_processors()`)
