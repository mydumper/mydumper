# Perf: Optimize mydumper Write Path - Eliminate Allocations and strlen() Calls

## Summary

This PR optimizes the hot path in `mydumper_write.c` by eliminating unnecessary memory allocations and strlen() calls that occur for every row/column during dumps. For billion-row dumps, this eliminates billions of strlen() calls and millions of memory allocations.

## Optimizations

### 1. GDateTime → g_get_monotonic_time() for Progress Timing

**File:** `src/mydumper/mydumper_write.c`

**Problem:** Progress timing used `g_date_time_new_now_local()` which allocates memory, requiring subsequent `g_date_time_unref()` for cleanup. This happened twice per statement flush.

**Solution:** Use `g_get_monotonic_time()` which returns a `gint64` directly with zero allocation overhead.

**Impact:** Eliminates 2 GDateTime allocations per statement flush (millions for large tables).

```c
// Before: 2 allocations per flush
GDateTime *from = g_date_time_new_now_local();
// ... work ...
GDateTime *to = g_date_time_new_now_local();
diff = g_date_time_difference(to, from) / G_TIME_SPAN_SECOND;
g_date_time_unref(from);
g_date_time_unref(to);

// After: 0 allocations
gint64 last_progress_time = g_get_monotonic_time();
// ... work ...
gint64 now_time = g_get_monotonic_time();
if ((now_time - last_progress_time) / G_TIME_SPAN_SECOND > 4) { ... }
```

### 2. mysql_hex_string() Return Value Usage

**File:** `src/mydumper/mydumper_write.c`

**Problem:** `mysql_hex_string()` returns the length of the hex string, but the code ignored it and then called `g_string_append()` which internally calls `strlen()`.

**Solution:** Capture the return value and use `g_string_append_len()`.

**Impact:** Eliminates 1 strlen() call per BLOB column.

```c
// Before: strlen() called internally
mysql_hex_string(buffers.escaped->str, *column, length);
g_string_append(buffers.column, buffers.escaped->str);

// After: strlen() eliminated
unsigned long hex_len = mysql_hex_string(buffers.escaped->str, *column, length);
g_string_append_len(buffers.column, buffers.escaped->str, hex_len);
```

### 3. mysql_real_escape_string() Return Value Usage

**File:** `src/mydumper/mydumper_write.c`

**Problem:** Same issue as above - return value ignored, strlen() called.

**Solution:** Capture return value, use `g_string_append_len()`.

**Impact:** Eliminates 1 strlen() call per string column.

```c
// Before: strlen() called internally
mysql_real_escape_string(conn, buffers.escaped->str, *column, length);
g_string_append(buffers.column, buffers.escaped->str);

// After: strlen() eliminated
unsigned long escaped_len = mysql_real_escape_string(conn, buffers.escaped->str, *column, length);
g_string_append_len(buffers.column, buffers.escaped->str, escaped_len);
```

### 4. g_string_append_len() with Known GString Lengths

**File:** `src/mydumper/mydumper_write.c`

**Problem:** After building column/row content into GStrings, the code used `g_string_append()` which calls strlen(), even though `GString->len` already contains the length.

**Solution:** Use `g_string_append_len()` with the known `->len` value.

**Impact:** Eliminates strlen() per column and per row append.

```c
// Before: strlen() called
g_string_append(buffers.row, buffers.column->str);

// After: strlen() eliminated
g_string_append_len(buffers.row, buffers.column->str, buffers.column->len);
```

## Performance Summary

| Optimization | Before | After | Improvement |
|-------------|--------|-------|-------------|
| Progress timing | 2 GDateTime allocs/flush | 0 allocs/flush | 100% eliminated |
| BLOB hex encoding | strlen() per column | Return value | 1 strlen/column saved |
| String escaping | strlen() per column | Return value | 1 strlen/column saved |
| Column append | strlen() per column | Known length | 1 strlen/column saved |
| Row append | strlen() per row | Known length | 1 strlen/row saved |

For a 1 billion row table with 10 columns: ~10 billion strlen() calls eliminated.

## Files Changed

| File | Changes |
|------|---------|
| `src/mydumper/mydumper_write.c` | g_get_monotonic_time, mysql_*_string return values, g_string_append_len |

## Testing

- Compile-tested
- No behavioral changes - only performance improvements
- Compatible with all existing mydumper options

## Backward Compatibility

These are pure performance optimizations with no API or behavioral changes.
