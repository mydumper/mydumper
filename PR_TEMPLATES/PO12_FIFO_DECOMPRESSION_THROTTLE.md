# PR: FIFO decompression process throttling

## Summary

Add a semaphore to limit concurrent decompression processes (zstd/gzip), preventing process exhaustion and FIFO deadlocks when restoring databases with many compressed files.

## Problem

When loading compressed dump files (`.zst`, `.gz`), `myl_open()` spawns a decompression subprocess for each file. For large databases with thousands of tables, this can spawn thousands of processes simultaneously, leading to:

1. **Process exhaustion**: Hitting system `ulimit -u` limits
2. **FIFO deadlocks**: Subprocesses die before opening FIFO, causing `fopen()` to block forever
3. **Resource starvation**: Memory and CPU exhaustion from too many concurrent processes

## Solution

Add a counting semaphore that limits concurrent decompression processes:

```c
static GCond *decompress_cond = NULL;
static GMutex *decompress_mutex = NULL;
static guint active_decompressors = 0;
static guint max_decompressors = 0;  // = min(num_threads, 32)

// In myl_open() - acquire slot before spawning
g_mutex_lock(decompress_mutex);
while (active_decompressors >= max_decompressors) {
  g_cond_wait(decompress_cond, decompress_mutex);
}
active_decompressors++;
g_mutex_unlock(decompress_mutex);

// In myl_close() - release slot
if (f->uses_decompressor) {
  release_decompressor_slot();
}
```

**Default limit**: `min(num_threads, 32)`, minimum 4

## Changes

```diff
 src/myloader/myloader_process.h | +1 (uses_decompressor field)
 src/myloader/myloader_process.c | +30 (throttle logic)
```

## Files Changed

- `src/myloader/myloader_process.h`: Add `uses_decompressor` to struct fifo
- `src/myloader/myloader_process.c`: Add semaphore initialization, acquire/release logic

## How It Works

1. **Initialization**: `initialize_process()` creates semaphore with limit based on `num_threads`
2. **Acquire**: Before spawning decompressor, wait for available slot
3. **Track**: Set `uses_decompressor=TRUE` in fifo struct
4. **Release**: In `myl_close()`, release slot if file used decompressor

## Testing

Tested with:
- Uncompressed dump: No change (no decompressors spawned)
- Compressed dump with 100 files: Works with throttling
- Compressed dump with 6000+ files: No longer exhausts processes

## Risk

**Low**:
- Minimal change to existing flow
- Only affects compressed file handling
- Uses proven GLib primitives (GMutex, GCond)
- Conservative default limit (num_threads, capped at 32)
