# CLAUDE.md - MyDumper Upstream Contribution Guide

This repo is for preparing upstream PRs to [mydumper/mydumper](https://github.com/mydumper/mydumper).

## Repository Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  FORK (Development)              UPSTREAM (PR Prep)         GitHub     │
│  mydumper-fork-optimize-myloader  mydumper-upstream         mydumper/  │
│  ─────────────────────────────    ─────────────────         mydumper   │
│  - All development work           - Clean PR branches        - PRs     │
│  - Testing & validation           - Minimal commits                    │
│  - Detailed CLAUDE.md             - Concise CLAUDE.md                  │
│  - PR_SUMMARY*.md                 - PR_TEMPLATES/                      │
│  - Binary deployment              - NO .md in commits                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Git Rules (CRITICAL)

- **NEVER commit `.md` files** - Local documentation only
- **NEVER commit `PR_TEMPLATES/`** - Internal reference
- **NEVER include Claude attribution** in commits
- Only commit source code and test cases

## Submitted PRs

### Wave 1 (Critical Fixes)
| PR | Branch | Status |
|----|--------|--------|
| [#2100](https://github.com/mydumper/mydumper/pull/2100) | `fix/schema-wait-race-condition` | OPEN |
| [#2101](https://github.com/mydumper/mydumper/pull/2101) | `fix/load-data-mutex-deadlock` | OPEN |
| [#2102](https://github.com/mydumper/mydumper/pull/2102) | `fix/macos-build-warnings` | OPEN |

### Wave 2 (Bug Fixes)
| PR | Branch | Status | Test |
|----|--------|--------|------|
| [#2103](https://github.com/mydumper/mydumper/pull/2103) | `fix/schema-job-retry-null` | OPEN | - |
| [#2104](https://github.com/mydumper/mydumper/pull/2104) | `fix/no-data-deadlock` | DRAFT | - |
| [#2105](https://github.com/mydumper/mydumper/pull/2105) | `fix/schema-queue-race` | DRAFT | test_811 |
| [#2106](https://github.com/mydumper/mydumper/pull/2106) | `fix/no-schema-mode` | DRAFT | test_812 |

### Wave 3 (Performance - Branches Pushed)
| ID | Branch | Commit | Optimization | Test |
|----|--------|--------|-------------|------|
| **PO-01** | `perf/batch-metadata-prefetch` | 16bcede3 | Batch metadata prefetch (250K→3 queries) | test_813 |
| **PO-02** | `perf/o1-ignore-errors-lookup` | 1ac941b5 | O(1) ignore_errors lookup | - |
| **PO-04** | `perf/larger-io-buffers` | 0476802a | Larger I/O buffers (64KB) | - |
| **PO-05** | `perf/cached-list-length` | 9c719893 | Cached g_list_length() in MList | - |
| **PO-06** | `perf/atomic-row-counter` | 9c6950af | Lock-free atomic row counter | - |
| **PO-07** | `perf/thread-local-row-batching` | a6c2ce53 | Thread-local row batching (1000x fewer ops) | - |
| **PO-08** | `perf/parallel-fsync` | 019f2b70 | 16x parallel fsync threads | - |
| **PO-09** | `perf/skip-metadata-sorting` | 5f54d5bb | Skip metadata sorting (opt-in) | - |

## Wave 3 Performance PRs

### PO-01: Batch Metadata Prefetch
**Branch**: `perf/batch-metadata-prefetch`
**Template**: `PR_TEMPLATES/PO1_BATCH_METADATA_PREFETCH.md`

Adds `--bulk-metadata-prefetch` option (opt-in) that prefetches JSON/generated column metadata in 3 bulk queries instead of per-table queries.

### PO-02: O(1) Ignore Errors Lookup
**Branch**: `perf/o1-ignore-errors-lookup`
**Template**: `PR_TEMPLATES/PO2_O1_IGNORE_ERRORS.md`

Adds GHashTable for O(1) error code lookups instead of O(n) `g_list_find()`.

### PO-04: Larger I/O Buffers
**Branch**: `perf/larger-io-buffers`
**Template**: `PR_TEMPLATES/PO4_LARGER_IO_BUFFERS.md`

Increases `read_data()` buffer from 4KB to 64KB, data file buffers from 256B to 64KB.

### PO-05: Cached List Length
**Branch**: `perf/cached-list-length`
**Template**: `PR_TEMPLATES/PO5_CACHED_LIST_LENGTH.md`

Add `count` field to MList struct for O(1) list length access instead of O(n) `g_list_length()`.

### PO-06: Atomic Row Counter
**Branch**: `perf/atomic-row-counter`
**Template**: `PR_TEMPLATES/PO6_ATOMIC_ROW_COUNTER.md`

Replace mutex-based row counter with `__sync_fetch_and_add()` for lock-free row counting.

### PO-07: Thread-Local Row Batching
**Branch**: `perf/thread-local-row-batching`
**Template**: `PR_TEMPLATES/PO7_THREAD_LOCAL_ROW_BATCHING.md`

Add thread-local row counter that batches updates and flushes every 10K rows.

### PO-08: Parallel Fsync
**Branch**: `perf/parallel-fsync`
**Template**: `PR_TEMPLATES/PO8_PARALLEL_FSYNC.md`

Increase `close_file_threads` from 1 to 16 for parallel fsync on high-latency storage.

### PO-09: Skip Metadata Sorting
**Branch**: `perf/skip-metadata-sorting`
**Template**: `PR_TEMPLATES/PO9_SKIP_METADATA_SORTING.md`

Add `--skip-metadata-sorting` opt-in flag to skip O(n log n) sorting at dump finalization.

## Wave 5 Performance (Branches Pushed - Dec 1, 2025)

| ID | Branch | Commit | Optimization |
|----|--------|--------|-------------|
| **PO-10** | `perf/simd-string-escaping` | f2fdfe22 | SIMD string escaping (memchr-based) |
| **PO-11** | `perf/o1-ready-table-queue` | d6fb077c | O(1) ready table queue |
| **PO-12** | `perf/fifo-decompression-throttle` | 984f1c1d | FIFO decompression throttle |
| **PO-13** | `perf/auto-thread-scaling` | cf8789d3 | Auto thread scaling |

## Recent CI Fixes (Nov 30 - Dec 1, 2025)

| Branch | Fix | Commit |
|--------|-----|--------|
| `fix/no-schema-mode` | Added `overwrite-tables` to test_812 | 3774b330 |
| `fix/no-schema-mode` | Added TRUNCATE to pre_myloader.sh | a163f716 |
| `fix/no-schema-mode` | Use myloader --no-data for Phase 1 schema creation | 6d260e36 |
| `fix/no-schema-mode` | Add explicit TRUNCATE for --no-schema mode | 7bf929fd |
| `fix/no-schema-mode` | **Simplify: remove overwrite-tables, use drop-table** | 62813fc6 |
| `perf/batch-metadata-prefetch` | Added `overwrite-tables` to test_813 | fc553f64 |
| `perf/batch-metadata-prefetch` | Added pre_myloader.sh to drop/recreate db | 9521f09b |
| `perf/batch-metadata-prefetch` | Simplify test_813 to cross-platform features | 16bcede3 |
| `perf/batch-metadata-prefetch` | Use drop-table for cleaner test state | 7d9b6936 |
| `perf/cached-list-length` | Add mutex protection for count access in PMM | 9c719893 |
| `perf/parallel-fsync` | **Reduce threads from 16 to 4 for stability** | 408c91da |
| `perf/skip-metadata-sorting` | **Add test_814 for --skip-metadata-sorting** | 9a9c71c8 |

## Test Cases

| Test | Purpose | PR |
|------|---------|-----|
| `test_451` | Streaming + LOAD DATA + checksums | Existing |
| `test_811` | Schema queue race (100 tables, 8 threads) | PR #2105 |
| `test_812` | --no-schema two-phase loading | PR #2106 |
| `test_813` | --bulk-metadata-prefetch (cross-platform collation test) | PO-01 |
| `test_814` | --skip-metadata-sorting | PO-09 |

## Creating a PR Branch

```bash
# Sync with upstream
git fetch upstream
git checkout master
git reset --hard upstream/master

# Create branch
git checkout -b fix/descriptive-name

# Make changes, commit (NO .md files, NO Claude)
git add src/ test/
git commit -m "Fix: one-line description"

# Push and create PR
git push -u origin fix/descriptive-name
gh pr create --title "Fix: description" --body-file PR_TEMPLATES/PRX.md
```

## Build

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_POLICY_VERSION_MINIMUM=3.5
make -j8
```

## Related

- **Fork**: `/Users/adwait.athale/Development/util_repos/mydumper-fork-optimize-myloader`
- **Upstream**: https://github.com/mydumper/mydumper
- **Roadmap**: `UPSTREAM_ROADMAP.md` (this repo)
