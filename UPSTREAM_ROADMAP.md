# MyDumper Upstream Contribution Roadmap

## Repository Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          WORKFLOW OVERVIEW                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────┐         ┌─────────────────────┐               │
│  │    FORK REPO        │         │   UPSTREAM REPO     │               │
│  │  (Development)      │         │  (PR Preparation)   │               │
│  │                     │         │                     │               │
│  │  mydumper-fork-     │ ──────► │  mydumper-upstream  │ ──────►  GitHub
│  │  optimize-myloader  │  cherry │                     │   PR    mydumper/
│  │                     │   pick  │                     │         mydumper
│  └─────────────────────┘         └─────────────────────┘               │
│         │                               │                               │
│         ▼                               ▼                               │
│  - All development                - Clean branches                     │
│  - Testing/validation             - Minimal commits                    │
│  - CLAUDE.md (detailed)           - CLAUDE.md (concise)               │
│  - PR_SUMMARY*.md                 - PR_TEMPLATES/                      │
│  - Binary deployment              - No .md in commits                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Current PR Status

### Submitted PRs

#### Wave 1 (Critical Fixes)
| PR | Branch | Status | Description |
|----|--------|--------|-------------|
| [#2100](https://github.com/mydumper/mydumper/pull/2100) | `fix/schema-wait-race-condition` | OPEN | Schema/data race condition fix |
| [#2101](https://github.com/mydumper/mydumper/pull/2101) | `fix/load-data-mutex-deadlock` | OPEN | LOAD DATA streaming sync |
| [#2102](https://github.com/mydumper/mydumper/pull/2102) | `fix/macos-build-warnings` | OPEN | macOS clang + va_list fixes |

#### Wave 2 (Bug Fixes)
| PR | Branch | Status | Description | Test |
|----|--------|--------|-------------|------|
| [#2103](https://github.com/mydumper/mydumper/pull/2103) | `fix/schema-job-retry-null` | OPEN | Schema job retry NULL fix (BF-02) | - |
| [#2104](https://github.com/mydumper/mydumper/pull/2104) | `fix/no-data-deadlock` | DRAFT | --no-data mode deadlock (BF-04) | - |
| [#2105](https://github.com/mydumper/mydumper/pull/2105) | `fix/schema-queue-race` | DRAFT | Schema queue race mutex (BF-01) | test_811 |
| [#2106](https://github.com/mydumper/mydumper/pull/2106) | `fix/no-schema-mode` | DRAFT | --no-schema mode fix (BF-03) | test_812 |

### Wave 3 (Performance - Branches Ready)

| ID | Branch | Commit | Optimization | Test | Status |
|----|--------|--------|-------------|------|--------|
| **PO-01** | `perf/batch-metadata-prefetch` | 7d9b6936 | Batch metadata prefetch (250K→3 queries) | test_813 | **PUSHED** |
| **PO-02** | `perf/o1-ignore-errors-lookup` | 1ac941b5 | O(1) ignore_errors lookup | - | **PUSHED** |
| **PO-04** | `perf/larger-io-buffers` | 0476802a | Larger I/O buffers (64KB) | - | **PUSHED** |
| **PO-05** | `perf/cached-list-length` | 9c719893 | Cached g_list_length() in MList | - | **PUSHED** |
| **PO-06** | `perf/atomic-row-counter` | 9c6950af | Lock-free atomic row counter | - | **PUSHED** |
| **PO-07** | `perf/thread-local-row-batching` | a6c2ce53 | Thread-local row batching (1000x fewer ops) | - | **PUSHED** |
| **PO-08** | `perf/parallel-fsync` | 408c91da | 4x parallel fsync threads | - | **PUSHED** |
| **PO-09** | `perf/skip-metadata-sorting` | 9a9c71c8 | Skip metadata sorting (opt-in) | test_814 | **PUSHED** |

---

## Wave 3 Performance PRs (Ready to Create)

### PO-01: Batch Metadata Prefetch

**Branch**: `perf/batch-metadata-prefetch`
**Template**: `PR_TEMPLATES/PO1_BATCH_METADATA_PREFETCH.md`
**Test**: `test_813`

Adds `--bulk-metadata-prefetch` CLI option (opt-in) that prefetches all collation→charset mappings, JSON columns, and generated columns in 3 bulk queries instead of per-table queries.

**Performance**: 250K tables startup: ~500K queries → 3 queries

### PO-02: O(1) Ignore Errors Lookup

**Branch**: `perf/o1-ignore-errors-lookup`
**Template**: `PR_TEMPLATES/PO2_O1_IGNORE_ERRORS.md`

Adds GHashTable alongside GList for O(1) error code lookups instead of O(n) `g_list_find()` scans.

**Files**: `common.h`, `common.c`, `common_options.c`, `myloader_restore.c`

### PO-04: Larger I/O Buffers

**Branch**: `perf/larger-io-buffers`
**Template**: `PR_TEMPLATES/PO4_LARGER_IO_BUFFERS.md`

Increases I/O buffer sizes: `read_data()` 4KB→64KB, data file buffers 256B→64KB.

**Files**: `common.c`, `myloader_restore.c`

### PO-05: Cached List Length

**Branch**: `perf/cached-list-length`
**Template**: `PR_TEMPLATES/PO5_CACHED_LIST_LENGTH.md`

Add `count` field to MList struct for O(1) list length access instead of O(n) `g_list_length()`.

**Files**: `mydumper_start_dump.h`, `mydumper_working_thread.c`, `mydumper_write.c`, `mydumper_pmm.c`

### PO-06: Atomic Row Counter

**Branch**: `perf/atomic-row-counter`
**Template**: `PR_TEMPLATES/PO6_ATOMIC_ROW_COUNTER.md`

Replace mutex-based row counter with `__sync_fetch_and_add()` for lock-free row counting.

**Files**: `mydumper_write.c`

### PO-07: Thread-Local Row Batching

**Branch**: `perf/thread-local-row-batching`
**Template**: `PR_TEMPLATES/PO7_THREAD_LOCAL_ROW_BATCHING.md`

Add thread-local row counter that batches updates and flushes every 10K rows. Reduces atomic ops ~1000x.

**Files**: `mydumper_working_thread.h`, `mydumper_working_thread.c`, `mydumper_write.h`, `mydumper_write.c`

### PO-08: Parallel Fsync

**Branch**: `perf/parallel-fsync`
**Template**: `PR_TEMPLATES/PO8_PARALLEL_FSYNC.md`

Increase `close_file_threads` from 1 to 4 for parallel fsync. Improves throughput on high-latency storage (NFS, cloud).

**Files**: `mydumper_file_handler.c`

### PO-09: Skip Metadata Sorting

**Branch**: `perf/skip-metadata-sorting`
**Template**: `PR_TEMPLATES/PO9_SKIP_METADATA_SORTING.md`

Add `--skip-metadata-sorting` opt-in flag to skip O(n log n) sorting of table/database keys in metadata file. Saves 30-60s on 250K tables.

**Files**: `mydumper.c`, `mydumper_global.h`, `mydumper_arguments.c`, `mydumper_start_dump.c`, `mydumper_database.c`, `mydumper_database.h`

---

## Wave 5 Performance Optimizations (Branches Ready)

| ID | Branch | Commit | Optimization | Status |
|----|--------|--------|-------------|--------|
| **PO-10** | `perf/simd-string-escaping` | f2fdfe22 | SIMD string escaping (memchr-based, zero allocs) | **PUSHED** |
| **PO-11** | `perf/o1-ready-table-queue` | d6fb077c | O(1) ready table queue for job dispatch | **PUSHED** |
| **PO-12** | `perf/fifo-decompression-throttle` | 984f1c1d | FIFO decompression process throttling | **PUSHED** |
| **PO-13** | `perf/auto-thread-scaling` | cf8789d3 | Auto-scale schema/index threads (8+ cores) | **PUSHED** |

### Wave 5 PR Templates

| ID | Template | Description |
|----|----------|-------------|
| **PO-10** | `PR_TEMPLATES/PO10_SIMD_STRING_ESCAPING.md` | SIMD-optimized escape functions |
| **PO-11** | `PR_TEMPLATES/PO11_O1_READY_TABLE_QUEUE.md` | O(1) job dispatch |
| **PO-12** | `PR_TEMPLATES/PO12_FIFO_DECOMPRESSION_THROTTLE.md` | Decompressor throttling |
| **PO-13** | `PR_TEMPLATES/PO13_AUTO_THREAD_SCALING.md` | Auto thread scaling |

### Fork-Only (Too Complex for Upstream)

| ID | Optimization | Reason |
|----|-------------|--------|
| **PO-03** | Pre-compiled regex | Requires significant refactoring |
| - | Remove metadata mutex | Requires careful testing |
| - | Unsorted database write | Minor benefit |

---

## CI Fixes Applied (Nov 30 - Dec 1, 2025)

| Branch | Fix | Commit |
|--------|-----|--------|
| `fix/no-schema-mode` | Added `overwrite-tables` to test_812 | 3774b330 |
| `fix/no-schema-mode` | Added TRUNCATE to pre_myloader.sh | a163f716 |
| `fix/no-schema-mode` | Use myloader --no-data for Phase 1 schema creation | 6d260e36 |
| `fix/no-schema-mode` | Add explicit TRUNCATE for --no-schema mode | 7bf929fd |
| `fix/no-schema-mode` | **Simplify: remove overwrite-tables, use drop-table** | 62813fc6 |
| `perf/batch-metadata-prefetch` | Added `overwrite-tables` to test_813 | fc553f64 |
| `perf/batch-metadata-prefetch` | Added pre_myloader.sh to drop/recreate database | 9521f09b |
| `perf/batch-metadata-prefetch` | Simplify test_813 to cross-platform features | 16bcede3 |
| `perf/batch-metadata-prefetch` | **Use drop-table for cleaner test state** | 7d9b6936 |
| `perf/cached-list-length` | Add mutex protection for count access in PMM | 9c719893 |
| `perf/parallel-fsync` | **Reduce threads from 16 to 4 for stability** | 408c91da |
| `perf/skip-metadata-sorting` | **Add test_814 for --skip-metadata-sorting** | 9a9c71c8 |

---

## Complete Backlog

### Tier 1: Critical Bug Fixes

| ID | Fix | Status | PR |
|----|-----|--------|-----|
| **BF-01** | Schema job queue race | **PR #2105** | DRAFT |
| **BF-02** | Silent retry queue push (NULL) | **PR #2103** | OPEN |
| **BF-03** | `--no-schema` mode broken | **PR #2106** | DRAFT |
| **BF-04** | `--no-data` mode deadlock | **PR #2104** | DRAFT |
| **BF-05** | Regex mutex TOCTOU | Fork only | - |
| **BF-06** | FIFO exhaustion deadlock | Fork only | - |
| **BF-07** | va_list uninitialized | **In PR #2102** | ✅ |
| **BF-08** | Memory leak in working_thread | Fork only | - |

### Tier 2: Race Condition Fixes

| ID | Fix | Status | PR |
|----|-----|--------|-----|
| **RC-01** | Lock-first schema state check | **In PR #2100** | ✅ |
| **RC-02** | Condition variable schema-wait | **In PR #2100** | ✅ |
| **RC-03** | READ COMMITTED isolation | Fork only | - |
| **RC-04** | Stale table list refresh | Fork only | - |
| **RC-05** | Dispatch loop wake | Fork only | - |

### Tier 3: Performance Optimizations

| ID | Optimization | Branch | Status |
|----|-------------|--------|--------|
| **PO-01** | Batch metadata prefetch | `perf/batch-metadata-prefetch` | **PUSHED** |
| **PO-02** | O(1) ignore_errors lookup | `perf/o1-ignore-errors-lookup` | **PUSHED** |
| **PO-04** | Larger I/O buffers (64KB) | `perf/larger-io-buffers` | **PUSHED** |
| **PO-05** | Cached g_list_length() | `perf/cached-list-length` | **PUSHED** |
| **PO-06** | Atomic row counter | `perf/atomic-row-counter` | **PUSHED** |
| **PO-07** | Thread-local row batching | `perf/thread-local-row-batching` | **PUSHED** |
| **PO-08** | 4x parallel fsync | `perf/parallel-fsync` | **PUSHED** |
| **PO-09** | Skip metadata sorting | `perf/skip-metadata-sorting` | **PUSHED** |
| **PO-10** | SIMD string escaping | - | Fork ready |
| **PO-11** | O(1) ready table queue | - | Fork ready |
| **PO-12** | FIFO decompression throttle | - | Fork ready |
| **PO-13** | Auto thread scaling | - | Fork ready |

---

## Git Workflow

```bash
cd /Users/adwait.athale/Development/util_repos/mydumper-upstream

# Sync and create branch
git fetch upstream
git checkout master && git reset --hard upstream/master
git checkout -b fix/descriptive-name

# Make changes, commit (NO .md, NO Claude)
git add src/ test/
git commit -m "Fix: description"

# Push and create PR
git push -u origin fix/descriptive-name
gh pr create --title "Fix: description" --body-file PR_TEMPLATES/PRX.md
```

### Rules
- NEVER commit .md files
- NEVER include Claude attribution
- One logical change per PR

---

## Next Actions

### Completed
1. [x] Create PR templates for #4, #5, #6, #7
2. [x] Create branches for Wave 2 (all 4 ready)
3. [x] Add test cases: test_811 (PR #5), test_812 (PR #6)
4. [x] Submit Wave 2 PRs (#2103, #2104, #2105, #2106)
5. [x] Fix PR #2106 CI failure (no-schemas → no-schema typo)
6. [x] Create PO-01 branch with maintainer feedback (opt-in flag)
7. [x] Add test_813 for PO-01
8. [x] Push `perf/batch-metadata-prefetch` (fc553f64)
9. [x] Create and push PO-02 branch (1ac941b5)
10. [x] Create and push PO-04 branch (0476802a)
11. [x] Fix test_812 CI (added overwrite-tables)
12. [x] Fix test_813 CI (added overwrite-tables)
13. [x] Create and push PO-05 branch (93670898)
14. [x] Create and push PO-06 branch (9c6950af)
15. [x] Create and push PO-07 branch (a6c2ce53)
16. [x] Fix test_812 CI AGAIN (added TRUNCATE to pre_myloader.sh) - a163f716
17. [x] Fix test_813 CI AGAIN (added pre_myloader.sh) - 9521f09b
18. [x] Create and push PO-08 branch (019f2b70)
19. [x] Create and push PO-09 branch (5f54d5bb)
20. [x] Fix test_812 CI: Use myloader --no-data for Phase 1 (6d260e36)
21. [x] Fix test_813 CI: Simplify to cross-platform features (16bcede3)
22. [x] Fix PO-05 CI: Add mutex protection in PMM (9c719893)
23. [x] Fix test_812 CI: Simplify - remove overwrite-tables, use drop-table (62813fc6)
24. [x] Fix test_813 CI: Use drop-table for cleaner test state (7d9b6936)
25. [x] Fix PO-08 CI: Reduce threads from 16 to 4 (408c91da)
26. [x] Add test_814 for PO-09 skip-metadata-sorting (9a9c71c8)

### Completed (Wave 5 - Dec 1, 2025)
27. [x] Create and push PO-10 branch (f2fdfe22) - SIMD string escaping
28. [x] Create and push PO-11 branch (d6fb077c) - O(1) ready table queue
29. [x] Create and push PO-12 branch (984f1c1d) - FIFO decompression throttle
30. [x] Create and push PO-13 branch (cf8789d3) - Auto thread scaling
31. [x] Create PR templates for PO-10, PO-11, PO-12, PO-13

### Pending
32. [ ] Create PRs for Wave 3: PO-01, PO-02, PO-04, PO-05, PO-06, PO-07, PO-08, PO-09
33. [ ] Create PRs for Wave 5: PO-10, PO-11, PO-12, PO-13
34. [ ] Monitor CI for all PRs, confirm fixes work
35. [ ] Move DRAFT PRs (#2104, #2105, #2106) to OPEN when ready for review
