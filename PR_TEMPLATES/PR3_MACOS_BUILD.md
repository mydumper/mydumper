# PR: Fix macOS/clang build errors

## Summary

Fix compilation errors when building with clang on macOS. These changes are **backward compatible** and improve code correctness on all platforms.

## Problem

Building mydumper on macOS with clang fails due to:

1. **Uninitialized `va_list` errors** (10 occurrences in `common.c`)
2. **Format specifier mismatch** (`%ld` for `guint64`)
3. **OpenSSL library not found** (bare `ssl crypto` names don't resolve)

```
src/common.c:1394:73: error: variable 'args' is used uninitialized
  whenever 'if' condition is false [-Werror,-Wsometimes-uninitialized]

src/mydumper/mydumper_start_dump.c:1340:59: error: format specifies type
  'long' but the argument has type 'guint64' [-Werror,-Wformat]

ld: library 'ssl' not found
```

## Changes

### 1. Fix `va_list` initialization (`src/common.c`)

**Before:**
```c
gboolean m_query(..., const char *fmt, ...){
  va_list args;
  if (fmt)
    va_start(args, fmt);
  return m_queryv(..., fmt, args);  // args uninitialized if fmt==NULL!
}
```

**After:**
```c
gboolean m_query(..., const char *fmt, ...){
  va_list args;
  va_start(args, fmt);
  gboolean result = m_queryv(..., fmt, args);
  va_end(args);
  return result;
}
```

**Functions fixed (10 total):**
- `m_query`
- `m_query_warning`
- `m_query_critical`
- `m_query_ext`
- `m_query_verbose`
- `m_store_result_critical`
- `m_store_result`
- `m_use_result`
- `m_store_result_row`
- `m_store_result_single_row`

### 2. Fix format specifier (`mydumper_start_dump.c`, `mydumper_write.c`)

**Before:**
```c
fprintf(mdfile, "max-statement-size = %ld\n", max_statement_size);
```

**After:**
```c
fprintf(mdfile, "max-statement-size = %" G_GUINT64_FORMAT "\n", max_statement_size);
```

`G_GUINT64_FORMAT` expands to the correct format specifier for the platform:
- Linux/glibc: `"lu"` or `"llu"`
- macOS: `"llu"`

### 3. Fix OpenSSL linking (`CMakeLists.txt`)

**Before:**
```cmake
target_link_libraries(mydumper ... ssl crypto)
```

**After:**
```cmake
find_package(OpenSSL REQUIRED)
# Use modern imported targets if available (CMake 3.4+), otherwise fall back to variables
if (TARGET OpenSSL::SSL)
  set(OPENSSL_LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto)
else ()
  set(OPENSSL_LINK_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
  include_directories(${OPENSSL_INCLUDE_DIR})
endif ()
target_link_libraries(mydumper ... ${OPENSSL_LINK_LIBRARIES})
```

Using CMake's `find_package(OpenSSL)` is the **modern, portable way** to link OpenSSL. It:
- Finds OpenSSL in standard locations (including Homebrew on macOS)
- Sets correct include paths automatically
- Works on Linux, macOS, and Windows
- **Falls back to legacy variables** for older CMake versions (EL7 compatibility)

## Compatibility Analysis

### va_list Changes - SAFE

| Aspect | Analysis |
|--------|----------|
| **C Standard** | `va_start()` on NULL `fmt` is **defined behavior** - it just means no varargs to process |
| **GCC behavior** | GCC allows uninitialized `va_list` but it's **undefined behavior** |
| **Clang behavior** | Clang with `-Werror` rejects uninitialized `va_list` |
| **Runtime impact** | None - `m_queryv()` checks `fmt` before using `args` |

The fix makes the code **correct on all compilers** while maintaining identical runtime behavior.

### Format Specifier - SAFE

| Platform | `guint64` type | `%ld` | `G_GUINT64_FORMAT` |
|----------|---------------|-------|-------------------|
| Linux x86_64 | `unsigned long` | Works | Works |
| Linux ARM64 | `unsigned long` | Works | Works |
| macOS x86_64 | `unsigned long long` | **WRONG** | Works |
| macOS ARM64 | `unsigned long long` | **WRONG** | Works |

`G_GUINT64_FORMAT` is the **GLib-standard portable way** to format `guint64`.

### OpenSSL CMake - SAFE

| Distro | CMake Version | Bare `ssl crypto` | Our Fix |
|--------|---------------|-------------------|---------|
| EL7 (CentOS/RHEL 7) | 2.8.12 | Works | Uses `${OPENSSL_SSL_LIBRARY}` fallback |
| AlmaLinux 8/9 | 3.x | Works | Uses `OpenSSL::SSL` targets |
| Ubuntu/Debian | 3.x | Works | Uses `OpenSSL::SSL` targets |
| macOS Homebrew | 3.x | **FAILS** | Uses `OpenSSL::SSL` targets |

The fix uses `if (TARGET OpenSSL::SSL)` to detect CMake 3.4+ and falls back to legacy variables for older CMake. This ensures **both EL7 and macOS work**.

## CI Impact

Your CI uses Docker images based on:
- AlmaLinux 7/8/9 (el7, el8, el9)
- Ubuntu Bionic/Focal/Jammy
- Debian Buster/Bullseye

**All these platforms:**
- Have `openssl-devel` / `libssl-dev` packages
- Support `find_package(OpenSSL)`
- Use GCC which accepts the va_list fix

**No CI changes required.**

## Testing

Verified build on:
- [x] macOS 14 (Sonoma) with clang 15, Homebrew MariaDB 11.7.2
- [x] Binaries run correctly: `mydumper --version`, `myloader --version`

## Files Changed

```
CMakeLists.txt                     | 15 ++++++++++-----
src/common.c                       | 43 ++++++++++++++++++++++-----------------
src/mydumper/mydumper_start_dump.c |  2 +-
src/mydumper/mydumper_write.c      |  2 +-
4 files changed, 36 insertions(+), 26 deletions(-)
```

## Risk Assessment

| Change | Risk | Reason |
|--------|------|--------|
| va_list fix | **None** | Makes undefined behavior defined |
| Format fix | **None** | Uses GLib's portable macro |
| OpenSSL fix | **None** | Uses CMake's standard module |

All changes are **strictly improvements** with no functional change to runtime behavior.
