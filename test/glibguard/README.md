# glibguard — checking GLib lock usage in tests

GLib implements `GMutex`, `GRecMutex`, `GCond` and `GAsyncQueue` on raw futexes
on Linux, so neither ThreadSanitizer nor Helgrind can see them. Two consequences:

* **Every access guarded by a GMutex is reported as a data race.** A plain TSan
  run of `myloader` produces several hundred reports, nearly all false, which
  makes an automated "no races on this path" assertion impossible.
* **Misuse of the locks themselves is never reported.** Unlocking a mutex from a
  thread that does not hold it, or acquiring two mutexes in opposite orders in
  different threads, goes unnoticed until it corrupts something or deadlocks.

This directory holds two `LD_PRELOAD` libraries that address one problem each.
Neither needs a change to mydumper, and neither is linked into the shipped
binaries.

## `glibguard.c` — lock-discipline checker

Interposes the GLib locking entry points and checks how they are used. Needs no
sanitizer, so it runs against an ordinary build at close to normal speed.

Findings: `unlock-not-held`, `unlock-wrong-thread`, `destroy-while-held`,
`double-lock`, `lock-order-inversion`, each reported once per call site as
`module+0xoffset`, which `resolve.sh` turns into `file:line`.

```sh
gcc -shared -fPIC -O2 -g glibguard.c $(pkg-config --cflags glib-2.0) \
    -o libglibguard.so -ldl -lpthread
GLIBGUARD_LOG=gg.log LD_PRELOAD=./libglibguard.so myloader -d dump -B target
./resolve.sh gg.log ./myloader
```

The last line of the log is `glibguard: N finding(s)`; a test asserts `N` is 0.
`GLIBGUARD_JITTER_US=<n>` sleeps up to *n* microseconds inside lock and unlock,
which widens the window a rare interleaving needs. `GLIBGUARD_ABORT=1` aborts on
the first finding, for a core dump. `LD_PRELOAD` also applies to wrappers such as
`timeout(1)`, so the log is opened on first write and processes that never touch
a GLib lock leave it alone.

`run_selftest.sh` drives `test_glibguard.c`, which commits one deliberate error
per run, and checks each is reported and that the clean run reports none. It is
registered with ctest as `glibguard_selftest`.

## `tsan_glib.c` — ThreadSanitizer annotations

Tells TSan about the happens-before edges GLib creates, with
`__tsan_release()` before a push or unlock and `__tsan_acquire()` after a pop or
lock. Requires a TSan build of the program:

```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_FLAGS_DEBUG="-fsanitize=thread -g -O1 -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
gcc -shared -fPIC -fsanitize=thread -g tsan_glib.c $(pkg-config --cflags glib-2.0) \
    -o libtsan_glib.so -ldl
echo 'called_from_lib:libglib-2.0.so.0' > tsan.supp
LD_PRELOAD=./libtsan_glib.so TSAN_OPTIONS="suppressions=./tsan.supp log_path=tsan" \
    build-tsan/myloader -d dump -B target
```

On a mixed compressed/uncompressed restore this took the report count from ~480
per run to ~36, all with mydumper frames on both sides. The suppression is
needed for GLib's own bookkeeping inside `GAsyncQueue`; it also hides a genuine
race whose top frame is inside GLib, such as an unlocked `GHashTable`.

Note that the two libraries are complementary: the checker finds misuse of the
locks, the annotations find unsynchronised data. Neither finds the other's bugs.
