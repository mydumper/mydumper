/*
 * test_stream_carryover.c
 *
 * Drives the REAL process_stream() from src/myloader/myloader_stream.c over a
 * hand-built stream and asserts that every byte that went in comes back out.
 *
 * Issue #1687: when a buffer boundary falls inside what could be a file
 * header, process_stream() moves the tail of the buffer to the front and reads
 * the rest on the next fread(). That move used to be g_strlcpy(), a string
 * function that stops at the first NUL. The carry-over length is computed
 * arithmetically (`diff`) while g_strlcpy()'s copy length depends on the data,
 * so on any byte range containing a NUL the two silently disagree: the bytes
 * after the NUL are left as whatever the previous buffer had there, and the
 * next read still starts at `diff`. The byte count is preserved, so total_size
 * never drifts and nothing is logged -- the file lands with the right size and
 * the wrong content.
 *
 * There are four carry-over sites. Two are on the mydumper-stream path and are
 * covered end-to-end by test/specific_41; the other two are on the mysqldump
 * path (myloader --mysqldump), which cannot be reached end to end because
 * mysqldump escapes 0x00 as the two characters \0 and never emits a literal
 * NUL. This test reaches all four, because it feeds process_stream() directly.
 *
 * No database and no server: process_stream() only needs stdin, a directory to
 * write into, and the handful of symbols stubbed below.
 *
 * Build/run:  ctest -R stream_carryover --output-on-failure
 *
 * REQUIRES: glib-2.0 and the MySQL/MariaDB client headers (common.h
 * includes <mysql.h>); no client library and no server.
 */

#include <glib.h>
#include <glib/gstdio.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* STREAM_BUFFER_SIZE, plus the prototypes of everything stubbed below -- taken
 * from the header rather than copied, so the cases follow the constant if it is
 * ever changed. */
#include "common.h"

/* The first fread() runs with diff == 0 and asks for stream_buffer_size - 1
 * bytes, so the first buffer holds stream offsets [0, STREAM_BUFFER_SIZE - 2]
 * and the last byte of the buffer is never written by fread(). */
#define FIRST_BUFFER_LEN (STREAM_BUFFER_SIZE - 1)

/* ── the globals and helpers process_stream() links against ───────────────── */

gchar   *directory = NULL;
gchar   *target_db = NULL;
gboolean no_stream = FALSE;
gboolean mysqldump = FALSE;

void set_thread_name(const char *format, ...) { (void)format; }
void trace(const char *format, ...) { (void)format; }

void m_critical(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  fprintf(stderr, "  m_critical: ");
  vfprintf(stderr, fmt, ap);
  fprintf(stderr, "\n");
  va_end(ap);
  exit(2);
}

GThread *m_thread_new(const gchar *title, GThreadFunc func, gpointer data, const gchar *error_text)
{
  (void)title;
  (void)func;
  (void)data;
  (void)error_text;
  return NULL;
}

gboolean m_filename_has_suffix(gchar const *str, gchar const *suffix)
{
  return g_str_has_suffix(str, suffix);
}

/* process_stream() hands finished filenames to the loader; here we drop them. */
void process_filename_push(const gchar *filename) { (void)filename; }
void process_filename_queue_end() {}

/* mirrors src/common.c: write(2), not fwrite -- the length can be negative */
int write_file(FILE *file, char *buff, int len)
{
  return (int)write(fileno(file), buff, len);
}

/* declared here rather than pulling in myloader.h, which needs <mysql.h> */
void *process_stream(void *stream_conf);

/* ── harness ──────────────────────────────────────────────────────────────── */

static int failed = 0;

static GString *stream_header(const char *name, gsize size)
{
  GString *s = g_string_new("\n-- ");
  g_string_append(s, name);
  g_string_append_c(s, ' ');
  g_string_append_printf(s, "%" G_GSIZE_FORMAT, size);
  g_string_append_c(s, '\n');
  return s;
}

/*
 * Deterministic filler that never contains 0x00 or 0x0a, so the only NUL and
 * the only newline in a payload are the ones the test places on purpose.
 */
static GString *filler(gsize n, guint32 seed)
{
  GString *s = g_string_sized_new(n);
  guint32  x = seed;
  for (gsize i = 0; i < n; i++)
  {
    x = x * 1103515245u + 12345u;
    guchar b = (guchar)((x >> 16) & 0xff);
    if (b == 0x00 || b == 0x0a)
      b = 0x41;
    g_string_append_c(s, (gchar)b);
  }
  return s;
}

/* Runs process_stream() over `wire` and returns the bytes it wrote to `name`.
 * The buffer is not called `stream`: common_options.h has a global of that
 * name and the project builds with -Wshadow -Werror. */
static GString *run_stream(const char *label, GString *wire, const char *name)
{
  gchar *dir = g_dir_make_tmp("md1687_XXXXXX", NULL);
  g_assert_nonnull(dir);
  gchar *in = g_build_filename(dir, "stdin.bin", NULL);
  g_assert_true(g_file_set_contents(in, wire->str, wire->len, NULL));

  directory = dir;
  g_assert_nonnull(freopen(in, "rb", stdin));
  process_stream(NULL);

  gchar   *out = g_build_filename(dir, name, NULL);
  gchar   *data = NULL;
  gsize    len = 0;
  GString *got = NULL;
  if (g_file_get_contents(out, &data, &len, NULL))
    got = g_string_new_len(data, len);
  else
    fprintf(stderr, "  [%s] %s was never written\n", label, name);

  g_free(data);
  g_unlink(out);
  g_unlink(in);
  g_rmdir(dir);
  g_free(out);
  g_free(in);
  g_free(dir);
  directory = NULL;
  return got;
}

static void expect_identical(const char *label, GString *sent, GString *got)
{
  if (got == NULL)
  {
    fprintf(stderr, "  FAIL [%s]: nothing received\n", label);
    failed++;
    return;
  }
  if (got->len != sent->len)
  {
    fprintf(stderr, "  FAIL [%s]: received %" G_GSIZE_FORMAT " bytes, sent %" G_GSIZE_FORMAT "\n",
        label, got->len, sent->len);
    failed++;
    return;
  }
  gsize bad = 0, first = 0;
  for (gsize i = 0; i < sent->len; i++)
    if (got->str[i] != sent->str[i])
    {
      if (!bad)
        first = i;
      bad++;
    }
  if (bad)
  {
    fprintf(stderr,
        "  FAIL [%s]: same size (%" G_GSIZE_FORMAT ") but %" G_GSIZE_FORMAT
        " byte(s) differ, first at offset %" G_GSIZE_FORMAT " (sent 0x%02x, got 0x%02x)\n",
        label, sent->len, bad, first,
        (guchar)sent->str[first], (guchar)got->str[first]);
    failed++;
  }
  else
  {
    fprintf(stdout, "  pass [%s]: %" G_GSIZE_FORMAT " bytes identical\n", label, sent->len);
  }
}

/* Compares the last `n` bytes of what was sent with the tail of what arrived. */
static void expect_suffix(const char *label, GString *sent, GString *got, gsize n)
{
  if (got == NULL)
  {
    fprintf(stderr, "  FAIL [%s]: nothing received\n", label);
    failed++;
    return;
  }
  if (got->len < n || sent->len < n)
  {
    fprintf(stderr, "  FAIL [%s]: too short (received %" G_GSIZE_FORMAT ", sent %" G_GSIZE_FORMAT ", want %" G_GSIZE_FORMAT ")\n",
        label, got->len, sent->len, n);
    failed++;
    return;
  }
  const gchar *a = sent->str + sent->len - n;
  const gchar *b = got->str + got->len - n;
  gsize        bad = 0, first = 0;
  for (gsize i = 0; i < n; i++)
    if (a[i] != b[i])
    {
      if (!bad)
        first = i;
      bad++;
    }
  if (bad)
  {
    fprintf(stderr,
        "  FAIL [%s]: %" G_GSIZE_FORMAT " of the last %" G_GSIZE_FORMAT
        " bytes differ, first at tail offset %" G_GSIZE_FORMAT " (sent 0x%02x, got 0x%02x)\n",
        label, bad, n, first, (guchar)a[first], (guchar)b[first]);
    failed++;
  }
  else
  {
    fprintf(stdout, "  pass [%s]: last %" G_GSIZE_FORMAT " bytes identical\n", label, n);
  }
}

/* Compares the first `n` bytes of what was sent with the head of what arrived. */
static void expect_prefix(const char *label, GString *sent, GString *got, gsize n)
{
  if (got == NULL)
  {
    fprintf(stderr, "  FAIL [%s]: nothing received\n", label);
    failed++;
    return;
  }
  if (got->len < n || sent->len < n)
  {
    fprintf(stderr, "  FAIL [%s]: too short (received %" G_GSIZE_FORMAT ", want %" G_GSIZE_FORMAT ")\n",
        label, got->len, n);
    failed++;
    return;
  }
  gsize bad = 0, first = 0;
  for (gsize i = 0; i < n; i++)
    if (sent->str[i] != got->str[i])
    {
      if (!bad)
        first = i;
      bad++;
    }
  if (bad)
  {
    fprintf(stderr,
        "  FAIL [%s]: %" G_GSIZE_FORMAT " of the first %" G_GSIZE_FORMAT
        " bytes differ, first at offset %" G_GSIZE_FORMAT " (sent 0x%02x, got 0x%02x)\n",
        label, bad, n, first, (guchar)sent->str[first], (guchar)got->str[first]);
    failed++;
  }
  else
  {
    fprintf(stdout, "  pass [%s]: first %" G_GSIZE_FORMAT " bytes identical\n", label, n);
  }
}

/* ── the two mydumper-stream carry-over sites ─────────────────────────────── */

/*
 * Short-tail site: the last three bytes of the first buffer are LF, NUL, X.
 * g_strndup()/strlen() shrink the tail to "\n", which is a substring of
 * "\n-- ", so the carry-over fires; g_strlcpy() then moves 1 byte of the 3.
 */
static void test_mydumper_short_tail(void)
{
  const char *name = "md1687.probe.bin";
  const gsize size = STREAM_BUFFER_SIZE + 200000; /* spans the boundary */
  GString    *hdr = stream_header(name, size);
  GString    *body = filler(size, 1687);

  gsize i0 = FIRST_BUFFER_LEN - 3 - hdr->len; /* payload index of the LF */
  body->str[i0] = '\n';
  body->str[i0 + 1] = '\0';
  body->str[i0 + 2] = 'X';

  GString *wire = g_string_new_len(hdr->str, hdr->len);
  g_string_append_len(wire, body->str, body->len);
  g_assert_cmpint(memcmp(wire->str + FIRST_BUFFER_LEN - 3, "\n\0X", 3), ==, 0);

  expect_identical("mydumper: short tail across the buffer boundary",
      body, run_stream("short tail", wire, name));

  g_string_free(wire, TRUE);
  g_string_free(body, TRUE);
  g_string_free(hdr, TRUE);
}

/*
 * Long-tail site: a chance "\n-- " inside the payload lands near the end of
 * the buffer, so the whole tail from it is carried over. The tail is payload
 * bytes, and g_strlcpy() stops at the first NUL in it.
 */
static void test_mydumper_chance_header(void)
{
  const char *name = "md1687.probea.bin";
  const gsize size = STREAM_BUFFER_SIZE + 200000; /* spans the boundary */
  GString    *hdr = stream_header(name, size);
  GString    *body = filler(size, 16871);

  gsize tag = FIRST_BUFFER_LEN - 99 - hdr->len; /* payload index of "\n-- " */
  memcpy(body->str + tag, "\n-- ", 4);
  body->str[tag + 10] = '\0';                          /* first NUL inside the tail */
  body->str[FIRST_BUFFER_LEN + 101 - hdr->len] = '\n'; /* ends the false line */

  GString *wire = g_string_new_len(hdr->str, hdr->len);
  g_string_append_len(wire, body->str, body->len);

  expect_identical("mydumper: chance \"\\n-- \" near the buffer boundary",
      body, run_stream("chance header", wire, name));

  g_string_free(wire, TRUE);
  g_string_free(body, TRUE);
  g_string_free(hdr, TRUE);
}

/* ── the mysqldump-path short-tail site ───────────────────────────────────── */

/*
 * myloader --mysqldump. The buffer ends with a partial line shorter than 20
 * bytes while a file is open, so process_stream() carries that tail back to
 * the front to get enough context for its CREATE TABLE / INSERT INTO prefix
 * match. If the tail holds a NUL, the old g_strlcpy() moved only the bytes
 * before it.
 *
 * Real mysqldump output cannot reach this: it escapes 0x00 as the two
 * characters \0 and never emits a literal NUL. The site is reachable here
 * because process_stream() parses whatever arrives on stdin.
 *
 * The assertion is on the tail of the payload rather than the whole file:
 * in mysqldump mode the file also receives the accumulated SET statements,
 * and opening it flushes a zero-length range, so only the payload part is
 * predictable.
 */
static void test_mysqldump_short_tail(void)
{
  GString *prologue = g_string_new(
      "/*!40101 SET NAMES utf8mb4 */;\n" /* -> set_buffer */
      "--\n"                             /* the "--" line   */
      "\n");                             /* blank: opens a file */
  const gsize size = STREAM_BUFFER_SIZE + 200000; /* spans the boundary */
  GString    *body = filler(size, 168701);

  /* every 40th byte a newline, so the payload is lines rather than one blob */
  for (gsize i = 39; i < size; i += 40)
    body->str[i] = '\n';

  /* a 9-byte tail: the LF sits 9 bytes before the end of buffer 1 */
  gsize eol = FIRST_BUFFER_LEN - 9 - prologue->len; /* payload index of that LF */
  body->str[eol] = '\n';
  body->str[eol + 3] = '\0'; /* NUL inside the 8-byte tail */
  for (gsize i = eol + 1; i < eol + 8; i++)
    if (i != eol + 3 && body->str[i] == '\n')
      body->str[i] = 'Z';   /* the tail must be one partial line */
  body->str[eol + 8] = 'Q'; /* last byte of the buffer, and of the tail */

  GString *wire = g_string_new_len(prologue->str, prologue->len);
  g_string_append_len(wire, body->str, body->len);
  g_assert_cmpint(wire->str[FIRST_BUFFER_LEN - 9], ==, '\n');
  g_assert_cmpint(wire->str[FIRST_BUFFER_LEN - 6], ==, '\0');

  mysqldump = TRUE;
  GString *got = run_stream("mysqldump short tail", wire, "mydumper_tmp.table_0.sql");
  mysqldump = FALSE;

  /* the whole payload: in mysqldump mode the file also starts with the SET
   * statements, so only the payload tail of it is predictable. */
  expect_suffix("mysqldump: short tail across the buffer boundary", body, got, size);

  g_string_free(wire, TRUE);
  g_string_free(body, TRUE);
  g_string_free(prologue, TRUE);
}

/*
 * myloader --mysqldump, header phase. Before the first file is opened every
 * non-comment line is accumulated into set_buffer, and a line that straddles
 * the buffer boundary is carried over the same way. A NUL in that partial line
 * truncated the old g_strlcpy(), so bytes vanished from the SET block that is
 * later written at the head of the first file.
 *
 * Real mysqldump output cannot reach this either: it puts a blank line after
 * the first "--" block about 1.5 KB in, so the header phase is over long
 * before the 1 MB boundary. Here the header is made larger than one buffer.
 */
static void test_mysqldump_header_line(void)
{
  const gsize hdr_size = STREAM_BUFFER_SIZE + 100000; /* larger than one buffer */
  GString    *head = filler(hdr_size, 168702);

  for (gsize i = 39; i < hdr_size; i += 40) /* lines, never two LF in a row */
    head->str[i] = '\n';
  for (gsize i = 0; i < hdr_size; i++) /* no line may start with "--" */
    if (head->str[i] == '-')
      head->str[i] = 'D';

  gsize eol = FIRST_BUFFER_LEN - 9; /* last complete line ends here */
  head->str[eol] = '\n';
  head->str[eol + 3] = '\0'; /* NUL inside the 8-byte tail */
  for (gsize i = eol + 1; i < FIRST_BUFFER_LEN; i++)
    if (i != eol + 3 && head->str[i] == '\n')
      head->str[i] = 'Z';
  head->str[hdr_size - 1] = '\n'; /* the block ends on a line */

  GString *wire = g_string_new_len(head->str, head->len);
  g_string_append(wire, "--\n"); /* the "--" line   */
  g_string_append(wire, "\n");   /* blank: opens a file */
  GString *body = filler(4000, 168703);
  g_string_append_len(wire, body->str, body->len);

  mysqldump = TRUE;
  GString *got = run_stream("mysqldump header", wire, "mydumper_tmp.table_0.sql");
  mysqldump = FALSE;

  /* set_buffer is written first, so the file must start with the whole block */
  expect_prefix("mysqldump: header line across the buffer boundary", head, got, hdr_size);

  g_string_free(wire, TRUE);
  g_string_free(body, TRUE);
  g_string_free(head, TRUE);
}

int main(void)
{
  fprintf(stdout, "=== test_stream_carryover ===\n");
  test_mydumper_short_tail();
  test_mydumper_chance_header();
  test_mysqldump_short_tail();
  test_mysqldump_header_line();

  fprintf(stdout, "\n");
  if (failed)
  {
    fprintf(stderr, "RESULT: %d test(s) FAILED\n", failed);
    return 1;
  }
  fprintf(stdout, "RESULT: ALL TESTS PASSED\n");
  return 0;
}
