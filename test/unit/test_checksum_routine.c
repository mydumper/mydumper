/* Exercise the shared checksum entrypoint without a database or dump/restore. */
#include <zlib.h>
#include "../../src/checksum.c"

static const char *ddl = "CREATE DEFINER=`root`@`localhost` PROCEDURE `p`()\nBEGIN SELECT '''A'''; END";
static const char *sql_mode = "";
static gboolean broken_metadata, missing_definition, empty_database;
static guint routine_row;
static char routine_type[] = "PROCEDURE", routine_name[] = "p", database[] = "fixture";
static char *routine[] = {routine_type, routine_name, NULL};

MYSQL_RES *m_store_result_critical(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)conn; (void)fmt;
  g_assert_nonnull(strstr(query, "ROUTINE_NAME"));
  g_assert_null(strstr(query, "ROUTINE_DEFINITION"));
  routine_row = 0;
  return (MYSQL_RES *)&routine_row;
}
MYSQL_ROW STDCALL mysql_fetch_row(MYSQL_RES *result)
{
  (void)result;
  return !empty_database && routine_row++ == 0 ? routine : NULL;
}
void STDCALL mysql_free_result(MYSQL_RES *result) { (void)result; }
struct M_ROW *m_store_result_single_row(MYSQL *conn, const gchar *query, const char *fmt, ...)
{
  (void)conn; (void)fmt;
  struct M_ROW *mr = g_new0(struct M_ROW, 1);
  mr->row = g_new0(char *, 4);
  if (strstr(query, "ROUTINE_DEFINITION"))
  {
    /* MySQL #116354: metadata loses doubled quotes; SHOW CREATE does not. */
    const char *body = broken_metadata ? "BEGIN SELECT ''A''; END" : "BEGIN SELECT '''A'''; END";
    gchar **parts = g_strsplit(body, " ", -1);
    gchar *legacy = g_strjoinv("", parts);
    mr->row[0] = g_strdup_printf("%lx", (unsigned long)crc32(0, (const Bytef *)legacy, strlen(legacy)));
    g_free(legacy);
    g_strfreev(parts);
  }
  else
  {
    g_assert_true(g_str_has_prefix(query, "SHOW CREATE "));
    mr->row[0] = g_strdup(routine[1]);
    mr->row[1] = g_strdup(sql_mode);
    mr->row[2] = missing_definition ? NULL : g_strdup(ddl);
  }
  return mr;
}
void m_store_result_row_free(struct M_ROW *mr)
{
  for (int i = 0; i < 3; i++) g_free(mr->row[i]);
  g_free(mr->row); g_free(mr);
}
int get_product(void) { return 0; }
int get_major(void) { return 0; }
int get_secondary(void) { return 0; }
void trace(const char *fmt, ...) { (void)fmt; }
void print_bool(const char *key, gboolean val) { (void)key; (void)val; }
void m_error(const char *fmt, ...) { (void)fmt; g_assert_not_reached(); }
gboolean machine_log_json_enabled(void) { return FALSE; }
void machine_log_event(const gchar *domain, GLogLevelFlags level, ...) { (void)domain; (void)level; }

static char *checksum(void) { return checksum_process_structure(NULL, database, NULL); }

static void test_metadata_quoting(void)
{
  broken_metadata = TRUE;
  char *mysql_sum = checksum();
  broken_metadata = FALSE;
  char *maria_sum = checksum();
  g_assert_nonnull(mysql_sum);
  g_assert_cmpstr(mysql_sum, ==, maria_sum);
  g_free(mysql_sum); g_free(maria_sum);
}

static void test_literals_and_definer(void)
{
  const char *original = ddl;
  char *first = checksum();
  ddl = "CREATE DEFINER=`other PROCEDURE user`@`remote` PROCEDURE `p`()\nBEGIN  SELECT '''A''';  END";
  char *second = checksum();
  g_assert_cmpstr(first, ==, second);
  g_free(second);
  ddl = "CREATE DEFINER=`root`@`localhost` PROCEDURE `p`()\nBEGIN SELECT '''B'''; END";
  second = checksum();
  g_assert_cmpstr(first, !=, second);
  g_free(first); g_free(second);
  ddl = original;
}

static void test_normalization(void)
{
  const char *cases[][3] = {
    {"CREATE DEFINER=`x`` PROCEDURE y`@`h` PROCEDURE `p`()\r \nBEGIN\r \nSELECT 'a b';\r \nEND", "", "BEGIN SELECT 'a b'; END"},
    {"CREATE PROCEDURE `p`()\nSELECT 'ab'", "", "SELECT 'ab'"},
    {"CREATE PROCEDURE `p`()\nSELECT 'a b'", "", "SELECT 'a b'"},
    {"CREATE PROCEDURE `p`()\nSELECT 'a'' b'", "", "SELECT 'a'' b'"},
    {"CREATE PROCEDURE `p`()\nSELECT 'a\\' b'", "", "SELECT 'a\\' b'"},
    {"CREATE PROCEDURE `p`()\nSELECT 'a\\',  1", "NO_BACKSLASH_ESCAPES", "SELECT 'a\\', 1"},
    {"CREATE PROCEDURE `p`()\nSELECT `a  b`", "", "SELECT `a  b`"},
    {"CREATE PROCEDURE `p`()\nBEGIN -- comment\nSELECT 1; END", "", "BEGIN -- comment\nSELECT 1; END"},
    {"CREATE PROCEDURE `p`()\nSELECT /* keep  comment */ 1", "", "SELECT /* keep comment */ 1"},
    {"CREATE PROCEDURE `p`()\nSELECT 'unterminated", "", NULL},
    {"CREATE PROCEDURE `p`()\nSELECT /* unterminated", "", NULL},
    {"CREATE PROCEDURE `p`(\n IN x DECIMAL(10,2),\n IN y ENUM('a)b','c'))\n    MODIFIES SQL DATA\n    SQL SECURITY INVOKER\n    COMMENT 'multi\nline ''comment'''\nlabel1: BEGIN SELECT x; END", "", "label1: BEGIN SELECT x; END"},
    {"CREATE FUNCTION `f`() RETURNS int(11)\n    DETERMINISTIC\n    NO SQL\nRETURN 1", "", "RETURN 1"},
    {"CREATE PROCEDURE `p`()\nSELECT \"a  b\"", "ANSI_QUOTES", "SELECT \"a  b\""},
    {"CREATE PROCEDURE `p`()\nSELECT /*! 'a  b' */ 1", "", "SELECT /*! 'a  b' */ 1"},
    {"CREATE PROCEDURE `p`()\nSELECT /*M! 'a  b' */ 1", "", "SELECT /*M! 'a  b' */ 1"},
    {"CREATE PROCEDURE `p`()\nSELECT /*+ MAX_EXECUTION_TIME(  1000 ) */ 1", "", "SELECT /*+ MAX_EXECUTION_TIME(  1000 ) */ 1"},
    {"CREATE TABLE `PROCEDURE` (n INT)", "", NULL},
  };
  for (guint i = 0; i < G_N_ELEMENTS(cases); i++)
  {
    char *value = normalize_routine_definition(cases[i][0], cases[i][1]);
    g_assert_cmpstr(value, ==, cases[i][2]);
    g_free(value);
  }
  char *value = normalize_routine_definition("CREATE FUNCTION `f`() RETURNS int\n    DETERMINISTIC\nRETURN 1", "");
  g_assert_cmpstr(value, ==, "RETURN 1");
  g_free(value);
  value = quote_routine_identifier("a`b");
  g_assert_cmpstr(value, ==, "`a``b`");
  g_free(value);
}

static void test_dump_comment_spacing(void)
{
  const char *source = "CREATE PROCEDURE `p`()\nBEGIN\n# comment\n/* multi\nline */\nSELECT 'keep  spaces';\nEND";
  gchar **lines = g_strsplit(source, "\n", -1);
  gchar *dumped = g_strjoinv(" \n", lines);
  gchar *before = normalize_routine_definition(source, "");
  gchar *after = normalize_routine_definition(dumped, "");
  g_assert_cmpstr(before, ==, after);
  g_free(before); g_free(after); g_free(dumped); g_strfreev(lines);
}

static void test_missing_and_empty(void)
{
  missing_definition = TRUE;
  g_assert_null(checksum());
  missing_definition = FALSE;
  empty_database = TRUE;
  char *value = checksum();
  g_assert_cmpstr(value, ==, "0");
  g_free(value);
  empty_database = FALSE;
}

int main(int argc, char **argv)
{
  g_test_init(&argc, &argv, NULL);
  g_test_add_func("/routine/metadata-quoting", test_metadata_quoting);
  g_test_add_func("/routine/literals-and-definer", test_literals_and_definer);
  g_test_add_func("/routine/normalization", test_normalization);
  g_test_add_func("/routine/missing-and-empty", test_missing_and_empty);
  g_test_add_func("/routine/dump-comment-spacing", test_dump_comment_spacing);
  return g_test_run();
}
