/* Unit test for issue #2367: ENCRYPTION='N' must be removable for MariaDB. */
#include <glib.h>
#include "myloader_encryption_utils.h"

static void check(const gchar *input, gboolean changed, const gchar *expected)
{
  GString *s = g_string_new(input);
  g_assert_cmpint(remove_encryption_option_from_create_table(s), ==, changed);
  g_assert_cmpstr(s->str, ==, expected);
  g_string_free(s, TRUE);
}

int main(void)
{
  const gchar *dep = "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N';\n";
  check(dep, TRUE, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY;\n");
  check("CREATE TABLE `v`(`id` int) ENGINE=MEMORY ENCRYPTION='N';", TRUE, "CREATE TABLE `v`(`id` int) ENGINE=MEMORY;");
  /* as passed by restore_data_in_gstring_extended: split on ";\n", no terminator */
  check("CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N'", TRUE,
        "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY");
  check("CREATE TABLE `t`(`id` int) ENGINE=InnoDB ENCRYPTION='Y'", FALSE, "CREATE TABLE `t`(`id` int) ENGINE=InnoDB ENCRYPTION='Y'");
  /* untouched: no option, other statements, versioned comment, other value */
  check("CREATE TABLE `t`(`id` int) ENGINE=InnoDB;\n", FALSE, "CREATE TABLE `t`(`id` int) ENGINE=InnoDB;\n");
  check("CREATE TABLE `t`(`id` int) ENGINE=InnoDB /*!80016 ENCRYPTION='N' */;\n", FALSE, "CREATE TABLE `t`(`id` int) ENGINE=InnoDB /*!80016 ENCRYPTION='N' */;\n");
  check("CREATE TABLE `t`(`id` int) ENGINE=InnoDB ENCRYPTION='Y';\n", FALSE, "CREATE TABLE `t`(`id` int) ENGINE=InnoDB ENCRYPTION='Y';\n");
  check("INSERT INTO `t` VALUES ('x ENCRYPTION=''N'';');\n", FALSE, "INSERT INTO `t` VALUES ('x ENCRYPTION=''N'';');\n");
  check("", FALSE, "");
  /* as built by global_process_create_table_statement: preceding SET statements in the buffer */
  check("/*!40101 SET NAMES binary*/;\nCREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N';\n\n", TRUE,
        "/*!40101 SET NAMES binary*/;\nCREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY;\n\n");

  /* --remove-encryption-option values */
  enum remove_encryption_option_mode mode = REMOVE_ENCRYPTION_OPTION_AUTO;
  g_assert_true(parse_remove_encryption_option_mode(NULL, &mode));
  g_assert_cmpint(mode, ==, REMOVE_ENCRYPTION_OPTION_ALWAYS);
  g_assert_true(parse_remove_encryption_option_mode("never", &mode));
  g_assert_cmpint(mode, ==, REMOVE_ENCRYPTION_OPTION_NEVER);
  g_assert_true(parse_remove_encryption_option_mode("OFF", &mode));
  g_assert_cmpint(mode, ==, REMOVE_ENCRYPTION_OPTION_NEVER);
  g_assert_true(parse_remove_encryption_option_mode("Always", &mode));
  g_assert_cmpint(mode, ==, REMOVE_ENCRYPTION_OPTION_ALWAYS);
  g_assert_true(parse_remove_encryption_option_mode("auto", &mode));
  g_assert_cmpint(mode, ==, REMOVE_ENCRYPTION_OPTION_AUTO);
  g_assert_false(parse_remove_encryption_option_mode("maybe", &mode));
  g_assert_cmpint(mode, ==, REMOVE_ENCRYPTION_OPTION_AUTO);

  g_assert_true(should_remove_encryption_option(REMOVE_ENCRYPTION_OPTION_AUTO, TRUE));
  g_assert_false(should_remove_encryption_option(REMOVE_ENCRYPTION_OPTION_AUTO, FALSE));
  g_assert_true(should_remove_encryption_option(REMOVE_ENCRYPTION_OPTION_ALWAYS, FALSE));
  g_assert_false(should_remove_encryption_option(REMOVE_ENCRYPTION_OPTION_NEVER, TRUE));
  return 0;
}
