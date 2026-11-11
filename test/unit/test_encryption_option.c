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
  return 0;
}
