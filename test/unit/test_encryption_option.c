/* Unit test for issue #2367: encryption options between MySQL and MariaDB. */
#include <glib.h>
#include "myloader_encryption_utils.h"

#define T(opts) "CREATE TABLE `t` (\n  `id` int NOT NULL\n) " opts

static void check(table_option_modifier_func modifier, const gchar *input, gboolean changed, const gchar *expected)
{
  GString *s = g_string_new(input);
  g_assert_cmpint(modifier(s), ==, changed);
  g_assert_cmpstr(s->str, ==, expected);
  g_string_free(s, TRUE);
}

static void check_untouched(table_option_modifier_func modifier, const gchar *input)
{
  check(modifier, input, FALSE, input);
}

int main(void)
{
  table_option_modifier_func to_mariadb = &convert_table_encryption_options_to_mariadb;
  table_option_modifier_func to_mysql = &convert_table_encryption_options_to_mysql;
  table_option_modifier_func remove = &remove_table_encryption_options;
  table_option_modifier_func remove_db = &remove_database_encryption_option;

  /* MySQL -> MariaDB */
  check(to_mariadb, T("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ENCRYPTION='Y';\n"), TRUE, T("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 `ENCRYPTED`=YES;\n"));
  check(to_mariadb, T("ENGINE=InnoDB ENCRYPTION='N' COMMENT='x'"), TRUE, T("ENGINE=InnoDB `ENCRYPTED`=NO COMMENT='x'"));
  check(to_mariadb, T("ENGINE=InnoDB ENCRYPTION='y'\n/*!50100 PARTITION BY HASH (`id`)\nPARTITIONS 2 */;\n"), TRUE,
      T("ENGINE=InnoDB `ENCRYPTED`=YES\n/*!50100 PARTITION BY HASH (`id`)\nPARTITIONS 2 */;\n"));
  /* no ENGINE: default engine, InnoDB */
  check(to_mariadb, T("ENCRYPTION='Y'"), TRUE, T("`ENCRYPTED`=YES"));
  /* view dependency table: MariaDB has no ENCRYPTED on MEMORY, it is removed */
  check(to_mariadb, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N';\n", TRUE, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY;\n");
  check(to_mariadb, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N'", TRUE, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY");
  /* versioned comment, as before #2367 */
  check(to_mariadb, T("ENGINE=InnoDB /*!80016 ENCRYPTION='Y' */;\n"), TRUE, T("ENGINE=InnoDB /*!80016 `ENCRYPTED`=YES */;\n"));
  /* preceding SET statements in the buffer */
  check(to_mariadb, "/*!40101 SET NAMES binary*/;\n" T("ENGINE=InnoDB ENCRYPTION='Y';\n\n"), TRUE, "/*!40101 SET NAMES binary*/;\n" T("ENGINE=InnoDB `ENCRYPTED`=YES;\n\n"));
  /* MariaDB syntax is kept */
  check_untouched(to_mariadb, T("ENGINE=InnoDB `ENCRYPTED`=YES `ENCRYPTION_KEY_ID`=2;\n"));

  /* MariaDB -> MySQL */
  check(to_mysql, T("ENGINE=InnoDB DEFAULT CHARSET=latin1 `ENCRYPTED`=YES `ENCRYPTION_KEY_ID`=2;\n"), TRUE, T("ENGINE=InnoDB DEFAULT CHARSET=latin1 ENCRYPTION='Y';\n"));
  check(to_mysql, T("ENGINE=InnoDB `ENCRYPTED`=NO DEFAULT CHARSET=latin1"), TRUE, T("ENGINE=InnoDB ENCRYPTION='N' DEFAULT CHARSET=latin1"));
  check(to_mysql, T("ENGINE=InnoDB encrypted = 'yes' COMMENT='x'"), TRUE, T("ENGINE=InnoDB ENCRYPTION='Y' COMMENT='x'"));
  check(to_mysql, T("ENGINE=InnoDB `ENCRYPTED`=DEFAULT"), TRUE, T("ENGINE=InnoDB"));
  check(to_mysql, T("ENGINE=InnoDB `ENCRYPTED`=YES\n PARTITION BY HASH (`id`)\nPARTITIONS 2;\n"), TRUE, T("ENGINE=InnoDB ENCRYPTION='Y'\n PARTITION BY HASH (`id`)\nPARTITIONS 2;\n"));
  /* MySQL syntax is kept */
  check_untouched(to_mysql, T("ENGINE=InnoDB ENCRYPTION='Y';\n"));
  check_untouched(to_mysql, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N';\n");

  /* REMOVE: both syntaxes */
  check(remove, T("ENGINE=InnoDB ENCRYPTION='Y' COMMENT='x';\n"), TRUE, T("ENGINE=InnoDB COMMENT='x';\n"));
  check(remove, T("ENGINE=InnoDB `ENCRYPTED`=YES `ENCRYPTION_KEY_ID`=2;\n"), TRUE, T("ENGINE=InnoDB;\n"));
  check(remove, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY ENCRYPTION='N';\n", TRUE, "CREATE TABLE IF NOT EXISTS `v`(\n`id` int\n) ENGINE=MEMORY;\n");

  /* untouched by everyone: comments, column names, similar options, other statements */
  table_option_modifier_func all[] = {to_mariadb, to_mysql, remove};
  for (guint i = 0; i < G_N_ELEMENTS(all); i++)
  {
    check_untouched(all[i], T("ENGINE=InnoDB COMMENT=' ENCRYPTION=''Y'' `ENCRYPTED`=YES';\n"));
    check_untouched(all[i], T("ENGINE=InnoDB COMMENT='it\\'s ENCRYPTION=\\'Y\\'';\n"));
    check_untouched(all[i], "CREATE TABLE `t` (\n  `ENCRYPTED` int,\n  `ENCRYPTION` int\n) ENGINE=InnoDB;\n");
    check_untouched(all[i], T("ENGINE=InnoDB `ENCRYPTED_X`=YES ENCRYPTION_X='Y';\n"));
    check_untouched(all[i], T("ENGINE=InnoDB;\n"));
    check_untouched(all[i], "INSERT INTO `t` VALUES ('\n) ENCRYPTION=''N'' `ENCRYPTED`=YES');\n");
    check_untouched(all[i], "");
  }

  /* CREATE DATABASE */
  check(remove_db, "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;\n", TRUE,
      "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */;\n");
  check(remove_db, "CREATE DATABASE `db` DEFAULT ENCRYPTION='Y'", TRUE, "CREATE DATABASE `db`");
  check(remove_db, "CREATE SCHEMA `db` ENCRYPTION = 'y';\n", TRUE, "CREATE SCHEMA `db`;\n");
  check_untouched(remove_db, "CREATE DATABASE `db` /*!40100 DEFAULT CHARACTER SET latin1 */;\n");
  check_untouched(remove_db, T("ENGINE=InnoDB ENCRYPTION='Y';\n"));

  /* --encryption-option values */
  enum encryption_option_mode mode = ENCRYPTION_OPTION_CONVERT;
  g_assert_true(parse_encryption_option_mode("remove", &mode));
  g_assert_cmpint(mode, ==, ENCRYPTION_OPTION_REMOVE);
  g_assert_true(parse_encryption_option_mode("KEEP", &mode));
  g_assert_cmpint(mode, ==, ENCRYPTION_OPTION_KEEP);
  g_assert_true(parse_encryption_option_mode("Convert", &mode));
  g_assert_cmpint(mode, ==, ENCRYPTION_OPTION_CONVERT);
  g_assert_false(parse_encryption_option_mode("maybe", &mode));
  g_assert_false(parse_encryption_option_mode(NULL, &mode));
  g_assert_cmpint(mode, ==, ENCRYPTION_OPTION_CONVERT);

  /* modifier selection */
  g_assert_true(get_table_encryption_modifier(ENCRYPTION_OPTION_CONVERT, TRUE) == to_mariadb);
  g_assert_true(get_table_encryption_modifier(ENCRYPTION_OPTION_CONVERT, FALSE) == to_mysql);
  g_assert_true(get_table_encryption_modifier(ENCRYPTION_OPTION_REMOVE, TRUE) == remove);
  g_assert_true(get_table_encryption_modifier(ENCRYPTION_OPTION_REMOVE, FALSE) == remove);
  g_assert_null(get_table_encryption_modifier(ENCRYPTION_OPTION_KEEP, TRUE));
  g_assert_true(get_database_encryption_modifier(ENCRYPTION_OPTION_CONVERT, TRUE) == remove_db);
  g_assert_null(get_database_encryption_modifier(ENCRYPTION_OPTION_CONVERT, FALSE));
  g_assert_true(get_database_encryption_modifier(ENCRYPTION_OPTION_REMOVE, FALSE) == remove_db);
  g_assert_null(get_database_encryption_modifier(ENCRYPTION_OPTION_KEEP, TRUE));
  return 0;
}
