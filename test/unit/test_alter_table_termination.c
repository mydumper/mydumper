#include <glib.h>

#include "myloader/myloader_common.h"

#define CHECK_ALTER "ALTER TABLE `t` ADD CONSTRAINT `chk_flaga` CHECK (flaga IN (0,1))"

static void check(const gchar *input, const gchar *expected)
{
  GString *statement = g_string_new(input);
  finish_alter_table(statement);
  g_assert_cmpstr(statement->str, ==, expected);
  g_string_free(statement, TRUE);
}

int main(void)
{
  check(CHECK_ALTER, CHECK_ALTER ";\n");
  check(CHECK_ALTER ",", CHECK_ALTER ";\n");
  check(CHECK_ALTER ", \t\r\n", CHECK_ALTER ";\n");
  check(CHECK_ALTER ",\n ADD KEY `idx_flaga` (`flaga`),",
      CHECK_ALTER ",\n ADD KEY `idx_flaga` (`flaga`);\n");
  return 0;
}
