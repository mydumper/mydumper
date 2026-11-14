#include <glib.h>
#include <string.h>

#include "common.h"

static void check_snapshot(guint count)
{
  GHashTable *variables = g_hash_table_new(g_str_hash, g_str_equal);
  GString    *apply = g_string_new(NULL), *restore = g_string_new(NULL);
  if (count > 0)
    g_hash_table_insert(variables, (gpointer) "NET_WRITE_TIMEOUT", (gpointer) "77");
  if (count > 1)
    g_hash_table_insert(variables, (gpointer) "MAX_CONNECTIONS", (gpointer) "211");

  refresh_set_global_from_hash(apply, restore, variables);
  if (count == 0)
  {
    g_assert_cmpuint(apply->len, ==, 0);
    g_assert_cmpuint(restore->len, ==, 0);
  }
  else
  {
    /* A session setting must never become the saved global value. */
    g_assert_nonnull(strstr(apply->str, "@@GLOBAL.NET_WRITE_TIMEOUT"));
    g_assert_null(strstr(apply->str, "@@NET_WRITE_TIMEOUT"));
    g_assert_nonnull(strstr(restore->str, "SET GLOBAL NET_WRITE_TIMEOUT = @NET_WRITE_TIMEOUT"));
    if (count > 1)
    {
      g_assert_nonnull(strstr(apply->str, "@@GLOBAL.MAX_CONNECTIONS"));
      g_assert_nonnull(strstr(restore->str, "SET GLOBAL MAX_CONNECTIONS = @MAX_CONNECTIONS"));
    }
  }
  g_string_free(apply, TRUE);
  g_string_free(restore, TRUE);
  g_hash_table_destroy(variables);
}

int main(void)
{
  for (guint count = 0; count <= 2; count++)
    check_snapshot(count);
  return 0;
}
