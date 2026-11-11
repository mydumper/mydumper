#include <glib.h>

#include "myloader/myloader_view.h"

static guint cases;

static void check(const gchar *sql, const gchar *expected, gchar quote, gboolean no_backslash_escapes)
{
  GHashTable *mapping = g_hash_table_new(g_str_hash, g_str_equal);
  g_hash_table_insert(mapping, (gpointer) "mapped", (gpointer) "other_target");
  GString *value = g_string_new(sql);
  rewrite_view_database_references(value, "old", "new", mapping, quote, no_backslash_escapes);
  g_assert_cmpstr(value->str, ==, expected);
  rewrite_view_database_references(value, "old", "new", mapping, quote, no_backslash_escapes);
  g_assert_cmpstr(value->str, ==, expected);
  g_string_free(value, TRUE);
  g_hash_table_destroy(mapping);
  cases++;
}

int main(void)
{
  check("CREATE VIEW old.v AS SELECT * FROM old.t",
      "CREATE VIEW `new`.v AS SELECT * FROM `new`.t", '`', FALSE);
  check("CREATE VIEW v AS SELECT old.id FROM old.t old",
      "CREATE VIEW v AS SELECT old.id FROM `new`.t old", '`', FALSE);
  check("CREATE VIEW v AS SELECT old.id FROM external.old",
      "CREATE VIEW v AS SELECT old.id FROM external.old", '`', FALSE);
  check("CREATE VIEW v AS SELECT old.t.id, old.f(1), other.f(2) FROM old.t",
      "CREATE VIEW v AS SELECT `new`.t.id, `new`.f(1), other.f(2) FROM `new`.t", '`', FALSE);
  check("CREATE VIEW v AS SELECT 'old.t', \"old.t\", '`old`.`t`' FROM `old`.`t`",
      "CREATE VIEW v AS SELECT 'old.t', \"old.t\", '`old`.`t`' FROM `new`.`t`", '`', FALSE);
  check("CREATE VIEW v AS SELECT * FROM (old.t a JOIN (old.u b, mapped.c c) ON a.id=b.id), external.t d",
      "CREATE VIEW v AS SELECT * FROM (`new`.t a JOIN (`new`.u b, `other_target`.c c) ON a.id=b.id), external.t d", '`', FALSE);
  check("CREATE VIEW v AS SELECT (SELECT COUNT(*) FROM old.u) n FROM old.t WHERE EXISTS (SELECT 1 FROM old.u)",
      "CREATE VIEW v AS SELECT (SELECT COUNT(*) FROM `new`.u) n FROM `new`.t WHERE EXISTS (SELECT 1 FROM `new`.u)", '`', FALSE);
  check("CREATE VIEW v AS SELECT EXTRACT(YEAR FROM old.date) n FROM old.t old",
      "CREATE VIEW v AS SELECT EXTRACT(YEAR FROM old.date) n FROM `new`.t old", '`', FALSE);
  check("CREATE VIEW v AS SELECT * FROM old.t WHERE old.a=1 GROUP BY old.b, old.c ORDER BY old.d, old.e",
      "CREATE VIEW v AS SELECT * FROM `new`.t WHERE old.a=1 GROUP BY old.b, old.c ORDER BY old.d, old.e", '`', FALSE);
  check("CREATE VIEW v AS SELECT 1 FROM old.t UNION SELECT 2 FROM old.u",
      "CREATE VIEW v AS SELECT 1 FROM `new`.t UNION SELECT 2 FROM `new`.u", '`', FALSE);
  check("CREATE VIEW v AS WITH x AS (SELECT * FROM old.t) SELECT * FROM x JOIN old.u ON x.id=u.id",
      "CREATE VIEW v AS WITH x AS (SELECT * FROM `new`.t) SELECT * FROM x JOIN `new`.u ON x.id=u.id", '`', FALSE);
  check("CREATE VIEW v AS SELECT 'it\\'s old.t' FROM old.t /* old.t */ -- old.t\n# old.t\n",
      "CREATE VIEW v AS SELECT 'it\\'s old.t' FROM `new`.t /* old.t */ -- old.t\n# old.t\n", '`', FALSE);
  check("CREATE VIEW v AS SELECT 'a\\' AS a, 'old.t' AS b FROM old.t",
      "CREATE VIEW v AS SELECT 'a\\' AS a, 'old.t' AS b FROM `new`.t", '`', TRUE);
  check("CREATE VIEW v AS SELECT 'it''s old.t' FROM old /* gap */ . t",
      "CREATE VIEW v AS SELECT 'it''s old.t' FROM `new` /* gap */ . t", '`', FALSE);
  check("CREATE VIEW \"old\".\"v\" AS SELECT \"old\".\"id\" FROM \"old\".\"t\" \"old\"",
      "CREATE VIEW \"new\".\"v\" AS SELECT \"old\".\"id\" FROM \"new\".\"t\" \"old\"", '"', FALSE);
  check("/*!50001 CREATE VIEW `v` AS SELECT * FROM `old`.`t` */;",
      "/*!50001 CREATE VIEW `v` AS SELECT * FROM `new`.`t` */;", '`', FALSE);
  check("CREATE DEFINER=`old`@`old.host` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `old`.`t`",
      "CREATE DEFINER=`old`@`old.host` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `new`.`t`", '`', FALSE);
  check("CREATE VIEW v AS SELECT * FROM oldish.t, Old.t, mapped.t",
      "CREATE VIEW v AS SELECT * FROM oldish.t, Old.t, `other_target`.t", '`', FALSE);
  check("SET @note='CREATE VIEW v AS SELECT * FROM old.t';",
      "SET @note='CREATE VIEW v AS SELECT * FROM old.t';", '`', FALSE);
  GString *escaped = g_string_new("CREATE VIEW v AS SELECT * FROM `o``ld`.`t`");
  rewrite_view_database_references(escaped, "o`ld", "n`ew", NULL, '`', FALSE);
  g_assert_cmpstr(escaped->str, ==, "CREATE VIEW v AS SELECT * FROM `n``ew`.`t`");
  g_string_free(escaped, TRUE);
  g_print("%u view mapping cases passed (including repeat application), plus quoted schema names.\n", cases);
  return 0;
}
