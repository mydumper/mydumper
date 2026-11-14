/* Exercise real table initialization without a database or restore. */
#include <glib.h>
#include "common.h"
#include "checksum.h"
#include "myloader/myloader_table.h"
#include "myloader/myloader_database.h"
#include "myloader/myloader_common.h"

GHashTable *conf_per_table;
guint max_threads_per_table = 1, num_threads = 1, retry_count = 0;
gboolean schema_checksums, data_checksums = FALSE;
gboolean skip_view_checksums = FALSE, skip_table_checksums = FALSE;
gboolean skip_index_checksums, skip_trigger_checksums, skip_data_checksums = FALSE;
gboolean skip_indexes = FALSE;

void trace(const char *fmt, ...) { (void)fmt; }
void refresh_table_list_without_table_hash_lock(struct configuration *conf, gboolean force)
{
  (void)conf;
  (void)force;
}
void parse_object_scope(struct object_scope *scope, gchar *value)
{
  (void)value;
  *scope = (struct object_scope){0};
}
gchar *build_dbt_key(gchar *a, gchar *b) { return g_strdup_printf("%s.%s", a, b); }
gchar *build_config_file_dbt_key(const gchar *a, const gchar *b) { return g_strdup_printf("%s.%s", a, b); }
void *m_coalesce_hash(GHashTable *hash, gchar *table_key, gchar *any_db_key, gchar *any_table_key)
{
  if (!hash)
    return NULL;
  gpointer value = g_hash_table_lookup(hash, table_key);
  if (!value)
    value = g_hash_table_lookup(hash, any_db_key);
  return value ? value : g_hash_table_lookup(hash, any_table_key);
}

static void check(gboolean schemas, gboolean skip_index, gboolean skip_trigger, gboolean table_override)
{
  schema_checksums = schemas;
  skip_index_checksums = skip_index;
  skip_trigger_checksums = skip_trigger;
  GMutex mutex;
  g_mutex_init(&mutex);
  struct configuration conf = {0};
  conf.table_hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
  conf.table_hash_mutex = &mutex;
  initialize_table(&conf);
  conf_per_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (GDestroyNotify)g_hash_table_destroy);
  if (table_override)
  {
    GHashTable *overrides = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    g_hash_table_insert(overrides, g_strdup("db.t"), GINT_TO_POINTER(TRUE));
    g_hash_table_insert(conf_per_table, g_strdup(SKIP_TRIGGER_CHECKSUMS), overrides);
  }
  gchar database_name[] = "db", table_name[] = "t";
  struct database database = {0};
  database.source_database = database.database_name_in_filename = database.target_database = database_name;
  struct db_table *table = NULL;
  g_assert_true(append_new_db_table(&table, &database, g_strdup(table_name), table_name, FALSE));
  g_assert_cmpint(table->checksum.skip_trigger, ==, table_override || !schemas || skip_trigger);
  g_assert_cmpint(table->checksum.skip_index, ==, !schemas || skip_index || skip_indexes);
  g_free(table->source_table_name);
  g_free(table->table_filename);
  g_mutex_free(table->mutex);
  g_cond_free(table->schema_cond);
  g_free(table);
  g_hash_table_destroy(conf.table_hash);
  g_hash_table_destroy(conf_per_table);
  g_mutex_clear(&mutex);
}

int main(int argc, char **argv)
{
  if (argc == 2 && !g_strcmp0(argv[1], "skip-index"))
  {
    check(TRUE, TRUE, FALSE, FALSE);
    return 0;
  }
  /* Both regressions are covered before the remaining flag combinations. */
  check(TRUE, FALSE, TRUE, FALSE);
  check(TRUE, TRUE, FALSE, FALSE);
  /* --skip-indexes: the indexes are not created, so they must not be verified (#2292). */
  skip_indexes = TRUE;
  check(TRUE, FALSE, FALSE, FALSE);
  skip_indexes = FALSE;
  for (int schemas = 0; schemas <= 1; schemas++)
    for (int index = 0; index <= 1; index++)
      for (int trigger = 0; trigger <= 1; trigger++)
        for (int override = 0; override <= 1; override++)
          check(schemas, index, trigger, override);
  g_print("19 trigger/index checksum flag cases passed.\n");
  return 0;
}
