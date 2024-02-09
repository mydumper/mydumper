/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
    Authors:        David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <glib.h>
#include <string.h>

GSequence *tables_skiplist = NULL;

/* Comparison function for skiplist sort and lookup */

int tables_skiplist_cmp(gconstpointer a, gconstpointer b, gpointer user_data) {
  /* Not using user_data, but needed for function prototype, shutting up
   * compiler warnings about unused variable */
  (void)user_data;
  /* Any sorting function would work, as long as its usage is consistent
   * between sort and lookup.  strcmp should be one of the fastest. */
  return strcmp(a, b);
}

/* Read the list of tables to skip from the given filename, and prepares them
 * for future lookups. */

void read_tables_skiplist(const gchar *filename, guint *errors) {

  GIOChannel *tables_skiplist_channel = NULL;
  gchar *buf = NULL;
  GError *error = NULL;
  /* Create skiplist if it does not exist */
  if (!tables_skiplist) {
    tables_skiplist = g_sequence_new(NULL);
  };
  tables_skiplist_channel = g_io_channel_new_file(filename, "r", &error);

  /* Error opening/reading the file? bail out. */
  if (!tables_skiplist_channel) {
    g_critical("cannot read/open file %s, %s\n", filename, error->message);
    (*errors)++;
    return;
  };

  /* Read lines, push them to the list */
  do {
    g_io_channel_read_line(tables_skiplist_channel, &buf, NULL, NULL, NULL);
    if (buf) {
      g_strchomp(buf);
      g_sequence_append(tables_skiplist, buf);
    };
  } while (buf);
  g_io_channel_shutdown(tables_skiplist_channel, FALSE, NULL);
  /* Sort the list, so that lookups work */
  g_sequence_sort(tables_skiplist, tables_skiplist_cmp, NULL);
  g_message("Omit list file contains %d tables to skip\n",
            g_sequence_get_length(tables_skiplist));
  return;
}

/* Check database.table string against skip list; returns TRUE if found */

gboolean check_skiplist(char *database, char *table) {
  gboolean b = g_sequence_lookup(tables_skiplist,
                        database,
                        tables_skiplist_cmp, NULL) != NULL;
  if (!table || b)
    return b;
  gchar * k=g_strdup_printf("%s.%s", database, table);
  b = g_sequence_lookup(tables_skiplist,
                        k,
                        tables_skiplist_cmp, NULL) != NULL;
  g_free(k);
  return b;
}
