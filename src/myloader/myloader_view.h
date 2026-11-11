#ifndef MYLOADER_VIEW_H
#define MYLOADER_VIEW_H

#include <glib.h>

void rewrite_view_database_references(GString *sql, const gchar *source, const gchar *target, GHashTable *mapping, gchar quote, gboolean no_backslash_escapes);

#endif
