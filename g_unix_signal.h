#ifndef G_UNIX_SIGNAL_H
#define G_UNIX_SIGNAL_H

#include <glib.h>

GSource *gg_unix_signal_source_new(gint signum);
guint gg_unix_signal_add(gint signum, GSourceFunc function, gpointer data);
guint gg_unix_signal_add_full(gint priority, gint signum, GSourceFunc function, gpointer data, GDestroyNotify notify);

#endif /* G_UNIX_SIGNAL_H */
