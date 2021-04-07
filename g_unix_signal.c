#define _POSIX_SOURCE
#include <signal.h>
#include <glib.h>

static GPtrArray *signal_data = NULL;

typedef struct _GUnixSignalData {
  guint source_id;
  GMainContext *context;
  gboolean triggered;
  gint signum;
} GUnixSignalData;

typedef struct _GUnixSignalSource {
  GSource source;
  GUnixSignalData *data;
} GUnixSignalSource;

static inline GUnixSignalData *get_signal_data(guint index) {
  return (GUnixSignalData *)g_ptr_array_index(signal_data, index);
}

static void handler(gint signum) {
  g_assert(signal_data != NULL);
  guint i;
  for (i = 0; i < signal_data->len; ++i)
    if (get_signal_data(i)->signum == signum)
      get_signal_data(i)->triggered = TRUE;

  struct sigaction action;
  action.sa_handler = handler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(signum, &action, NULL);
}

static gboolean check(GSource *source) {
  GUnixSignalSource *signal_source = (GUnixSignalSource *)source;
  return signal_source->data->triggered;
}

static gboolean prepare(GSource *source, gint *timeout_) {
  GUnixSignalSource *signal_source = (GUnixSignalSource *)source;
  if (signal_source->data->context == NULL) {
    g_main_context_ref(signal_source->data->context =
                           g_source_get_context(source));
    signal_source->data->source_id = g_source_get_id(source);
  }

  *timeout_ = -1;
  return signal_source->data->triggered;
}

static gboolean dispatch(GSource *source, GSourceFunc callback,
                         gpointer user_data) {
  GUnixSignalSource *signal_source = (GUnixSignalSource *)source;
  signal_source->data->triggered = FALSE;
  return callback(user_data) ? TRUE : FALSE;
}
static void finalize(GSource *source) {
  GUnixSignalSource *signal_source = (GUnixSignalSource *)source;

  struct sigaction action;
  action.sa_handler = NULL;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;

  sigaction(signal_source->data->signum, &action, NULL);
  g_main_context_unref(signal_source->data->context);
  g_ptr_array_remove_fast(signal_data, signal_source->data);
  if (signal_data->len == 0)
    signal_data = (GPtrArray *)g_ptr_array_free(signal_data, TRUE);
  g_free(signal_source->data);
}
static GSourceFuncs SourceFuncs = {.prepare = prepare,
                                   .check = check,
                                   .dispatch = dispatch,
                                   .finalize = finalize,
                                   .closure_callback = NULL,
                                   .closure_marshal = NULL};

static void g_unix_signal_source_init(GSource *source, gint signum) {
  GUnixSignalSource *signal_source = (GUnixSignalSource *)source;
  signal_source->data = g_new(GUnixSignalData, 1);
  signal_source->data->triggered = FALSE;
  signal_source->data->signum = signum;
  signal_source->data->context = NULL;

  if (signal_data == NULL)
    signal_data = g_ptr_array_new();
  g_ptr_array_add(signal_data, signal_source->data);
}

GSource *g_unix_signal_source_new(gint signum) {
  GSource *source = g_source_new(&SourceFuncs, sizeof(GUnixSignalSource));
  g_unix_signal_source_init(source, signum);
  struct sigaction action;
  action.sa_handler = handler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(signum, &action, NULL);
  return source;
}

guint g_unix_signal_add_full(gint priority, gint signum, GSourceFunc function,
                             gpointer data, GDestroyNotify notify) {
  g_return_val_if_fail(function != NULL, 0);
  GSource *source = g_unix_signal_source_new(signum);
  if (priority != G_PRIORITY_DEFAULT)
    g_source_set_priority(source, priority);
  g_source_set_callback(source, function, data, notify);
  guint id = g_source_attach(source, NULL);
  g_source_unref(source);
  return id;
}

guint g_unix_signal_add(gint signum, GSourceFunc function, gpointer data) {
  return g_unix_signal_add_full(G_PRIORITY_DEFAULT, signum, function, data,
                                NULL);
}
