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

    Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <errno.h>
#include <glib-unix.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>

#include "common.h"

#include "common_options.h"
#include "signal_prompt.h"
#include "logging.h"

/* The callbacks below are dispatched by the GMainLoop that runs the signal
   sources, so they must never block waiting for the user: while one of them
   is blocked no other signal can be dispatched. The answer is read by a
   stdin watch on the same loop instead. */
static struct signal_prompt *prompt = NULL;
/* Never freed: a mutex can still be referenced from the queue. */
static GPtrArray *pause_mutexes = NULL;
static guint      paused_slots = 0;
static gboolean   prompt_pending = FALSE;
static gboolean   cancelled = FALSE;
static guint      prompt_stdin_watch = 0;
static guint      sigint_source = 0;

static void log_signal_event(GLogLevelFlags level, const char *event, const char *status, const char *json_message, const char *text)
{
  char  *datetimestr = prompt->timestamp ? m_date_time_new_now_local() : NULL;
  gchar *event_name = g_strdup_printf("%s_%s", prompt->event_prefix, event);
  if (machine_log_json)
    /* The list ends early when there is no timestamp. */
    machine_log_event(G_LOG_DOMAIN, level,
        "MESSAGE", json_message,
        "EVENT", event_name,
        "PHASE", "signal",
        "STATUS", status,
        datetimestr ? "TIMESTAMP_TEXT" : NULL, datetimestr,
        NULL);
  else
  {
    if (datetimestr)
      fprintf(stdout, "%s: ", datetimestr);
    fprintf(stdout, "%s", text);
    fflush(stdout);
  }
  g_free(event_name);
  g_free(datetimestr);
}

/* "backup" -> "Backup" */
static gchar *capitalized_name()
{
  return g_strdup_printf("%c%s", g_ascii_toupper(prompt->name[0]), prompt->name + 1);
}

static void pause_workers()
{
  guint i;
  paused_slots = prompt->pause_slots ? prompt->pause_slots() : num_threads;
  if (pause_mutexes == NULL)
    pause_mutexes = g_ptr_array_new();
  while (pause_mutexes->len < paused_slots)
    g_ptr_array_add(pause_mutexes, g_mutex_new());
  if (*prompt->pause_resume == NULL)
    *prompt->pause_resume = g_async_queue_new();
  for (i = 0; i < paused_slots; i++)
  {
    g_mutex_lock(g_ptr_array_index(pause_mutexes, i));
    g_async_queue_push(*prompt->pause_resume, g_ptr_array_index(pause_mutexes, i));
  }
}

/* Ends a pending question, if any, and releases the workers. When cancelling,
   shutdown_triggered is already set, so they stop at their next check. */
static void close_prompt()
{
  guint i;
  if (!prompt_pending)
    return;
  prompt_pending = FALSE;
  if (prompt_stdin_watch != 0)
  {
    g_source_remove(prompt_stdin_watch);
    prompt_stdin_watch = 0;
  }
  /* Slots of threads that were not running are still queued: drop them, so
     that they do not pile up across pauses. */
  while (g_async_queue_try_pop(*prompt->pause_resume) != NULL)
    ;
  for (i = 0; i < paused_slots; i++)
    g_mutex_unlock(g_ptr_array_index(pause_mutexes, i));
}

static void cancel(const char *json_message)
{
  cancelled = TRUE;
  if (json_message != NULL)
  {
    gchar *name = capitalized_name();
    gchar *text = g_strdup_printf("%s cancelled\n", name);
    log_signal_event(G_LOG_LEVEL_WARNING, "signal_cancel", "cancelled", json_message, text);
    g_free(text);
    g_free(name);
  }
  if (prompt->before_cancel)
    prompt->before_cancel();
  /* Set before the workers are released, so they stop instead of going on. */
  *prompt->shutdown_triggered = TRUE;
  close_prompt();
  prompt->on_cancel();
}

static gboolean prompt_answered(gint fd, GIOCondition condition, gpointer user_data)
{
  (void)condition;
  (void)user_data;
  char    c = 0;
  ssize_t n = read(fd, &c, 1);
  if (n < 0 && (errno == EINTR || errno == EAGAIN))
    return G_SOURCE_CONTINUE;
  if (n > 0 && c != 'Y' && c != 'y' && c != 'N' && c != 'n')
    return G_SOURCE_CONTINUE;
  /* Returning G_SOURCE_REMOVE drops this watch, close_prompt must not. */
  prompt_stdin_watch = 0;
  if (c == 'N' || c == 'n')
  {
    gchar *text = g_strdup_printf("Resuming %s\n", prompt->name);
    gchar *json = g_strdup_printf("%s resumed after confirmation", prompt->name);
    log_signal_event(G_LOG_LEVEL_MESSAGE, "signal_resume", "finished", json, text);
    g_free(json);
    g_free(text);
    close_prompt();
    if (prompt->on_resume)
      prompt->on_resume();
    return G_SOURCE_REMOVE;
  }
  /* 'Y', end of file or a read error: nobody can say no any more. */
  gchar *json = g_strdup_printf("%s cancelled after confirmation", prompt->name);
  cancel(json);
  g_free(json);
  /* Once cancelled a further Ctrl+c is not trapped. */
  if (sigint_source != 0)
  {
    g_source_remove(sigint_source);
    sigint_source = 0;
  }
  return G_SOURCE_REMOVE;
}

static gboolean can_ask()
{
  if (!isatty(STDIN_FILENO) || !isatty(STDOUT_FILENO))
    return FALSE;
  return prompt->can_ask == NULL || prompt->can_ask();
}

static gboolean sig_triggered(int signal)
{
  if (cancelled)
  {
    /* Already shutting down: nothing to ask. */
  }
  else if (prompt_pending)
  {
    /* Second Ctrl+c (or a SIGTERM) while waiting for the answer. */
    if (!machine_log_json)
      fprintf(stdout, "\n");
    gchar *json = g_strdup_printf("%s cancelled by a second signal", prompt->name);
    cancel(json);
    g_free(json);
  }
  else if (signal == SIGTERM || !can_ask())
  {
    /* Nobody can answer the question (and it would never reach a log
       file), so SIGINT acts as SIGTERM. */
    cancel(NULL);
  }
  else
  {
    if (prompt->on_prompt)
      prompt->on_prompt();
    pause_workers();
    log_signal_event(G_LOG_LEVEL_WARNING, "signal_prompt", "started",
        "Ctrl+c detected; waiting for confirmation",
        "Ctrl+c detected! Are you sure you want to cancel(Y/N)? Press Ctrl+c again to cancel now.");
    prompt_pending = TRUE;
    prompt_stdin_watch = g_unix_fd_add(STDIN_FILENO, G_IO_IN | G_IO_HUP | G_IO_ERR, prompt_answered, NULL);
    return G_SOURCE_CONTINUE;
  }
  if (signal == SIGINT)
    sigint_source = 0;
  return G_SOURCE_REMOVE;
}

static gboolean sig_triggered_int(gpointer user_data)
{
  (void)user_data;
  return sig_triggered(SIGINT);
}

static gboolean sig_triggered_term(gpointer user_data)
{
  (void)user_data;
  return sig_triggered(SIGTERM);
}

void signal_prompt_add(struct signal_prompt *sp)
{
  prompt = sp;
  sigint_source = g_unix_signal_add(SIGINT, sig_triggered_int, NULL);
  g_unix_signal_add(SIGTERM, sig_triggered_term, NULL);
}
