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

#ifndef _src_signal_prompt_h
#define _src_signal_prompt_h

#include <glib.h>

/* Ctrl+c / SIGTERM handling shared by mydumper and myloader.

   On the first Ctrl+c the workers are paused and the user is asked whether
   to cancel. The question is asked without blocking the main loop that
   dispatches the signals, so a second Ctrl+c (or a SIGTERM) while it is
   pending cancels right away. SIGTERM, or a Ctrl+c when nobody can answer,
   cancels without asking.

   Workers pause by popping a GMutex from *pause_resume and locking it. */
struct signal_prompt
{
  /* "backup" or "restore": used in the messages. */
  const char *name;
  /* "dump" or "restore": prefix of the machine log events. */
  const char *event_prefix;
  /* Prefix the text messages with the local time. */
  gboolean timestamp;
  /* Queue the workers pop their pause mutex from; created when NULL. */
  GAsyncQueue **pause_resume;
  /* Optional: how many threads can pause, one mutex is pushed for each.
     Without it, num_threads. */
  guint (*pause_slots)(void);
  /* The tool's shutdown flag, set when cancelling. */
  gboolean *shutdown_triggered;
  /* Optional: FALSE when the question must not be asked. Without a
     terminal on stdin and stdout it is never asked. */
  gboolean (*can_ask)(void);
  /* Optional: called before the workers are paused to ask the question. */
  void (*on_prompt)(void);
  /* Optional: called after the user answered N and the workers resumed. */
  void (*on_resume)(void);
  /* Optional: called first when cancelling, before *shutdown_triggered is
     set. */
  void (*before_cancel)(void);
  /* Called once when cancelling, after *shutdown_triggered is set and the
     workers are released. */
  void (*on_cancel)(void);
};

/* Adds the SIGINT and SIGTERM sources to the default main context, which
   must be run by the caller. sp must stay valid while they are installed. */
void signal_prompt_add(struct signal_prompt *sp);

#endif
