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

        Authors:        Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)

*/

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <zlib.h>
#include <signal.h>
#include <glib/gstdio.h>
#include "common.h"

gchar *logfile;
FILE *logoutfile;
static guint current_log_verbosity = 2;
static gboolean writer_initialized = FALSE;
static gboolean writer_machine_mode = FALSE;
static gchar *machine_run_id = NULL;
static volatile gint machine_log_sequence = 0;
static volatile gint machine_warning_count = 0;
static GPrivate machine_parent_seq_key = G_PRIVATE_INIT(NULL);
static const gchar machine_log_schema_version[] = "1";

static gchar *generate_uuid_v7_string(void) {
  guint64 unix_ts_ms = (guint64)(g_get_real_time() / 1000);
  guint32 rand_a = g_random_int() & 0x0FFFU;
  guint64 rand_b = (((guint64)g_random_int()) << 32) | (guint64)g_random_int();
  guint32 time_high = (guint32)((unix_ts_ms >> 16) & 0xFFFFFFFFU);
  guint16 time_low = (guint16)(unix_ts_ms & 0xFFFFU);
  guint16 version_and_rand = (guint16)(0x7000U | rand_a);
  guint16 variant_and_rand = (guint16)(0x8000U | ((rand_b >> 48) & 0x3FFFU));
  guint64 rand_tail = rand_b & G_GUINT64_CONSTANT(0x0000FFFFFFFFFFFF);

  return g_strdup_printf("%08x-%04x-%04x-%04x-%012" G_GINT64_MODIFIER "x",
                         time_high,
                         time_low,
                         version_and_rand,
                         variant_and_rand,
                         rand_tail);
}


void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data) {
  (void)log_domain;
  (void)log_level;
  (void)message;
  (void)user_data;
}

void write_log_file(const gchar *log_domain, GLogLevelFlags log_level,
                    const gchar *message, gpointer user_data) {
  (void)log_domain;
  (void)user_data;

  gchar date[20];
  time_t rawtime;
  struct tm timeinfo;

  // Don't log debug if debugging off
#if GLIB_CHECK_VERSION(2,68,0)
  if ((log_level & G_LOG_LEVEL_DEBUG) &&
      g_log_writer_default_would_drop(log_level, log_domain)) {
    return;
  }
#endif

  time(&rawtime);
  localtime_r(&rawtime, &timeinfo);
  strftime(date, 20, "%Y-%m-%d %H:%M:%S", &timeinfo);

  GString *message_out = g_string_new(date);
  if (log_level & G_LOG_LEVEL_DEBUG) {
    g_string_append(message_out, " [DEBUG] - ");
  } else if ((log_level & G_LOG_LEVEL_INFO) ||
             (log_level & G_LOG_LEVEL_MESSAGE)) {
    g_string_append(message_out, " [INFO] - ");
  } else if (log_level & G_LOG_LEVEL_WARNING) {
    g_string_append(message_out, " [WARNING] - ");
  } else if ((log_level & G_LOG_LEVEL_ERROR) ||
             (log_level & G_LOG_LEVEL_CRITICAL)) {
    g_string_append(message_out, " [ERROR] - ");
  }

  g_string_append_printf(message_out, "%s\n", message);
  if (write(fileno(logoutfile), message_out->str, message_out->len) <= 0) {
    fprintf(stderr, "Cannot write to log file with error %d.  Exiting...",
            errno);
  }
  g_string_free(message_out, TRUE);
}

static gboolean should_log_level(GLogLevelFlags log_level) {
  if (current_log_verbosity == 0) {
    return FALSE;
  }
  if ((log_level & G_LOG_LEVEL_DEBUG) != 0U) {
    return current_log_verbosity >= 4;
  }
  if (((log_level & G_LOG_LEVEL_INFO) != 0U) ||
      ((log_level & G_LOG_LEVEL_MESSAGE) != 0U)) {
    return current_log_verbosity >= 3;
  }
  if ((log_level & G_LOG_LEVEL_WARNING) != 0U) {
    return current_log_verbosity >= 2;
  }
  if (((log_level & G_LOG_LEVEL_ERROR) != 0U) ||
      ((log_level & G_LOG_LEVEL_CRITICAL) != 0U)) {
    return current_log_verbosity >= 1;
  }
  return TRUE;
}

static const gchar *log_level_name(GLogLevelFlags log_level) {
  if ((log_level & G_LOG_LEVEL_DEBUG) != 0U) {
    return "DEBUG";
  }
  if (((log_level & G_LOG_LEVEL_INFO) != 0U) ||
      ((log_level & G_LOG_LEVEL_MESSAGE) != 0U)) {
    return "INFO";
  }
  if ((log_level & G_LOG_LEVEL_WARNING) != 0U) {
    return "WARNING";
  }
  if (((log_level & G_LOG_LEVEL_ERROR) != 0U) ||
      ((log_level & G_LOG_LEVEL_CRITICAL) != 0U)) {
    return "ERROR";
  }
  return "LOG";
}

static const gchar *default_runtime_status(GLogLevelFlags log_level) {
  if (((log_level & G_LOG_LEVEL_ERROR) != 0U) ||
      ((log_level & G_LOG_LEVEL_CRITICAL) != 0U)) {
    return "failed";
  }
  if ((log_level & G_LOG_LEVEL_WARNING) != 0U) {
    return "warning";
  }
  if ((log_level & G_LOG_LEVEL_DEBUG) != 0U) {
    return "progress";
  }
  return "progress";
}

static void append_json_escaped_len(GString *json, const gchar *value, gssize len) {
  gsize i = 0;
  gsize limit = 0;

  g_string_append_c(json, '"');
  if (value == NULL) {
    g_string_append_c(json, '"');
    return;
  }

  limit = len >= 0 ? (gsize)len : strlen(value);
  for (i = 0; i < limit; i++) {
    const unsigned char c = (unsigned char)value[i];
    switch (c) {
    case '\\':
      g_string_append(json, "\\\\");
      break;
    case '"':
      g_string_append(json, "\\\"");
      break;
    case '\b':
      g_string_append(json, "\\b");
      break;
    case '\f':
      g_string_append(json, "\\f");
      break;
    case '\n':
      g_string_append(json, "\\n");
      break;
    case '\r':
      g_string_append(json, "\\r");
      break;
    case '\t':
      g_string_append(json, "\\t");
      break;
    default:
      if (c < 0x20U) {
        g_string_append_printf(json, "\\u%04x", c);
      } else {
        g_string_append_c(json, (gchar)c);
      }
      break;
    }
  }
  g_string_append_c(json, '"');
}

static void append_json_field(GString *json, gboolean *first, const gchar *key,
                              const gchar *value, gssize len) {
  if (!(*first)) {
    g_string_append_c(json, ',');
  }
  *first = FALSE;
  append_json_escaped_len(json, key, -1);
  g_string_append_c(json, ':');
  append_json_escaped_len(json, value, len);
}

static void append_json_bool_field(GString *json, gboolean *first,
                                   const gchar *key, gboolean value) {
  if (!(*first)) {
    g_string_append_c(json, ',');
  }
  *first = FALSE;
  append_json_escaped_len(json, key, -1);
  g_string_append_printf(json, ":%s", value ? "true" : "false");
}

static void append_json_int_field(GString *json, gboolean *first,
                                  const gchar *key, gint64 value) {
  if (!(*first)) {
    g_string_append_c(json, ',');
  }
  *first = FALSE;
  append_json_escaped_len(json, key, -1);
  g_string_append_printf(json, ":%" G_GINT64_FORMAT, value);
}

static const GLogField *find_log_field(const GLogField *fields, gsize n_fields,
                                       const gchar *key) {
  gsize i = 0;
  for (i = 0; i < n_fields; i++) {
    if (fields[i].key != NULL && strcmp(fields[i].key, key) == 0) {
      return &(fields[i]);
    }
  }
  return NULL;
}

static gboolean should_skip_machine_field(const gchar *key) {
  return strcmp(key, "MESSAGE") == 0 || strcmp(key, "PRIORITY") == 0 ||
         strcmp(key, "GLIB_DOMAIN") == 0 || strcmp(key, "EVENT") == 0 ||
         strcmp(key, "PHASE") == 0 || strcmp(key, "STATUS") == 0 ||
         strcmp(key, "PARENT_SEQ") == 0 ||
         strcmp(key, "MYSQL_ERRNO") == 0 || strcmp(key, "SQLSTATE") == 0 ||
         strcmp(key, "RETRYABLE") == 0 || strcmp(key, "FATAL") == 0;
}

static const gchar *default_runtime_event_name(GLogLevelFlags log_level) {
  if (((log_level & G_LOG_LEVEL_ERROR) != 0U) ||
      ((log_level & G_LOG_LEVEL_CRITICAL) != 0U)) {
    return "process_error";
  }
  if ((log_level & G_LOG_LEVEL_WARNING) != 0U) {
    return "process_warning";
  }
  return "process_notice";
}

static const gchar *field_string_value(const GLogField *field) {
  if (field == NULL || field->value == NULL) {
    return NULL;
  }
  return (const gchar *)field->value;
}

static gchar *normalize_machine_key(const gchar *key) {
  return g_ascii_strdown(key, -1);
}

static gint extract_mysql_errno(const gchar *message) {
  const gchar *error_marker = NULL;
  gchar *endptr = NULL;
  long value = 0;

  if (message == NULL) {
    return 0;
  }

  error_marker = strstr(message, "ERROR ");
  if (error_marker == NULL) {
    return 0;
  }

  value = strtol(error_marker + 6, &endptr, 10);
  if (endptr == error_marker + 6 || value <= 0 || value > G_MAXINT) {
    return 0;
  }

  return (gint)value;
}

static GLogWriterOutput machine_json_log_writer(GLogLevelFlags log_level,
                                                const GLogField *fields,
                                                gsize n_fields,
                                                gpointer user_data) {
  const GLogField *message_field = NULL;
  const GLogField *domain_field = NULL;
  const GLogField *event_field = NULL;
  const GLogField *phase_field = NULL;
  const GLogField *status_field = NULL;
  const GLogField *db_field = NULL;
  const GLogField *table_field = NULL;
  const GLogField *filename_field = NULL;
  const GLogField *part_index_field = NULL;
  const GLogField *mysql_errno_field = NULL;
  const GLogField *sqlstate_field = NULL;
  const GLogField *retryable_field = NULL;
  const GLogField *fatal_field = NULL;
  const GLogField *parent_seq_field = NULL;
  FILE *destination = logoutfile != NULL ? logoutfile : stderr;
  GString *json = NULL;
  gchar *timestamp = NULL;
  gchar *object_id = NULL;
  gchar *chunk_id = NULL;
  gchar *job_id = NULL;
  const gchar *thread_name = NULL;
  const gchar *message = NULL;
  const gchar *db_name = NULL;
  const gchar *table_name = NULL;
  const gchar *filename = NULL;
  const gchar *part_index = NULL;
  const gchar *mysql_errno_text = NULL;
  const gchar *sqlstate = NULL;
  const gchar *retryable_text = NULL;
  const gchar *fatal_text = NULL;
  const gchar *parent_seq_text = NULL;
  const gchar *event_name = NULL;
  const gchar *phase_name = NULL;
  const gchar *status_name = NULL;
  gboolean first = TRUE;
  gboolean has_explicit_event = FALSE;
  gboolean is_fatal = ((log_level & G_LOG_LEVEL_ERROR) != 0U) ||
                      ((log_level & G_LOG_LEVEL_CRITICAL) != 0U);
  gboolean retryable = FALSE;
  gint parsed_mysql_errno = 0;
  gint seq = 0;
  gint parent_seq = 0;
  gsize i = 0;
  (void)user_data;

  event_field = find_log_field(fields, n_fields, "EVENT");
  if ((log_level & G_LOG_LEVEL_WARNING) != 0U) {
    g_atomic_int_inc(&machine_warning_count);
  }
  if (!should_log_level(log_level) &&
      (event_field == NULL || event_field->value == NULL)) {
    return G_LOG_WRITER_HANDLED;
  }

  message_field = find_log_field(fields, n_fields, "MESSAGE");
  domain_field = find_log_field(fields, n_fields, "GLIB_DOMAIN");
  phase_field = find_log_field(fields, n_fields, "PHASE");
  status_field = find_log_field(fields, n_fields, "STATUS");
  db_field = find_log_field(fields, n_fields, "DB");
  table_field = find_log_field(fields, n_fields, "TABLE");
  filename_field = find_log_field(fields, n_fields, "FILENAME");
  part_index_field = find_log_field(fields, n_fields, "PART_INDEX");
  mysql_errno_field = find_log_field(fields, n_fields, "MYSQL_ERRNO");
  sqlstate_field = find_log_field(fields, n_fields, "SQLSTATE");
  retryable_field = find_log_field(fields, n_fields, "RETRYABLE");
  fatal_field = find_log_field(fields, n_fields, "FATAL");
  parent_seq_field = find_log_field(fields, n_fields, "PARENT_SEQ");
  timestamp = m_date_time_new_now_local();
  thread_name = get_thread_name();
  message = field_string_value(message_field);
  db_name = field_string_value(db_field);
  table_name = field_string_value(table_field);
  filename = field_string_value(filename_field);
  part_index = field_string_value(part_index_field);
  mysql_errno_text = field_string_value(mysql_errno_field);
  sqlstate = field_string_value(sqlstate_field);
  retryable_text = field_string_value(retryable_field);
  fatal_text = field_string_value(fatal_field);
  parent_seq_text = field_string_value(parent_seq_field);
  has_explicit_event = event_field != NULL && event_field->value != NULL;
  if (retryable_text != NULL && (!g_ascii_strcasecmp(retryable_text, "true") ||
      !strcmp(retryable_text, "1"))) {
    retryable = TRUE;
  }
  if (fatal_text != NULL) {
    is_fatal = !g_ascii_strcasecmp(fatal_text, "true") || !strcmp(fatal_text, "1");
  }
  if (mysql_errno_text == NULL) {
    parsed_mysql_errno = extract_mysql_errno(message);
  }
  if (db_name != NULL && table_name != NULL) {
    object_id = g_strdup_printf("%s.%s", db_name, table_name);
  } else if (db_name != NULL) {
    object_id = g_strdup(db_name);
  }
  if (object_id != NULL && part_index != NULL) {
    chunk_id = g_strdup_printf("%s:%s", object_id, part_index);
  }
  if (filename != NULL) {
    job_id = g_strdup(filename);
  }
  event_name = has_explicit_event ? field_string_value(event_field) : default_runtime_event_name(log_level);
  phase_name = phase_field != NULL && phase_field->value != NULL ?
               field_string_value(phase_field) : "runtime";
  status_name = status_field != NULL && status_field->value != NULL ?
                field_string_value(status_field) : default_runtime_status(log_level);
  seq = g_atomic_int_add(&machine_log_sequence, 1) + 1;
  if (parent_seq_text != NULL) {
    parent_seq = (gint)g_ascii_strtoll(parent_seq_text, NULL, 10);
  } else {
    parent_seq = GPOINTER_TO_INT(g_private_get(&machine_parent_seq_key));
  }
  g_private_set(&machine_parent_seq_key, GINT_TO_POINTER(seq));

  json = g_string_new("{");
  append_json_field(json, &first, "schema_version", machine_log_schema_version, -1);
  append_json_field(json, &first, "event_version", machine_log_schema_version, -1);
  append_json_int_field(json, &first, "seq", seq);
  append_json_field(json, &first, "run_id",
                    machine_run_id != NULL ? machine_run_id : "", -1);
  append_json_field(json, &first, "kind", "event", -1);
  append_json_field(json, &first, "ts", timestamp, -1);
  append_json_field(json, &first, "level", log_level_name(log_level), -1);
  append_json_field(json, &first, "tool", g_get_prgname(), -1);
  append_json_field(json, &first, "program", g_get_prgname(), -1);
  append_json_int_field(json, &first, "pid", getpid());

  if (thread_name != NULL) {
    append_json_field(json, &first, "thread", thread_name, -1);
  }
  if (domain_field != NULL && domain_field->value != NULL) {
    append_json_field(json, &first, "domain", (const gchar *)domain_field->value,
                      domain_field->length);
  }
  append_json_field(json, &first, "event", event_name, -1);
  append_json_field(json, &first, "phase", phase_name, -1);
  append_json_field(json, &first, "status", status_name, -1);
  if (parent_seq > 0) {
    append_json_int_field(json, &first, "parent_seq", parent_seq);
  }
  if (message_field != NULL && message_field->value != NULL) {
    append_json_field(json, &first, "message",
                      (const gchar *)message_field->value, message_field->length);
  }
  append_json_bool_field(json, &first, "fatal", is_fatal);
  if (is_fatal || retryable || mysql_errno_text != NULL || parsed_mysql_errno > 0) {
    append_json_bool_field(json, &first, "retryable", retryable);
  }
  if (mysql_errno_text != NULL) {
    append_json_field(json, &first, "mysql_errno", mysql_errno_text, -1);
    append_json_field(json, &first, "error_code", mysql_errno_text, -1);
  } else if (parsed_mysql_errno > 0) {
    gchar *mysql_errno_buf = g_strdup_printf("%d", parsed_mysql_errno);
    append_json_field(json, &first, "mysql_errno", mysql_errno_buf, -1);
    append_json_field(json, &first, "error_code", mysql_errno_buf, -1);
    g_free(mysql_errno_buf);
  }
  if (sqlstate != NULL) {
    append_json_field(json, &first, "sqlstate", sqlstate, -1);
  }
  if (object_id != NULL) {
    append_json_field(json, &first, "object_id", object_id, -1);
  }
  if (job_id != NULL) {
    append_json_field(json, &first, "job_id", job_id, -1);
  }
  if (chunk_id != NULL) {
    append_json_field(json, &first, "chunk_id", chunk_id, -1);
  }
  for (i = 0; i < n_fields; i++) {
    gchar *normalized_key = NULL;
    if (fields[i].key == NULL || fields[i].value == NULL ||
        should_skip_machine_field(fields[i].key)) {
      continue;
    }
    normalized_key = normalize_machine_key(fields[i].key);
    append_json_field(json, &first, normalized_key,
                      (const gchar *)fields[i].value, fields[i].length);
    g_free(normalized_key);
  }

  g_string_append(json, "}\n");
  if (write(fileno(destination), json->str, json->len) <= 0) {
    fprintf(stderr, "Cannot write machine log output with error %d.\n", errno);
  }

  g_free(timestamp);
  g_free(object_id);
  g_free(chunk_id);
  g_free(job_id);
  g_string_free(json, TRUE);
  return G_LOG_WRITER_HANDLED;
}

void configure_log_output(guint verbosity) {
  current_log_verbosity = verbosity;
  if (!writer_initialized) {
    writer_machine_mode = machine_log_json_enabled();
    if (writer_machine_mode) {
      if (machine_run_id == NULL) {
        machine_run_id = generate_uuid_v7_string();
      }
      g_log_set_writer_func(machine_json_log_writer, NULL, NULL);
    } else {
      g_log_set_writer_func(g_log_writer_default, NULL, NULL);
    }
    writer_initialized = TRUE;
  }
}

void machine_log_event(const gchar *log_domain, GLogLevelFlags log_level, ...) {
  GArray *fields = NULL;
  GLogField field = {0};
  const gchar *key = NULL;
  va_list args;

  fields = g_array_new(FALSE, FALSE, sizeof(GLogField));

  if (log_domain != NULL) {
    field.key = "GLIB_DOMAIN";
    field.value = log_domain;
    field.length = -1;
    g_array_append_val(fields, field);
  }

  va_start(args, log_level);
  while ((key = va_arg(args, const gchar *)) != NULL) {
    const gchar *value = va_arg(args, const gchar *);
    field.key = key;
    field.value = value;
    field.length = -1;
    g_array_append_val(fields, field);
  }
  va_end(args);

  if (writer_machine_mode) {
    machine_json_log_writer(log_level, (const GLogField *)fields->data,
                            fields->len, NULL);
  } else {
    g_log_structured_array(log_level, (const GLogField *)fields->data, fields->len);
  }

  g_array_free(fields, TRUE);
}

guint machine_log_warning_count_get(void) {
  return (guint)g_atomic_int_get(&machine_warning_count);
}
