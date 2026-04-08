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
#include <mysql.h>
#include <string.h>
#include <glib.h>
#include "common.h"
#include "checksum.h"
#include "logging.h"

enum checksum_modes checksum_mode= CHECKSUM_WARN;  // Issue #1975: Warn by default instead of fail

gboolean dump_checksums = FALSE;
gboolean data_checksums = FALSE;
gboolean schema_checksums = TRUE;  // Issue #1975: Enable schema checksums by default
gboolean routine_checksums = FALSE;

gboolean skip_database_checksums=FALSE;
gboolean skip_data_checksums=FALSE;
gboolean skip_table_checksums=FALSE;
gboolean skip_index_checksums=FALSE;
gboolean skip_view_checksums=FALSE;
gboolean skip_trigger_checksums=FALSE;
gboolean skip_routine_checksums=FALSE;
gboolean skip_event_checksums=FALSE;

GOptionEntry common_checksum_entries[] = {
    {"checksum-all", 'M', 0, G_OPTION_ARG_NONE, &dump_checksums,
      "Enables checksums for all elements", NULL},
     {"data-checksums", 0, 0, G_OPTION_ARG_NONE, &data_checksums,
      "Disables table checksums with the data", NULL},
    {"schema-checksums", 0, 0, G_OPTION_ARG_NONE, &schema_checksums,
      "Enables schema, table, indexes and view creation checksums. "
      "(Defaults to on; use --no-schema-checksums to disable)", NULL},
    {"no-schema-checksums", 0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, &schema_checksums,
      "Disables schema, table and view creation checksums", NULL},
    {"routine-checksums", 0, 0, G_OPTION_ARG_NONE, &routine_checksums,
      "Enables triggers, functions and routines checksums.", NULL}, 
    {"skip-database-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_database_checksums,
      "Disables checksums over the schema of the database", NULL},
    {"skip-data-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_data_checksums,
      "Disables checksums over the data of the table.", NULL},
    {"skip-table-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_table_checksums,
      "Disables checksums over the table schema", NULL},
    {"skip-index-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_index_checksums,
      "Disables checksums over the indexes of the table", NULL},
    {"skip-view-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_view_checksums,
      "Disables checksums over the schema of the view", NULL},
    {"skip-trigger-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_trigger_checksums,
      "Disables checksums over the triggers of the table or the database.", NULL},
    {"skip-routine-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_routine_checksums,
      "Disables checksums over the funtions and store procedures.", NULL},
    {"skip-event-checksums", 0, 0, G_OPTION_ARG_NONE, &skip_event_checksums,
      "Disables checksums over the events.", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

void initilize_checksum(){
  if (dump_checksums){
    data_checksums = TRUE;
    schema_checksums = TRUE;
    routine_checksums = TRUE;
  }
  if (data_checksums && skip_data_checksums )
    m_error("Incompatible settings data checksums are enable and disable at the same time.");
}


static
char *generic_checksum(MYSQL *conn, const gchar *query_template, ...){
  va_list args;
  va_start(args, query_template);
  int column_number = va_arg(args, int);
  char *query =g_strdup_vprintf(query_template,args);
  gchar * database = va_arg(args, gchar *);
  gchar * table = va_arg(args, gchar *);
  struct M_ROW *mr = m_store_result_single_row( conn, query, "Error dumping checksum (%s.%s)", database, table);
  g_free(query);
  char * r=NULL;
  if (mr->row)
    r=g_strdup_printf("%s",mr->row[column_number]);
  m_store_result_row_free(mr);
  va_end(args);
  return r;
}

char * checksum_table(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "CHECKSUM TABLE `%s`.`%s`", 
      1, database, table);
}

char * checksum_table_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 
      0, database, table);
}

char * checksum_process_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 
      0, database, table);
}

char * checksum_trigger_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';",
      0, database, table);
}

char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';",
      0, database, table);
}

char * checksum_view_structure(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';",
      0, database, table);
}

char * checksum_database_defaults(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn,
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;",
      0, database, table);
}

char * checksum_table_indexes(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, 
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32( col ) AS UNSIGNED)), 10, 16)), 0) AS crc FROM ( SELECT CONCAT_WS(INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME) AS col FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME) A",
      0, database, table);
}

char * checksum_events_structure_from_database(MYSQL *conn, char *database, char *table){
  return generic_checksum(conn, 
      "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(EVENT_DEFINITION, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.events WHERE EVENT_SCHEMA='%s';",
      0, database, table);
}

gboolean should_write_database_checksum(struct database_level_checksum *database_checksum){
  return 
    ( !database_checksum->skip_schema  && database_checksum->schema ) || 
    ( !database_checksum->skip_routine && database_checksum->routine) ||
    ( !database_checksum->skip_event   && database_checksum->event  ) ||
    ( !database_checksum->skip_trigger && database_checksum->trigger);
}

void write_database_checksum(FILE *mdfile, struct database_level_checksum *database_checksum){
  if ( !database_checksum->skip_schema && database_checksum->schema)
    fprintf(mdfile, "%s = %s\n", "schema_checksum", database_checksum->schema);
  if ( !database_checksum->skip_routine && database_checksum->routine)
    fprintf(mdfile, "%s = %s\n", "post_checksum", database_checksum->routine);
  if ( !database_checksum->skip_event && database_checksum->event)
    fprintf(mdfile, "%s = %s\n", "events_checksum", database_checksum->event);
  if ( !database_checksum->skip_trigger && database_checksum->trigger)
    fprintf(mdfile, "%s = %s\n", "triggers_checksum", database_checksum->trigger);
}

static inline gboolean
emit_checksum_event(GLogLevelFlags level, const char *status, const char *scope,
                    const char *checksum_type, const char *_db, const char *_table,
                    const char *expected_checksum, const char *actual_checksum)
{
  if (!machine_log_json_enabled()) {
    return TRUE;
  }

  machine_log_event(G_LOG_DOMAIN, level,
                    "MESSAGE", "checksum verification",
                    "EVENT", "checksum_check",
                    "PHASE", "checksum",
                    "STATUS", status,
                    "SCOPE", scope,
                    "CHECKSUM_TYPE", checksum_type,
                    "DB", _db != NULL ? _db : "",
                    "TABLE", _table != NULL ? _table : "",
                    "EXPECTED_CHECKSUM", expected_checksum != NULL ? expected_checksum : "",
                    "ACTUAL_CHECKSUM", actual_checksum != NULL ? actual_checksum : "",
                    "MODE", checksum_mode == CHECKSUM_FAIL ? "fail" :
                            checksum_mode == CHECKSUM_WARN ? "warn" : "skip",
                    "RETRYABLE", "false",
                    "FATAL", level == G_LOG_LEVEL_CRITICAL ? "true" : "false",
                    NULL);
  return TRUE;
}

static inline gboolean
checksum_template(const char *dbt_checksum, const char *checksum, const char *err_templ,
                  const char *info_templ, const char *message, const char *_db, const char *_table,
                  const char *scope, const char *checksum_type)
{
  g_assert(checksum_mode != CHECKSUM_SKIP);
  emit_checksum_event(G_LOG_LEVEL_MESSAGE, "started", scope, checksum_type, _db, _table,
                      dbt_checksum, checksum);
  if (dbt_checksum && checksum && g_ascii_strcasecmp(dbt_checksum, checksum)) {
    emit_checksum_event(checksum_mode == CHECKSUM_WARN ? G_LOG_LEVEL_WARNING : G_LOG_LEVEL_CRITICAL,
                        "failed", scope, checksum_type, _db, _table, dbt_checksum, checksum);
    if (_table) {
      if (checksum_mode == CHECKSUM_WARN)
        g_warning(err_templ, message, _db, _table, checksum, dbt_checksum);
      else
        g_critical(err_templ, message, _db, _table, checksum, dbt_checksum);
    } else {
      if (checksum_mode == CHECKSUM_WARN)
        g_warning(err_templ, message, _db, checksum, dbt_checksum);
      else
        g_critical(err_templ, message, _db, checksum, dbt_checksum);
    }
    return FALSE;
  } else {
    emit_checksum_event(G_LOG_LEVEL_MESSAGE, "finished", scope, checksum_type, _db, _table,
                        dbt_checksum, checksum);
    g_message(info_templ, message, _db, _table);
  }
  return TRUE;
}

static
gboolean checksum_database_template(gchar *target_database, gchar *source_checksum,  MYSQL *conn,
                                const gchar *message, const gchar *checksum_type,
                                gchar* fun(MYSQL *,gchar *,gchar *))
{
  const char *target_checksum= fun(conn, target_database, NULL);
  return checksum_template(source_checksum, target_checksum,
                    "%s mismatch found for %s: got %s, expecting %s",
                    "%s confirmed for %s", message, target_database, NULL,
                    "database", checksum_type);
}

gboolean checksum_database(gchar *target_database, struct database_level_checksum *database_checksum, MYSQL *conn){
  gboolean checksum_ok=TRUE;
  if ( !database_checksum->skip_schema && database_checksum->schema)
    checksum_ok&=checksum_database_template(target_database, database_checksum->schema, conn,
                              "Schema create checksum", "database_schema", checksum_database_defaults);
  if ( !database_checksum->skip_routine && database_checksum->routine)
    checksum_ok&=checksum_database_template(target_database, database_checksum->routine, conn,
                              "Post checksum", "database_post", checksum_process_structure);
  if ( !database_checksum->skip_event && database_checksum->event)
    checksum_ok&=checksum_database_template(target_database, database_checksum->event, conn,
                              "Events checksum", "database_events", checksum_events_structure_from_database);
  if ( !database_checksum->skip_trigger && database_checksum->trigger)
    checksum_ok&=checksum_database_template(target_database, database_checksum->trigger, conn,
                              "Triggers checksum", "database_triggers", checksum_trigger_structure_from_database);
  return checksum_ok;
}


static
gboolean checksum_dbt_template(gchar *target_database, gchar *source_table_name, gchar *dbt_checksum,  MYSQL *conn,
                           const gchar *message, const gchar *checksum_type,
                           gchar* fun(MYSQL *,gchar *,gchar *))
{
  trace("checksum_dbt_template:: %s %s", target_database, source_table_name);
  const char *checksum= fun(conn, target_database, source_table_name);
  trace("checksum_dbt_template:: function executed: %s %s", target_database, source_table_name);
  return checksum_template(dbt_checksum, checksum,
                    "%s mismatch found for %s.%s: got %s, expecting %s",
                    "%s confirmed for %s.%s", message, target_database, source_table_name,
                    "table", checksum_type);
}

gboolean checksum_dbt(gchar *target_database, gchar *source_table_name, gboolean is_view, struct table_level_checksum *table_checksum,  MYSQL *conn)
{
  gboolean checksum_ok=TRUE;
  if (checksum_mode != CHECKSUM_SKIP){
    if ( !table_checksum->skip_schema && table_checksum->schema){
      if (is_view)
        checksum_ok&=checksum_dbt_template(target_database, source_table_name, table_checksum->schema, conn,
                              "View checksum", "view_schema", checksum_view_structure);
      else
        checksum_ok&=checksum_dbt_template(target_database, source_table_name, table_checksum->schema, conn,
                              "Structure checksum", "table_schema", checksum_table_structure);
    }
    if ( !table_checksum->skip_index && table_checksum->index)
      checksum_ok&=checksum_dbt_template(target_database, source_table_name, table_checksum->index, conn,
                            "Schema index checksum", "table_indexes", checksum_table_indexes);
    
    if ( !table_checksum->skip_trigger && table_checksum->trigger)
      checksum_ok&=checksum_dbt_template(target_database, source_table_name, table_checksum->trigger, conn,
                            "Trigger checksum", "table_triggers", checksum_trigger_structure);

    if ( !table_checksum->skip_data && table_checksum->data)
      checksum_ok&=checksum_dbt_template(target_database, source_table_name, table_checksum->data, conn,
                            "Data checksum", "table_data", checksum_table);
  }
  return checksum_ok;
}

void print_checksum_help(){
  print_bool("checksum-all",dump_checksums);
  print_bool("data-checksums",data_checksums);
  print_bool("schema-checksums",schema_checksums);
  print_bool("no-schema-checksums",!schema_checksums);
  print_bool("routine-checksums",routine_checksums);
  print_bool("skip-database-checksums", skip_database_checksums);
  print_bool("skip-data-checksums", skip_data_checksums);
  print_bool("skip-table-checksums", skip_table_checksums);
  print_bool("skip-index-checksums", skip_index_checksums);
  print_bool("skip-view-checksums", skip_view_checksums);
  print_bool("skip-trigger-checksums", skip_trigger_checksums);
  print_bool("skip-routine-checksums", skip_routine_checksums);
  print_bool("skip-event-checksums", skip_event_checksums);
}


