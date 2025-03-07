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

        Authors:    Domas Mituzas, Facebook ( domas at fb dot com )
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
                    David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <errno.h>
#include <gio/gio.h>

#include "mydumper_start_dump.h"
#include "mydumper_common.h"
#include "mydumper_jobs.h"
#include "mydumper_database.h"
#include "mydumper_write.h"
#include "mydumper_global.h"

/* Program options */
gboolean dump_triggers = FALSE;
gboolean ignore_generated_fields = FALSE;
gchar *exec_per_thread = NULL;
const gchar *exec_per_thread_extension = NULL;
gchar **exec_per_thread_cmd=NULL;
gboolean skip_definer = FALSE;
gboolean success_on_1146 = FALSE;

extern gchar *table_engine_for_view_dependency;

// Shared variables
int (*m_open)(char **filename, const char *);

void initialize_jobs(){
  initialize_database();
  if (ignore_generated_fields)
    g_warning("Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns");

  if (exec_per_thread_extension != NULL && strlen(exec_per_thread_extension)>0){
    if(exec_per_thread == NULL)
      m_error("--exec-per-thread needs to be set when --exec-per-thread-extension (%s) is used", exec_per_thread_extension);
  }

  if (exec_per_thread!=NULL){
    if (exec_per_thread[0]!='/'){
      m_error("Absolute path is only allowed when --exec-per-thread is used");
    }
    exec_per_thread_cmd=g_strsplit(exec_per_thread, " ", 0);
  }
}

static
gchar * write_checksum_into_file(MYSQL *conn, struct database *database, char *table, gchar *fun(MYSQL *,gchar *,gchar *,int*)) {
  int errn=0;
  gchar *checksum=fun(conn, database->name, table, &errn);
//  g_message("Checksum value: %s", checksum);
  if (errn != 0 && !(success_on_1146 && errn == 1146)) {
    errors++;
    return NULL;
  }
  if (checksum == NULL)
    checksum = g_strdup("0");
  return checksum;
}

static
gchar * get_tablespace_query(){
  if ( server_support_tablespaces()){
    if ( get_major() == 5 && get_secondary() == 7)
      return g_strdup("select NAME, PATH, FS_BLOCK_SIZE from information_schema.INNODB_SYS_TABLESPACES join information_schema.INNODB_SYS_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';");
    if ( get_major() == 8 )
      return g_strdup("select NAME,PATH,FS_BLOCK_SIZE,ENCRYPTION from information_schema.INNODB_TABLESPACES join information_schema.INNODB_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';");
  }
  return NULL;
}

static
void write_tablespace_definition_into_file(MYSQL *conn,char *filename){
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  int outfile = m_open(&filename,"w");
  if (!outfile) {
    g_critical("Error: Could not create output file %s (%d)",
               filename, errno);
    errors++;
    return;
  }
  query=get_tablespace_query();
  if (query == NULL ){
    g_warning("Tablespace resquested, but not possible due to server version not supported");
    return;
  }
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping create tablespace: %s",
                mysql_error(conn));
    } else {
      g_critical("Error dumping create tablespace: %s",
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }

  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);

  while ((row = mysql_fetch_row(result))) {
    g_string_append_printf(statement, "CREATE TABLESPACE %c%s%c ADD DATAFILE '%s' FILE_BLOCK_SIZE = %s ENGINE=INNODB;\n", q, row[0], q, row[1], row[2]);
    if (!write_data(outfile, statement)) {
      g_critical("Could not write tablespace data for %s", row[0]);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
  }
}

static
void write_schema_definition_into_file(MYSQL *conn, struct database *database, char *filename) {
  int outfile=0;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;

  outfile = m_open(&filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database->name,
               filename, errno);
    errors++;
    return;
  }

  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);

  query = g_strdup_printf("SHOW CREATE DATABASE IF NOT EXISTS %c%s%c", q, database->name, q);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping create database (%s): %s", database->name,
                mysql_error(conn));
    } else {
      g_critical("Error dumping create database (%s): %s", database->name,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  if (!strstr(row[1], identifier_quote_character_str)) {
    g_critical("Identifier quote [%s] not found when fetching %s",
               identifier_quote_character_str, database->name);
    errors++;
  }
  g_string_append(statement, row[1]);
  g_string_append(statement, ";\n");
  if (!write_data(outfile, statement)) {
    g_critical("Could not write create database for %s", database->name);
    errors++;
  }
  g_free(query);
  m_close(0, outfile, filename, 1, NULL);
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);


  if (schema_checksums)
    database->schema_checksum = write_checksum_into_file(conn, database, NULL, checksum_database_defaults);
  return;
}

static
void write_table_definition_into_file(MYSQL *conn, struct db_table *dbt,
                      char *filename, gboolean checksum_filename, gboolean checksum_index_filename) {
  int outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  outfile = m_open(&filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", dbt->database->name,
               filename, errno);
    errors++;
    return;
  }

  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);

  if (!write_data(outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  query = g_strdup_printf("SHOW CREATE TABLE %c%s%c.%c%s%c", q, dbt->database->name, q, q, dbt->table, q);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }

  g_string_set_size(statement, 0);

  /* There should never be more than one row */
  row = mysql_fetch_row(result);

  char *create_table;
  if (schema_sequence_fix) {
    create_table = filter_sequence_schemas(row[1]);
  } else {
    create_table = row[1];
  }

  g_string_append(statement, create_table);
  if (schema_sequence_fix) {
    g_free(create_table);
  }
  g_string_append(statement, ";\n");

  if (skip_indexes || skip_constraints){
    GString *alter_table_statement=g_string_sized_new(statement_size);
    GString *alter_table_constraint_statement=g_string_sized_new(statement_size);
    GString *create_table_statement=g_string_sized_new(statement_size);
    global_process_create_table_statement(statement->str, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt->table, TRUE);
    if (!write_data(outfile, create_table_statement)) {
      g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
      errors++;
    }
    if (!skip_indexes)
      write_data(outfile, alter_table_statement );
    if (!skip_constraints)
      write_data(outfile, alter_table_constraint_statement);
  }else{
    if (!write_data(outfile, statement)) {
      g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
      errors++;
    }
  }
  g_free(query);
  m_close(0, outfile, filename, 1, dbt);
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  if (checksum_filename){
    dbt->schema_checksum=write_checksum_into_file(conn, dbt->database, dbt->table, checksum_table_structure);
//    g_message("Checksum for table schema: %s", dbt->schema_checksum);
  }
  if (checksum_index_filename){
    dbt->indexes_checksum=write_checksum_into_file(conn, dbt->database, dbt->table, checksum_table_indexes);
  }
  return;
}

static
void write_triggers_definition_into_file(MYSQL *conn, MYSQL_RES *result, struct database *database, gchar *message, int outfile) {
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row2;
  MYSQL_ROW row;
  gchar *query = NULL;
  gchar **splited_st = NULL;
  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);

  if (!write_data(outfile, statement)) {
    g_critical("Could not write triggers for %s", message);
    errors++;
    return;
  }

  while ((row = mysql_fetch_row(result))) {
    set_charset(statement, row[8], row[9]);
    if (!write_data(outfile, statement)) {
      g_critical("Could not write triggers data for %s", message);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
    query = g_strdup_printf("SHOW CREATE TRIGGER %c%s%c.%c%s%c", q, database->name, q, q, row[0], q);
    mysql_query(conn, query);
    result2 = mysql_store_result(conn);
    row2 = mysql_fetch_row(result2);
    if ( skip_definer && g_str_has_prefix(row2[2],"CREATE")){
      remove_definer_from_gchar(row2[2]);
    }
    g_string_append_printf(statement, "%s", row2[2]);
    splited_st = g_strsplit(statement->str, ";\n", 0);
    g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
    g_strfreev(splited_st);
    g_string_append(statement, ";\n");
    restore_charset(statement);
    if (!write_data(outfile, statement)) {
      g_critical("Could not write triggers data for %s", message);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
  }
  return;
}

static
void write_triggers_definition_into_file_from_dbt(MYSQL *conn, struct db_table *dbt, char *filename, gboolean checksum_filename) {
  int outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;

  outfile = m_open(&filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", dbt->database->name,
               filename, errno);
    errors++;
    return;
  }

  // get triggers
  const char q= identifier_quote_character;
  query = g_strdup_printf("SHOW TRIGGERS FROM %c%s%c WHERE %cTable%c = '%s'", q, dbt->database->name, q, q, q, dbt->table);

  if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping triggers (%s.%s): %s", dbt->database->name, dbt->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping triggers (%s.%s): %s", dbt->database->name, dbt->table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_free(query);

  gchar *message=g_strdup_printf("%s.%s",dbt->database->name, dbt->table);
  write_triggers_definition_into_file(conn, result, dbt->database, message, outfile);
  g_free(message);

  m_close(0, outfile, filename, 1, dbt);
  if (result)
    mysql_free_result(result);
  if (checksum_filename)
    dbt->triggers_checksum=write_checksum_into_file(conn, dbt->database, dbt->table, checksum_trigger_structure);
  return;
}

void write_triggers_definition_into_file_from_database(MYSQL *conn, struct database *database, char *filename, gboolean checksum_filename) {
  int outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;

  outfile = m_open(&filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database->name,
               filename, errno);
    errors++;
    return;
  }

  // get triggers
  const char q= identifier_quote_character;
  query = g_strdup_printf("SHOW TRIGGERS FROM %c%s%c", q, database->name, q);
  if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping triggers (%s): %s", database->name,
                mysql_error(conn));
    } else {
      g_critical("Error dumping triggers (%s): %s", database->name,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_free(query);

  write_triggers_definition_into_file(conn, result, database, database->name, outfile);

  m_close(0, outfile, filename, 1, NULL);
  if (result)
    mysql_free_result(result);
  if (checksum_filename)
    database->triggers_checksum=write_checksum_into_file(conn, database, NULL, checksum_trigger_structure_from_database);
  return;
}

static
void write_view_definition_into_file(MYSQL *conn, struct db_table *dbt, char *filename, char *filename2, gboolean checksum_filename) {
  int outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);

  if (mysql_select_db(conn, dbt->database->name)) {
    g_critical("Could not select database: %s (%s)", dbt->database->name,
              mysql_error(conn));
    errors++;
    return;
  }

  outfile = m_open(&filename,"w");


  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file (%d)", dbt->database->name,
               errno);
    errors++;
    return;
  }

  if (!write_data(outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  // we create tables as workaround
  // for view dependencies
  query = g_strdup_printf("SHOW FIELDS FROM %c%s%c.%c%s%c", q, dbt->database->name, q, q, dbt->table, q);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_free(query);
  g_string_set_size(statement, 0);
  g_string_append_printf(statement, "CREATE TABLE IF NOT EXISTS %c%s%c(\n", q, dbt->table, q);
  row = mysql_fetch_row(result);
  g_string_append_printf(statement, "%c%s%c int", q, row[0], q);
  while ((row = mysql_fetch_row(result))) {
    g_string_append(statement, ",\n");
    g_string_append_printf(statement, "%c%s%c int", q, row[0], q);
  }
  g_string_append(statement, "\n) ENGINE=");
  g_string_append(statement, table_engine_for_view_dependency);
  if (get_product() == SERVER_TYPE_PERCONA || get_product() == SERVER_TYPE_MYSQL || get_product() == SERVER_TYPE_DOLT)
    g_string_append(statement," ENCRYPTION='N'");
  g_string_append(statement,";\n");

  if (result)
    mysql_free_result(result);

  if (!write_data(outfile, statement)) {
    g_critical("Could not write view schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
  }

  // real view
  query = g_strdup_printf("SHOW CREATE VIEW %c%s%c.%c%s%c", q, dbt->database->name, q, q, dbt->table, q);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }

  m_close(0, outfile, filename, 1, dbt);
  g_string_set_size(statement, 0);

  int outfile2;
  outfile2 = m_open(&filename2,"w");
  if (!outfile2) {
    g_critical("Error: DB: %s Could not create output file (%d)", dbt->database->name,
               errno);
    errors++;
    return;
  }

  initialize_sql_statement(statement);
  g_string_append_printf(statement, "DROP TABLE IF EXISTS %c%s%c;\n", q, dbt->table, q);
  g_string_append_printf(statement, "DROP VIEW IF EXISTS %c%s%c;\n", q, dbt->table, q);

  if (!write_data(outfile2, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  g_string_set_size(statement, 0);

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  set_charset(statement, row[2], row[3]);
  if ( skip_definer && g_str_has_prefix(row[1],"CREATE")){
    remove_definer_from_gchar(row[1]);
  }
  g_string_append(statement, row[1]);
  g_string_append(statement, ";\n");
  restore_charset(statement);
  if (!write_data(outfile2, statement)) {
    g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
  }
  g_free(query);

  m_close(0, outfile2, filename2, 1, dbt);
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  if (checksum_filename)
    // build_meta_filename(database,table,"schema-view-checksum"),
    dbt->schema_checksum=write_checksum_into_file(conn, dbt->database, dbt->table, checksum_view_structure);
  return;
}

static
void write_sequence_definition_into_file(MYSQL *conn, struct db_table *dbt, char *filename, gboolean checksum_filename) {
  int outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);

  mysql_select_db(conn, dbt->database->name);

  outfile = m_open(&filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file (%d)", dbt->database->name,
               errno);
    errors++;
    return;
  }

  // DROP TABLE works for sequences
  g_string_append_printf(statement, "DROP TABLE IF EXISTS %c%s%c;\n", q, dbt->table, q);
  g_string_append_printf(statement, "DROP VIEW IF EXISTS %c%s%c;\n", q, dbt->table, q);

  if (!write_data(outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  query = g_strdup_printf("SHOW CREATE SEQUENCE %c%s%c.%c%s%c", q, dbt->database->name, q, q, dbt->table, q);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_string_set_size(statement, 0);

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  if ( skip_definer && g_str_has_prefix(row[1],"CREATE")){
    remove_definer_from_gchar(row[1]);
  }
  g_string_append(statement, row[1]);
  g_string_append(statement, ";\n");
  if (!write_data(outfile, statement)) {
    g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
  }
  g_free(query);
  if (result) {
    mysql_free_result(result);
    result = NULL;
  }

  // Get current sequence position
  query = g_strdup_printf("SELECT next_not_cached_value FROM %c%s%c.%c%s%c", q, dbt->database->name, q, q, dbt->table, q);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", dbt->database->name, dbt->table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_string_set_size(statement, 0);
  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  g_string_printf(statement, "DO SETVAL(%c%s%c, %s, 0);\n", q, dbt->table, q, row[0]);
  if (!write_data(outfile, statement)) {
    g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
  }

  g_free(query);
  m_close(0, outfile, filename, 1, dbt);
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  // Table checksum should cover the basics, but doesn't checksum the current sequence position
  if (checksum_filename)
    write_checksum_into_file(conn, dbt->database, dbt->table, checksum_table_structure);
  return;
}

// Routines, Functions and Events
// TODO: We need to split it in 3 functions 
static
void write_routines_definition_into_file(MYSQL *conn, struct database *database, char *filename, gboolean checksum_filename) {
  int outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row;
  MYSQL_ROW row2;
  gchar **splited_st = NULL;

  outfile = m_open(&filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database->name,
               filename, errno);
    errors++;
    return;
  }

  const char q= identifier_quote_character;
  GString *statement = g_string_sized_new(statement_size);
  initialize_sql_statement(statement);


  if (!write_data(outfile, statement)) {
    g_critical("Could not write %s", filename);
    errors++;
    return;
  }
  guint charcol=0,collcol=0;
  if (dump_routines) {
    g_assert(nroutines > 0);
    for (guint r= 0; r < nroutines; r++) {
      query= g_strdup_printf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", routine_type[r], database->escaped);
      if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
        if (success_on_1146 && mysql_errno(conn) == 1146) {
          g_warning("Error dumping %s from %s: %s", routine_type[r], database->name,
                    mysql_error(conn));
        } else {
          g_critical("Error dumping %s from %s: %s", routine_type[r], database->name,
                    mysql_error(conn));
          errors++;
        }
        g_free(query);
        return;
      }
      determine_charset_and_coll_columns_from_show(result, &charcol, &collcol);

      while ((row= mysql_fetch_row(result))) {
        set_charset(statement, row[charcol], row[collcol]);
        g_string_append_printf(statement, "DROP %s IF EXISTS %c%s%c;\n", routine_type[r], q, row[1], q);
        if (!write_data(outfile, statement)) {
          g_critical("Could not write %s data for %s.%s", routine_type[r], database->name,
                    row[1]);
          errors++;
          mysql_free_result(result);
          return;
        }
        g_string_set_size(statement, 0);
        query= g_strdup_printf("SHOW CREATE %s %c%s%c.%c%s%c", routine_type[r], q, database->name, q,  q, row[1], q);
        mysql_query(conn, query);
        result2= mysql_store_result(conn);
        row2= mysql_fetch_row(result2);
        g_string_printf(statement, "%s", row2[2]);
        if (skip_definer && g_str_has_prefix(statement->str, "CREATE")) {
          remove_definer(statement);
        }
        splited_st= g_strsplit(statement->str, ";\n", 0);
        g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
        g_string_append(statement, ";\n");
        restore_charset(statement);
        if (!write_data(outfile, statement)) {
          g_critical("Could not write %s data for %s.%s", routine_type[r], database->name, row[1]);
          errors++;
          mysql_free_result(result);
          if (result2)
            mysql_free_result(result2);
          return;
        }
        g_string_set_size(statement, 0);
      }

      mysql_free_result(result);
      if (result2) {
        mysql_free_result(result2);
        result2= NULL;
      }
    } // for (guint r= 0; r < nroutines; r++)

    if (checksum_filename)
     database->post_checksum=write_checksum_into_file(conn, database, NULL, checksum_process_structure);
  } // if (dump_routines)

  // get events
  if (dump_events) {
    query = g_strdup_printf("SHOW EVENTS FROM %c%s%c", q, database->name, q);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      if (success_on_1146 && mysql_errno(conn) == 1146) {
        g_warning("Error dumping events from %s: %s", database->name,
                  mysql_error(conn));
      } else {
        g_critical("Error dumping events from %s: %s", database->name,
                   mysql_error(conn));
        errors++;
      }
      g_free(query);
      return;
    }
    determine_charset_and_coll_columns_from_show(result, &charcol, &collcol);
    while ((row = mysql_fetch_row(result))) {
      set_charset(statement, row[charcol], row[collcol]);
      g_string_append_printf(statement, "DROP EVENT IF EXISTS %c%s%c;\n", q, row[1], q);
      if (!write_data(outfile, statement)) {
        g_critical("Could not write stored procedure data for %s.%s", database->name,
                   row[1]);
        errors++;
        mysql_free_result(result);
        return;
      }
      query = g_strdup_printf("SHOW CREATE EVENT %c%s%c.%c%s%c", q, database->name, q, q, row[1], q);
      mysql_query(conn, query);
      result2 = mysql_store_result(conn);
      // DROP EVENT IF EXISTS event_name
      row2 = mysql_fetch_row(result2);
      g_string_printf(statement, "%s", row2[3]);
      if ( skip_definer && g_str_has_prefix(statement->str,"CREATE")){
        remove_definer(statement);
      }
      splited_st = g_strsplit(statement->str, ";\n", 0);
      g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
      g_string_append(statement, ";\n");
      restore_charset(statement);
      if (!write_data(outfile, statement)) {
        g_critical("Could not write event data for %s.%s", database->name, row[1]);
        errors++;
        mysql_free_result(result);
        if (result2)
          mysql_free_result(result2);
        return;
      }
      g_string_set_size(statement, 0);
    }
    mysql_free_result(result);
    if (result2)
      mysql_free_result(result2);
  }

  g_free(query);
  m_close(0, outfile, filename, 1, NULL);
  g_string_free(statement, TRUE);
  g_strfreev(splited_st);

  return;
}

void free_schema_job(struct schema_job *sj){
  if (sj->filename){
    g_free(sj->filename);
    sj->filename=NULL;
  }
  g_free(sj);
}

void free_view_job(struct view_job *vj){
  if (vj->tmp_table_filename)
    g_free(vj->tmp_table_filename);
  if (vj->view_filename)
    g_free(vj->view_filename);
//  g_free(vj);
}

void free_create_tablespace_job(struct create_tablespace_job * ctj){
  if (ctj->filename)
    g_free(ctj->filename);
//  g_free(cdj);
}

void free_database_job(struct database_job * dj){
  if (dj->filename)
    g_free(dj->filename);
  g_free(dj);
}

void free_table_checksum_job(struct table_checksum_job*tcj){
      if (tcj->filename)
        g_free(tcj->filename);
      g_free(tcj);
}

void do_JOB_CREATE_DATABASE(struct thread_data *td, struct job *job){
  struct database_job * tj = (struct database_job *)job->job_data;
  g_message("Thread %d: dumping schema create for %s%s%s", td->thread_id,
            identifier_quote_character_str, masquerade_filename?tj->database->filename:tj->database->name, identifier_quote_character_str);
  write_schema_definition_into_file(td->thrconn, tj->database, tj->filename);
  free_database_job(tj);
  g_free(job);
}

void do_JOB_CREATE_TABLESPACE(struct thread_data *td, struct job *job){
  struct create_tablespace_job * ctj = (struct create_tablespace_job *)job->job_data;
  g_message("Thread %d: dumping create tablespace if any", td->thread_id);
  write_tablespace_definition_into_file(td->thrconn, ctj->filename);
  free_create_tablespace_job(ctj);
  g_free(job);
}

void do_JOB_SCHEMA_POST(struct thread_data *td, struct job *job){
  struct database_job * tj = (struct database_job *)job->job_data;
  g_message("Thread %d: dumping SP and VIEWs for %s%s%s", td->thread_id,
            identifier_quote_character_str, masquerade_filename?tj->database->filename:tj->database->name, identifier_quote_character_str);
  write_routines_definition_into_file(td->thrconn, tj->database, tj->filename, tj->checksum_filename);
  free_database_job(tj);
  g_free(job);
}


void do_JOB_SCHEMA_TRIGGERS(struct thread_data *td, struct job *job){
  struct database_job * tj = (struct database_job *)job->job_data;
  g_message("Thread %d: dumping triggers for %s%s%s", td->thread_id,
            identifier_quote_character_str, masquerade_filename?tj->database->filename:tj->database->name, identifier_quote_character_str);
  write_triggers_definition_into_file_from_database(td->thrconn, tj->database, tj->filename, tj->checksum_filename);
  free_database_job(tj);
  g_free(job);
}

void do_JOB_VIEW(struct thread_data *td, struct job *job){
  struct view_job * tj = (struct view_job *)job->job_data;
  g_message("Thread %d: dumping view for %s%s%s.%s%s%s", td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->filename:tj->dbt->database->name, identifier_quote_character_str,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str);

  write_view_definition_into_file(td->thrconn, tj->dbt, tj->tmp_table_filename,
                 tj->view_filename, tj->checksum_filename);
  g_free(job);
}

void do_JOB_SEQUENCE(struct thread_data *td, struct job *job){
  struct sequence_job * tj = (struct sequence_job *)job->job_data;
  g_message("Thread %d dumping sequence for %s%s%s.%s%s%s", td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->filename:tj->dbt->database->name, identifier_quote_character_str,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str);
  write_sequence_definition_into_file(td->thrconn, tj->dbt, tj->filename,
                 tj->checksum_filename);
//  free_sequence_job(sj);
  g_free(job);
}

void do_JOB_SCHEMA(struct thread_data *td, struct job *job){
  struct schema_job *tj = (struct schema_job *)job->job_data;
  g_message("Thread %d: dumping schema for %s%s%s.%s%s%s", td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->filename:tj->dbt->database->name, identifier_quote_character_str,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str);
  write_table_definition_into_file(td->thrconn, tj->dbt, tj->filename, tj->checksum_filename, tj->checksum_index_filename);
  free_schema_job(tj);
  g_free(job);
}

void do_JOB_TRIGGERS(struct thread_data *td, struct job *job){
  struct schema_job * tj = (struct schema_job *)job->job_data;
  g_message("Thread %d: dumping triggers for %s%s%s.%s%s%s", td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->filename:tj->dbt->database->name, identifier_quote_character_str,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str);
  write_triggers_definition_into_file_from_dbt(td->thrconn, tj->dbt, tj->filename, tj->checksum_filename);
  free_schema_job(tj);
  g_free(job);
}

void do_JOB_CHECKSUM(struct thread_data *td, struct job *job){
  struct table_checksum_job *tj = (struct table_checksum_job *)job->job_data;
  g_message("Thread %d: dumping checksum for %s%s%s.%s%s%s", td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->filename:tj->dbt->database->name, identifier_quote_character_str,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str);
  if (use_savepoints && mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
    g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
  }
  tj->dbt->data_checksum=write_checksum_into_file(td->thrconn, tj->dbt->database, tj->dbt->table, checksum_table);
  if (use_savepoints &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }
  free_table_checksum_job(tj);
  g_free(job);
}

