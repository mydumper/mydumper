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
#include <mysql.h>
#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <pcre.h>
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "common.h"
#include "regex.h"
#include "mydumper_common.h"
#include "mydumper_jobs.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_write.h"
#include "mydumper_chunks.h"
#include "mydumper_global.h"
#include <sys/wait.h>

extern int (*m_close)(guint thread_id, void *file, gchar *filename, guint size, struct db_table * dbt);

gboolean dump_triggers = FALSE;
gboolean order_by_primary_key = FALSE;
gboolean ignore_generated_fields = FALSE;
gchar *exec_per_thread = NULL;
const gchar *exec_per_thread_extension = NULL;
gchar **exec_per_thread_cmd=NULL;
gboolean skip_definer = FALSE;

void initialize_jobs(){
  initialize_database();
  if (ignore_generated_fields)
    g_warning("Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns");

  if (exec_per_thread_extension != NULL && strlen(exec_per_thread_extension)>0){
    if(exec_per_thread == NULL)
      m_error("--exec-per-thread needs to be set when --exec-per-thread-extension (%s) is used", exec_per_thread_extension);
  }

  if (exec_per_thread!=NULL){
    exec_per_thread_cmd=g_strsplit(exec_per_thread, " ", 0);
  }
}

gchar * write_checksum_into_file(MYSQL *conn, struct database *database, char *table, gchar *fun()) {
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

gchar * get_tablespace_query(){
  if ( get_product() == SERVER_TYPE_PERCONA || get_product() == SERVER_TYPE_MYSQL ){
    if ( get_major() == 5 && get_secondary() == 7)
      return g_strdup("select NAME, PATH, FS_BLOCK_SIZE from information_schema.INNODB_SYS_TABLESPACES join information_schema.INNODB_SYS_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';");
    if ( get_major() == 8 )
      return g_strdup("select NAME,PATH,FS_BLOCK_SIZE,ENCRYPTION from information_schema.INNODB_TABLESPACES join information_schema.INNODB_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';");
  }
  return NULL;
}

void write_tablespace_definition_into_file(MYSQL *conn,char *filename){
  void *outfile = NULL;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  outfile = m_open(filename,"w");
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
  GString *statement = g_string_sized_new(statement_size);
  while ((row = mysql_fetch_row(result))) {
    g_string_printf(statement, "CREATE TABLESPACE `%s` ADD DATAFILE '%s' FILE_BLOCK_SIZE = %s ENGINE=INNODB;\n", row[0],row[1],row[2]);
    if (!write_data((FILE *)outfile, statement)) {
      g_critical("Could not write tablespace data for %s", row[0]);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
  }
}

void write_schema_definition_into_file(MYSQL *conn, struct database *database, char *filename) {
  void *outfile = NULL;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database->name,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  query = g_strdup_printf("SHOW CREATE DATABASE IF NOT EXISTS `%s`", database->name);
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
  g_string_append(statement, row[1]);
  g_string_append(statement, ";\n");
  if (!write_data((FILE *)outfile, statement)) {
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

void write_table_definition_into_file(MYSQL *conn, struct db_table *dbt,
                      char *filename, gboolean checksum_filename, gboolean checksum_index_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", dbt->database->name,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  initialize_sql_statement(statement);

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  query = g_strdup_printf("SHOW CREATE TABLE `%s`.`%s`", dbt->database->name, dbt->table);
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
  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
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

void write_triggers_definition_into_file(MYSQL *conn, MYSQL_RES *result, struct database *database, gchar *message, FILE *outfile) {
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row2;
  MYSQL_ROW row;
  gchar *query = NULL;
  gchar **splited_st = NULL;
  GString *statement = g_string_sized_new(statement_size);
  while ((row = mysql_fetch_row(result))) {
    set_charset(statement, row[8], row[9]);
    if (!write_data((FILE *)outfile, statement)) {
      g_critical("Could not write triggers data for %s", message);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
    query = g_strdup_printf("SHOW CREATE TRIGGER `%s`.`%s`", database->name, row[0]);
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
    if (!write_data((FILE *)outfile, statement)) {
      g_critical("Could not write triggers data for %s", message);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
  }
  return;
}

void write_triggers_definition_into_file_from_dbt(MYSQL *conn, struct db_table *dbt, char *filename, gboolean checksum_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", dbt->database->name,
               filename, errno);
    errors++;
    return;
  }

  // get triggers
  query = g_strdup_printf("SHOW TRIGGERS FROM `%s` LIKE '%s'", dbt->database->name, dbt->table);
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
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database->name,
               filename, errno);
    errors++;
    return;
  }

  // get triggers
  query = g_strdup_printf("SHOW TRIGGERS FROM `%s`", database->name);
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

void write_view_definition_into_file(MYSQL *conn, struct db_table *dbt, char *filename, char *filename2, gboolean checksum_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  GString *statement = g_string_sized_new(statement_size);

  mysql_select_db(conn, dbt->database->name);

  outfile = m_open(filename,"w");


  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file (%d)", dbt->database->name,
               errno);
    errors++;
    return;
  }

  if (is_mysql_like() && set_names_statement) {
    g_string_printf(statement,"%s;\n",set_names_statement);
  }

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  // we create tables as workaround
  // for view dependencies
  query = g_strdup_printf("SHOW FIELDS FROM `%s`.`%s`", dbt->database->name, dbt->table);
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
  g_string_append_printf(statement, "CREATE TABLE IF NOT EXISTS %c%s%c(\n", identifier_quote_character,dbt->table,identifier_quote_character);
  row = mysql_fetch_row(result);
  g_string_append_printf(statement, "%c%s%c int", identifier_quote_character,row[0],identifier_quote_character);
  while ((row = mysql_fetch_row(result))) {
    g_string_append(statement, ",\n");
    g_string_append_printf(statement, "%c%s%c int", identifier_quote_character,row[0],identifier_quote_character);
  }
  g_string_append(statement, "\n);\n");

  if (result)
    mysql_free_result(result);

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write view schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
  }

  // real view
  query = g_strdup_printf("SHOW CREATE VIEW `%s`.`%s`", dbt->database->name, dbt->table);
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

  void *outfile2;
  outfile2 = m_open(filename2,"w");
  if (!outfile2) {
    g_critical("Error: DB: %s Could not create output file (%d)", dbt->database->name,
               errno);
    errors++;
    return;
  }

  if (is_mysql_like() && set_names_statement) {
    g_string_printf(statement,"%s;\n",set_names_statement);
  }

  g_string_append_printf(statement, "DROP TABLE IF EXISTS `%s`;\n", dbt->table);
  g_string_append_printf(statement, "DROP VIEW IF EXISTS `%s`;\n", dbt->table);

  if (!write_data((FILE *)outfile2, statement)) {
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
  if (!write_data((FILE *)outfile2, statement)) {
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

void write_sequence_definition_into_file(MYSQL *conn, struct db_table *dbt, char *filename, gboolean checksum_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  GString *statement = g_string_sized_new(statement_size);

  mysql_select_db(conn, dbt->database->name);

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file (%d)", dbt->database->name,
               errno);
    errors++;
    return;
  }

  if (set_names_str) {
    g_string_printf(statement,"%s;\n",set_names_str);
  }

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  // DROP TABLE works for sequences
  g_string_append_printf(statement, "DROP TABLE IF EXISTS `%s`;\n", dbt->table);
  g_string_append_printf(statement, "DROP VIEW IF EXISTS `%s`;\n", dbt->table);

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", dbt->database->name, dbt->table);
    errors++;
    return;
  }

  query = g_strdup_printf("SHOW CREATE SEQUENCE `%s`.`%s`", dbt->database->name, dbt->table);
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
  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema for %s.%s", dbt->database->name, dbt->table);
    errors++;
  }
  g_free(query);
  if (result) {
    mysql_free_result(result);
    result = NULL;
  }

  // Get current sequence position
  query = g_strdup_printf("SELECT next_not_cached_value FROM `%s`.`%s`", dbt->database->name, dbt->table);
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
  g_string_printf(statement, "SELECT SETVAL(`%s`, %s, 0);\n", dbt->table, row[0]);
  if (!write_data((FILE *)outfile, statement)) {
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
void write_routines_definition_into_file(MYSQL *conn, struct database *database, char *filename, gboolean checksum_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row;
  MYSQL_ROW row2;
  gchar **splited_st = NULL;

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database->name,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  if (dump_routines) {
    // get functions
    query = g_strdup_printf("SHOW FUNCTION STATUS WHERE CAST(Db AS BINARY) = '%s'", database->escaped);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      if (success_on_1146 && mysql_errno(conn) == 1146) {
        g_warning("Error dumping functions from %s: %s", database->name,
                  mysql_error(conn));
      } else {
        g_critical("Error dumping functions from %s: %s", database->name,
                   mysql_error(conn));
        errors++;
      }
      g_free(query);
      return;
    }

    while ((row = mysql_fetch_row(result))) {
      set_charset(statement, row[8], row[9]);
      g_string_append_printf(statement, "DROP FUNCTION IF EXISTS `%s`;\n",
                             row[1]);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write stored procedure data for %s.%s", database->name,
                   row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
      query =
          g_strdup_printf("SHOW CREATE FUNCTION `%s`.`%s`", database->name, row[1]);
      mysql_query(conn, query);
      result2 = mysql_store_result(conn);
      row2 = mysql_fetch_row(result2);
      g_string_printf(statement, "%s", row2[2]);
      if ( skip_definer && g_str_has_prefix(statement->str,"CREATE")){
        remove_definer(statement);
      }
      splited_st = g_strsplit(statement->str, ";\n", 0);
      g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
      g_string_append(statement, ";\n");
      restore_charset(statement);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write function data for %s.%s", database->name, row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
    }

    // get sp
    query = g_strdup_printf("SHOW PROCEDURE STATUS WHERE CAST(Db AS BINARY) = '%s'", database->escaped);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      if (success_on_1146 && mysql_errno(conn) == 1146) {
        g_warning("Error dumping stored procedures from %s: %s", database->name,
                  mysql_error(conn));
      } else {
        g_critical("Error dumping stored procedures from %s: %s", database->name,
                   mysql_error(conn));
        errors++;
      }
      g_free(query);
      return;
    }

    while ((row = mysql_fetch_row(result))) {
      set_charset(statement, row[8], row[9]);
      g_string_append_printf(statement, "DROP PROCEDURE IF EXISTS `%s`;\n",
                             row[1]);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write stored procedure data for %s.%s", database->name,
                   row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
      query =
          g_strdup_printf("SHOW CREATE PROCEDURE `%s`.`%s`", database->name, row[1]);
      mysql_query(conn, query);
      result2 = mysql_store_result(conn);
      row2 = mysql_fetch_row(result2);
      g_string_printf(statement, "%s", row2[2]);
      if ( skip_definer && g_str_has_prefix(statement->str,"CREATE")){
        remove_definer(statement);
      }
      splited_st = g_strsplit(statement->str, ";\n", 0);
      g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
      g_string_append(statement, ";\n");
      restore_charset(statement);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write stored procedure data for %s.%s", database->name,
                   row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
    }
    if (checksum_filename)
     database->post_checksum=write_checksum_into_file(conn, database, NULL, checksum_process_structure);
  }

  // get events
  if (dump_events) {
    query = g_strdup_printf("SHOW EVENTS FROM `%s`", database->name);
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

    while ((row = mysql_fetch_row(result))) {
      set_charset(statement, row[12], row[13]);
      g_string_append_printf(statement, "DROP EVENT IF EXISTS `%s`;\n", row[1]);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write stored procedure data for %s.%s", database->name,
                   row[1]);
        errors++;
        return;
      }
      query = g_strdup_printf("SHOW CREATE EVENT `%s`.`%s`", database->name, row[1]);
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
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write event data for %s.%s", database->name, row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
    }
  }

  g_free(query);
  m_close(0, outfile, filename, 1, NULL);
  g_string_free(statement, TRUE);
  g_strfreev(splited_st);
  if (result)
    mysql_free_result(result);
  if (result2)
    mysql_free_result(result2);

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

//void free_sequence_job(struct sequence_job *sj){
//  g_free(sj);
//}

/*
void free_schema_post_job(struct schema_post_job *sp){
  if (sp->filename)
    g_free(sp->filename);
//  g_free(sp);
}
*/
/*
void free_create_database_job(struct create_database_job * cdj){
  if (cdj->filename)
    g_free(cdj->filename);
  g_free(cdj);
}
*/
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
  struct database_job * dj = (struct database_job *)job->job_data;
  g_message("Thread %d: dumping schema create for `%s`", td->thread_id,
            dj->database->name);
  write_schema_definition_into_file(td->thrconn, dj->database, dj->filename);
  free_database_job(dj);
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
  struct database_job * sp = (struct database_job *)job->job_data;
  g_message("Thread %d: dumping SP and VIEWs for `%s`", td->thread_id,
            sp->database->name);
  write_routines_definition_into_file(td->thrconn, sp->database, sp->filename, sp->checksum_filename);
  free_database_job(sp);
  g_free(job);
}


void do_JOB_SCHEMA_TRIGGERS(struct thread_data *td, struct job *job){
  struct database_job * sj = (struct database_job *)job->job_data;
  g_message("Thread %d: dumping triggers for `%s`", td->thread_id,
            sj->database->name);
  write_triggers_definition_into_file_from_database(td->thrconn, sj->database, sj->filename, sj->checksum_filename);
  free_database_job(sj);
  g_free(job);
}

void do_JOB_VIEW(struct thread_data *td, struct job *job){
  struct view_job * vj = (struct view_job *)job->job_data;
  g_message("Thread %d: dumping view for `%s`.`%s`", td->thread_id,
            vj->dbt->database->name, vj->dbt->table);
  write_view_definition_into_file(td->thrconn, vj->dbt, vj->tmp_table_filename,
                 vj->view_filename, vj->checksum_filename);
//  free_view_job(vj);
  g_free(job);
}

void do_JOB_SEQUENCE(struct thread_data *td, struct job *job){
  struct sequence_job * sj = (struct sequence_job *)job->job_data;
  g_message("Thread %d dumping sequence for `%s`.`%s`", td->thread_id,
            sj->dbt->database->name, sj->dbt->table);
  write_sequence_definition_into_file(td->thrconn, sj->dbt, sj->filename,
                 sj->checksum_filename);
//  free_sequence_job(sj);
  g_free(job);
}

void do_JOB_SCHEMA(struct thread_data *td, struct job *job){
  struct schema_job *sj = (struct schema_job *)job->job_data;
  g_message("Thread %d: dumping schema for `%s`.`%s`", td->thread_id,
            sj->dbt->database->name, sj->dbt->table);
  write_table_definition_into_file(td->thrconn, sj->dbt, sj->filename, sj->checksum_filename, sj->checksum_index_filename);
  free_schema_job(sj);
  g_free(job);
//  if (g_atomic_int_dec_and_test(&table_counter)) {
//    g_message("Unlocing ready_table_dump_mutex");
//    g_mutex_unlock(ready_table_dump_mutex);
//  }
}

void do_JOB_TRIGGERS(struct thread_data *td, struct job *job){
  struct schema_job * sj = (struct schema_job *)job->job_data;
  g_message("Thread %d: dumping triggers for `%s`.`%s`", td->thread_id,
            sj->dbt->database->name, sj->dbt->table);
  write_triggers_definition_into_file_from_dbt(td->thrconn, sj->dbt, sj->filename, sj->checksum_filename);
  free_schema_job(sj);
  g_free(job);
}

void do_JOB_CHECKSUM(struct thread_data *td, struct job *job){
  struct table_checksum_job *tcj = (struct table_checksum_job *)job->job_data;
  g_message("Thread %d: dumping checksum for `%s`.`%s`", td->thread_id,
            tcj->dbt->database->name, tcj->dbt->table);
  if (use_savepoints && mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
    g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
  }
  tcj->dbt->data_checksum=write_checksum_into_file(td->thrconn, tcj->dbt->database, tcj->dbt->table, checksum_table);
  if (use_savepoints &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }
  free_table_checksum_job(tcj);
  g_free(job);
}

void create_job_to_dump_metadata(struct configuration *conf, FILE *mdfile){
  struct job *j = g_new0(struct job, 1);
  j->job_data = (void *)mdfile;
  j->type = JOB_WRITE_MASTER_STATUS;
  g_async_queue_push(conf->initial_queue, j);
}

void create_job_to_dump_tablespaces(struct configuration *conf){
  struct job *j = g_new0(struct job, 1);
  struct create_tablespace_job *ctj = g_new0(struct create_tablespace_job, 1);
  j->job_data = (void *)ctj;
  j->type = JOB_CREATE_TABLESPACE;
  ctj->filename = build_tablespace_filename();
  g_async_queue_push(conf->schema_queue, j);
}

void create_database_related_job(struct database *database, struct configuration *conf, enum job_type type, const gchar *suffix) {
  struct job *j = g_new0(struct job, 1);
  struct database_job *dj = g_new0(struct database_job, 1);
  j->job_data = (void *)dj;
  dj->database = database;
  j->type = type;
  dj->filename = build_schema_filename(database->filename, suffix);
  dj->checksum_filename = schema_checksums;
  g_async_queue_push(conf->schema_queue, j);
  return;
}

void create_job_to_dump_schema(struct database *database, struct configuration *conf) {
  create_database_related_job(database, conf, JOB_CREATE_DATABASE, "schema-create");
}

void create_job_to_dump_post(struct database *database, struct configuration *conf) {
  create_database_related_job(database, conf, JOB_SCHEMA_POST, "schema-post");
}

void create_job_to_dump_triggers(MYSQL *conn, struct db_table *dbt, struct configuration *conf) {
  char *query = NULL;
  MYSQL_RES *result = NULL;

  query =
      g_strdup_printf("SHOW TRIGGERS FROM `%s` LIKE '%s'", dbt->database->name, dbt->escaped_table);
  if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
    g_critical("Error Checking triggers for %s.%s. Err: %s St: %s", dbt->database->name, dbt->table,
               mysql_error(conn),query);
    errors++;
  } else {
    if (mysql_num_rows(result)) {
      struct job *t = g_new0(struct job, 1);
      struct schema_job *st = g_new0(struct schema_job, 1);
      t->job_data = (void *)st;
      t->type = JOB_TRIGGERS;
      st->dbt = dbt;
      st->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-triggers");
      st->checksum_filename=routine_checksums;
      g_async_queue_push(conf->post_data_queue, t);
    }
  }
  g_free(query);
  if (result) {
    mysql_free_result(result);
  }
}

void create_job_to_dump_schema_triggers(struct database *database, struct configuration *conf) {
  struct job *t = g_new0(struct job, 1);
  struct database_job *st = g_new0(struct database_job, 1);
  t->job_data = (void *)st;
  t->type = JOB_SCHEMA_TRIGGERS;
  st->database = database;
  st->filename = build_schema_filename(database->filename, "schema-triggers");
  st->checksum_filename=routine_checksums;
  g_async_queue_push(conf->post_data_queue, t);
}

void create_job_to_dump_table_schema(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct schema_job *sj = g_new0(struct schema_job, 1);
  j->job_data = (void *)sj;
  sj->dbt = dbt;
  j->type = JOB_SCHEMA;
  sj->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema");
  sj->checksum_filename=schema_checksums;
  sj->checksum_index_filename=schema_checksums;
  g_async_queue_push(conf->schema_queue, j);
}

void create_job_to_dump_view(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct view_job *vj = g_new0(struct view_job, 1);
  j->job_data = (void *)vj;
  vj->dbt = dbt;
//  j->conf = conf;
  j->type = JOB_VIEW;
  vj->tmp_table_filename  = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema");
  vj->view_filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-view");
  vj->checksum_filename = schema_checksums;
  g_async_queue_push(conf->post_data_queue, j);
  return;
}

void create_job_to_dump_sequence(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct sequence_job *sj = g_new0(struct sequence_job, 1);
  j->job_data = (void *)sj;
  sj->dbt = dbt;
  j->type = JOB_SEQUENCE;
  sj->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-sequence");
  sj->checksum_filename=schema_checksums;
  g_async_queue_push(conf->post_data_queue, j);
  return;
}

void create_job_to_dump_checksum(struct db_table * dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct table_checksum_job *tcj = g_new0(struct table_checksum_job, 1);
  tcj->dbt=dbt;
  j->job_data = (void *)tcj;
  j->type = JOB_CHECKSUM;
  tcj->filename = build_meta_filename(dbt->database->filename, dbt->table_filename,"checksum");
  g_async_queue_push(conf->post_data_queue, j);
  return;
}

int initialize_fn(gchar ** sql_filename, struct db_table * dbt, FILE ** sql_file, guint64 fn, guint sub_part, const gchar *extension, gchar * f(), gchar **stdout_fn){
(void)stdout_fn;
(void)extension;
  int r=0;
  if (*sql_filename)
    g_free(*sql_filename);
  *sql_filename = f(dbt->database->filename, dbt->table_filename, fn, sub_part);
  *sql_file = m_open(*sql_filename,"w");
  return r;
}

void initialize_sql_fn(struct table_job * tj){
  tj->child_process=initialize_fn(&(tj->sql_filename),tj->dbt,&(tj->sql_file), tj->nchunk, tj->sub_part,"sql", &build_data_filename, &(tj->exec_out_filename));
}

void initialize_load_data_fn(struct table_job * tj){
  tj->child_process=initialize_fn(&(tj->dat_filename),tj->dbt,&(tj->dat_file), tj->nchunk, tj->sub_part,"dat", &build_load_data_filename, &(tj->exec_out_filename));
}

gboolean update_files_on_table_job(struct table_job *tj){
  if (tj->sql_file == NULL){
    if ( tj->chunk_step && min_rows_per_file == rows_per_file && max_rows_per_file == rows_per_file){
      if (tj->chunk_step->integer_step.is_unsigned)
        tj->sub_part = tj->chunk_step->integer_step.type.unsign.min / tj->chunk_step->integer_step.step + 1; 
      else
        tj->sub_part = tj->chunk_step->integer_step.type.sign.min   / tj->chunk_step->integer_step.step + 1;
    }

    if (load_data){
      initialize_load_data_fn(tj);
      tj->sql_filename = build_data_filename(tj->dbt->database->filename, tj->dbt->table_filename, tj->nchunk, tj->sub_part);
      tj->sql_file = m_open(tj->sql_filename,"w");
      return TRUE;
    }else{
      initialize_sql_fn(tj);
    }
  }
  return FALSE;
}


struct table_job * new_table_job(struct db_table *dbt, char *partition, guint64 nchunk, char *order_by, union chunk_step *chunk_step, gboolean update_where){
  struct table_job *tj = g_new0(struct table_job, 1);
// begin Refactoring: We should review this, as dbt->database should not be free, so it might be no need to g_strdup.
  // from the ref table?? TODO
//  tj->database=dbt->database->name;
//  tj->table=g_strdup(dbt->table);
// end
  tj->partition=g_strdup(partition);
  tj->chunk_step = chunk_step;
  tj->where=NULL;
  tj->order_by=g_strdup(order_by);
  tj->nchunk=nchunk;
  tj->sub_part = 0;
  tj->dat_file = NULL;
  tj->dat_filename = NULL;
  tj->sql_file = NULL;
  tj->sql_filename = NULL;
  tj->exec_out_filename = NULL;
  tj->dbt=dbt;
  tj->st_in_file=0;
  tj->filesize=0;
  tj->char_chunk_part=char_chunk;
  tj->child_process=0;
  if (update_where)
    update_where_on_table_job(NULL, tj);
  return tj;
}

struct job * create_job_to_dump_chunk_without_enqueuing(struct db_table *dbt, char *partition, guint64 nchunk, char *order_by, union chunk_step *chunk_step, gboolean update_where){
  struct job *j = g_new0(struct job,1);
  struct table_job *tj = new_table_job(dbt, partition, nchunk, order_by, chunk_step, update_where);
  j->job_data=(void*) tj;
  j->type= dbt->is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
  j->job_data = (void *)tj;
  return j;
}

void create_job_to_dump_chunk(struct db_table *dbt, char *partition, guint64 nchunk, char *order_by, union chunk_step *chunk_step, void f(), GAsyncQueue *queue, gboolean update_where){
  struct job *j = g_new0(struct job,1);
  struct table_job *tj = new_table_job(dbt, partition, nchunk, order_by, chunk_step, update_where);
  j->job_data=(void*) tj;
  j->type= dbt->is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
  f(queue,j);
}

void create_job_to_determine_chunk_type(struct db_table *dbt, void f(), GAsyncQueue *queue){
  struct job *j = g_new0(struct job,1);
  j->type = JOB_DETERMINE_CHUNK_TYPE;
  j->job_data=(void*) dbt;
  f(queue,j);
}

void create_job_to_dump_all_databases(struct configuration *conf) {
  g_atomic_int_inc(&database_counter);
  struct job *j = g_new0(struct job, 1);
  j->job_data = NULL;
  j->type = JOB_DUMP_ALL_DATABASES;
  g_async_queue_push(conf->initial_queue, j);
  return;
}

void create_job_to_dump_table_list(gchar **table_list, struct configuration *conf) {
  g_atomic_int_inc(&database_counter);
  struct job *j = g_new0(struct job, 1);
  struct dump_table_list_job *dtlj = g_new0(struct dump_table_list_job, 1);
  j->job_data = (void *)dtlj;
  dtlj->table_list = table_list;
  j->type = JOB_DUMP_TABLE_LIST;
  g_async_queue_push(conf->initial_queue, j);
  return;
}

void create_job_to_dump_database(struct database *database, struct configuration *conf) {
  g_atomic_int_inc(&database_counter);
  struct job *j = g_new0(struct job, 1);
  struct dump_database_job *ddj = g_new0(struct dump_database_job, 1);
  j->job_data = (void *)ddj;
  ddj->database = database;
  j->type = JOB_DUMP_DATABASE;
  g_async_queue_push(conf->initial_queue, j);
  return;
}

gchar *get_primary_key_string(MYSQL *conn, char *database, char *table) {
  if (!order_by_primary_key) return NULL;

  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
          g_strdup_printf("SELECT k.COLUMN_NAME, ORDINAL_POSITION "
                          "FROM information_schema.table_constraints t "
                          "LEFT JOIN information_schema.key_column_usage k "
                          "USING(constraint_name,table_schema,table_name) "
                          "WHERE t.constraint_type IN ('PRIMARY KEY', 'UNIQUE') "
                          "AND t.table_schema='%s' "
                          "AND t.table_name='%s' "
                          "ORDER BY t.constraint_type, ORDINAL_POSITION; ",
                          database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else if (atoi(row[1]) > 1) {
      g_string_append(field_list, ",");
    } else {
      break;
    }

    gchar *tb = g_strdup_printf("`%s`", row[0]);
    g_string_append(field_list, tb);
    g_free(tb);
  }
  mysql_free_result(res);
  // Return NULL if we never found a PRIMARY or UNIQUE key
  if (first) {
    g_string_free(field_list, TRUE);
    return NULL;
  } else {
    return g_string_free(field_list, FALSE);
  }
}
