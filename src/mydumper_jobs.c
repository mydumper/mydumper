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
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
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
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "common.h"
#include "regex.h"
#include "mydumper_common.h"
#include "mydumper_jobs.h"
#include "mydumper_database.h"
extern gchar *where_option;
extern gboolean success_on_1146;
extern int detected_server;
extern FILE * (*m_open)(const char *filename, const char *);
extern int (*m_close)(void *file);
extern guint errors;
extern guint statement_size;
extern int skip_tz;
extern gchar *set_names_str;
extern GAsyncQueue *stream_queue;
extern gboolean stream;
extern gboolean dump_routines;
extern gboolean dump_events;
extern gboolean use_savepoints;
extern gint database_counter;
extern guint rows_per_file;
extern gint non_innodb_table_counter;
gboolean dump_triggers = FALSE;
gboolean split_partitions = FALSE;
gboolean order_by_primary_key = FALSE;
guint64 max_rows=1000000;
gboolean ignore_generated_fields = FALSE;

extern gboolean schema_checksums;
extern gboolean routine_checksums;

static GOptionEntry dump_into_file_entries[] = {
    {"triggers", 'G', 0, G_OPTION_ARG_NONE, &dump_triggers, "Dump triggers. By default, it do not dump triggers",
     NULL},
    { "split-partitions", 0, 0, G_OPTION_ARG_NONE, &split_partitions,
      "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.", NULL},
    {"max-rows", 0, 0, G_OPTION_ARG_INT64, &max_rows,
     "Limit the number of rows per block after the table is estimated, default 1000000", NULL},
    { "no-check-generated-fields", 0, 0, G_OPTION_ARG_NONE, &ignore_generated_fields,
      "Queries related to generated fields are not going to be executed."
      "It will lead to restoration issues if you have generated columns", NULL },
    {"order-by-primary", 0, 0, G_OPTION_ARG_NONE, &order_by_primary_key,
     "Sort the data by Primary Key or Unique key if no primary key exists",
     NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

void load_dump_into_file_entries(GOptionGroup *main_group){
  g_option_group_add_entries(main_group, dump_into_file_entries);
}

void initialize_dump_into_file(){
  initialize_database();
  if (ignore_generated_fields)
    g_warning("Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns");
}

void write_checksum_into_file(MYSQL *conn, char *database, char *table, char *filename, gchar *fun()) {
  int errn=0;
  gchar * checksum=fun(conn, database, table, &errn);

  if (errn != 0 && !(success_on_1146 && errn == 1146)) {
    errors++;
    return;
  }

  if (checksum == NULL)
    checksum = g_strdup("0");

  void *outfile = NULL;

  outfile = g_fopen(filename, "w");

  if (!outfile) {
    g_critical("Error: DB: %s TABLE: %s Could not create output file %s (%d)",
               database, table, filename, errno);
    errors++;
    return;
  }

  fprintf(outfile, "%s", checksum);
  fclose(outfile);

  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
  g_free(checksum);

  return;
}


void write_table_metadata_into_file(struct db_table * dbt){
  char *filename = build_meta_filename(dbt->database->filename, dbt->table_filename, "metadata");
  FILE *table_meta = g_fopen(filename, "w");
  if (!table_meta) {
    g_critical("Couldn't write table metadata file %s (%d)", filename, errno);
    exit(EXIT_FAILURE);
  }
  fprintf(table_meta, "%"G_GUINT64_FORMAT, dbt->rows);
  fclose(table_meta);
  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
}

gchar * get_tablespace_query(){
  if ( get_product() == SERVER_TYPE_PERCONA || get_product() == SERVER_TYPE_MYSQL){
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

void write_schema_definition_into_file(MYSQL *conn, char *database, char *filename, char *checksum_filename) {
  void *outfile = NULL;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  query = g_strdup_printf("SHOW CREATE DATABASE IF NOT EXISTS `%s`", database);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping create database (%s): %s", database,
                mysql_error(conn));
    } else {
      g_critical("Error dumping create database (%s): %s", database,
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
    g_critical("Could not write create database for %s", database);
    errors++;
  }
  g_free(query);

  m_close(outfile);
  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);


  if (schema_checksums)
    write_checksum_into_file(conn, database, NULL, checksum_filename, checksum_database_defaults);
  return;
}

void write_table_definition_into_file(MYSQL *conn, char *database, char *table,
                      char *filename, char *checksum_filename, char *checksum_index_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  if (detected_server == SERVER_TYPE_MYSQL) {
    if (set_names_str)
      g_string_printf(statement,"%s;\n",set_names_str);
    g_string_append(statement, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\n");
    if (!skip_tz) {
      g_string_append(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
    }
  } else if (detected_server == SERVER_TYPE_TIDB) {
    if (!skip_tz) {
      g_string_printf(statement, "/*!40103 SET TIME_ZONE='+00:00' */;\n");
    }
  } else {
    g_string_printf(statement, "SET FOREIGN_KEY_CHECKS=0;\n");
  }

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", database, table);
    errors++;
    return;
  }

  query = g_strdup_printf("SHOW CREATE TABLE `%s`.`%s`", database, table);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", database, table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", database, table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }

  g_string_set_size(statement, 0);

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  g_string_append(statement, row[1]);
  g_string_append(statement, ";\n");
  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema for %s.%s", database, table);
    errors++;
  }
  g_free(query);

  m_close(outfile);
  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  if (checksum_filename)
    write_checksum_into_file(conn, database, table, checksum_filename, checksum_table_structure);
  if (checksum_index_filename)
    write_checksum_into_file(conn, database, table, checksum_index_filename, checksum_table_indexes);
  return;
}

void write_triggers_definition_into_file(MYSQL *conn, char *database, char *table, char *filename, char *checksum_filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row;
  MYSQL_ROW row2;
  gchar **splited_st = NULL;

  outfile = m_open(filename,"w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  // get triggers
  query = g_strdup_printf("SHOW TRIGGERS FROM `%s` LIKE '%s'", database, table);
  if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping triggers (%s.%s): %s", database, table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping triggers (%s.%s): %s", database, table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }

  while ((row = mysql_fetch_row(result))) {
    set_charset(statement, row[8], row[9]);
    if (!write_data((FILE *)outfile, statement)) {
      g_critical("Could not write triggers data for %s.%s", database, table);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
    query = g_strdup_printf("SHOW CREATE TRIGGER `%s`.`%s`", database, row[0]);
    mysql_query(conn, query);
    result2 = mysql_store_result(conn);
    row2 = mysql_fetch_row(result2);
    g_string_append_printf(statement, "%s", row2[2]);
    splited_st = g_strsplit(statement->str, ";\n", 0);
    g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
    g_string_append(statement, ";\n");
    restore_charset(statement);
    if (!write_data((FILE *)outfile, statement)) {
      g_critical("Could not write triggers data for %s.%s", database, table);
      errors++;
      return;
    }
    g_string_set_size(statement, 0);
  }
  g_free(query);
  m_close(outfile);
  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
  g_string_free(statement, TRUE);
  g_strfreev(splited_st);
  if (result)
    mysql_free_result(result);
  if (result2)
    mysql_free_result(result2);
  if (checksum_filename)
    write_checksum_into_file(conn, database, table, checksum_filename, checksum_trigger_structure);
  return;
}

void write_view_definition_into_file(MYSQL *conn, char *database, char *table, char *filename, char *filename2, char *checksum_filename) {
  void *outfile, *outfile2;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  GString *statement = g_string_sized_new(statement_size);

  mysql_select_db(conn, database);

  outfile = m_open(filename,"w");
  outfile2 = m_open(filename2,"w");

  if (!outfile || !outfile2) {
    g_critical("Error: DB: %s Could not create output file (%d)", database,
               errno);
    errors++;
    return;
  }

  if (detected_server == SERVER_TYPE_MYSQL && set_names_str) {
    g_string_printf(statement,"%s;\n",set_names_str);
  }

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write schema data for %s.%s", database, table);
    errors++;
    return;
  }

  g_string_append_printf(statement, "DROP TABLE IF EXISTS `%s`;\n", table);
  g_string_append_printf(statement, "DROP VIEW IF EXISTS `%s`;\n", table);

  if (!write_data((FILE *)outfile2, statement)) {
    g_critical("Could not write schema data for %s.%s", database, table);
    errors++;
    return;
  }

  // we create tables as workaround
  // for view dependencies
  query = g_strdup_printf("SHOW FIELDS FROM `%s`.`%s`", database, table);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", database, table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", database, table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_free(query);
  g_string_set_size(statement, 0);
  g_string_append_printf(statement, "CREATE TABLE IF NOT EXISTS `%s`(\n", table);
  row = mysql_fetch_row(result);
  g_string_append_printf(statement, "`%s` int", row[0]);
  while ((row = mysql_fetch_row(result))) {
    g_string_append(statement, ",\n");
    g_string_append_printf(statement, "`%s` int", row[0]);
  }
  g_string_append(statement, "\n);\n");

  if (result)
    mysql_free_result(result);

  if (!write_data((FILE *)outfile, statement)) {
    g_critical("Could not write view schema for %s.%s", database, table);
    errors++;
  }

  // real view
  query = g_strdup_printf("SHOW CREATE VIEW `%s`.`%s`", database, table);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping schemas (%s.%s): %s", database, table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping schemas (%s.%s): %s", database, table,
                 mysql_error(conn));
      errors++;
    }
    g_free(query);
    return;
  }
  g_string_set_size(statement, 0);

  /* There should never be more than one row */
  row = mysql_fetch_row(result);
  set_charset(statement, row[2], row[3]);
  g_string_append(statement, row[1]);
  g_string_append(statement, ";\n");
  restore_charset(statement);
  if (!write_data((FILE *)outfile2, statement)) {
    g_critical("Could not write schema for %s.%s", database, table);
    errors++;
  }
  g_free(query);
  m_close(outfile);

  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
  m_close(outfile2);
  if (stream) g_async_queue_push(stream_queue, g_strdup(filename2));
  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  if (checksum_filename)
    // build_meta_filename(database,table,"schema-view-checksum"),
    write_checksum_into_file(conn, database, table, checksum_filename, checksum_view_structure);
  return;
}

// Routines, Functions and Events
// TODO: We need to split it in 3 functions 
void write_routines_definition_into_file(MYSQL *conn, struct database *database, char *filename, char *checksum_filename) {
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
      write_checksum_into_file(conn, database->name, NULL, checksum_filename, checksum_process_structure);
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
  m_close(outfile);
  if (stream) g_async_queue_push(stream_queue, g_strdup(filename));
  g_string_free(statement, TRUE);
  g_strfreev(splited_st);
  if (result)
    mysql_free_result(result);
  if (result2)
    mysql_free_result(result2);

  return;
}

void free_schema_job(struct schema_job *sj){
  if (sj->table)
    g_free(sj->table);
  if (sj->filename)
    g_free(sj->filename);
//  g_free(sj);
}

void free_view_job(struct view_job *vj){
  if (vj->table)
    g_free(vj->table);
  if (vj->filename)
    g_free(vj->filename);
  if (vj->filename2)
    g_free(vj->filename2);
//  g_free(vj);
}

void free_schema_post_job(struct schema_post_job *sp){
  if (sp->filename)
    g_free(sp->filename);
//  g_free(sp);
}
void free_create_database_job(struct create_database_job * cdj){
  if (cdj->filename)
    g_free(cdj->filename);
//  g_free(cdj);
}

void free_create_tablespace_job(struct create_tablespace_job * ctj){
  if (ctj->filename)
    g_free(ctj->filename);
//  g_free(cdj);
}

void free_table_checksum_job(struct table_checksum_job*tcj){
      if (tcj->table)
        g_free(tcj->table);
      if (tcj->filename)
        g_free(tcj->filename);
 //     g_free(tcj);
}

void do_JOB_CREATE_DATABASE(struct thread_data *td, struct job *job){
  struct create_database_job * cdj = (struct create_database_job *)job->job_data;
  g_message("Thread %d dumping schema create for `%s`", td->thread_id,
            cdj->database);
  write_schema_definition_into_file(td->thrconn, cdj->database, cdj->filename, cdj->checksum_filename);
  free_create_database_job(cdj);
  g_free(job);
}

void do_JOB_CREATE_TABLESPACE(struct thread_data *td, struct job *job){
  struct create_tablespace_job * ctj = (struct create_tablespace_job *)job->job_data;
  g_message("Thread %d dumping create tablespace if any", td->thread_id);
  write_tablespace_definition_into_file(td->thrconn, ctj->filename);
  free_create_tablespace_job(ctj);
  g_free(job);
}

void do_JOB_SCHEMA_POST(struct thread_data *td, struct job *job){
  struct schema_post_job * sp = (struct schema_post_job *)job->job_data;
  g_message("Thread %d dumping SP and VIEWs for `%s`", td->thread_id,
            sp->database->name);
  write_routines_definition_into_file(td->thrconn, sp->database, sp->filename, sp->checksum_filename);
  free_schema_post_job(sp);
  g_free(job);
}

void do_JOB_VIEW(struct thread_data *td, struct job *job){
  struct view_job * vj = (struct view_job *)job->job_data;
  g_message("Thread %d dumping view for `%s`.`%s`", td->thread_id,
            vj->database, vj->table);
  write_view_definition_into_file(td->thrconn, vj->database, vj->table, vj->filename,
                 vj->filename2, vj->checksum_filename);
  free_view_job(vj);
  g_free(job);
}

void do_JOB_SCHEMA(struct thread_data *td, struct job *job){
  struct schema_job *sj = (struct schema_job *)job->job_data;
  g_message("Thread %d dumping schema for `%s`.`%s`", td->thread_id,
            sj->database, sj->table);
  write_table_definition_into_file(td->thrconn, sj->database, sj->table, sj->filename, sj->checksum_filename, sj->checksum_index_filename);
  free_schema_job(sj);
  g_free(job);
}

void do_JOB_TRIGGERS(struct thread_data *td, struct job *job){
  struct schema_job * sj = (struct schema_job *)job->job_data;
  g_message("Thread %d dumping triggers for `%s`.`%s`", td->thread_id,
            sj->database, sj->table);
  write_triggers_definition_into_file(td->thrconn, sj->database, sj->table, sj->filename, sj->checksum_filename);
  free_schema_job(sj);
  g_free(job);
}


void do_JOB_CHECKSUM(struct thread_data *td, struct job *job){
  struct table_checksum_job *tcj = (struct table_checksum_job *)job->job_data;
  g_message("Thread %d dumping checksum for `%s`.`%s`", td->thread_id,
            tcj->database, tcj->table);
  if (use_savepoints && mysql_query(td->thrconn, "SAVEPOINT mydumper")) {
    g_critical("Savepoint failed: %s", mysql_error(td->thrconn));
  }
  write_checksum_into_file(td->thrconn, tcj->database, tcj->table, tcj->filename, checksum_table);
  if (use_savepoints &&
      mysql_query(td->thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
    g_critical("Rollback to savepoint failed: %s", mysql_error(td->thrconn));
  }
  free_table_checksum_job(tcj);
  g_free(job);
}

void create_job_to_dump_tablespaces(MYSQL *conn, struct configuration *conf){
(void)conn;
(void)conf;
  struct job *j = g_new0(struct job, 1);
  struct create_tablespace_job *ctj = g_new0(struct create_tablespace_job, 1);
  j->job_data = (void *)ctj;
  j->conf = conf;
  j->type = JOB_CREATE_TABLESPACE;
  ctj->filename = build_tablespace_filename();
  g_async_queue_push(conf->queue, j);
  return;
}


void create_job_to_dump_schema(char *database, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct create_database_job *cdj = g_new0(struct create_database_job, 1);
  j->job_data = (void *)cdj;
  gchar *d=get_ref_table(database);
  cdj->database = g_strdup(database);
  j->conf = conf;
  j->type = JOB_CREATE_DATABASE;
  cdj->filename = build_schema_filename(d, "schema-create");
  if (schema_checksums)
    cdj->checksum_filename = build_meta_filename(database,NULL,"schema-create-checksum"); 
  g_async_queue_push(conf->queue, j);
  return;
}

void create_job_to_dump_triggers(MYSQL *conn, struct db_table *dbt, struct configuration *conf) {
  if (dump_triggers) {
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
        st->database = dbt->database->name;
        st->table = g_strdup(dbt->table);
        t->conf = conf;
        t->type = JOB_TRIGGERS;
        st->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-triggers");
        if ( routine_checksums )
          st->checksum_filename=build_meta_filename(dbt->database->filename,dbt->table_filename,"schema-triggers-checksum");
        g_async_queue_push(conf->queue, t);
      }
    }
    g_free(query);
    if (result) {
      mysql_free_result(result);
    }
  }

}

void create_job_to_dump_table_schema(struct db_table *dbt, struct configuration *conf, GAsyncQueue *queue) {
  struct job *j = g_new0(struct job, 1);
  struct schema_job *sj = g_new0(struct schema_job, 1);
  j->job_data = (void *)sj;
  sj->database = dbt->database->name;
  sj->table = g_strdup(dbt->table);
  j->conf = conf;
  j->type = JOB_SCHEMA;
  sj->filename = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema");
  if ( schema_checksums ){
    sj->checksum_filename=build_meta_filename(dbt->database->filename,dbt->table_filename,"schema-checksum");
    sj->checksum_index_filename = build_meta_filename(dbt->database->filename,dbt->table_filename,"schema-indexes-checksum");
  }
  g_async_queue_push(queue, j);
}

void create_job_to_dump_view(struct db_table *dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct view_job *vj = g_new0(struct view_job, 1);
  j->job_data = (void *)vj;
  vj->database = dbt->database->name;
  vj->table = g_strdup(dbt->table);
  j->conf = conf;
  j->type = JOB_VIEW;
  vj->filename  = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema");
  vj->filename2 = build_schema_table_filename(dbt->database->filename, dbt->table_filename, "schema-view");
  if ( schema_checksums )
    vj->checksum_filename = build_meta_filename(dbt->database->filename, dbt->table_filename, "schema-view-checksum");
  g_async_queue_push(conf->queue, j);
  return;
}

void create_job_to_dump_post(struct database *database, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct schema_post_job *sp = g_new0(struct schema_post_job, 1);
  j->job_data = (void *)sp;
  sp->database = database;
  j->conf = conf;
  j->type = JOB_SCHEMA_POST;
  sp->filename = build_schema_filename(sp->database->filename,"schema-post");
  if ( routine_checksums )
    sp->checksum_filename = build_meta_filename(sp->database->filename, NULL, "schema-post-checksum");
  g_async_queue_push(conf->queue, j);
  return;
}

void create_job_to_dump_checksum(struct db_table * dbt, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct table_checksum_job *tcj = g_new0(struct table_checksum_job, 1);
  j->job_data = (void *)tcj;
  tcj->database = dbt->database->name;
  tcj->table = g_strdup(dbt->table);
  j->conf = conf;
  j->type = JOB_CHECKSUM;
  tcj->filename = build_meta_filename(dbt->database->filename, dbt->table_filename,"checksum");
  g_async_queue_push(conf->queue, j);
  return;
}

void create_job_to_dump_database(struct database *database, struct configuration *conf, gboolean less_locking) {

  g_atomic_int_inc(&database_counter);

  struct job *j = g_new0(struct job, 1);
  struct dump_database_job *ddj = g_new0(struct dump_database_job, 1);
  j->job_data = (void *)ddj;
  ddj->database = database;
  j->conf = conf;
  j->type = JOB_DUMP_DATABASE;

  if (less_locking)
    g_async_queue_push(conf->queue_less_locking, j);
  else
    g_async_queue_push(conf->queue, j);
  return;
}

void m_async_queue_push_conservative(GAsyncQueue *queue, struct job *element){
  // Each job weights 500 bytes aprox.
  // if we reach to 200k of jobs, which is 100MB of RAM, we are going to wait 5 seconds
  // which is not too much considering that it will impossible to proccess 200k of jobs
  // in 5 seconds.
  // I don't think that we need to this values as parameters, unless that a user needs to
  // set hundreds of threads
  while (g_async_queue_length(queue)>200000){
    g_warning("Too many jobs in the queue. We are pausing the jobs creation for 5 seconds.");
    sleep(5);
  }
  g_async_queue_push(queue, element);
}

GList * get_partitions_for_table(MYSQL *conn, char *database, char *table){
  MYSQL_RES *res=NULL;
  MYSQL_ROW row;

  GList *partition_list = NULL;

  gchar *query = g_strdup_printf("select PARTITION_NAME from information_schema.PARTITIONS where PARTITION_NAME is not null and TABLE_SCHEMA='%s' and TABLE_NAME='%s'", database, table);
  mysql_query(conn,query);
  g_free(query);

  res = mysql_store_result(conn);
  if (res == NULL)
    //partitioning is not supported
    return partition_list;
  while ((row = mysql_fetch_row(res))) {
    partition_list = g_list_append(partition_list, strdup(row[0]));
  }
  mysql_free_result(res);

  return partition_list;
}

/* Try to get EXPLAIN'ed estimates of row in resultset */
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field,
                       char *from, char *to) {
  char *querybase, *query;
  int ret;

  g_assert(conn && database && table);

  querybase = g_strdup_printf("EXPLAIN SELECT `%s` FROM `%s`.`%s`",
                              (field ? field : "*"), database, table);
  if (from || to) {
    g_assert(field != NULL);
    char *fromclause = NULL, *toclause = NULL;
    char *escaped;
    if (from) {
      escaped = g_new(char, strlen(from) * 2 + 1);
      mysql_real_escape_string(conn, escaped, from, strlen(from));
      fromclause = g_strdup_printf(" `%s` >= %s ", field, escaped);
      g_free(escaped);
    }
    if (to) {
      escaped = g_new(char, strlen(to) * 2 + 1);
      mysql_real_escape_string(conn, escaped, to, strlen(to));
      toclause = g_strdup_printf(" `%s` <= %s", field, escaped);
      g_free(escaped);
    }
    query = g_strdup_printf("%s WHERE %s %s %s", querybase,
                            (from ? fromclause : ""),
                            ((from && to) ? "AND" : ""), (to ? toclause : ""));

    if (toclause)
      g_free(toclause);
    if (fromclause)
      g_free(fromclause);
    ret = mysql_query(conn, query);
    g_free(querybase);
    g_free(query);
  } else {
    ret = mysql_query(conn, querybase);
    g_free(querybase);
  }

  if (ret) {
    g_warning("Unable to get estimates for %s.%s: %s", database, table,
              mysql_error(conn));
  }

  MYSQL_RES *result = mysql_store_result(conn);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);

  guint i;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcmp(fields[i].name, "rows"))
      break;
  }

  MYSQL_ROW row = NULL;

  guint64 count = 0;

  if (result)
    row = mysql_fetch_row(result);

  if (row && row[i])
    count = strtoul(row[i], NULL, 10);

  if (result)
    mysql_free_result(result);

  return (count);
}




GList *get_chunks_for_table_by_rows(MYSQL *conn, struct db_table *dbt, char *field){
  GList *chunks = NULL;
  gchar *query = NULL;
  MYSQL_ROW row;
  MYSQL_RES *minmax = NULL;
  int showed_nulls = 0;
  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(`%s`),MAX(`%s`) FROM `%s`.`%s` %s %s",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, field, dbt->database->name, dbt->table, where_option ? "WHERE" : "", where_option ? where_option : ""));
  g_free(query);
  minmax = mysql_store_result(conn);

  if (!minmax)
    goto cleanup;

  row = mysql_fetch_row(minmax);
  MYSQL_FIELD *fields = mysql_fetch_fields(minmax);

  /* Check if all values are NULL */
  if (row[0] == NULL)
    goto cleanup;

  char *min = row[0];
  char *max = row[1];
  char cmin,cmax;
  guint64 estimated_chunks, estimated_step, nmin, nmax, cutoff, rows;

  /* Support just bigger INTs for now, very dumb, no verify approach */
  switch (fields[0].type) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_SHORT:
    /* Got total number of rows, skip chunk logic if estimates are low */
    rows = estimate_count(conn, dbt->database->name, dbt->table, field, min, max);
    if (rows <= rows_per_file){
      g_message("Table %s.%s too small to split", dbt->database->name, dbt->table);
      goto cleanup;
    }

    /* This is estimate, not to use as guarantee! Every chunk would have eventual
 *      * adjustments */
    estimated_chunks = rows / rows_per_file;
    /* static stepping */
    nmin = strtoul(min, NULL, 10);
    nmax = strtoul(max, NULL, 10);
    estimated_step = (nmax - nmin) / estimated_chunks + 1;
    if (estimated_step > max_rows)
      estimated_step = max_rows;
    cutoff = nmin;
    while (cutoff <= nmax) {
      chunks = g_list_prepend(
          chunks,
          g_strdup_printf("%s%s%s%s(`%s` >= %llu AND `%s` < %llu)",
                          !showed_nulls ? "`" : "",
                          !showed_nulls ? field : "",
                          !showed_nulls ? "`" : "",
                          !showed_nulls ? " IS NULL OR " : "", field,
                          (unsigned long long)cutoff, field,
                          (unsigned long long)(cutoff + estimated_step)));
      cutoff += estimated_step;
      showed_nulls = 1;
    }
    chunks = g_list_reverse(chunks);
    break;
  case MYSQL_TYPE_STRING:
    /* static stepping */
    cmin = min[0];
    cmax = max[0];
    while (cmin <= cmax ) {
      chunks = g_list_prepend(
          chunks,
          g_strdup_printf("%s%s%s%s(`%s` like '%c%%')",
                          !showed_nulls ? "`" : "",
                          !showed_nulls ? field : "",
                          !showed_nulls ? "`" : "",
                          !showed_nulls ? " IS NULL OR " : "", field,
                          cmin
                          ));
      cmin++;
      showed_nulls = 1;
    }
    chunks = g_list_reverse(chunks);   
    break;
    default:
      ;
   }
cleanup:
  if (minmax)
    mysql_free_result(minmax);
  return chunks;
}




GList *get_chunks_for_table(MYSQL *conn, struct db_table * dbt,
                            struct configuration *conf) {

  GList *chunks = NULL;
  MYSQL_RES *indexes = NULL;
  MYSQL_ROW row;
  char *field = NULL;

  if (dbt->limit != NULL)
    return chunks;

  /* first have to pick index, in future should be able to preset in
   * configuration too */
  gchar *query = g_strdup_printf("SHOW INDEX FROM `%s`.`%s`", dbt->database->name, dbt->table);
  mysql_query(conn, query);
  g_free(query);
  indexes = mysql_store_result(conn);

  if (indexes){
    while ((row = mysql_fetch_row(indexes))) {
      if (!strcmp(row[2], "PRIMARY") && (!strcmp(row[3], "1"))) {
        /* Pick first column in PK, cardinality doesn't matter */
        field = row[4];
        break;
      }
    }

    /* If no PK found, try using first UNIQUE index */
    if (!field) {
      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[1], "0") && (!strcmp(row[3], "1"))) {
          /* Again, first column of any unique index */
          field = row[4];
          break;
        }
      }
    }
    /* Still unlucky? Pick any high-cardinality index */
    if (!field && conf->use_any_index) {
      guint64 max_cardinality = 0;
      guint64 cardinality = 0;

      mysql_data_seek(indexes, 0);
      while ((row = mysql_fetch_row(indexes))) {
        if (!strcmp(row[3], "1")) {
          if (row[6])
            cardinality = strtoul(row[6], NULL, 10);
          if (cardinality > max_cardinality) {
            field = row[4];
            max_cardinality = cardinality;
          }
        }
      }
    }
  }
  /* Oh well, no chunks today - no suitable index */
  if (!field)
    goto cleanup;
  chunks = get_chunks_for_table_by_rows(conn,dbt,field);

cleanup:
  if (indexes)
    mysql_free_result(indexes);
  return chunks;
}


struct table_job * new_table_job(struct db_table *dbt, char *partition, char *where, guint nchunk, char *order_by){
  struct table_job *tj = g_new0(struct table_job, 1);
// begin Refactoring: We should review this, as dbt->database should not be free, so it might be no need to g_strdup.
  // from the ref table?? TODO
  tj->database=dbt->database->name;
  tj->table=g_strdup(dbt->table);
// end
  tj->partition=partition;
  tj->where=where;
  tj->order_by=order_by;
  tj->nchunk=nchunk;
//  tj->filename = build_data_filename(dbt->database->filename, dbt->table_filename, tj->nchunk, 0);
  tj->dbt=dbt;
  return tj;
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

void create_job_to_dump_table(MYSQL *conn, struct db_table *dbt,
                struct configuration *conf, gboolean is_innodb) {
//  char *database = dbt->database;
//  char *table = dbt->table;
  GList * partitions = NULL;
  if (split_partitions)
    partitions = get_partitions_for_table(conn, dbt->database->name, dbt->table);

  GList *chunks = NULL;
  if (rows_per_file)
    chunks = get_chunks_for_table(conn, dbt, conf);

  if (partitions){
    int npartition=0;
    for (partitions = g_list_first(partitions); partitions; partitions=g_list_next(partitions)) {
      struct job *j = g_new0(struct job,1);
      struct table_job *tj = NULL;
      j->job_data=(void*) tj;
      j->conf=conf;
      j->type= is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
      tj = new_table_job(dbt, (char *) g_strdup_printf(" PARTITION (%s) ", (char *)partitions->data), NULL, npartition, get_primary_key_string(conn, dbt->database->name, dbt->table));
      j->job_data = (void *)tj;
      if (!is_innodb && npartition)
        g_atomic_int_inc(&non_innodb_table_counter);
      g_async_queue_push(conf->queue,j);
      npartition++;
    }
    g_list_free_full(g_list_first(partitions), (GDestroyNotify)g_free);

  } else if (chunks) {
    int nchunk = 0;
    GList *iter;
    for (iter = chunks; iter != NULL; iter = iter->next) {
      struct job *j = g_new0(struct job, 1);
      struct table_job *tj = NULL;
      j->conf = conf;
      j->type = is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
      tj = new_table_job(dbt, NULL, (char *)iter->data, nchunk, get_primary_key_string(conn, dbt->database->name, dbt->table));
      j->job_data = (void *)tj;
      if (!is_innodb && nchunk)
        g_atomic_int_inc(&non_innodb_table_counter);
      m_async_queue_push_conservative(conf->queue, j);
      nchunk++;
    }
    g_list_free(chunks);
  } else {
    struct job *j = g_new0(struct job, 1);
    struct table_job *tj = NULL;
    j->conf = conf;
    j->type = is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
    tj = new_table_job(dbt, NULL, NULL, 0, get_primary_key_string(conn, dbt->database->name, dbt->table));
    j->job_data = (void *)tj;
    g_async_queue_push(conf->queue, j);
  }
}

void create_jobs_for_non_innodb_table_list_in_less_locking_mode(MYSQL *conn, GList *noninnodb_tables_list,
                 struct configuration *conf) {
  struct db_table *dbt=NULL;
  GList *chunks = NULL;
  GList * partitions = NULL;

  struct job *j = g_new0(struct job, 1);
  struct tables_job *tjs = g_new0(struct tables_job, 1);
  j->conf = conf;
  j->type = JOB_LOCK_DUMP_NON_INNODB;
  j->job_data = (void *)tjs;

  GList *iter;

  for (iter = noninnodb_tables_list; iter != NULL; iter = iter->next) {
    dbt = (struct db_table *)iter->data;

    if (rows_per_file)
      chunks = get_chunks_for_table(conn, dbt, conf);

    if (split_partitions)
      partitions = get_partitions_for_table(conn, dbt->database->name, dbt->table);

    if (partitions){
      int npartition=0;
      for (partitions = g_list_first(partitions); partitions; partitions=g_list_next(partitions)) {
        struct table_job *tj = NULL;
        tj = new_table_job(dbt, (char *) g_strdup_printf(" PARTITION (%s) ", (char *)partitions->data), NULL, npartition, get_primary_key_string(conn, dbt->database->name, dbt->table));
        tjs->table_job_list = g_list_prepend(tjs->table_job_list, tj);
        npartition++;
      }
      g_list_free_full(g_list_first(partitions), (GDestroyNotify)g_free);

    } else if (chunks) {
      int nchunk = 0;
      GList *citer;
      for (citer = chunks; citer != NULL; citer = citer->next) {
        struct table_job *tj = new_table_job(dbt, NULL, (char *)citer->data, nchunk, get_primary_key_string(conn, dbt->database->name, dbt->table));
        tjs->table_job_list = g_list_prepend(tjs->table_job_list, tj);
        nchunk++;
      }
      g_list_free(chunks);
    } else {
      struct table_job *tj = NULL;
      tj = new_table_job(dbt, NULL, NULL, 0, get_primary_key_string(conn, dbt->database->name, dbt->table));
      tjs->table_job_list = g_list_prepend(tjs->table_job_list, tj);
    }
  }
  tjs->table_job_list = g_list_reverse(tjs->table_job_list);
  g_async_queue_push(conf->queue_less_locking, j);
}
