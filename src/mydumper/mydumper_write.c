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

#include <glib/gstdio.h>
#include <math.h>
#include <errno.h>

#include "mydumper.h"
#include "mydumper_start_dump.h"
#include "mydumper_common.h"
#include "mydumper_jobs.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_write.h"
#include "mydumper_masquerade.h"
#include "mydumper_global.h"
#include "mydumper_arguments.h"

/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

/* Program options */
gchar *where_option=NULL;




const gchar *insert_statement=INSERT;
guint statement_size = 1000000;
guint64 max_statement_size=0;
GMutex *max_statement_size_mutex=NULL;
guint complete_insert = 0;
guint chunk_filesize = 0;
gboolean load_data = FALSE;
gboolean csv = FALSE;
gboolean clickhouse = FALSE;
gboolean include_header = FALSE;
const gchar *fields_enclosed_by=NULL;
gchar *fields_escaped_by=NULL;
gchar *fields_terminated_by=NULL;
gchar *lines_starting_by=NULL;
gchar *lines_terminated_by=NULL;
gchar *statement_terminated_by=NULL;
gchar *row_delimiter=NULL;
const gchar *fields_enclosed_by_ld=NULL;
gchar *lines_starting_by_ld=NULL;
gchar *lines_terminated_by_ld=NULL;
gchar *statement_terminated_by_ld=NULL;
gchar *fields_terminated_by_ld=NULL;
gboolean insert_ignore = FALSE;
gboolean replace = FALSE;
gboolean hex_blob = FALSE;


gboolean update_files_on_table_job(struct table_job *tj)
{
  if (tj->rows->file < 0){
    tj->rows->filename = build_rows_filename(tj->dbt->database->database_name_in_filename, tj->dbt->table_filename, tj->part, tj->sub_part);
    tj->rows->file = m_open(&(tj->rows->filename),"w");
    trace("Thread %d: Filename assigned(%d): %s", tj->td->thread_id, tj->rows->file, tj->rows->filename);

    if (tj->sql){
      tj->sql->filename =build_sql_filename(tj->dbt->database->database_name_in_filename, tj->dbt->table_filename, tj->part, tj->sub_part);
      tj->sql->file = m_open(&(tj->sql->filename),"w");
      trace("Thread %d: Filename assigned: %s", tj->td->thread_id, tj->sql->filename);
      return TRUE;
    }
  }
  return FALSE;
}

void message_dumping_data_short(struct table_job *tj){
  g_mutex_lock(transactional_table->mutex);
  guint transactional_table_size = g_list_length(transactional_table->list);
  g_mutex_unlock(transactional_table->mutex);
  g_mutex_lock(non_transactional_table->mutex);
  guint non_transactional_table_size = g_list_length(non_transactional_table->list);
  g_mutex_unlock(non_transactional_table->mutex);
  g_message("Thread %d: %s%s%s.%s%s%s [ %"G_GINT64_FORMAT"%% ] | Tables: %u/%u",
                    tj->td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->database_name_in_filename:tj->dbt->database->source_database, identifier_quote_character_str,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str,
                    tj->dbt->rows_total!=0?100*tj->dbt->rows/tj->dbt->rows_total:0, non_transactional_table_size+transactional_table_size, g_hash_table_size(all_dbts));
}

void message_dumping_data_long(struct table_job *tj){
  g_mutex_lock(transactional_table->mutex);
  guint transactional_table_size = g_list_length(transactional_table->list);
  g_mutex_unlock(transactional_table->mutex);
  g_mutex_lock(non_transactional_table->mutex);
  guint non_transactional_table_size = g_list_length(non_transactional_table->list);
  g_mutex_unlock(non_transactional_table->mutex);
  g_message("Thread %d: dumping data from %s%s%s.%s%s%s%s%s%s%s%s%s%s%s%s%s into %s | Completed: %"G_GINT64_FORMAT"%% | Remaining tables: %u / %u",
                    tj->td->thread_id,
                    identifier_quote_character_str, masquerade_filename?tj->dbt->database->database_name_in_filename:tj->dbt->database->source_database, identifier_quote_character_str, 
                    identifier_quote_character_str, masquerade_filename?tj->dbt->table_filename:tj->dbt->table, identifier_quote_character_str,
                    tj->partition?" ":"",tj->partition?tj->partition:"",
                     (tj->where->len || where_option   || tj->dbt->where) ? " WHERE " : "" , tj->where->len ? tj->where->str : "",
                     (tj->where->len && where_option )                    ? " AND "   : "" ,   where_option ?   where_option : "",
                    ((tj->where->len || where_option ) && tj->dbt->where) ? " AND "   : "" , tj->dbt->where ? tj->dbt->where : "",
                    order_by_primary_key && tj->dbt->primary_key_separated_by_comma ? " ORDER BY " : "", order_by_primary_key && tj->dbt->primary_key_separated_by_comma ? tj->dbt->primary_key_separated_by_comma : "",
                    tj->rows->filename, tj->dbt->rows_total!=0?100*tj->dbt->rows/tj->dbt->rows_total:0, non_transactional_table_size+transactional_table_size,g_hash_table_size(all_dbts));
}

void (*message_dumping_data)(struct table_job *tj);

void initialize_write(){
  if (verbose > 3)
    message_dumping_data=&message_dumping_data_long;
  else
    message_dumping_data=&message_dumping_data_short;

  // rows chunks have precedence over chunk_filesize
  if (starting_chunk_step_size > 0 && chunk_filesize > 0) {
//    chunk_filesize = 0;
//    g_warning("--chunk-filesize disabled by --rows option");
    g_warning("We are going to chunk by row and by filesize when possible");
  }

  g_assert(fields_enclosed_by); // initialized in detect_quote_character()

  if(fields_enclosed_by_ld && strlen(fields_enclosed_by_ld)>1)
    m_critical("--fields-enclosed-by must be a single character");
  if(fields_escaped_by && strlen(fields_escaped_by)>1)
    m_critical("--fields-escaped-by must be a single character");

  max_statement_size_mutex=g_mutex_new();

  switch (output_format){
		case CLICKHOUSE:
		case SQL_INSERT:
      if (fields_enclosed_by_ld)
				fields_enclosed_by= fields_enclosed_by_ld;

      if (fields_terminated_by_ld==NULL)
        fields_terminated_by=g_strdup(",");
      else if (g_strcmp0(fields_terminated_by_ld, "\\t")==0)
        fields_terminated_by=g_strdup("\t");
      else
        fields_terminated_by=replace_escaped_strings(g_strdup(fields_terminated_by_ld));

      if (!lines_starting_by_ld)
        lines_starting_by=g_strdup("(");
      else
        lines_starting_by=replace_escaped_strings(g_strdup(lines_starting_by_ld));

      if (!lines_terminated_by_ld)
        lines_terminated_by=g_strdup(")\n");
      else
        lines_terminated_by=replace_escaped_strings(g_strdup(lines_terminated_by_ld));

      if (!statement_terminated_by_ld)
        statement_terminated_by=g_strdup(";\n");
      else
        statement_terminated_by=replace_escaped_strings(g_strdup(statement_terminated_by_ld));

      row_delimiter=g_strdup(",");
			break;
		case LOAD_DATA:
      if (!fields_enclosed_by_ld){
				fields_enclosed_by= "";
        fields_enclosed_by_ld= fields_enclosed_by;
			}else
        fields_enclosed_by= fields_enclosed_by_ld;

      if (fields_escaped_by){
        if (strcmp(fields_escaped_by,"\\")==0)
          fields_escaped_by=g_strdup("\\\\");
      }else
        fields_escaped_by=g_strdup("\\\\");

      if (fields_terminated_by_ld==NULL){
        fields_terminated_by=g_strdup("\t");
        fields_terminated_by_ld=g_strdup("\\t");
      }else if (g_strcmp0(fields_terminated_by_ld, "\\t")==0){
        fields_terminated_by=g_strdup("\t");
        fields_terminated_by_ld=g_strdup("\\t");
      }else
        fields_terminated_by=replace_escaped_strings(g_strdup(fields_terminated_by_ld));

			if (!lines_starting_by_ld){
        lines_starting_by=g_strdup("");
        lines_starting_by_ld=lines_starting_by;
      }else
        lines_starting_by=replace_escaped_strings(g_strdup(lines_starting_by_ld));

      if (!lines_terminated_by_ld){
        lines_terminated_by=g_strdup("\n");
        lines_terminated_by_ld=g_strdup("\\n");
      }else
        lines_terminated_by=replace_escaped_strings(g_strdup(lines_terminated_by_ld));

      if (!statement_terminated_by_ld){
        statement_terminated_by=g_strdup("");
        statement_terminated_by_ld=statement_terminated_by;
      }else
        statement_terminated_by=replace_escaped_strings(g_strdup(statement_terminated_by_ld));

      row_delimiter=g_strdup("");
			break;
		case CSV:
			if (!fields_enclosed_by_ld){
        fields_enclosed_by= "\"";
        fields_enclosed_by_ld= fields_enclosed_by;
      }else
        fields_enclosed_by= fields_enclosed_by_ld;

			if (fields_escaped_by){
        if (strcmp(fields_escaped_by,"\\")==0)
          fields_escaped_by=g_strdup("\\\\");
      }else
        fields_escaped_by=g_strdup("\\\\");

      if (fields_terminated_by_ld==NULL){
        fields_terminated_by=g_strdup(",");
        fields_terminated_by_ld=fields_terminated_by;
      }else if (g_strcmp0(fields_terminated_by_ld, "\\t")==0){
        fields_terminated_by=g_strdup("\t");
        fields_terminated_by_ld=g_strdup("\\t");
      }else
        fields_terminated_by=replace_escaped_strings(g_strdup(fields_terminated_by_ld));

      if (!lines_starting_by_ld){
        lines_starting_by=g_strdup("");
        lines_starting_by_ld=lines_starting_by;
      }else
        lines_starting_by=replace_escaped_strings(g_strdup(lines_starting_by_ld));

      if (!lines_terminated_by_ld){
        lines_terminated_by=g_strdup("\n");
        lines_terminated_by_ld=g_strdup("\\n");
      }else
        lines_terminated_by=replace_escaped_strings(g_strdup(lines_terminated_by_ld));

      if (!statement_terminated_by_ld){
        statement_terminated_by=g_strdup("");
        statement_terminated_by_ld=statement_terminated_by;
      }else
        statement_terminated_by=replace_escaped_strings(g_strdup(statement_terminated_by_ld));
      row_delimiter=g_strdup("");
			break;
	}


  if ( insert_ignore && replace ){
    m_error("You can't use --insert-ignore and --replace at the same time");
  }

  if (insert_ignore)
    insert_statement=INSERT_IGNORE;

  if (replace)
    insert_statement=REPLACE;
}

void finalize_write(){
  g_free(fields_terminated_by);
  g_free(lines_starting_by);
  g_free(lines_terminated_by);
  g_free(statement_terminated_by);
}

gboolean is_hex_blob (MYSQL_FIELD field){
  return hex_blob && (field.type == MYSQL_TYPE_BLOB || ( field.charsetnr== 63 && (field.type == MYSQL_TYPE_VAR_STRING || field.type == MYSQL_TYPE_STRING )));
}

GString *append_load_data_columns(GString *statement, MYSQL_FIELD *fields, guint num_fields){
  guint i = 0;
  GString *str=g_string_new("SET ");
  gboolean appendable=FALSE;
  for (i = 0; i < num_fields; ++i) {
    if (i > 0) {
      g_string_append_c(statement, ',');
    }
    if (fields[i].type == MYSQL_TYPE_JSON){
      g_string_append_c(statement,'@');
      g_string_append(statement, fields[i].name);
      if (str->len > 4)
        g_string_append_c(str, ',');
      g_string_append(str,identifier_quote_character_str);
      g_string_append(str,fields[i].name);
      g_string_append(str,identifier_quote_character_str);
      g_string_append(str,"=CONVERT(@");
      g_string_append(str, fields[i].name);
      g_string_append(str, " USING UTF8MB4)");
      appendable=TRUE;
    }else if ( is_hex_blob(fields[i])){
      g_string_append_c(statement,'@');
      g_string_append(statement, fields[i].name);
      if (str->len > 4)
        g_string_append_c(str, ',');
      g_string_append(str,identifier_quote_character_str);
      g_string_append(str,fields[i].name);
      g_string_append(str,identifier_quote_character_str);
      g_string_append(str,"=UNHEX(@");
      g_string_append(str, fields[i].name);
      g_string_append(str, ")");
      appendable=TRUE;
    }else{
      g_string_append(statement, identifier_quote_character_str);
      g_string_append(statement, fields[i].name);
      g_string_append(statement, identifier_quote_character_str);
    }
  }
  if (appendable)
    return str;
  else{
    g_string_free(str,TRUE);
    return NULL;
  }
}

void append_columns (GString *statement, MYSQL_FIELD *fields, guint num_fields){
  guint i = 0;
  for (i = 0; i < num_fields; ++i) {
    if (i > 0) {
      g_string_append_c(statement, ',');
    }
    g_string_append_c(statement,identifier_quote_character);
    g_string_append(statement, fields[i].name);    
    g_string_append_c(statement,identifier_quote_character);

  }
}


void set_anonymized_function_list(struct db_table * dbt, MYSQL_FIELD *fields, guint num_fields){
  gchar *database=dbt->database->source_database;
  gchar *table=dbt->table;

  gchar * k = g_strdup_printf("`%s`.`%s`",database,table);
  GHashTable *ht = g_hash_table_lookup(conf_per_table.all_anonymized_function,k);
  g_free(k);
  struct function_pointer ** anonymized_function_list=NULL;

  if (ht){
    anonymized_function_list = g_new0(struct function_pointer *, num_fields);
    guint i = 0;
    struct function_pointer *fp=NULL;
    for (i = 0; i < num_fields; ++i) {
      fp=(struct function_pointer*)g_hash_table_lookup(ht,fields[i].name);
      if (fp != NULL){
        g_message("Masquerade function found on `%s`.`%s`.`%s`", database, table, fields[i].name);
        anonymized_function_list[i]=fp;
      }else{
        anonymized_function_list[i]=&identity_function_pointer;
      }
    }
    dbt->anonymized_function=anonymized_function_list;
  }
}

void build_insert_statement(struct db_table * dbt, MYSQL_FIELD *fields, guint num_fields){
  GString * i_s=g_string_new(insert_statement);
  g_string_append(i_s, " INTO ");
  g_string_append_c(i_s, identifier_quote_character);
  g_string_append(i_s, dbt->table);
  g_string_append_c(i_s, identifier_quote_character);
  set_anonymized_function_list(dbt,fields,num_fields);
  if (dbt->columns_on_insert){
    g_string_append(i_s, " (");
    g_string_append(i_s, dbt->columns_on_insert);
    g_string_append(i_s, ")");
  }else{
    if (dbt->complete_insert) {
      g_string_append(i_s, " (");
      append_columns(i_s,fields,num_fields);
      g_string_append(i_s, ")");
    }
  } 
  g_string_append(i_s, " VALUES");
  dbt->insert_statement=i_s;
}

gboolean real_write_data(int file, float *filesize, GString *data) {
  size_t written = 0;
  ssize_t r = 0;
  gboolean second_write_zero = FALSE;
  while (written < data->len) {
    r=write(file, data->str + written, data->len - written);
    if (r < 0) {
      g_critical("Couldn't write data to a file(%d): %s", file, strerror(errno));
      errors++;
      return FALSE;
    }
    if ( r == 0 ) {
      if (second_write_zero){
        g_critical("Couldn't write data to a file: %s", strerror(errno));
        errors++;
        return FALSE;
      }
      second_write_zero=TRUE;
    }else{
      second_write_zero=FALSE;
    }
    written += r;
  }
  *filesize+=written;
  return TRUE;
}


gboolean write_data(int file, GString *data) {
  float f=0;
  return real_write_data(file, &f, data);
}

void initialize_load_data_statement_suffix(struct db_table *dbt, MYSQL_FIELD * fields, guint num_fields){
  gchar *character_set=set_names_in_conn_by_default != NULL ? set_names_in_conn_by_default : dbt->character_set /* "BINARY"*/;
  GString *load_data_suffix=g_string_sized_new(statement_size);
  g_string_append_printf(load_data_suffix, "%s' INTO TABLE %s%s%s ", exec_per_thread_extension, identifier_quote_character_str, dbt->table, identifier_quote_character_str);
  if (character_set && strlen(character_set)!=0)
    g_string_append_printf(load_data_suffix, "CHARACTER SET %s ",character_set);
  if (fields_terminated_by_ld)
    g_string_append_printf(load_data_suffix, "FIELDS TERMINATED BY '%s' ",fields_terminated_by_ld);
  if (fields_enclosed_by_ld)
    g_string_append_printf(load_data_suffix, "ENCLOSED BY '%s' ",fields_enclosed_by_ld);
  if (fields_escaped_by)
    g_string_append_printf(load_data_suffix, "ESCAPED BY '%s' ",fields_escaped_by);
  g_string_append(load_data_suffix, "LINES ");
  if (lines_starting_by_ld)
    g_string_append_printf(load_data_suffix, "STARTING BY '%s' ",lines_starting_by_ld);
  g_string_append_printf(load_data_suffix, "TERMINATED BY '%s' ", lines_terminated_by_ld);
  if (include_header)
    g_string_append(load_data_suffix, "IGNORE 1 LINES ");
  g_string_append_printf(load_data_suffix, "(");
  if (dbt->columns_on_insert){
    g_string_append(load_data_suffix,dbt->columns_on_insert);
    g_string_append(load_data_suffix,")");
  }else{
    GString * set_statement=append_load_data_columns(load_data_suffix,fields,num_fields);
    g_string_append(load_data_suffix,")");
    if (set_statement != NULL){
      g_string_append(load_data_suffix,set_statement->str);
      g_string_free(set_statement,TRUE);
    }
  }
  g_string_append(load_data_suffix,";\n");
  dbt->load_data_suffix=load_data_suffix;
}

void initialize_clickhouse_statement_suffix(struct db_table *dbt, MYSQL_FIELD * fields, guint num_fields){
  gchar *character_set=set_names_in_conn_by_default != NULL ? set_names_in_conn_by_default : dbt->character_set /* "BINARY"*/;
  dbt->load_data_suffix=g_string_sized_new(statement_size);
  g_string_append_printf(dbt->load_data_suffix, "%s' INTO TABLE %s%s%s ", exec_per_thread_extension, identifier_quote_character_str, dbt->table, identifier_quote_character_str);
  if (character_set && strlen(character_set)!=0)
    g_string_append_printf(dbt->load_data_suffix, "CHARACTER SET %s ",character_set);
  if (fields_terminated_by_ld)
    g_string_append_printf(dbt->load_data_suffix, "FIELDS TERMINATED BY '%s' ",fields_terminated_by_ld);
  if (fields_enclosed_by_ld)
    g_string_append_printf(dbt->load_data_suffix, "ENCLOSED BY '%s' ",fields_enclosed_by_ld);
  if (fields_escaped_by)
    g_string_append_printf(dbt->load_data_suffix, "ESCAPED BY '%s' ",fields_escaped_by);
  g_string_append(dbt->load_data_suffix, "LINES ");
  if (lines_starting_by_ld)
    g_string_append_printf(dbt->load_data_suffix, "STARTING BY '%s' ",lines_starting_by_ld);
  g_string_append_printf(dbt->load_data_suffix, "TERMINATED BY '%s' ", lines_terminated_by_ld);
  if (include_header)
    g_string_append(dbt->load_data_suffix, "IGNORE 1 LINES ");
  g_string_append_printf(dbt->load_data_suffix, "(");
  if (dbt->columns_on_insert){
    g_string_append(dbt->load_data_suffix,dbt->columns_on_insert);
    g_string_append(dbt->load_data_suffix,")");
  }else{
    GString * set_statement=append_load_data_columns(dbt->load_data_suffix,fields,num_fields);
    g_string_append(dbt->load_data_suffix,")");
    if (set_statement != NULL){
      g_string_append(dbt->load_data_suffix,set_statement->str);
      g_string_free(set_statement,TRUE);
    }
  }
  g_string_append(dbt->load_data_suffix,";\n");
}

void initialize_load_data_header(struct db_table *dbt, MYSQL_FIELD *fields, guint num_fields){
  dbt->load_data_header = g_string_sized_new(statement_size);
  guint i = 0;
  for (i = 0; i < num_fields-1; ++i) {
    g_string_append(dbt->load_data_header,fields_enclosed_by);
    g_string_append(dbt->load_data_header,fields[i].name);
    g_string_append(dbt->load_data_header,fields_enclosed_by);
    g_string_append(dbt->load_data_header,fields_terminated_by);
  }
  g_string_append(dbt->load_data_header,fields_enclosed_by);
  g_string_append(dbt->load_data_header,fields[i].name);
  g_string_append(dbt->load_data_header,fields_enclosed_by);
  g_string_append(dbt->load_data_header,lines_terminated_by);
}

static
gboolean write_statement(int load_data_file, float *filessize, GString *statement, struct db_table * dbt){
  if (!real_write_data(load_data_file, filessize, statement)) {
    g_critical("Could not write out data for %s.%s", dbt->database->source_database, dbt->table);
    return FALSE;
  }
  g_mutex_lock(max_statement_size_mutex);
  if (statement->len > max_statement_size)
    max_statement_size=statement->len;
  g_mutex_unlock(max_statement_size_mutex);
  g_string_set_size(statement, 0);
  return TRUE;
}

void initialize_config_on_string(GString *output){
  g_mutex_lock(max_statement_size_mutex);
  g_string_append_printf(output,"[config]\nmax-statement-size = %" G_GUINT64_FORMAT "\n", max_statement_size);
  g_mutex_unlock(max_statement_size_mutex);
  g_string_append_printf(output, "num-sequences = %d\n", num_sequences);
}

void write_load_data_statement(struct table_job * tj){
  GString *statement = g_string_sized_new(statement_size);
  char * basename=g_path_get_basename(tj->rows->filename);
  initialize_sql_statement(statement);
  g_string_append_printf(statement, "%s%s%s", LOAD_DATA_PREFIX, basename, tj->dbt->load_data_suffix->str);
  if (!write_data(tj->sql->file, statement)) {
    g_critical("Could not write out data for %s.%s", tj->dbt->database->source_database, tj->dbt->table);
  }
}

void write_clickhouse_statement(struct table_job * tj){
  GString *statement = g_string_sized_new(statement_size);
  char * basename=g_path_get_basename(tj->rows->filename);
  initialize_sql_statement(statement);
  g_string_append_printf(statement, "%s INTO %s%s%s FROM INFILE '%s' FORMAT MySQLDump;", insert_statement, identifier_quote_character_str, tj->dbt->table, identifier_quote_character_str, basename); // , tj->dbt->load_data_suffix->str);
  if (!write_data(tj->sql->file, statement)) {
    g_critical("Could not write out data for %s.%s", tj->dbt->database->source_database, tj->dbt->table);
  }
}

gboolean write_header(struct table_job * tj){
  if (tj->dbt->load_data_header && !write_data(tj->rows->file, tj->dbt->load_data_header)) {
    g_critical("Could not write header for %s.%s", tj->dbt->database->source_database, tj->dbt->table);
    return FALSE;
  }
  return TRUE;
}

void write_load_data_column_into_string( MYSQL *conn, gchar **column, MYSQL_FIELD field, gulong length, struct thread_data_buffers buffers){
    if (!*column) {
      g_string_append(buffers.column, "\\N");
    } else if ( is_hex_blob(field) ) {
      g_string_set_size(buffers.escaped, length * 2 + 1);
      mysql_hex_string(buffers.escaped->str,*column,length);
      g_string_append(buffers.column,buffers.escaped->str);
    }else if (field.type != MYSQL_TYPE_LONG && field.type != MYSQL_TYPE_LONGLONG  && field.type != MYSQL_TYPE_INT24  && field.type != MYSQL_TYPE_SHORT ){
      g_string_append(buffers.column,fields_enclosed_by);
      // this will reserve the memory needed if the current size is not enough.
      g_string_set_size(buffers.escaped, length * 2 + 1);
      unsigned long new_length = mysql_real_escape_string(conn, buffers.escaped->str, *column, length);
      new_length++;
      //g_string_set_size(escaped, new_length);
      m_replace_char_with_char('\\',*fields_escaped_by,buffers.escaped->str, new_length);
      m_escape_char_with_char(*fields_terminated_by, *fields_escaped_by, buffers.escaped->str, new_length);
      g_string_append(buffers.column,buffers.escaped->str);
      g_string_append(buffers.column,fields_enclosed_by);
    }else
      g_string_append(buffers.column, *column);
}

void write_sql_column_into_string( MYSQL *conn, gchar **column, MYSQL_FIELD field, gulong length, struct thread_data_buffers buffers){
    if (!*column) {
      g_string_append(buffers.column, "NULL");
    } else if (field.flags & NUM_FLAG) {
      g_string_append(buffers.column, *column);
    } else if ( length == 0){
      g_string_append_c(buffers.column,*fields_enclosed_by);
      g_string_append_c(buffers.column,*fields_enclosed_by);
    } else if ( is_hex_blob(field) ) {
      g_string_set_size(buffers.escaped, length * 2 + 1);
      g_string_append(buffers.column,"0x");
      mysql_hex_string(buffers.escaped->str,*column,length);
      g_string_append(buffers.column,buffers.escaped->str);
    } else {
      /* We reuse buffers for string escaping, growing is expensive just at
 *        * the beginning */
      g_string_set_size(buffers.escaped, length * 2 + 1);
      mysql_real_escape_string(conn, buffers.escaped->str, *column, length);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(buffers.column, "CONVERT(");
      g_string_append_c(buffers.column, *fields_enclosed_by);
      g_string_append(buffers.column, buffers.escaped->str);
      g_string_append_c(buffers.column, *fields_enclosed_by);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(buffers.column, " USING UTF8MB4)");
    }
}



void write_column_into_string_with_terminated_by(MYSQL *conn, gchar * row, MYSQL_FIELD field, gulong length, struct thread_data_buffers buffers, void write_column_into_string(MYSQL *, gchar **, MYSQL_FIELD , gulong ,struct thread_data_buffers), struct function_pointer * f, gchar * terminated_by){
  gchar *column=NULL;
  gulong rlength=length;
  g_string_set_size(buffers.column,0);
  if (row)
    column=row;
  if (f){
   if (f->is_pre){
     // apply and constant as they alter the data
     write_column_into_string( conn, &(column), field, rlength, buffers);
     column=f->function(&(buffers.column->str), &rlength, f);
     g_string_printf(buffers.column,"%s",column);
   }else{
     column=f->function(&(column), &rlength, f);
     write_column_into_string( conn, &(column), field, rlength, buffers);
   }
  }else{
    write_column_into_string( conn, &(column), field, rlength, buffers);
  }
  g_string_append(buffers.row, buffers.column->str);
  g_string_append(buffers.row, terminated_by);

  if (column && column != row)
    g_free(column);
}

void write_row_into_string(MYSQL *conn, struct db_table * dbt, MYSQL_ROW row, MYSQL_FIELD *fields, gulong *lengths, guint num_fields, struct thread_data_buffers buffers, void write_column_into_string(MYSQL *, gchar **, MYSQL_FIELD , gulong , struct thread_data_buffers)){
  guint i = 0;
  g_string_append(buffers.row, lines_starting_by);
  struct function_pointer ** f = dbt->anonymized_function;

  for (i = 0; i < num_fields-1; i++) {
    write_column_into_string_with_terminated_by(conn, row[i], fields[i], lengths[i], buffers, write_column_into_string,f==NULL?NULL:f[i], fields_terminated_by);
  }
  write_column_into_string_with_terminated_by(conn, row[i], fields[i], lengths[i], buffers, write_column_into_string,f==NULL?NULL:f[i], lines_terminated_by);
}

void update_dbt_rows(struct db_table * dbt, guint64 num_rows){
  g_mutex_lock(dbt->rows_lock);
  dbt->rows+=num_rows;
  g_mutex_unlock(dbt->rows_lock);
}

void close_file(struct table_job * tj, struct table_job_file *tjf){
  if (tjf->file >= 0){
    m_close(tj->td->thread_id, tjf->file, tjf->filename, 1, tj->dbt);
    tjf->file=-1;
    g_free(tjf->filename);
    tjf->filename=NULL;
  }
}

void close_files(struct table_job * tj){
  switch (output_format){
    case LOAD_DATA:
    case CSV:
    case CLICKHOUSE:
      close_file(tj, tj->sql);
      break;
    case SQL_INSERT:
      break;
  }
  close_file(tj, tj->rows);
}

static
void reopen_files(struct table_job * tj){
  close_files(tj);
  switch (output_format){
    case LOAD_DATA:
    case CSV:
      if (update_files_on_table_job(tj)){
        write_load_data_statement(tj);
        write_header(tj);
      }
      break;
    case CLICKHOUSE:
      if (update_files_on_table_job(tj)){
        write_clickhouse_statement(tj);
        write_header(tj);
      }
      break;
    case SQL_INSERT:
      update_files_on_table_job(tj);
      break;
  }
}


void write_result_into_file(MYSQL *conn, MYSQL_RES *result, struct table_job * tj){
	struct db_table * dbt = tj->dbt;
	guint num_fields = mysql_num_fields(result);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  MYSQL_ROW row;
  g_string_set_size(tj->td->thread_data_buffers.statement,0);
  g_string_set_size(tj->td->thread_data_buffers.row,0);
  g_string_set_size(tj->td->thread_data_buffers.escaped,0);
  gulong *lengths = NULL;
  guint64 num_rows=0;
  guint64 num_rows_st = 0;
  void (*write_column_into_string)(MYSQL *, gchar **, MYSQL_FIELD , gulong , struct thread_data_buffers) = write_sql_column_into_string;
  switch (output_format){
    case LOAD_DATA:
    case CSV:
  		write_column_into_string=write_load_data_column_into_string;
    	if (dbt->load_data_suffix==NULL){
        g_mutex_lock(dbt->chunks_mutex);
        if (dbt->load_data_suffix==NULL){
          initialize_load_data_statement_suffix(tj->dbt, fields, num_fields);
        if (include_header)
          initialize_load_data_header(tj->dbt, fields, num_fields);
        }
        g_mutex_unlock(dbt->chunks_mutex);
      }
      if (update_files_on_table_job(tj)){
        write_load_data_statement(tj);
        write_header(tj);
      }
	  	break;
    case CLICKHOUSE:
      if (tj->rows->file < 0){
        update_files_on_table_job(tj);
      }
			if (dbt->load_data_suffix==NULL){
        g_mutex_lock(dbt->chunks_mutex);
        if (dbt->load_data_suffix==NULL)
          initialize_clickhouse_statement_suffix(tj->dbt, fields, num_fields);
  			g_mutex_unlock(dbt->chunks_mutex);
      }
			if (dbt->insert_statement==NULL){
        g_mutex_lock(dbt->chunks_mutex);
        if (dbt->insert_statement==NULL)
          build_insert_statement(dbt, fields, num_fields);
        g_mutex_unlock(dbt->chunks_mutex);
      }
      if (!tj->st_in_file){
        initialize_sql_statement(tj->td->thread_data_buffers.statement);
				write_clickhouse_statement(tj);
			}
      g_string_append(tj->td->thread_data_buffers.statement, dbt->insert_statement->str);
      break;
		case SQL_INSERT:
      if (tj->rows->file < 0){
        update_files_on_table_job(tj);
  		}
      if (dbt->insert_statement==NULL){
        g_mutex_lock(dbt->chunks_mutex);
        if (dbt->insert_statement==NULL)
          build_insert_statement(dbt, fields, num_fields);
        g_mutex_unlock(dbt->chunks_mutex);
      }
	  	if (!tj->st_in_file)
  	  	initialize_sql_statement(tj->td->thread_data_buffers.statement);
  		g_string_append(tj->td->thread_data_buffers.statement, dbt->insert_statement->str);
	  	break;
	}

  message_dumping_data(tj);

  GDateTime *from = g_date_time_new_now_local();
  GDateTime *to = NULL;
  GTimeSpan diff=0;
	while ((row = mysql_fetch_row(result))) {
// Uncomment next line if you need to simulate a slow read which is useful when calculate the chunk size
//    g_usleep(1);
    lengths = mysql_fetch_lengths(result);
    num_rows++;
    // prepare row into statement_row
		write_row_into_string(conn, dbt, row, fields, lengths, num_fields, tj->td->thread_data_buffers, write_column_into_string);

    // if row exceeded statement_size then FLUSH buffer to disk
		if (tj->td->thread_data_buffers.statement->len + tj->td->thread_data_buffers.row->len + 1 > statement_size){
      if (num_rows_st == 0) {
        g_string_append(tj->td->thread_data_buffers.statement, tj->td->thread_data_buffers.row->str);
        g_string_set_size(tj->td->thread_data_buffers.row, 0);
        g_warning("Row bigger than statement_size for %s.%s", dbt->database->source_database,
                dbt->table);
      }
      g_string_append(tj->td->thread_data_buffers.statement, statement_terminated_by);
      if (!write_statement(tj->rows->file, &(tj->filesize), tj->td->thread_data_buffers.statement, dbt)) {
        g_critical("Fail to write on %s", tj->rows->filename);
        return;
      }
			update_dbt_rows(dbt, num_rows);
      tj->num_rows_of_last_run+=num_rows;
			num_rows=0;
			num_rows_st=0;
			tj->st_in_file++;
    // initilize buffer if needed (INSERT INTO)
      if (output_format == SQL_INSERT || output_format == CLICKHOUSE){
				g_string_append(tj->td->thread_data_buffers.statement, dbt->insert_statement->str);
			}
      to = g_date_time_new_now_local();
      diff=g_date_time_difference(to,from)/G_TIME_SPAN_SECOND;
      if (diff > 4){
        g_date_time_unref(from);
        from=to;
        to=NULL;
        message_dumping_data(tj);
      }else{
        g_date_time_unref(to);
      }

			check_pause_resume(tj->td);
      if (shutdown_triggered) {
        return;
      }
		}
		// if file size exceeded limit, we need to rotate
		if (dbt->chunk_filesize && (guint)ceil((float)tj->filesize / 1024 / 1024) >
              dbt->chunk_filesize){
			tj->sub_part++;
      reopen_files(tj);
			if (output_format == SQL_INSERT){
        initialize_sql_statement(tj->td->thread_data_buffers.statement);
        g_string_append(tj->td->thread_data_buffers.statement, dbt->insert_statement->str);
      }
      tj->st_in_file = 0;
      tj->filesize = 0;			
		}
		//
		// write row to buffer
    if (num_rows_st && (output_format == SQL_INSERT || output_format == CLICKHOUSE))
      g_string_append(tj->td->thread_data_buffers.statement, row_delimiter);
    g_string_append(tj->td->thread_data_buffers.statement, tj->td->thread_data_buffers.row->str);
		if (tj->td->thread_data_buffers.row->len>0)
      num_rows_st++;
    g_string_set_size(tj->td->thread_data_buffers.row, 0);
  }
  update_dbt_rows(dbt, num_rows);
  tj->num_rows_of_last_run+=num_rows;
  if (num_rows_st > 0 && tj->td->thread_data_buffers.statement->len > 0){
    if (output_format == SQL_INSERT || output_format == CLICKHOUSE)
			g_string_append(tj->td->thread_data_buffers.statement, statement_terminated_by);
    if (!write_statement(tj->rows->file, &(tj->filesize), tj->td->thread_data_buffers.statement, dbt)) {
      g_critical("Fail to write on %s", tj->rows->filename);
      return;
    }
		tj->st_in_file++;
  }
  g_date_time_unref(from);

//  g_string_free(statement, TRUE);
//  g_string_free(escaped, TRUE);
//  g_string_free(statement_row, TRUE);	
  return;
}

/* Do actual data chunk reading/writing magic */
void write_table_job_into_file(struct table_job * tj){
  MYSQL *conn = tj->td->thrconn;
  char *query = NULL;

//  if (throttle_time)
  g_usleep(throttle_time);

  tj->num_rows_of_last_run=0;

  /* Ghm, not sure if this should be statement_size - but default isn't too big
   * for now */
  /* Poor man's database code */
  MYSQL_RES *result = m_use_result(conn, query = g_strdup_printf(
      "SELECT %s %s FROM %s%s%s.%s%s%s %s %s %s %s %s %s %s %s %s %s %s",
      is_mysql_like() ? "/*!40001 SQL_NO_CACHE */" : "",
      tj->dbt->select_fields?tj->dbt->select_fields->str:"*",
      identifier_quote_character_str,tj->dbt->database->source_database, identifier_quote_character_str, identifier_quote_character_str, tj->dbt->table, identifier_quote_character_str, tj->partition?tj->partition:"",
       (tj->where->len || where_option   || tj->dbt->where) ? "WHERE"  : "" , tj->where->len ? tj->where->str : "",
       (tj->where->len && where_option )                    ? "AND"    : "" ,   where_option ?   where_option : "",
      ((tj->where->len || where_option ) && tj->dbt->where) ? "AND"    : "" , tj->dbt->where ? tj->dbt->where : "",
      order_by_primary_key && tj->dbt->primary_key_separated_by_comma ? " ORDER BY " : "", order_by_primary_key && tj->dbt->primary_key_separated_by_comma ? tj->dbt->primary_key_separated_by_comma : "",
      tj->dbt->limit ?  "LIMIT" : "", tj->dbt->limit ? tj->dbt->limit : ""
  ), m_warning, "Failed to execute query", NULL);

  if (!result){
    if (!it_is_a_consistent_backup){
      g_warning("Thread %d: Error dumping table (%s.%s) data: %s\nQuery: %s", tj->td->thread_id, tj->dbt->database->source_database, tj->dbt->table,
                mysql_error(conn), query);

      if (mysql_ping(tj->td->thrconn)) {
        m_connect(tj->td->thrconn);
        execute_gstring(tj->td->thrconn, set_session);
      }
      g_warning("Thread %d: Retrying last failed executed statement", tj->td->thread_id);

      result = m_use_result(conn, query, NULL, "Failed to execute query on second try", NULL);
      if (!result) 
        goto cleanup;
    }else
      goto cleanup;
  }

  /* Poor man's data dump code */
  write_result_into_file(conn, result, tj);

  if (mysql_errno(conn)) {
    g_critical("Thread %d: Could not read data from %s.%s to write on %s at byte %.0f: %s", tj->td->thread_id, tj->dbt->database->source_database, tj->dbt->table, tj->rows->filename, tj->filesize,
               mysql_error(conn));
    errors++;
    if (mysql_ping(tj->td->thrconn)) {
      if (!it_is_a_consistent_backup){
        g_warning("Thread %d: Reconnecting due errors", tj->td->thread_id);
        m_connect(tj->td->thrconn);
        execute_gstring(tj->td->thrconn, set_session);
      }
    } 
 }

cleanup:
  g_free(query);

  if (result) {
    mysql_free_result(result);
  }
}

