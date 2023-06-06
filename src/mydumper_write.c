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
#include <math.h>
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "regex.h"
#include "mydumper_common.h"
#include "mydumper_jobs.h"
#include "mydumper_database.h"
#include "mydumper_working_thread.h"
#include "mydumper_write.h"
#include "mydumper_masquerade.h"
#include "mydumper_global.h"

const gchar *insert_statement=INSERT;
guint statement_size = 1000000;
guint complete_insert = 0;
guint chunk_filesize = 0;
gboolean load_data = FALSE;
gboolean csv = FALSE;
gchar *fields_enclosed_by=NULL;
gchar *fields_escaped_by=NULL;
gchar *fields_terminated_by=NULL;
gchar *lines_starting_by=NULL;
gchar *lines_terminated_by=NULL;
gchar *statement_terminated_by=NULL;
gchar *fields_enclosed_by_ld=NULL;
gchar *lines_starting_by_ld=NULL;
gchar *lines_terminated_by_ld=NULL;
gchar *statement_terminated_by_ld=NULL;
gchar *fields_terminated_by_ld=NULL;
gboolean insert_ignore = FALSE;
gboolean replace = FALSE;
gboolean hex_blob = FALSE;

void initialize_write(){

  // rows chunks have precedence over chunk_filesize
  if (rows_per_file > 0 && chunk_filesize > 0) {
//    chunk_filesize = 0;
//    g_warning("--chunk-filesize disabled by --rows option");
    g_warning("We are going to chunk by row and by filesize");
  }

  fields_enclosed_by=g_strdup("\"");
  if (csv){
    load_data=TRUE;
    if (!fields_terminated_by_ld) fields_terminated_by_ld=g_strdup(",");
    if (!fields_enclosed_by_ld) fields_enclosed_by_ld=g_strdup("\"");
    if (!fields_escaped_by) fields_escaped_by=g_strdup("\\");
    if (!lines_terminated_by_ld) lines_terminated_by_ld=g_strdup("\\n");
  }
//  if (load_data){
    if (!fields_enclosed_by_ld){
      if (load_data){
        g_free(fields_enclosed_by);
        fields_enclosed_by=g_strdup("");
      }
      fields_enclosed_by_ld=fields_enclosed_by;
    }else if(strlen(fields_enclosed_by_ld)>1){
      m_critical("--fields-enclosed-by must be a single character");
    }else{
      fields_enclosed_by=fields_enclosed_by_ld;
    }

  if (load_data){
    if (fields_escaped_by){
      if(strlen(fields_escaped_by)>1){
        m_critical("--fields-escaped-by must be a single character");
      }else if (strcmp(fields_escaped_by,"\\")==0){
        fields_escaped_by=g_strdup("\\\\");
      }
    }else{
      fields_escaped_by=g_strdup("\\\\");
    }
  }


  if (fields_terminated_by_ld==NULL){
    if (load_data){
      fields_terminated_by=g_strdup("\t");
      fields_terminated_by_ld=g_strdup("\\t");
    }else
      fields_terminated_by=g_strdup(",");
  }else if (g_strcmp0(fields_terminated_by_ld, "\\t")==0){
      fields_terminated_by=g_strdup("\t");
      fields_terminated_by_ld=g_strdup("\\t");
  }else
    fields_terminated_by=replace_escaped_strings(g_strdup(fields_terminated_by_ld));
  if (!lines_starting_by_ld){
    if (load_data){
      lines_starting_by=g_strdup("");
      lines_starting_by_ld=lines_starting_by;
    }else
      lines_starting_by=g_strdup("(");
  }else
    lines_starting_by=replace_escaped_strings(g_strdup(lines_starting_by_ld));
  if (!lines_terminated_by_ld){
    if (load_data){
      lines_terminated_by=g_strdup("\n");
      lines_terminated_by_ld=g_strdup("\\n");
    }else
      lines_terminated_by=g_strdup(")\n");
  }else
    lines_terminated_by=replace_escaped_strings(g_strdup(lines_terminated_by_ld));
  if (!statement_terminated_by_ld){
    if (load_data){
      statement_terminated_by=g_strdup("");
      statement_terminated_by_ld=statement_terminated_by;
    }else
      statement_terminated_by=g_strdup(";\n");
  }else
    statement_terminated_by=replace_escaped_strings(g_strdup(statement_terminated_by_ld));

  if ( insert_ignore && replace ){
    m_error("You can't use --insert-ignore and --replace at the same time");
  }

  if (insert_ignore)
    insert_statement=INSERT_IGNORE;

  if (replace)
    insert_statement=REPLACE;
}

void finalize_write(){
  g_free(fields_enclosed_by);
  g_free(fields_terminated_by);
  g_free(lines_starting_by);
  g_free(lines_terminated_by);
  g_free(statement_terminated_by);
}


GString *append_load_data_columns(GString *statement, MYSQL_FIELD *fields, guint num_fields){
  guint i = 0;
  GString *str=g_string_new("SET ");
  for (i = 0; i < num_fields; ++i) {
    if (i > 0) {
      g_string_append_c(statement, ',');
    }
//    g_string_append_printf(statement, "`%s`", fields[i].name);
    if (fields[i].type == MYSQL_TYPE_JSON){
      g_string_append_c(statement,'@');
      g_string_append(statement, fields[i].name);
      if (str->len > 4)
        g_string_append_c(str, ',');
      g_string_append_c(str,'`');
      g_string_append(str,fields[i].name);
      g_string_append(str,"`=CONVERT(@");
      g_string_append(str, fields[i].name);
      g_string_append(str, " USING UTF8MB4)");
    }else{
      g_string_append_c(statement,'`');
      g_string_append(statement, fields[i].name);
      g_string_append_c(statement,'`');
    }
  }
  if (str->len > 4)
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
    g_string_append_c(statement,'`');
    g_string_append(statement, fields[i].name);    
    g_string_append_c(statement,'`');
  }
}

void build_insert_statement(struct db_table * dbt, MYSQL_FIELD *fields, guint num_fields){
  dbt->insert_statement=g_string_new(insert_statement);
  g_string_append(dbt->insert_statement, " INTO ");
  g_string_append_c(dbt->insert_statement, identifier_quote_character);
  g_string_append(dbt->insert_statement, dbt->table);
  g_string_append_c(dbt->insert_statement, identifier_quote_character);

  if (dbt->complete_insert) {
    g_string_append(dbt->insert_statement, " (");
    append_columns(dbt->insert_statement,fields,num_fields);
    g_string_append(dbt->insert_statement, ")");
  } 
  g_string_append(dbt->insert_statement, " VALUES");
}

gboolean real_write_data(FILE *file, float *filesize, GString *data) {
  size_t written = 0;
  ssize_t r = 0;
  gboolean second_write_zero = FALSE;
  while (written < data->len) {
    r=m_write(file, data->str + written, data->len);
    if (r < 0) {
      g_critical("Couldn't write data to a file: %s", strerror(errno));
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


gboolean write_data(FILE *file, GString *data) {
  float f=0;
  return real_write_data(file, &f, data);
}


void initialize_load_data_statement(GString *statement, gchar * table, const gchar *character_set, gchar *basename, MYSQL_FIELD * fields, guint num_fields){
  g_string_append_printf(statement, "LOAD DATA LOCAL INFILE '%s%s' REPLACE INTO TABLE `%s` ", basename, exec_per_thread_extension, table);
  if (character_set && strlen(character_set)!=0)
    g_string_append_printf(statement, "CHARACTER SET %s ",character_set);
  if (fields_terminated_by_ld)
    g_string_append_printf(statement, "FIELDS TERMINATED BY '%s' ",fields_terminated_by_ld);
  if (fields_enclosed_by_ld)
    g_string_append_printf(statement, "ENCLOSED BY '%s' ",fields_enclosed_by_ld);
  if (fields_escaped_by)
    g_string_append_printf(statement, "ESCAPED BY '%s' ",fields_escaped_by);
  g_string_append(statement, "LINES ");
  if (lines_starting_by_ld)
    g_string_append_printf(statement, "STARTING BY '%s' ",lines_starting_by_ld);
  g_string_append_printf(statement, "TERMINATED BY '%s' (", lines_terminated_by_ld);
  GString * set_statement=append_load_data_columns(statement,fields,num_fields);
  g_string_append(statement,")");
  if (set_statement != NULL){
    g_string_append(statement,set_statement->str);
    g_string_free(set_statement,TRUE);
  }
  g_string_append(statement,";\n");
}

gboolean write_statement(FILE *load_data_file, float *filessize, GString *statement, struct db_table * dbt){
  if (!real_write_data(load_data_file, filessize, statement)) {
    g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
    return FALSE;
  }
  g_string_set_size(statement, 0);
  return TRUE;
}

gboolean write_load_data_statement(struct table_job * tj, MYSQL_FIELD *fields, guint num_fields){
  GString *statement = g_string_sized_new(statement_size);
  char * basename=g_path_get_basename(tj->dat_filename);
  initialize_sql_statement(statement);
  initialize_load_data_statement(statement, tj->dbt->table, set_names_str != NULL ? set_names_str : tj->dbt->character_set /* "BINARY"*/, basename, fields, num_fields);
  if (!write_data(tj->sql_file, statement)) {
    g_critical("Could not write out data for %s.%s", tj->dbt->database->name, tj->dbt->table);
    return FALSE;
  }
  return TRUE;
}

guint64 get_estimated_remaining_chunks_on_dbt(struct db_table *dbt){
  GList *l=dbt->chunks;
  guint64 total=0;
  while (l!=NULL){
    total+=((union chunk_step *)(l->data))->integer_step.estimated_remaining_steps;
    l=l->next;
  }
  return total;
}

guint64 get_estimated_remaining_of(GList *list){
  GList *tl=list;
  guint64 total=0;
  while (tl!=NULL){
    total+=((struct db_table *)(tl->data))->estimated_remaining_steps;
    tl=tl->next;
  }
  return total;
}

guint64 get_estimated_remaining_of_all_chunks(){
  return get_estimated_remaining_of(non_innodb_table) + get_estimated_remaining_of(innodb_table);
}



void message_dumping_data(struct thread_data *td, struct table_job *tj){
  g_message("Thread %d: dumping data for `%s`.`%s` %s %s %s %s %s %s %s %s %s into %s| Remaining jobs in this table: %"G_GINT64_FORMAT" All remaining jobs: %"G_GINT64_FORMAT,
                    td->thread_id,
                    tj->dbt->database->name, tj->dbt->table, tj->partition?tj->partition:"",
                     (tj->where || where_option   || tj->dbt->where) ? "WHERE" : "" ,      tj->where ?      tj->where : "",
                     (tj->where && where_option )                    ? "AND"   : "" ,   where_option ?   where_option : "",
                    ((tj->where || where_option ) && tj->dbt->where) ? "AND"   : "" , tj->dbt->where ? tj->dbt->where : "",
                    tj->order_by ? "ORDER BY" : "", tj->order_by ? tj->order_by : "",
                    tj->sql_filename, tj->dbt->estimated_remaining_steps, get_estimated_remaining_of_all_chunks()); // get_estimated_remaining_chunks_on_dbt(tj->dbt)); //, get_estimated_remaining_of_all_chunks());
//                    g_async_queue_length(td->conf->innodb_queue) + g_async_queue_length(td->conf->non_innodb_queue) + g_async_queue_length(td->conf->schema_queue));
}

void write_load_data_column_into_string( MYSQL *conn, gchar **column, MYSQL_FIELD field, gulong length,GString *escaped, GString *statement_row, struct function_pointer *fun_ptr_i){
    if (*column)
      fun_ptr_i->function(column,&length,fun_ptr_i);
    if (!*column) {
      g_string_append(statement_row, "\\N");
    }else if (field.type != MYSQL_TYPE_LONG && field.type != MYSQL_TYPE_LONGLONG  && field.type != MYSQL_TYPE_INT24  && field.type != MYSQL_TYPE_SHORT ){
      g_string_append(statement_row,fields_enclosed_by);
      g_string_set_size(escaped, length * 2 + 1);
//      unsigned long new_length = 
      mysql_real_escape_string(conn, escaped->str, *column, length);
//      g_string_set_size(escaped, new_length);
      m_replace_char_with_char('\\',*fields_escaped_by,escaped->str,escaped->len);
      m_escape_char_with_char(*fields_terminated_by, *fields_escaped_by, escaped->str,escaped->len);
      g_string_append(statement_row,escaped->str);
      g_string_append(statement_row,fields_enclosed_by);
    }else
      g_string_append(statement_row, *column);
}

void write_sql_column_into_string( MYSQL *conn, gchar **column, MYSQL_FIELD field, gulong length,GString *escaped, GString *statement_row, struct function_pointer *fun_ptr_i){
    /* Don't escape safe formats, saves some time */
    if (*column)
      fun_ptr_i->function(column,&length, fun_ptr_i);

    if (!*column) {
      g_string_append(statement_row, "NULL");
    } else if (field.flags & NUM_FLAG) {
      g_string_append(statement_row, *column);
    } else if ( length == 0){
      g_string_append_c(statement_row,*fields_enclosed_by);
      g_string_append_c(statement_row,*fields_enclosed_by);
    } else if ( field.type == MYSQL_TYPE_BLOB || hex_blob ) {
      g_string_set_size(escaped, length * 2 + 1);
      g_string_append(statement_row,"0x");
      mysql_hex_string(escaped->str,*column,length);
      g_string_append(statement_row,escaped->str);
    } else {
      /* We reuse buffers for string escaping, growing is expensive just at
 *        * the beginning */
      g_string_set_size(escaped, length * 2 + 1);
      mysql_real_escape_string(conn, escaped->str, *column, length);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(statement_row, "CONVERT(");
      g_string_append_c(statement_row, *fields_enclosed_by);
      g_string_append(statement_row, escaped->str);
      g_string_append_c(statement_row, *fields_enclosed_by);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(statement_row, " USING UTF8MB4)");
    }
}

void write_row_into_string(MYSQL *conn, struct db_table * dbt, MYSQL_ROW row, MYSQL_FIELD *fields, gulong *lengths, guint num_fields, GString *escaped, GString *statement_row, void write_column_into_string(MYSQL *, gchar **, MYSQL_FIELD , gulong ,GString *, GString *, struct function_pointer * )){
  guint i = 0;
  g_string_append(statement_row, lines_starting_by);
  (void) dbt;
  struct function_pointer ** f = dbt->anonymized_function;

  for (i = 0; i < num_fields-1; i++) {
    write_column_into_string( conn, &(row[i]), fields[i], lengths[i], escaped, statement_row,f==NULL?&pp:f[i]);
    g_string_append(statement_row, fields_terminated_by);
  }
  write_column_into_string( conn, &(row[i]), fields[i], lengths[i], escaped, statement_row,f==NULL?&pp:f[i]);

  g_string_append(statement_row, lines_terminated_by);

}

guint64 write_row_into_file_in_load_data_mode(MYSQL *conn, MYSQL_RES *result, struct table_job * tj){
  struct db_table * dbt = tj->dbt;
  guint64 num_rows=0;
  GString *escaped = g_string_sized_new(3000);
  guint num_fields = mysql_num_fields(result);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  MYSQL_ROW row;
  GString *statement_row = g_string_sized_new(0);
  GString *statement = g_string_sized_new(2*statement_size);

  if (update_files_on_table_job(tj)){
    write_load_data_statement(tj, fields, num_fields);
  }
  message_dumping_data(tj->td,tj);

  while ((row = mysql_fetch_row(result))) {
    gulong *lengths = mysql_fetch_lengths(result);
    num_rows++;
    if (chunk_filesize &&
        (guint)ceil((float)tj->filesize / 1024 / 1024) >
            chunk_filesize ) {
      if (!write_statement(tj->dat_file, &(tj->filesize), statement_row, dbt)) {
        return num_rows;
      }

      m_close(tj->td->thread_id, tj->sql_file, g_strdup(tj->sql_filename), 1);
      m_close(tj->td->thread_id, tj->dat_file, g_strdup(tj->dat_filename), 1);
      tj->sql_file=NULL;
      tj->dat_file=NULL;

      g_free(tj->sql_filename);
      g_free(tj->dat_filename);

      tj->sql_filename=NULL;
      tj->dat_filename=NULL;
      tj->sub_part++;

      if (update_files_on_table_job(tj)){
        write_load_data_statement(tj, fields, num_fields);
      }
      tj->st_in_file = 0;
      tj->filesize = 0;
      
      check_pause_resume(tj->td);
    }
    g_string_set_size(statement_row, 0);

    write_row_into_string(conn, dbt, row, fields, lengths, num_fields, escaped, statement_row, write_load_data_column_into_string);
    tj->filesize+=statement_row->len+1;
    g_string_append(statement, statement_row->str);
    /* INSERT statement is closed before over limit but this is load data, so we only need to flush the data to disk*/
    if (statement->len > statement_size) {
      if (!write_statement(tj->dat_file, &(tj->filesize), statement, dbt)) {
        return num_rows;
      }

    }
  }
  if (statement->len > 0){
    if (!real_write_data(tj->dat_file, &(tj->filesize), statement)) {
      g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
      return num_rows;
    }
  }
  return num_rows;
}


guint64 write_row_into_file_in_sql_mode(MYSQL *conn, MYSQL_RES *result, struct table_job * tj){
  // There are 2 possible options to chunk the files:
  // - no chunk: this means that will be just 1 data file
  // - chunk_filesize: this function will be spliting the per filesize, this means that multiple files will be created
  // Split by row is before this step
  // It could write multiple INSERT statments in a data file if statement_size is reached
  struct db_table * dbt = tj->dbt;
//  guint sections = tj->where==NULL?1:2;
  guint num_fields = mysql_num_fields(result);
  GString *escaped = g_string_sized_new(3000);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  MYSQL_ROW row;
  GString *statement = g_string_sized_new(statement_size);
  GString *statement_row = g_string_sized_new(0);
  gulong *lengths = NULL;
  guint64 num_rows = 0;
  guint64 num_rows_st = 0;
//  guint fn = tj->nchunk;
//  if (tj->sql_file == NULL)
//    initialize_sql_fn(tj);

  update_files_on_table_job(tj);
  message_dumping_data(tj->td,tj);
  if (dbt->insert_statement==NULL){
    g_mutex_lock(dbt->chunks_mutex);
    if (dbt->insert_statement==NULL)
      build_insert_statement(dbt, fields, num_fields);
    g_mutex_unlock(dbt->chunks_mutex);
  }
  while ((row = mysql_fetch_row(result))) {
    lengths = mysql_fetch_lengths(result);
    num_rows++;

    if (!statement->len) {
      // if statement->len is 0 we consider that new statement needs to be written
      // A file can be chunked by amount of rows or file size.
      if (!tj->st_in_file) {
        // File Header
        initialize_sql_statement(statement);
        if (!real_write_data(tj->sql_file, &(tj->filesize), statement)) {
          g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
          return num_rows;
        }
        g_string_set_size(statement, 0);
      }
      g_mutex_lock(dbt->chunks_mutex);
      g_string_append(statement, dbt->insert_statement->str);
      g_mutex_unlock(dbt->chunks_mutex);
      num_rows_st = 0;
    }

    if (statement_row->len) {
      // previous row needs to be written
      g_string_append(statement, statement_row->str);
      g_string_set_size(statement_row, 0);
      num_rows_st++;
    }

    write_row_into_string(conn, dbt, row, fields, lengths, num_fields, escaped, statement_row, write_sql_column_into_string);

    if (statement->len + statement_row->len + 1 > statement_size) {
      // We need to flush the statement into disk
      if (num_rows_st == 0) {
        g_string_append(statement, statement_row->str);
        g_string_set_size(statement_row, 0);
        g_warning("Row bigger than statement_size for %s.%s", dbt->database->name,
                  dbt->table);
      }
      g_string_append(statement, statement_terminated_by);

      if (!real_write_data(tj->sql_file, &(tj->filesize), statement)) {
        g_critical("Could not write out data for %s.%s", dbt->database->name, dbt->table);
        return num_rows;
      }
      tj->st_in_file++;
      if (chunk_filesize &&
          (guint)ceil((float)tj->filesize / 1024 / 1024) >
              chunk_filesize) {
        // We reached the file size limit, we need to rotate the file
//        if (sections == 1){
//          fn++;
//        }else{
          tj->sub_part++;
//        }
        m_close(tj->td->thread_id, tj->sql_file, tj->sql_filename, 1);
        tj->sql_file=NULL;
        //initialize_sql_fn(tj);
        update_files_on_table_job(tj);
        tj->st_in_file = 0;
        tj->filesize = 0;
        check_pause_resume(tj->td);
      }
      g_string_set_size(statement, 0);
    } else {
      if (num_rows_st)
        g_string_append_c(statement, ',');
      g_string_append(statement, statement_row->str);
      num_rows_st++;
      g_string_set_size(statement_row, 0);
    }
  }
  if (statement_row->len > 0) {
    /* this last row has not been written out */
    if (!statement->len)
        g_string_append(statement, dbt->insert_statement->str);
    g_string_append(statement, statement_row->str);
  }

  if (statement->len > 0) {
    g_string_append(statement, statement_terminated_by);
    if (!real_write_data(tj->sql_file, &(tj->filesize), statement)) {
      g_critical(
          "Could not write out closing newline for %s.%s, now this is sad!",
          dbt->database->name, dbt->table);
      return num_rows;
    }
    tj->st_in_file++;
  }
  g_mutex_lock(dbt->rows_lock);
  dbt->rows+=num_rows;
  g_mutex_unlock(dbt->rows_lock);
  g_string_free(statement, TRUE);
  g_string_free(escaped, TRUE);
  g_string_free(statement_row, TRUE);
  return num_rows;
}

/* Do actual data chunk reading/writing magic */
guint64 write_table_data_into_file(MYSQL *conn, struct table_job * tj){
  guint64 num_rows = 0;
//  guint64 num_rows_st = 0;
  MYSQL_RES *result = NULL;
  char *query = NULL;

  /* Ghm, not sure if this should be statement_size - but default isn't too big
   * for now */
  /* Poor man's database code */
  query = g_strdup_printf(
      "SELECT %s %s FROM `%s`.`%s` %s %s %s %s %s %s %s %s %s %s %s",
      (detected_server == SERVER_TYPE_MYSQL || detected_server == SERVER_TYPE_MARIADB) ? "/*!40001 SQL_NO_CACHE */" : "",
      tj->dbt->select_fields->str,
      tj->dbt->database->name, tj->dbt->table, tj->partition?tj->partition:"",
       (tj->where || where_option   || tj->dbt->where) ? "WHERE"  : "" ,      tj->where ?      tj->where : "",
       (tj->where && where_option )                    ? "AND"    : "" ,   where_option ?   where_option : "",
      ((tj->where || where_option ) && tj->dbt->where) ? "AND"    : "" , tj->dbt->where ? tj->dbt->where : "",
      tj->order_by ? "ORDER BY" : "", tj->order_by   ? tj->order_by   : "",
      tj->dbt->limit ?  "LIMIT" : "", tj->dbt->limit ? tj->dbt->limit : ""
  );
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    // ERROR 1146
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping table (%s.%s) data: %s\nQuery: %s", tj->dbt->database->name, tj->dbt->table,
                mysql_error(conn), query);
    } else {
      g_critical("Error dumping table (%s.%s) data: %s\nQuery: %s ", tj->dbt->database->name, tj->dbt->table,
                 mysql_error(conn), query);
      errors++;
    }
    goto cleanup;
  }

  /* Poor man's data dump code */
  if (load_data)
    num_rows = write_row_into_file_in_load_data_mode(conn, result, tj);
  else
    num_rows=write_row_into_file_in_sql_mode(conn, result, tj);

  if (mysql_errno(conn)) {
    g_critical("Could not read data from %s.%s: %s", tj->dbt->database->name, tj->dbt->table,
               mysql_error(conn));
    errors++;
  }

cleanup:
  g_free(query);

  if (result) {
    mysql_free_result(result);
  }

  return num_rows;
}

