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

#include <mysql.h>
#include <errmsg.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "myloader.h"
#include "myloader_common.h"
#include "myloader_global.h"
#include "myloader_intermediate_queue.h"
#include "myloader_process.h"
#include "myloader_restore.h"

struct statement * new_statement();
gboolean skip_definer = FALSE;
GAsyncQueue *connection_pool = NULL;
GAsyncQueue *restore_queues=NULL;
GAsyncQueue *free_results_queue=NULL;

void *restore_thread(MYSQL *thrconn);
struct statement release_connection_statement = {0, 0, NULL, NULL, CLOSE, FALSE, NULL, 0};
struct io_restore_result end_restore_thread = { NULL, NULL};

GThread **restore_threads=NULL;

struct connection_data *new_connection_data(MYSQL *thrconn){
  struct connection_data *cd=g_new(struct connection_data,1);
  if (thrconn)
    cd->thrconn = thrconn;
  else{
    cd->thrconn = mysql_init(NULL);
    m_connect(cd->thrconn);
  }
  cd->current_database=NULL;
  cd->thread_id=mysql_thread_id(cd->thrconn);
  cd->ready=g_async_queue_new();
  cd->queue=NULL;
  cd->in_use=g_mutex_new();
  g_message("Executing set session");
  execute_gstring(cd->thrconn, set_session);
  g_async_queue_push(connection_pool,cd);
  return cd;
}
struct io_restore_result *new_io_restore_result(){
  struct io_restore_result *iors=g_new(struct io_restore_result,1);
  iors->result=g_async_queue_new();
  iors->restore=g_async_queue_new();
  return iors;
}

gchar *ignore_errors=NULL;

void initialize_connection_pool(){
  guint n=0;
  if (ignore_errors){
    gchar **tmp_ignore_errors_list = g_strsplit(ignore_errors, ",", 0);
    while(tmp_ignore_errors_list[n]!=NULL){
      ignore_errors_list=g_list_append(ignore_errors_list,GINT_TO_POINTER(atoi(tmp_ignore_errors_list[n])));
      n++;
    }
  }
  connection_pool=g_async_queue_new();
  restore_queues=g_async_queue_new();
  free_results_queue=g_async_queue_new();
  struct io_restore_result *iors=NULL;
  restore_threads=g_new(GThread *, num_threads);
  for (n = 0; n < num_threads; n++) {
    iors=new_io_restore_result();
    g_async_queue_push(restore_queues, iors);
//    restore_threads[n]=g_thread_new("myloader_conn",(GThreadFunc)restore_thread, thrconn);
//    thrconn=NULL;
  }
  for (n = 0; n < 8*num_threads; n++) {
    g_async_queue_push(free_results_queue, new_statement());
  }
}

void start_connection_pool(MYSQL *thrconn){
  guint n=0;
  for (n = 0; n < num_threads; n++) {
    restore_threads[n]=g_thread_new("myloader_conn",(GThreadFunc)restore_thread, thrconn);
    thrconn=NULL;
  }
}

void wait_restore_threads_to_close(){
  guint n=0;
  for (n = 0; n < num_threads; n++)
    g_thread_join(restore_threads[n]);
}

void reconnect_connection_data(struct connection_data *cd){
  mysql_close(cd->thrconn);
  cd->thrconn=mysql_init(NULL);
  m_connect(cd->thrconn);
  cd->thread_id=mysql_thread_id(cd->thrconn);
  execute_use(cd);
  execute_gstring(cd->thrconn, set_session);
}

int restore_data_in_gstring_by_statement(struct connection_data *cd, GString *data, gboolean is_schema, guint *query_counter)
{
  guint en=mysql_real_query(cd->thrconn, data->str, data->len);
  if (en) {
    if (is_schema)
      g_warning("Connection %ld - ERROR %d: %s\n%s", cd->thread_id, mysql_errno(cd->thrconn), mysql_error(cd->thrconn), data->str);
    else{
      g_warning("Connection %ld - ERROR %d: %s"    , cd->thread_id, mysql_errno(cd->thrconn), mysql_error(cd->thrconn));
    }

    if ( mysql_errno(cd->thrconn) != 0 && !g_list_find(ignore_errors_list, GINT_TO_POINTER(mysql_errno(cd->thrconn) ))){
      if (mysql_ping(cd->thrconn)) {
        reconnect_connection_data(cd);
        if (!is_schema && commit_count > 1) {
          g_critical("Connection %ld - ERROR %d: Lost connection error. %s", cd->thread_id,  mysql_errno(cd->thrconn), mysql_error(cd->thrconn));
          errors++;
          return 2;
        }
      }

      g_atomic_int_inc(&(detailed_errors.retries));
      if (mysql_real_query(cd->thrconn, data->str, data->len)) {
        if (is_schema)
          g_critical("Connection %ld - ERROR %d: %s\n%s", cd->thread_id, mysql_errno(cd->thrconn), mysql_error(cd->thrconn), data->str);
        else{
          g_critical("Connection %ld - ERROR %d: %s"    , cd->thread_id, mysql_errno(cd->thrconn), mysql_error(cd->thrconn));
        }
        errors++;
        return 1;
      }
    }
  }
  *query_counter=*query_counter+1;
  g_string_set_size(data, 0);
  return 0;
}

struct connection_data *close_restore_thread(gboolean return_connection){
  struct connection_data *cd=g_async_queue_pop(connection_pool);
  g_async_queue_push(cd->ready, &end_restore_thread);
  if (return_connection)
    return cd;
  return NULL;
}


void setup_connection(struct connection_data *cd, struct thread_data *td, struct io_restore_result *io_restore_result , gboolean start_transaction, struct database *use_database, GString *header){
  trace("Thread %d: Connection %ld granted", td->thread_id, cd->thread_id);
  if (mysql_ping(cd->thrconn)) {
    g_warning("Thread %d: Connection %ld failed", td->thread_id, cd->thread_id);
    reconnect_connection_data(cd);
    g_warning("Thread %d: New connection %ld established", td->thread_id, cd->thread_id);
  }
  cd->transaction=start_transaction;
  if (use_database)
    execute_use_if_needs_to(cd, use_database, "request_another_connection");
  if (td){
    td->granted_connections++;
  }

  if (cd->transaction){
    m_query(cd->thrconn, "START TRANSACTION", m_warning, "START TRANSACTION failed");
  }
  cd->queue = io_restore_result;
  if (header)
    execute_gstring(cd->thrconn,header);
  g_async_queue_push(cd->ready, cd->queue);
}

struct connection_data *wait_for_available_restore_thread(struct thread_data *td, gboolean start_transaction, struct database *use_database){
  struct connection_data *cd=g_async_queue_pop(connection_pool);
  setup_connection(cd,td,g_async_queue_pop(restore_queues), start_transaction, use_database, NULL);
  return cd;
}

extern gboolean control_job_ended;
gboolean request_another_connection(struct thread_data *td, struct io_restore_result *io_restore_result, gboolean start_transaction, struct database *use_database, GString *header){
  if ( control_job_ended && td->granted_connections < td->dbt->max_threads && g_list_length(td->dbt->restore_job_list)==0 ){
    g_assert(header);
    struct connection_data *cd=g_async_queue_try_pop(connection_pool);
    if(cd){
      setup_connection(cd,td,io_restore_result,start_transaction, use_database, header);
      return TRUE;
    }
  }
  return FALSE;
}


int m_commit(struct connection_data *cd){
  if (!m_query(cd->thrconn, "COMMIT", m_warning, "COMMIT failed")) {
    errors++;
    return 2;
  }
  return 0;
}

int m_commit_and_start_transaction(struct connection_data *cd, guint* query_counter){
  int e=m_commit(cd);
  if (e) return e;
  *query_counter=0;
  m_query(cd->thrconn, "START TRANSACTION", m_warning, "START TRANSACTION failed");
  return 0;
}

int restore_insert(struct connection_data *cd,
                  GString *data, guint *query_counter, guint offset_line)
{
  char *next_line=g_strstr_len(data->str,-1,"VALUES") + 6;
  char *insert_statement_prefix=g_strndup(data->str,next_line - data->str);
  int r=0;
  guint tr=0,current_offset_line=offset_line-1;
  gchar *current_line=next_line;
  next_line=g_strstr_len(current_line, -1, "\n");
  GString * new_insert=g_string_sized_new(strlen(insert_statement_prefix));
  guint current_rows=0;
  do {
    current_rows=0;
    g_string_set_size(new_insert, 0);
    new_insert=g_string_append(new_insert,insert_statement_prefix);
    guint line_len=0;
    do {
      char *line=g_strndup(current_line, next_line - current_line);
      line_len=strlen(line);
      g_string_append(new_insert, line);
      g_free(line);
      current_rows++;
      current_line=next_line+1;
      next_line=g_strstr_len(current_line, -1, "\n");
      current_offset_line++;
    } while ((rows == 0 || current_rows < rows) && next_line != NULL);
    if (current_rows > 1 || (current_rows==1 && line_len>0) ){
      tr=restore_data_in_gstring_by_statement(cd, new_insert, FALSE, query_counter);

      if (cd->transaction && *query_counter == commit_count) {
        tr+=m_commit_and_start_transaction(cd,query_counter);
      }

      if (tr > 0){
        g_error("Connection %ld: Error occurs between lines: %d and %d in a splited INSERT: %s",cd->thread_id, offset_line,current_offset_line,mysql_error(cd->thrconn));
      }
      if (mysql_warning_count(cd->thrconn)){
        g_warning("Connection %ld: Warnings found during INSERT between lines: %d and %d: %s",cd->thread_id, offset_line,current_offset_line, show_warnings_if_possible(cd->thrconn));
      }
    }else
      tr=0;
    r+=tr;
    offset_line=current_offset_line+1;
    current_line++; // remove trailing ,
  } while (next_line != NULL);
  cd=NULL;
  g_string_free(new_insert,TRUE);
  g_free(insert_statement_prefix);
  return r;
}



void *restore_thread(MYSQL *thrconn){
  struct connection_data *cd=new_connection_data(thrconn);
  struct statement *ir=NULL;
  guint query_counter=0;
//  g_mutex_lock(cd->in_use);
  while (1){
    cd->queue=g_async_queue_pop(cd->ready);
    if (cd->queue->restore == NULL)
      break;
    while(1) {
      ir=g_async_queue_pop(cd->queue->restore);
      if (ir->kind_of_statement == CLOSE){
        trace("Releasing connection: %ld", cd->thread_id);
        if (cd->transaction && query_counter > 0)
          m_commit(cd);
        g_async_queue_push(cd->queue->result,ir);
        cd->queue=NULL;
        ir=NULL;
        break;
      }
      if (ir->kind_of_statement==INSERT){
        ir->result=restore_insert(cd, ir->buffer, &query_counter,ir->preline);
        if (ir->result>0){
          ir->error=g_strdup(mysql_error(cd->thrconn));
          ir->error_number=mysql_errno(cd->thrconn);
          // FIXME: CLI option for max_errors (and AUTO for --identifier-quote-character), test
          // TODO: max_errrors is not being used at the moment
    	    if (max_errors && errors > max_errors) {
            if (ir->filename==NULL){
              m_critical("Error occurs processing statement: %s",mysql_error(cd->thrconn));
            }else{
              m_critical("Error occurs starting at line: %d on file %s: %s",ir->preline,ir->filename,mysql_error(cd->thrconn));
            }
          } else {
            if (ir->filename==NULL){
              g_critical("Error occurs processing statement: %s",mysql_error(cd->thrconn));
            }else{
              g_critical("Error occurs between lines: %d on file %s: %s",ir->preline,ir->filename,mysql_error(cd->thrconn));
            }
          }
        }
        g_async_queue_push(cd->queue->result,ir);
      }else{
        ir->result=restore_data_in_gstring_by_statement(cd, ir->buffer, ir->is_schema, &query_counter);
        if (ir->result>0){
          ir->error=g_strdup(mysql_error(cd->thrconn));
          ir->error_number=mysql_errno(cd->thrconn);
        }
        g_async_queue_push(cd->queue->result,ir);
      }
    }
    trace("Returning connection to pool: %ld", cd->thread_id);
    g_async_queue_push(connection_pool,cd);
  }
//  g_mutex_unlock(cd->in_use);
  return NULL;
}



gboolean load_data_mutex_locate( gchar * filename , GMutex ** mutex){
  g_mutex_lock(load_data_list_mutex);
  gchar * orig_key=NULL;
  if (!g_hash_table_lookup_extended(load_data_list,filename, (gpointer*) orig_key, (gpointer*) *mutex)){
    *mutex=g_mutex_new();
    g_mutex_lock(*mutex);
    g_hash_table_insert(load_data_list, g_strdup(filename), *mutex);
    g_mutex_unlock(load_data_list_mutex);
    return TRUE;
  }
  if (orig_key!=NULL){
    g_hash_table_remove(load_data_list, orig_key);
//    g_mutex_free(*mutex);
  }
  g_mutex_unlock(load_data_list_mutex);
  return FALSE;
}

void release_load_data_as_it_is_close( gchar * filename ){
  g_mutex_lock(load_data_list_mutex);
  GMutex *mutex = g_hash_table_lookup(load_data_list,filename);
  if (mutex == NULL){
    g_hash_table_insert(load_data_list,g_strdup(filename), NULL);
  }else{
    g_mutex_unlock(mutex);
  }
  g_mutex_unlock(load_data_list_mutex);
}

void free_statement(struct statement *statement){
  g_string_free(statement->buffer, TRUE);
  g_free(statement->error);
  g_free(statement);
}


struct statement *initialize_statement(struct statement *ir){
  ir->result=0;
  ir->error_number=0;
  ir->error=NULL;
  return ir;
}

struct statement * new_statement(){
  struct statement *stmt= g_new0(struct statement, 1);
  initialize_statement(stmt);
  stmt->filename=NULL;
  stmt->buffer=g_string_new_len("",30);
  return stmt;
}

void assing_statement(struct statement *ir, gchar *stmt, guint preline, gboolean is_schema, enum kind_of_statement kind_of_statement){
  initialize_statement(ir);
  g_assert(stmt); 
  g_string_assign(ir->buffer,stmt);
  ir->preline=preline;
  ir->is_schema=is_schema;
  ir->kind_of_statement=kind_of_statement;
}


guint process_result_vstatement_pop(GAsyncQueue * get_insert_result_queue, struct statement **ir, void log_fun(const char *, ...) , const char *fmt, va_list args, void * g_async_queue_pop_fun(GAsyncQueue *) ){
  *ir=g_async_queue_pop_fun(get_insert_result_queue);
  if (*ir==NULL)
    return 0;
  if ((*ir)->kind_of_statement!=CLOSE && (*ir)->result>0){
    gchar *c=g_strdup_vprintf(fmt,args);
    log_fun("%s: %s (%d)", c, (*ir)->error, (*ir)->error_number);
    g_free(c);
  }
  return (*ir)->result;
}

guint process_result_vstatement(GAsyncQueue * get_insert_result_queue, struct statement **ir, void log_fun(const char *, ...) , const char *fmt, va_list args){
  return process_result_vstatement_pop(get_insert_result_queue,ir,log_fun,fmt,args,g_async_queue_pop);
}

guint process_result_statement(GAsyncQueue * get_insert_result_queue, struct statement **ir, void log_fun(const char *, ...) , const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  guint r=process_result_vstatement(get_insert_result_queue,ir, log_fun, fmt, args);
  va_end(args);
  return r;
}

int restore_data_from_file(struct thread_data *td, const char *filename, gboolean is_schema, struct database *use_database){

  FILE *infile=NULL;
  gboolean eof = FALSE;
  GString *data = g_string_sized_new(256);
  guint line=0,preline=0;
  gchar *path = g_build_filename(directory, filename, NULL);
  infile=myl_open(path,"r");

  g_log_set_always_fatal(G_LOG_LEVEL_ERROR|G_LOG_LEVEL_CRITICAL);

  if (!infile) {
    g_critical("cannot open file %s (%d)", filename, errno);
    errors++;
    return 1;
  }
  guint r=0;
  gchar *load_data_filename=NULL;
  gchar *load_data_fifo_filename=NULL;
  gchar *new_load_data_fifo_filename=NULL;
  struct connection_data *cd=wait_for_available_restore_thread(td, !is_schema && (commit_count > 1), use_database );
  g_assert(g_async_queue_length(cd->queue->restore)<=0);
  g_assert(g_async_queue_length(cd->queue->result)<=0);
  guint i=0;
//  GAsyncQueue *local_result_statement_queue=g_async_queue_new();

//  g_async_queue_push(local_result_statement_queue,ir);
  struct statement *ir=g_async_queue_pop(free_results_queue);
  gboolean results_added=FALSE;
  //  g_assert(ir->kind_of_statement!=CLOSE);
  GString *header=g_string_sized_new(256);
  while (eof == FALSE) {
    if (read_data(infile, data, &eof, &line)) {
      if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {
        if ( skip_definer && g_str_has_prefix(data->str,"CREATE")){
          remove_definer(data);
        }
        if ( g_strrstr_len(data->str,6,"INSERT")){
          request_another_connection(td, cd->queue, cd->transaction, use_database, header);
          if (!results_added){
            results_added=TRUE;
            struct statement * other_ir=NULL;
            for(i=0;i<7;i++){
              other_ir=g_async_queue_pop(free_results_queue);
              g_async_queue_push(cd->queue->result,initialize_statement(other_ir));
            }
          } 
          assing_statement(ir, data->str, preline, FALSE, INSERT);
/*          initialize_statement(ir);
          GString *tmp=data;
          data=ir->buffer;
          ir->buffer=tmp;
          ir->preline=preline;
          ir->is_schema=FALSE;
          ir->kind_of_statement=INSERT;
*/
          g_async_queue_push(cd->queue->restore, ir);
          ir=NULL;
          process_result_statement(cd->queue->result, &ir, m_critical, "(2)Error occurs processing file %s", filename);
        }else if (g_strrstr_len(data->str,10,"LOAD DATA ")){
//          ir=g_async_queue_pop(local_result_statement_queue);
          GString *new_data = NULL;
          gchar *from = g_strstr_len(data->str, -1, "'");
          from++;
          gchar *to = g_strstr_len(from, -1, "'");
          load_data_filename=g_strndup(from, to-from);
          GMutex * mutex=NULL;
          if (load_data_mutex_locate(load_data_filename, &mutex))
            g_mutex_lock(mutex);
	      // TODO we need to free filename and mutex from the hash.
          gchar **command=NULL;
          gboolean is_fifo = get_command_and_basename(load_data_filename, &command, &load_data_fifo_filename);
          if (is_fifo){ 
            if (fifo_directory != NULL){
              new_data = g_string_new_len(data->str, from - data->str);
              g_string_append(new_data, fifo_directory);
              g_string_append_c(new_data, '/');
              g_string_append(new_data, from);
              from = g_strstr_len(new_data->str, -1, "'") + 1;
              g_string_free(data, TRUE);
              data=new_data;
              to = g_strstr_len(from, -1, "'");
            }
            guint a=0;
            for(;a<strlen(load_data_filename)-strlen(load_data_fifo_filename);a++){
              *to=' '; to--;
            }
            *to='\'';

            if (fifo_directory != NULL){
              new_load_data_fifo_filename=g_strdup_printf("%s/%s", fifo_directory, load_data_fifo_filename);
              g_free(load_data_fifo_filename);
              load_data_fifo_filename=new_load_data_fifo_filename;
            }
            if (mkfifo(load_data_fifo_filename,0666)){
              g_critical("cannot create named pipe %s (%d)", load_data_fifo_filename, errno);
            }
            execute_file_per_thread(load_data_filename, load_data_fifo_filename, command );
            release_load_data_as_it_is_close(load_data_fifo_filename);
//              g_free(fifo_name);
          }

          assing_statement(ir, data->str, preline, FALSE, OTHER);
          g_async_queue_push(cd->queue->restore,ir);
          ir=NULL;
          process_result_statement(cd->queue->result, &ir, m_critical, "(2)Error occurs processing file %s", filename);
          if (is_fifo) 
            m_remove0(NULL, load_data_fifo_filename);
          else
            m_remove(NULL, load_data_filename);
        }else{
          if (g_strrstr_len(data->str,3,"/*!")){
            gchar *from_equal=g_strstr_len(data->str, strlen(data->str),"=");
            if (from_equal && ignore_set_list ){
              *from_equal='\0';
              if (!is_in_ignore_set_list(data->str)) {
                *from_equal='=';
                g_string_append(header,data->str);
              }else{
                *from_equal='=';
              }
            }else{
              g_string_append(header,data->str);
            }
          }else{
            header=NULL;
          }
          assing_statement(ir,data->str, preline, is_schema, OTHER);
          g_async_queue_push(cd->queue->restore,ir);
          ir=NULL;
          process_result_statement(cd->queue->result, &ir, m_critical, "(2)Error occurs processing file %s", filename);
        }
        r|= ir->result;

        g_string_set_size(data, 0);
        preline=line+1;
        if (ir->result>0){
          g_critical("(1)Error occurs processing file %s",filename);
        }
      }
    } else {
      g_critical("error reading file %s (%d)", filename, errno);
      errors++;
      return errno;
    }
  }
  struct io_restore_result *queue= cd->queue;
  g_async_queue_push(free_results_queue,ir);
  if (results_added){
    for(i=0;i<7;i++){
      process_result_statement(queue->result, &ir, m_critical, "(2)Error occurs processing file %s", filename);
      g_assert(ir->kind_of_statement!=CLOSE);
      g_async_queue_push(free_results_queue,ir);
    }
  }
  for(;td->granted_connections>0;td->granted_connections--){
    g_async_queue_push(queue->restore,&release_connection_statement);
    process_result_statement(queue->result, &ir, m_critical, "(2)Error occurs processing file %s", filename);
    g_assert(ir->kind_of_statement==CLOSE);
  }
  g_async_queue_push(restore_queues, queue);

  g_string_free(data, TRUE);
  g_free(load_data_filename);

  myl_close(filename, infile, TRUE);
  g_free(path);
  return r;
}

// return 0 means everything was ok
int restore_data_in_gstring_extended(struct thread_data *td, GString *data, gboolean is_schema, struct database *use_database, void log_fun(const char *, ...) , const char *fmt, ...){
  va_list    args;
  va_start(args, fmt);
  struct connection_data *cd=wait_for_available_restore_thread(td, !is_schema && (commit_count > 1), use_database );
  struct io_restore_result *queue= cd->queue;
  cd=NULL;
  struct statement *ir=g_async_queue_pop(free_results_queue);
  int i=0;
  int r=0;
  if (data != NULL && data->len > 4){
    gchar** line=g_strsplit(data->str, ";\n", -1);
    for (i=0; i < (int)g_strv_length(line);i++){
       if (strlen(line[i])>2){
          assing_statement(ir, line[i], 0, is_schema, OTHER);
          if(ir->error)
            g_free(ir->error);
          ir->error=NULL;
          g_async_queue_push(queue->restore,ir);
          r+=process_result_vstatement(queue->result, &ir, log_fun, fmt, args);
       }
    }
    g_strfreev(line);
  }
  g_async_queue_push(free_results_queue,ir);
  g_async_queue_push(queue->restore,&release_connection_statement);
  td->granted_connections--;
  r+=process_result_vstatement(queue->result, &ir, log_fun, fmt, args);
  g_assert(g_async_queue_length(queue->restore)<=0);
  g_assert(g_async_queue_length(queue->result)<=0);
  g_async_queue_push(restore_queues, queue);
  va_end(args);
  return r;
}


int restore_data_in_gstring(struct thread_data *td, GString *data, gboolean is_schema, struct database *use_database){
  return restore_data_in_gstring_extended(td, data, is_schema, use_database, m_warning, "Failed to execute statement", NULL);
}
