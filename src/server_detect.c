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

    Authors:        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)
*/

#include <glib.h>
#include <string.h>
#include "server_detect.h"
#include "common.h"

int product=SERVER_TYPE_UNKNOWN;
int major=0;
int secondary=0;
int revision=0;

const gchar *start_replica=NULL;
const gchar *stop_replica=NULL;
const gchar *start_replica_sql_thread=NULL;
const gchar *stop_replica_sql_thread=NULL;
const gchar *reset_replica=NULL;
const gchar *show_replica_status=NULL;
const gchar *show_all_replicas_status=NULL;
const gchar *show_binary_log_status=NULL;
const gchar *change_replication_source=NULL;
const gchar *case_sensitive_prefix=NULL;
const gchar *case_sensitive_suffix=NULL;

extern gchar *server_version_arg;

int get_product(){
  return product;
}
int get_major(){
    return major;
}
int get_secondary(){
      return secondary;
}
int get_revision(){
      return revision;
}

const gchar * get_product_name(){
  switch (get_product()){
    case SERVER_TYPE_PERCONA:   return "Percona"; break;
    case SERVER_TYPE_MYSQL:     return "MySQL";   break;
    case SERVER_TYPE_MARIADB:   return "MariaDB"; break;
    case SERVER_TYPE_TIDB:      return "TiDB"; break;
    case SERVER_TYPE_CLICKHOUSE:return "Clickhouse"; break;
    case SERVER_TYPE_DOLT:      return "Dolt"; break;
    case SERVER_TYPE_RDS:       return "RDS"; break;
    case SERVER_TYPE_UNKNOWN:   return "unknown"; break;
    default: return "";
  }
}

gboolean is_mysql_like(){
  return get_product() == SERVER_TYPE_PERCONA || get_product() == SERVER_TYPE_MARIADB || get_product() == SERVER_TYPE_MYSQL || get_product() == SERVER_TYPE_DOLT || get_product() == SERVER_TYPE_UNKNOWN || get_product() == SERVER_TYPE_RDS;
}

gboolean server_support_tablespaces(){ 
  return get_product() == SERVER_TYPE_PERCONA || get_product() == SERVER_TYPE_MYSQL || get_product() == SERVER_TYPE_UNKNOWN || get_product() == SERVER_TYPE_RDS;
}

static
void detect_product(gchar *_ascii_version_comment, gchar *_ascii_version){
  gchar *ascii_version=_ascii_version?g_ascii_strdown(_ascii_version,-1):NULL;
  gchar *ascii_version_comment=_ascii_version_comment?g_ascii_strdown(_ascii_version_comment,-1):NULL;

  if ( (ascii_version && g_strstr_len(ascii_version, -1, "percona") ) || (ascii_version_comment && g_strstr_len(ascii_version_comment, -1, "percona"))){
    product = SERVER_TYPE_PERCONA;
  }else
  if ( (ascii_version && g_strstr_len(ascii_version, -1, "mariadb")) || (ascii_version_comment && g_strstr_len(ascii_version_comment, -1, "mariadb"))){
    product = SERVER_TYPE_MARIADB;
  }else
  if ( (ascii_version && g_strstr_len(ascii_version, -1, "tidb"))    || (ascii_version_comment && g_strstr_len(ascii_version_comment, -1, "tidb"))){
    product = SERVER_TYPE_TIDB;
  }else
  if ( (ascii_version && g_strstr_len(ascii_version, -1, "dolt"))    || (ascii_version_comment && g_strstr_len(ascii_version_comment, -1, "dolt"))){
    product = SERVER_TYPE_DOLT;
  }else
  if ( (ascii_version && g_strstr_len(ascii_version, -1, "mysql"))   || (ascii_version_comment && g_strstr_len(ascii_version_comment, -1, "mysql")) || 
       (ascii_version && g_strstr_len(ascii_version, -1, "source"))  || (ascii_version_comment && g_strstr_len(ascii_version_comment, -1, "source"))){
    product = SERVER_TYPE_MYSQL;
  }
}

static
void detect_version(gchar ** sver){
  major=strtol(sver[0], NULL, 10);
  secondary=strtol(sver[1], NULL, 10);
  revision=strtol(sver[2], NULL, 10);
}

static
void detect_server_version(MYSQL * conn) {
  struct M_ROW *mr = m_store_result_row(conn, "SELECT @@version_comment, @@version",m_warning, m_message, "Not able to determine database version", NULL);
  //struct M_ROW *mr = m_store_result_row(conn, "SELECT '4f582c03', '8.4.42' ",m_warning, m_message, "Not able to determine database version", NULL);
  //struct M_ROW *mr = m_store_result_row(conn, "SELECT 'Source distribution', '8.0.40-azure' ",m_warning, m_message, "Not able to determine database version", NULL);

  gchar *ascii_version_comment=NULL;
  gchar *_version=NULL;
  if (mr->row){
    ascii_version_comment=g_ascii_strdown(mr->row[0],-1);
    detect_product(mr->row[0],mr->row[1]);
    _version=g_strdup(mr->row[1]);
  }

	gchar ** sver=NULL;
  if (product == SERVER_TYPE_UNKNOWN){
    m_store_result_row_free(mr);

    mr = m_store_result_row(conn, "SHOW DATABASES LIKE 'system'",m_warning,m_message,"Not able to show database 'system'", NULL);
    if (mr->row){
      m_store_result_row_free(mr);

      mr = m_store_result_row(conn, "SELECT value FROM system.build_options where name='VERSION_FULL' LIMIT 1",m_warning,m_message,"Not able to determine database version", NULL);
      if (mr->row){
        gchar * ascii_version=g_ascii_strdown(mr->row[0],-1);
        gchar ** psver=g_strsplit(ascii_version," ",2);
        if (g_strstr_len(ascii_version, -1, "clickhouse") || g_strstr_len(ascii_version_comment, -1, "clickhouse")){
          product = SERVER_TYPE_CLICKHOUSE;
        sver=g_strsplit(psver[1],".",4);
        }
        g_strfreev(psver);
        g_free(ascii_version);
        goto cleanup;
      }
    }
    m_store_result_row_free(mr);
    mr = m_store_result_row(conn, "SHOW GLOBAL VARIABLES LIKE 'aurora_version'",m_warning, m_message, "Not able to determine if it is an Aurora database", NULL);
//    mr = m_store_result_row(conn, "SHOW GLOBAL VARIABLES LIKE 'version'",m_warning, m_message, "Not able to determine if it is an Aurora database", NULL);
    if (mr->row){
       product=SERVER_TYPE_RDS;
       sver=g_strsplit(_version,".",3);
    }else
      sver=g_strsplit("0.0.0",".",3);
  }else
    sver=g_strsplit(mr->row[1],".",3);
  g_free(_version);
cleanup:  m_store_result_row_free(mr);

  detect_version(sver);

  g_strfreev(sver);
  g_free(ascii_version_comment);
}

static
void detect_lower_case_table_names(MYSQL * conn) {
  guint lower_case_table_names=0;
  struct M_ROW *mr = m_store_result_row(conn, "SELECT @@lower_case_table_names",m_warning, m_message, "Not able to determine lower_case_table_names", NULL);
  if (mr->row)
    lower_case_table_names=atoi(mr->row[0]);
  if (lower_case_table_names){
    case_sensitive_prefix=CAST;
    case_sensitive_suffix=AS_BINARY;
  }else{
    case_sensitive_prefix=EMPTY_STRING;
    case_sensitive_suffix=EMPTY_STRING;
  }
  m_store_result_row_free(mr);
}

static
void detect_replica() {
  show_replica_status=SHOW_SLAVE_STATUS;
  show_binary_log_status=SHOW_MASTER_STATUS;

  if (source_control_command==TRADITIONAL){
    start_replica=START_SLAVE;
    stop_replica=STOP_SLAVE;
    start_replica_sql_thread=START_SLAVE_SQL_THREAD;
    stop_replica_sql_thread=STOP_SLAVE_SQL_THREAD;
    reset_replica=RESET_SLAVE;
    change_replication_source=CHANGE_MASTER;
    switch (get_product()){
      case SERVER_TYPE_MARIADB:
        if (get_major()<10){
          show_all_replicas_status=SHOW_ALL_SLAVES_STATUS;
          if (get_secondary()>=5)
            if (get_revision()>=2)
              show_binary_log_status=SHOW_BINLOG_STATUS;
        }else {
          if (get_secondary()<=5){
            show_all_replicas_status=SHOW_ALL_SLAVES_STATUS;
          }else{
            start_replica=START_REPLICA;
            stop_replica=STOP_REPLICA;
            start_replica_sql_thread=START_REPLICA_SQL_THREAD;
            stop_replica_sql_thread=STOP_REPLICA_SQL_THREAD;
            reset_replica=RESET_REPLICA;
            show_replica_status=SHOW_REPLICA_STATUS;
            show_all_replicas_status=SHOW_ALL_REPLICAS_STATUS;
          }
        } 
        break;
      case SERVER_TYPE_MYSQL:
      case SERVER_TYPE_RDS:
      case SERVER_TYPE_PERCONA:
      case SERVER_TYPE_UNKNOWN:
        if (get_major()>=8 && (get_secondary()>0 || (get_secondary()==0 && get_revision()>=22))) {
            start_replica=START_REPLICA;
            stop_replica=STOP_REPLICA;
            start_replica_sql_thread=START_REPLICA_SQL_THREAD;
            stop_replica_sql_thread=STOP_REPLICA_SQL_THREAD;
            reset_replica=RESET_REPLICA;
            show_replica_status=SHOW_REPLICA_STATUS;
            if (get_secondary()>=2)
              show_binary_log_status=SHOW_BINARY_LOG_STATUS;
            change_replication_source=CHANGE_REPLICATION_SOURCE;
        }
        break;
      case SERVER_TYPE_DOLT:
        if (get_major()>=8 && get_secondary()>=0) {
            start_replica=START_REPLICA;
            stop_replica=STOP_REPLICA;
            start_replica_sql_thread=START_REPLICA_SQL_THREAD;
            stop_replica_sql_thread=STOP_REPLICA_SQL_THREAD;
            reset_replica=RESET_REPLICA;
            show_replica_status=SHOW_REPLICA_STATUS;
            if (get_secondary()>=2)
              show_binary_log_status=SHOW_BINARY_LOG_STATUS;
            change_replication_source=CHANGE_REPLICATION_SOURCE;
        }
        break;
    }
  }else{
    start_replica=CALL_START_REPLICATION;
    start_replica_sql_thread=CALL_START_REPLICATION;
    stop_replica=CALL_STOP_REPLICATION;
    stop_replica_sql_thread=CALL_STOP_REPLICATION;
    reset_replica=CALL_RESET_EXTERNAL_MASTER;
  }
}

void server_detect(MYSQL * conn){
  if (server_version_arg){
    gchar ** _product=g_strsplit(server_version_arg,"-",2);
    if (g_strv_length(_product) != 2){
      m_error("Not able to correctly determine the product and version which should be <product>-<version> where version will 3 number delimited by dots");
    }
    detect_product(_product[0],_product[1]);
    if (_product[1]){
      gchar ** sver=g_strsplit(_product[1],".",3);
      if (g_strv_length(sver) != 3){
        m_error("Not able to correctly determine the product and version which should be <product>-<version> where version will 3 number delimited by dots");
      }
      detect_version(sver);
      g_strfreev(sver);
    }
    g_strfreev(_product);
  }else
    detect_server_version(conn);
  detect_lower_case_table_names(conn);
  detect_replica();  
}


