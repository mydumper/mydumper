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

#include <pcre.h>
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

gboolean is_mysql_like(){
  return get_product() == SERVER_TYPE_PERCONA || get_product() == SERVER_TYPE_MARIADB || get_product() == SERVER_TYPE_MYSQL || get_product() == SERVER_TYPE_UNKNOWN;
}

void detect_server_version(MYSQL * conn) {
  if (mysql_query(conn, "SELECT @@version_comment, @@version")){
    g_warning("Not able to determine database version: %s",
                 mysql_error(conn));
    return;
  }

  MYSQL_RES *res = mysql_store_result(conn);

  if (!res){
    g_warning("Not able to determine database version");
    return;
  }

  MYSQL_ROW ver;
  ver = mysql_fetch_row(res);
  gchar *ascii_version=g_ascii_strdown(ver[1],-1);
  gchar *ascii_version_comment=g_ascii_strdown(ver[0],-1);

  if (g_strstr_len(ascii_version, -1, "percona") || g_strstr_len(ascii_version_comment, -1, "percona")){
    product = SERVER_TYPE_PERCONA;
  }else
  if (g_strstr_len(ascii_version, -1, "mariadb") || g_strstr_len(ascii_version_comment, -1, "mariadb")){
    product = SERVER_TYPE_MARIADB;
  }else
  if (g_strstr_len(ascii_version, -1, "tidb") || g_strstr_len(ascii_version_comment, -1, "tidb")){
    product = SERVER_TYPE_TIDB;
  }else
  if (g_strstr_len(ascii_version, -1, "mysql") || g_strstr_len(ascii_version_comment, -1, "mysql")){
    product = SERVER_TYPE_MYSQL;    
  }
	gchar ** sver=g_strsplit(ver[1],".",3);

  if (product == SERVER_TYPE_UNKNOWN){
		mysql_free_result(res);
		mysql_query(conn, "SELECT value FROM system.build_options where name='VERSION_FULL'");
		res = mysql_store_result(conn);
		if (res){
			ver = mysql_fetch_row(res);
			ascii_version=g_ascii_strdown(ver[0],-1);
			gchar ** psver=g_strsplit(ascii_version," ",2);
	    if (g_strstr_len(ascii_version, -1, "clickhouse") || g_strstr_len(ascii_version_comment, -1, "clickhouse")){
        product = SERVER_TYPE_CLICKHOUSE;
				sver=g_strsplit(psver[1],".",4);
      }
			g_strfreev(psver);
		}
	}


  major=strtol(sver[0], NULL, 10);
  secondary=strtol(sver[1], NULL, 10);
  revision=strtol(sver[2], NULL, 10);
  g_strfreev(sver);
  mysql_free_result(res);
  g_free(ascii_version);
  g_free(ascii_version_comment);

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
    }
  }else{
    start_replica=CALL_START_REPLICATION;
    start_replica_sql_thread=CALL_START_REPLICATION;
    stop_replica=CALL_STOP_REPLICATION;
    stop_replica_sql_thread=CALL_STOP_REPLICATION;
    reset_replica=CALL_RESET_EXTERNAL_MASTER;
  }
}

const gchar * get_product_name(){
  switch (get_product()){
  case SERVER_TYPE_PERCONA:   return "Percona"; break;
  case SERVER_TYPE_MYSQL:     return "MySQL";   break;
  case SERVER_TYPE_MARIADB:   return "MariaDB"; break;
  case SERVER_TYPE_TIDB:      return "TiDB"; break;
  case SERVER_TYPE_CLICKHOUSE:return "Clickhouse"; break;
	case SERVER_TYPE_UNKNOWN:   return "unknown"; break;
  default: return "";
}


}




