#include <glib.h>
#include <my_global.h>
#include <mysql.h>
#include <string.h>
#include "mydumper.h"
#include "binlog.h"

enum event_type {
	ROTATE_EVENT= 4,
	FORMAT_DESCRIPTION_EVENT= 15,
	EVENT_TOO_SHORT= 10000 // arbitrary high number, in 5.1 the max event type number is 27 so this should be fine for a while
};


void get_binlogs(MYSQL *conn, struct configuration *conf) {
	// TODO: find logs we already have, use start position based on position of last log.
	if (mysql_query(conn, "SHOW BINARY LOGS")) {
		g_critical("Error: Could not execute query: %s", mysql_error(conn));
		return;		
	}
	MYSQL_RES *result = mysql_store_result(conn);
	MYSQL_ROW row;
	while ((row = mysql_fetch_row(result))) {
		struct job *j = g_new0(struct job,1);
		struct binlog_job *bj = g_new0(struct binlog_job,1);
		j->job_data=(void*) bj;
		bj->filename=g_strdup(row[0]);
		bj->start_position=0;
		j->conf=conf;
		j->type=JOB_BINLOG;
	}
	mysql_free_result(result);
}

void get_binlog_file(MYSQL *conn, char *binlog_file, guint64 start_position) {
	// set serverID = max serverID - threadID to try an eliminate conflicts, 0 is bad because mysqld will disconnect at the end of the last log (for mysqlbinlog).
	// TODO: figure out a better way of doing this.
	uchar buf[128];
	// We need to read the raw network packets
	NET* net;
	net= &conn->net;
	unsigned long len;
	unsigned int event_type;
	gboolean read_error= FALSE;
	unsigned int server_id = 4294967295 - mysql_thread_id(conn);
	int4store(buf, (guint32)start_position);
	// Binlog flags (2 byte int)
	int2store(buf + 4, 0);
	// ServerID
	int4store(buf + 6, server_id);
	memcpy(buf + 10, binlog_file, strlen(binlog_file));
	simple_command(conn, COM_BINLOG_DUMP, buf, strlen(binlog_file) + 10, 1);
	while(1) {
		len=my_net_read(net);
		if (len == ~(unsigned long) 0) {
			g_critical("Error: binlog: Network packet read error getting binlog file: %s", binlog_file);
			return;
		}
		if (len < 8 && net->read_pos[0]) {
			// end of data
			break;
		}
		event_type= get_event((const char*)net->read_pos + 1, len -1);
		switch (event_type) {
			case EVENT_TOO_SHORT:
				g_critical("Error: binlog: Event too short in binlog file: %s", binlog_file);
				read_error= TRUE;
				break;
			case ROTATE_EVENT:
				// rotate file
				break;
			default:
				// if we get this far this is a normal event to record
				break;
		}
		if (read_error)
			break;
	}
}


unsigned int get_event(const char *buf, unsigned int len) {
	// Event type offset is 9
	if (len < 0)
		return EVENT_TOO_SHORT;
	return buf[9];

	// TODO: Would be good if we can check for valid event type, unfortunately this check can change from version to version
}
