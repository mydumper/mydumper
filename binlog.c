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

        Authors: 	Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle
   dot com) Andrew Hutchings, SkySQL (andrew at skysql dot com)

*/

#include <glib.h>
#include <glib/gstdio.h>
#include <my_global.h>
#include <mysql.h>
#include <my_sys.h>
#include <mysqld_error.h>
#include <sql_common.h>
#include <string.h>
#include <zlib.h>
#include "mydumper.h"
#include "binlog.h"

#define BINLOG_MAGIC "\xfe\x62\x69\x6e"

#define EVENT_HEADER_LENGTH 19
#define EVENT_ROTATE_FIXED_LENGTH 8

enum event_postions {
  EVENT_TIMESTAMP_POSITION = 0,
  EVENT_TYPE_POSITION = 4,
  EVENT_SERVERID_POSITION = 5,
  EVENT_LENGTH_POSITION = 9,
  EVENT_NEXT_POSITION = 13,
  EVENT_FLAGS_POSITION = 17,
  EVENT_EXTRA_FLAGS_POSITION =
      19 // currently unused in v4 binlogs, but a good marker for end of header
};

enum event_type {
  ROTATE_EVENT = 4,
  FORMAT_DESCRIPTION_EVENT = 15,
  EVENT_TOO_SHORT = 254 // arbitrary high number, in 5.1 the max event type
                        // number is 27 so this should be fine for a while
};

extern int compress_output;
extern gboolean daemon_mode;
extern gboolean shutdown_triggered;

FILE *new_binlog_file(char *binlog_file, const char *binlog_dir);
void close_binlog_file(FILE *outfile);
char *rotate_file_name(const char *buf);

void get_binlogs(MYSQL *conn, struct configuration *conf) {
  // TODO: find logs we already have, use start position based on position of
  // last log.
  MYSQL_RES *result;
  MYSQL_ROW row;
  char *last_filename = NULL;
  guint64 last_position;

  // Only snapshot dump the binlogs once in daemon mode
  static gboolean got_binlogs = FALSE;
  if (got_binlogs)
    return;
  else
    got_binlogs = TRUE;

  if (mysql_query(conn, "SHOW MASTER STATUS")) {
    g_critical("Error: Could not execute query: %s", mysql_error(conn));
    return;
  }

  result = mysql_store_result(conn);
  if ((row = mysql_fetch_row(result))) {
    last_filename = g_strdup(row[0]);
    last_position = strtoll(row[1], NULL, 10);
  } else {
    g_critical("Error: Could not obtain binary log stop position");
    if (last_filename != NULL)
      g_free(last_filename);
    return;
  }
  mysql_free_result(result);

  if (mysql_query(conn, "SHOW BINARY LOGS")) {
    g_critical("Error: Could not execute query: %s", mysql_error(conn));
    if (last_filename != NULL)
      g_free(last_filename);
    return;
  }

  result = mysql_store_result(conn);
  while ((row = mysql_fetch_row(result))) {
    struct job *j = g_new0(struct job, 1);
    struct binlog_job *bj = g_new0(struct binlog_job, 1);
    j->job_data = (void *)bj;
    bj->filename = g_strdup(row[0]);
    bj->start_position = 4;
    bj->stop_position =
        (!strcasecmp(row[0], last_filename)) ? last_position : 0;
    j->conf = conf;
    j->type = JOB_BINLOG;
    g_async_queue_push(conf->queue, j);
  }
  mysql_free_result(result);
  if (last_filename != NULL)
    g_free(last_filename);
}

void get_binlog_file(MYSQL *conn, char *binlog_file,
                     const char *binlog_directory, guint64 start_position,
                     guint64 stop_position, gboolean continuous) {
  // set serverID = max serverID - threadID to try an eliminate conflicts,
  // 0 is bad because mysqld will disconnect at the end of the last log
  // dupes aren't too bad since it is up to the client to check for them
  uchar buf[128];
  // We need to read the raw network packets
  NET *net;
  net = &conn->net;
  unsigned long len;
  FILE *outfile;
  guint32 event_type;
  gboolean read_error = FALSE;
  gboolean read_end = FALSE;
  gboolean rotated = FALSE;
  guint32 server_id = G_MAXUINT32 - mysql_thread_id(conn);
  guint64 pos_counter = 0;

  int4store(buf, (guint32)start_position);
  // Binlog flags (2 byte int)
  int2store(buf + 4, 0);
  // ServerID
  int4store(buf + 6, server_id);
  memcpy(buf + 10, binlog_file, strlen(binlog_file));
#if MYSQL_VERSION_ID < 50100
  if (simple_command(conn, COM_BINLOG_DUMP, (const char *)buf,
#else
  if (simple_command(conn, COM_BINLOG_DUMP, buf,
#endif
                     strlen(binlog_file) + 10, 1)) {
    g_critical("Error: binlog: Critical error whilst requesting binary log");
  }

  while (1) {
    outfile = new_binlog_file(binlog_file, binlog_directory);
    if (outfile == NULL) {
      g_critical("Error: binlog: Could not create binlog file '%s', %d",
                 binlog_file, errno);
      return;
    }

    write_binlog(outfile, BINLOG_MAGIC, 4);
    while (1) {
      len = 0;
      if (net->vio != 0)
        len = my_net_read(net);
      if ((len == 0) || (len == ~(unsigned long)0)) {
        // Net timeout (set to 1 second)
        if (mysql_errno(conn) == ER_NET_READ_INTERRUPTED) {
          if (shutdown_triggered) {
            close_binlog_file(outfile);
            return;
          } else {
            continue;
          }
          // A real error
        } else {
          g_critical("Error: binlog: Network packet read error getting binlog "
                     "file: %s",
                     binlog_file);
          close_binlog_file(outfile);
          return;
        }
      }
      if (len < 8 && net->read_pos[0]) {
        // end of data
        break;
      }
      pos_counter += len;
      event_type = get_event((const char *)net->read_pos + 1, len - 1);
      switch (event_type) {
      case EVENT_TOO_SHORT:
        g_critical("Error: binlog: Event too short in binlog file: %s",
                   binlog_file);
        read_error = TRUE;
        break;
      case ROTATE_EVENT:
        if (rotated) {
          read_end = TRUE;
        } else {
          len = 1;
          rotated = TRUE;
        }
        break;
      default:
        // if we get this far this is a normal event to record
        break;
      }
      if (read_error)
        break;
      write_binlog(outfile, (const char *)net->read_pos + 1, len - 1);
      if (read_end) {
        if (!continuous) {
          break;
        } else {
          g_free(binlog_file);
          binlog_file = rotate_file_name((const char *)net->read_pos + 1);
          break;
        }
      }
      // stop if we are at requested end of last log
      if ((stop_position > 0) && (pos_counter >= stop_position))
        break;
    }
    close_binlog_file(outfile);
    if ((!continuous) || (!read_end))
      break;

    if (continuous && read_end) {
      read_end = FALSE;
      rotated = FALSE;
    }
  }
}

char *rotate_file_name(const char *buf) {
  guint32 event_length = 0;

  // event length is 4 bytes at position 9
  event_length = uint4korr(&buf[EVENT_LENGTH_POSITION]);
  // event length includes the header, plus a rotate event has a fixed 8byte
  // part we don't need
  event_length = event_length - EVENT_HEADER_LENGTH - EVENT_ROTATE_FIXED_LENGTH;

  return g_strndup(&buf[EVENT_HEADER_LENGTH + EVENT_ROTATE_FIXED_LENGTH],
                   event_length);
}

FILE *new_binlog_file(char *binlog_file, const char *binlog_dir) {
  FILE *outfile;
  char *filename;

  if (!compress_output) {
    filename = g_strdup_printf("%s/%s", binlog_dir, binlog_file);
    outfile = g_fopen(filename, "w");
  } else {
    filename = g_strdup_printf("%s/%s.gz", binlog_dir, binlog_file);
    outfile = (void *)gzopen(filename, "w");
  }
  g_free(filename);

  return outfile;
}

void close_binlog_file(FILE *outfile) {
  if (!compress_output)
    fclose(outfile);
  else
    gzclose((gzFile)outfile);
}

unsigned int get_event(const char *buf, unsigned int len) {
  if (len < EVENT_TYPE_POSITION)
    return EVENT_TOO_SHORT;
  return buf[EVENT_TYPE_POSITION];

  // TODO: Would be good if we can check for valid event type, unfortunately
  // this check can change from version to version
}

void write_binlog(FILE *file, const char *data, guint64 len) {
  int err;

  if (len > 0) {
    int write_result;

    if (!compress_output)
      write_result = write(fileno(file), data, len);
    else
      write_result = gzwrite((gzFile)file, data, len);

    if (write_result <= 0) {
      if (!compress_output)
        g_critical("Error: binlog: Error writing binary log: %s",
                   strerror(errno));
      else
        g_critical("Error: binlog: Error writing compressed binary log: %s",
                   gzerror((gzFile)file, &err));
    }
  }
}
