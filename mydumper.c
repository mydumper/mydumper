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
                    Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                    Andrew Hutchings, SkySQL (andrew at skysql dot com)
                    Max Bubenick, Percona RDBA (max dot bubenick at percona dot com)
*/

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <mysql.h>

#if defined MARIADB_CLIENT_VERSION_STR && !defined MYSQL_SERVER_VERSION
#define MYSQL_SERVER_VERSION MARIADB_CLIENT_VERSION_STR
#endif

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <zlib.h>
#include <pcre.h>
#include <signal.h>
#include <glib/gstdio.h>
#include "config.h"
#ifdef WITH_BINLOG
#include "binlog.h"
#else
#include "mydumper.h"
#endif
#include "server_detect.h"
#include "connection.h"
#include "common.h"
#include "g_unix_signal.h"
#include <math.h>
#include "getPassword.h"

char *regexstring = NULL;

const char DIRECTORY[] = "export";
#ifdef WITH_BINLOG
const char BINLOG_DIRECTORY[] = "binlog_snapshot";
const char DAEMON_BINLOGS[] = "binlogs";
#endif

/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif

static GMutex *init_mutex = NULL;

/* Program options */
gchar *output_directory = NULL;
guint statement_size = 1000000;
guint rows_per_file = 0;
guint chunk_filesize = 0;
int longquery = 60;
int build_empty_files = 0;
int skip_tz = 0;
int need_dummy_read = 0;
int need_dummy_toku_read = 0;
int compress_output = 0;
int killqueries = 0;
int detected_server = 0;
int lock_all_tables = 0;
guint snapshot_interval = 60;
gboolean daemon_mode = FALSE;
gboolean have_snapshot_cloning = FALSE;

gchar *ignore_engines = NULL;
char **ignore = NULL;

gchar *tables_list = NULL;
gchar *tidb_snapshot = NULL;
GSequence *tables_skiplist = NULL;
gchar *tables_skiplist_file = NULL;
char **tables = NULL;
GList *no_updated_tables = NULL;

#ifdef WITH_BINLOG
gboolean need_binlogs = FALSE;
gchar *binlog_directory = NULL;
gchar *daemon_binlog_directory = NULL;
#endif

gchar *logfile = NULL;
FILE *logoutfile = NULL;

gboolean no_schemas = FALSE;
gboolean no_data = FALSE;
gboolean no_locks = FALSE;
gboolean dump_triggers = FALSE;
gboolean dump_events = FALSE;
gboolean dump_routines = FALSE;
gboolean no_dump_views = FALSE;
gboolean less_locking = FALSE;
gboolean use_savepoints = FALSE;
gboolean success_on_1146 = FALSE;
gboolean no_backup_locks = FALSE;
gboolean insert_ignore = FALSE;

GList *innodb_tables = NULL;
GMutex *innodb_tables_mutex = NULL;
GList *non_innodb_table = NULL;
GMutex *non_innodb_table_mutex = NULL;
GList *table_schemas = NULL;
GMutex *table_schemas_mutex = NULL;
GList *view_schemas = NULL;
GMutex *view_schemas_mutex = NULL;
GList *schema_post = NULL;
GMutex *schema_post_mutex = NULL;
gint database_counter = 0;
gint non_innodb_table_counter = 0;
gint non_innodb_done = 0;
guint less_locking_threads = 0;
guint updated_since = 0;
guint trx_consistency_only = 0;
guint complete_insert = 0;

// For daemon mode, 0 or 1
guint dump_number = 0;
guint binlog_connect_id = 0;
gboolean shutdown_triggered = FALSE;
GAsyncQueue *start_scheduled_dump;
GMainLoop *m1;
static GCond *ll_cond = NULL;
static GMutex *ll_mutex = NULL;

int errors;

static GOptionEntry entries[] = {
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db, "Database to dump", NULL},
    {"tables-list", 'T', 0, G_OPTION_ARG_STRING, &tables_list,
     "Comma delimited table list to dump (does not exclude regex option)",
     NULL},
    {"omit-from-file", 'O', 0, G_OPTION_ARG_STRING, &tables_skiplist_file,
     "File containing a list of database.table entries to skip, one per line "
     "(skips before applying regex option)",
     NULL},
    {"outputdir", 'o', 0, G_OPTION_ARG_FILENAME, &output_directory,
     "Directory to output files to", NULL},
    {"statement-size", 's', 0, G_OPTION_ARG_INT, &statement_size,
     "Attempted size of INSERT statement in bytes, default 1000000", NULL},
    {"rows", 'r', 0, G_OPTION_ARG_INT, &rows_per_file,
     "Try to split tables into chunks of this many rows. This option turns off "
     "--chunk-filesize",
     NULL},
    {"chunk-filesize", 'F', 0, G_OPTION_ARG_INT, &chunk_filesize,
     "Split tables into chunks of this output file size. This value is in MB",
     NULL},
    {"compress", 'c', 0, G_OPTION_ARG_NONE, &compress_output,
     "Compress output files", NULL},
    {"build-empty-files", 'e', 0, G_OPTION_ARG_NONE, &build_empty_files,
     "Build dump files even if no data available from table", NULL},
    {"regex", 'x', 0, G_OPTION_ARG_STRING, &regexstring,
     "Regular expression for 'db.table' matching", NULL},
    {"ignore-engines", 'i', 0, G_OPTION_ARG_STRING, &ignore_engines,
     "Comma delimited list of storage engines to ignore", NULL},
    {"insert-ignore", 'N', 0, G_OPTION_ARG_NONE, &insert_ignore,
     "Dump rows with INSERT IGNORE", NULL},
    {"no-schemas", 'm', 0, G_OPTION_ARG_NONE, &no_schemas,
     "Do not dump table schemas with the data", NULL},
    {"no-data", 'd', 0, G_OPTION_ARG_NONE, &no_data, "Do not dump table data",
     NULL},
    {"triggers", 'G', 0, G_OPTION_ARG_NONE, &dump_triggers, "Dump triggers",
     NULL},
    {"events", 'E', 0, G_OPTION_ARG_NONE, &dump_events, "Dump events", NULL},
    {"routines", 'R', 0, G_OPTION_ARG_NONE, &dump_routines,
     "Dump stored procedures and functions", NULL},
    {"no-views", 'W', 0, G_OPTION_ARG_NONE, &no_dump_views, "Do not dump VIEWs",
     NULL},
    {"no-locks", 'k', 0, G_OPTION_ARG_NONE, &no_locks,
     "Do not execute the temporary shared read lock.  WARNING: This will cause "
     "inconsistent backups",
     NULL},
    {"no-backup-locks", 0, 0, G_OPTION_ARG_NONE, &no_backup_locks,
     "Do not use Percona backup locks", NULL},
    {"less-locking", 0, 0, G_OPTION_ARG_NONE, &less_locking,
     "Minimize locking time on InnoDB tables.", NULL},
    {"long-query-guard", 'l', 0, G_OPTION_ARG_INT, &longquery,
     "Set long query timer in seconds, default 60", NULL},
    {"kill-long-queries", 'K', 0, G_OPTION_ARG_NONE, &killqueries,
     "Kill long running queries (instead of aborting)", NULL},
#ifdef WITH_BINLOG
    {"binlogs", 'b', 0, G_OPTION_ARG_NONE, &need_binlogs,
     "Get a snapshot of the binary logs as well as dump data", NULL},
#endif
    {"daemon", 'D', 0, G_OPTION_ARG_NONE, &daemon_mode, "Enable daemon mode",
     NULL},
    {"snapshot-interval", 'I', 0, G_OPTION_ARG_INT, &snapshot_interval,
     "Interval between each dump snapshot (in minutes), requires --daemon, "
     "default 60",
     NULL},
    {"logfile", 'L', 0, G_OPTION_ARG_FILENAME, &logfile,
     "Log file name to use, by default stdout is used", NULL},
    {"tz-utc", 0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, &skip_tz,
     "SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data "
     "when a server has data in different time zones or data is being moved "
     "between servers with different time zones, defaults to on use "
     "--skip-tz-utc to disable.",
     NULL},
    {"skip-tz-utc", 0, 0, G_OPTION_ARG_NONE, &skip_tz, "", NULL},
    {"use-savepoints", 0, 0, G_OPTION_ARG_NONE, &use_savepoints,
     "Use savepoints to reduce metadata locking issues, needs SUPER privilege",
     NULL},
    {"success-on-1146", 0, 0, G_OPTION_ARG_NONE, &success_on_1146,
     "Not increment error count and Warning instead of Critical in case of "
     "table doesn't exist",
     NULL},
    {"lock-all-tables", 0, 0, G_OPTION_ARG_NONE, &lock_all_tables,
     "Use LOCK TABLE for all, instead of FTWRL", NULL},
    {"updated-since", 'U', 0, G_OPTION_ARG_INT, &updated_since,
     "Use Update_time to dump only tables updated in the last U days", NULL},
    {"trx-consistency-only", 0, 0, G_OPTION_ARG_NONE, &trx_consistency_only,
     "Transactional consistency only", NULL},
    {"complete-insert", 0, 0, G_OPTION_ARG_NONE, &complete_insert,
     "Use complete INSERT statements that include column names", NULL},
    {"tidb-snapshot", 'z', 0, G_OPTION_ARG_STRING, &tidb_snapshot,
     "Snapshot to use for TiDB", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

struct tm tval;

void dump_schema_data(MYSQL *conn, char *database, char *table, char *filename);
void dump_triggers_data(MYSQL *conn, char *database, char *table,
                        char *filename);
void dump_view_data(MYSQL *conn, char *database, char *table, char *filename,
                    char *filename2);
void dump_schema(MYSQL *conn, char *database, char *table,
                 struct configuration *conf);
void dump_view(char *database, char *table, struct configuration *conf);
void dump_table(MYSQL *conn, char *database, char *table,
                struct configuration *conf, gboolean is_innodb);
void dump_tables(MYSQL *, GList *, struct configuration *);
void dump_schema_post(char *database, struct configuration *conf);
void restore_charset(GString *statement);
void set_charset(GString *statement, char *character_set,
                 char *collation_connection);
void dump_schema_post_data(MYSQL *conn, char *database, char *filename);
guint64 dump_table_data(MYSQL *, FILE *, char *, char *, char *, char *);
void dump_database(char *, struct configuration *);
void dump_database_thread(MYSQL *, char *);
void dump_create_database(char *, struct configuration *);
void dump_create_database_data(MYSQL *, char *, char *);
void get_tables(MYSQL *conn, struct configuration *);
void get_not_updated(MYSQL *conn, FILE *);
GList *get_chunks_for_table(MYSQL *, char *, char *,
                            struct configuration *conf);
guint64 estimate_count(MYSQL *conn, char *database, char *table, char *field,
                       char *from, char *to);
void dump_table_data_file(MYSQL *conn, char *database, char *table, char *where,
                          char *filename);
void create_backup_dir(char *directory);
gboolean write_data(FILE *, GString *);
gboolean check_regex(char *database, char *table);
gboolean check_skiplist(char *database, char *table);
int tables_skiplist_cmp(gconstpointer a, gconstpointer b, gpointer user_data);
void read_tables_skiplist(const gchar *filename);
void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data);
void set_verbose(guint verbosity);
#ifdef WITH_BINLOG
MYSQL *reconnect_for_binlog(MYSQL *thrconn);
void *binlog_thread(void *data);
#endif
void start_dump(MYSQL *conn);
MYSQL *create_main_connection();
void *exec_thread(void *data);
void write_log_file(const gchar *log_domain, GLogLevelFlags log_level,
                    const gchar *message, gpointer user_data);

void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data) {
  (void)log_domain;
  (void)log_level;
  (void)message;
  (void)user_data;
}

void set_verbose(guint verbosity) {
  if (logfile) {
    logoutfile = g_fopen(logfile, "w");
    if (!logoutfile) {
      g_critical("Could not open log file '%s' for writing: %d", logfile,
                 errno);
      exit(EXIT_FAILURE);
    }
  }

  switch (verbosity) {
  case 0:
    g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK), no_log, NULL);
    break;
  case 1:
    g_log_set_handler(
        NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_MESSAGE),
        no_log, NULL);
    if (logfile)
      g_log_set_handler(
          NULL, (GLogLevelFlags)(G_LOG_LEVEL_ERROR | G_LOG_LEVEL_CRITICAL),
          write_log_file, NULL);
    break;
  case 2:
    g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE), no_log,
                      NULL);
    if (logfile)
      g_log_set_handler(
          NULL,
          (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR |
                           G_LOG_LEVEL_WARNING | G_LOG_LEVEL_ERROR |
                           G_LOG_LEVEL_CRITICAL),
          write_log_file, NULL);
    break;
  default:
    if (logfile)
      g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK),
                        write_log_file, NULL);
    break;
  }
}

gboolean sig_triggered(gpointer user_data) {
  (void)user_data;

  g_message("Shutting down gracefully");
  shutdown_triggered = TRUE;
  g_main_loop_quit(m1);
  return FALSE;
}

void clear_dump_directory() {
  GError *error = NULL;
  char *dump_directory =
      g_strdup_printf("%s/%d", output_directory, dump_number);
  GDir *dir = g_dir_open(dump_directory, 0, &error);

  if (error) {
    g_critical("cannot open directory %s, %s\n", dump_directory,
               error->message);
    errors++;
    return;
  }

  const gchar *filename = NULL;

  while ((filename = g_dir_read_name(dir))) {
    gchar *path = g_build_filename(dump_directory, filename, NULL);
    if (g_unlink(path) == -1) {
      g_critical("error removing file %s (%d)\n", path, errno);
      errors++;
      return;
    }
    g_free(path);
  }

  g_dir_close(dir);
  g_free(dump_directory);
}

gboolean run_snapshot(gpointer *data) {
  (void)data;

  g_async_queue_push(start_scheduled_dump, GINT_TO_POINTER(1));

  return (shutdown_triggered) ? FALSE : TRUE;
}

/* Check database.table string against regular expression */

gboolean check_regex(char *database, char *table) {
  /* This is not going to be used in threads */
  static pcre *re = NULL;
  int rc;
  int ovector[9] = {0};
  const char *error;
  int erroroffset;

  char *p;

  /* Let's compile the RE before we do anything */
  if (!re) {
    re = pcre_compile(regexstring, PCRE_CASELESS | PCRE_MULTILINE, &error,
                      &erroroffset, NULL);
    if (!re) {
      g_critical("Regular expression fail: %s", error);
      exit(EXIT_FAILURE);
    }
  }

  p = g_strdup_printf("%s.%s", database, table);
  rc = pcre_exec(re, NULL, p, strlen(p), 0, 0, ovector, 9);
  g_free(p);

  return (rc > 0) ? TRUE : FALSE;
}

/* Check database.table string against skip list; returns TRUE if found */

gboolean check_skiplist(char *database, char *table) {
  if (g_sequence_lookup(tables_skiplist,
                        g_strdup_printf("%s.%s", database, table),
                        tables_skiplist_cmp, NULL)) {
    return TRUE;
  } else {
    return FALSE;
  };
}

/* Comparison function for skiplist sort and lookup */

int tables_skiplist_cmp(gconstpointer a, gconstpointer b, gpointer user_data) {
  /* Not using user_data, but needed for function prototype, shutting up
   * compiler warnings about unused variable */
  (void)user_data;
  /* Any sorting function would work, as long as its usage is consistent
   * between sort and lookup.  strcmp should be one of the fastest. */
  return strcmp(a, b);
}

/* Read the list of tables to skip from the given filename, and prepares them
 * for future lookups. */

void read_tables_skiplist(const gchar *filename) {
  GIOChannel *tables_skiplist_channel = NULL;
  gchar *buf = NULL;
  GError *error = NULL;
  /* Create skiplist if it does not exist */
  if (!tables_skiplist) {
    tables_skiplist = g_sequence_new(NULL);
  };
  tables_skiplist_channel = g_io_channel_new_file(filename, "r", &error);

  /* Error opening/reading the file? bail out. */
  if (!tables_skiplist_channel) {
    g_critical("cannot read/open file %s, %s\n", filename, error->message);
    errors++;
    return;
  };

  /* Read lines, push them to the list */
  do {
    g_io_channel_read_line(tables_skiplist_channel, &buf, NULL, NULL, NULL);
    if (buf) {
      g_strchomp(buf);
      g_sequence_append(tables_skiplist, buf);
    };
  } while (buf);
  g_io_channel_shutdown(tables_skiplist_channel, FALSE, NULL);
  /* Sort the list, so that lookups work */
  g_sequence_sort(tables_skiplist, tables_skiplist_cmp, NULL);
  g_message("Omit list file contains %d tables to skip\n",
            g_sequence_get_length(tables_skiplist));
  return;
}

/* Write some stuff we know about snapshot, before it changes */
void write_snapshot_info(MYSQL *conn, FILE *file) {
  MYSQL_RES *master = NULL, *slave = NULL, *mdb = NULL;
  MYSQL_FIELD *fields;
  MYSQL_ROW row;

  char *masterlog = NULL;
  char *masterpos = NULL;
  char *mastergtid = NULL;

  char *connname = NULL;
  char *slavehost = NULL;
  char *slavelog = NULL;
  char *slavepos = NULL;
  char *slavegtid = NULL;
  guint isms;
  guint i;

  mysql_query(conn, "SHOW MASTER STATUS");
  master = mysql_store_result(conn);
  if (master && (row = mysql_fetch_row(master))) {
    masterlog = row[0];
    masterpos = row[1];
    /* Oracle/Percona GTID */
    if (mysql_num_fields(master) == 5) {
      mastergtid = row[4];
    } else {
      /* Let's try with MariaDB 10.x */
      /* Use gtid_binlog_pos due to issue with gtid_current_pos with galera
       * cluster, gtid_binlog_pos works as well with normal mariadb server
       * https://jira.mariadb.org/browse/MDEV-10279 */
      mysql_query(conn, "SELECT @@gtid_binlog_pos");
      mdb = mysql_store_result(conn);
      if (mdb && (row = mysql_fetch_row(mdb))) {
        mastergtid = row[0];
      }
    }
  }

  if (masterlog) {
    fprintf(file, "SHOW MASTER STATUS:\n\tLog: %s\n\tPos: %s\n\tGTID:%s\n\n",
            masterlog, masterpos, mastergtid);
    g_message("Written master status");
  }

  isms = 0;
  mysql_query(conn, "SELECT @@default_master_connection");
  MYSQL_RES *rest = mysql_store_result(conn);
  if (rest != NULL && mysql_num_rows(rest)) {
    mysql_free_result(rest);
    g_message("Multisource slave detected.");
    isms = 1;
  }

  if (isms)
    mysql_query(conn, "SHOW ALL SLAVES STATUS");
  else
    mysql_query(conn, "SHOW SLAVE STATUS");

  slave = mysql_store_result(conn);
  while (slave && (row = mysql_fetch_row(slave))) {
    fields = mysql_fetch_fields(slave);
    for (i = 0; i < mysql_num_fields(slave); i++) {
      if (isms && !strcasecmp("connection_name", fields[i].name))
        connname = row[i];
      if (!strcasecmp("exec_master_log_pos", fields[i].name)) {
        slavepos = row[i];
      } else if (!strcasecmp("relay_master_log_file", fields[i].name)) {
        slavelog = row[i];
      } else if (!strcasecmp("master_host", fields[i].name)) {
        slavehost = row[i];
      } else if (!strcasecmp("Executed_Gtid_Set", fields[i].name) ||
                 !strcasecmp("Gtid_Slave_Pos", fields[i].name)) {
        slavegtid = row[i];
      }
    }
    if (slavehost) {
      fprintf(file, "SHOW SLAVE STATUS:");
      if (isms)
        fprintf(file, "\n\tConnection name: %s", connname);
      fprintf(file, "\n\tHost: %s\n\tLog: %s\n\tPos: %s\n\tGTID:%s\n\n",
              slavehost, slavelog, slavepos, slavegtid);
      g_message("Written slave status");
    }
  }

  fflush(file);
  if (master)
    mysql_free_result(master);
  if (slave)
    mysql_free_result(slave);
  if (mdb)
    mysql_free_result(mdb);
}

void *process_queue(struct thread_data *td) {
  struct configuration *conf = td->conf;
  // mysql_init is not thread safe, especially in Connector/C
  g_mutex_lock(init_mutex);
  MYSQL *thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);

  configure_connection(thrconn, "mydumper");

  if (!mysql_real_connect(thrconn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Failed to connect to database: %s", mysql_error(thrconn));
    exit(EXIT_FAILURE);
  } else {
    g_message("Thread %d connected using MySQL connection ID %lu",
              td->thread_id, mysql_thread_id(thrconn));
  }

  if (use_savepoints && mysql_query(thrconn, "SET SQL_LOG_BIN = 0")) {
    g_critical("Failed to disable binlog for the thread: %s",
               mysql_error(thrconn));
    exit(EXIT_FAILURE);
  }
  if ((detected_server == SERVER_TYPE_MYSQL) &&
      mysql_query(thrconn, "SET SESSION wait_timeout = 2147483")) {
    g_warning("Failed to increase wait_timeout: %s", mysql_error(thrconn));
  }
  if (mysql_query(thrconn,
                  "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")) {
    g_critical("Failed to set isolation level: %s", mysql_error(thrconn));
    exit(EXIT_FAILURE);
  }
  if (mysql_query(thrconn,
                  "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")) {
    g_critical("Failed to start consistent snapshot: %s", mysql_error(thrconn));
    exit(EXIT_FAILURE);
  }
  if (!skip_tz && mysql_query(thrconn, "/*!40103 SET TIME_ZONE='+00:00' */")) {
    g_critical("Failed to set time zone: %s", mysql_error(thrconn));
  }

  if (detected_server == SERVER_TYPE_TIDB) {

    // Worker threads must set their tidb_snapshot in order to be safe
    // Because no locking has been used.

    gchar *query =
        g_strdup_printf("SET SESSION tidb_snapshot = '%s'", tidb_snapshot);

    if (mysql_query(thrconn, query)) {
      g_critical("Failed to set tidb_snapshot: %s", mysql_error(thrconn));
      exit(EXIT_FAILURE);
    }
    g_free(query);

    g_message("Thread %d set to tidb_snapshot '%s'", td->thread_id,
              tidb_snapshot);
  }

  /* Unfortunately version before 4.1.8 did not support consistent snapshot
   * transaction starts, so we cheat */
  if (need_dummy_read) {
    mysql_query(thrconn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy");
    MYSQL_RES *res = mysql_store_result(thrconn);
    if (res)
      mysql_free_result(res);
  }
  if (need_dummy_toku_read) {
    mysql_query(thrconn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy");
    MYSQL_RES *res = mysql_store_result(thrconn);
    if (res)
      mysql_free_result(res);
  }
  mysql_query(thrconn, "/*!40101 SET NAMES binary*/");

  g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

  struct job *job = NULL;
  struct table_job *tj = NULL;
  struct dump_database_job *ddj = NULL;
  struct create_database_job *cdj = NULL;
  struct schema_job *sj = NULL;
  struct view_job *vj = NULL;
  struct schema_post_job *sp = NULL;
#ifdef WITH_BINLOG
  struct binlog_job *bj = NULL;
#endif
  /* if less locking we need to wait until that threads finish
      progressively waking up these threads */
  if (less_locking) {
    g_mutex_lock(ll_mutex);

    while (less_locking_threads >= td->thread_id) {
      g_cond_wait(ll_cond, ll_mutex);
    }

    g_mutex_unlock(ll_mutex);
  }

  for (;;) {

    GTimeVal tv;
    g_get_current_time(&tv);
    g_time_val_add(&tv, 1000 * 1000 * 1);
    job = (struct job *)g_async_queue_pop(conf->queue);
    if (shutdown_triggered && (job->type != JOB_SHUTDOWN)) {
      continue;
    }

    switch (job->type) {
    case JOB_DUMP:
      tj = (struct table_job *)job->job_data;
      if (tj->where)
        g_message("Thread %d dumping data for `%s`.`%s` where %s",
                  td->thread_id, tj->database, tj->table, tj->where);
      else
        g_message("Thread %d dumping data for `%s`.`%s`", td->thread_id,
                  tj->database, tj->table);
      if (use_savepoints && mysql_query(thrconn, "SAVEPOINT mydumper")) {
        g_critical("Savepoint failed: %s", mysql_error(thrconn));
      }
      dump_table_data_file(thrconn, tj->database, tj->table, tj->where,
                           tj->filename);
      if (use_savepoints &&
          mysql_query(thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
        g_critical("Rollback to savepoint failed: %s", mysql_error(thrconn));
      }
      if (tj->database)
        g_free(tj->database);
      if (tj->table)
        g_free(tj->table);
      if (tj->where)
        g_free(tj->where);
      if (tj->filename)
        g_free(tj->filename);
      g_free(tj);
      g_free(job);
      break;
    case JOB_DUMP_NON_INNODB:
      tj = (struct table_job *)job->job_data;
      if (tj->where)
        g_message("Thread %d dumping data for `%s`.`%s` where %s",
                  td->thread_id, tj->database, tj->table, tj->where);
      else
        g_message("Thread %d dumping data for `%s`.`%s`", td->thread_id,
                  tj->database, tj->table);
      if (use_savepoints && mysql_query(thrconn, "SAVEPOINT mydumper")) {
        g_critical("Savepoint failed: %s", mysql_error(thrconn));
      }
      dump_table_data_file(thrconn, tj->database, tj->table, tj->where,
                           tj->filename);
      if (use_savepoints &&
          mysql_query(thrconn, "ROLLBACK TO SAVEPOINT mydumper")) {
        g_critical("Rollback to savepoint failed: %s", mysql_error(thrconn));
      }
      if (tj->database)
        g_free(tj->database);
      if (tj->table)
        g_free(tj->table);
      if (tj->where)
        g_free(tj->where);
      if (tj->filename)
        g_free(tj->filename);
      g_free(tj);
      g_free(job);
      if (g_atomic_int_dec_and_test(&non_innodb_table_counter) &&
          g_atomic_int_get(&non_innodb_done)) {
        g_async_queue_push(conf->unlock_tables, GINT_TO_POINTER(1));
      }
      break;
    case JOB_DUMP_DATABASE:
      ddj = (struct dump_database_job *)job->job_data;
      g_message("Thread %d dumping db information for `%s`", td->thread_id,
                ddj->database);
      dump_database_thread(thrconn, ddj->database);
      if (ddj->database)
        g_free(ddj->database);
      g_free(ddj);
      g_free(job);
      if (g_atomic_int_dec_and_test(&database_counter)) {
        g_async_queue_push(conf->ready_database_dump, GINT_TO_POINTER(1));
      }
      break;
    case JOB_CREATE_DATABASE:
      cdj = (struct create_database_job *)job->job_data;
      g_message("Thread %d dumping schema create for `%s`", td->thread_id,
                cdj->database);
      dump_create_database_data(thrconn, cdj->database, cdj->filename);
      if (cdj->database)
        g_free(cdj->database);
      if (cdj->filename)
        g_free(cdj->filename);
      g_free(cdj);
      g_free(job);
      break;
    case JOB_SCHEMA:
      sj = (struct schema_job *)job->job_data;
      g_message("Thread %d dumping schema for `%s`.`%s`", td->thread_id,
                sj->database, sj->table);
      dump_schema_data(thrconn, sj->database, sj->table, sj->filename);
      if (sj->database)
        g_free(sj->database);
      if (sj->table)
        g_free(sj->table);
      if (sj->filename)
        g_free(sj->filename);
      g_free(sj);
      g_free(job);
      break;
    case JOB_VIEW:
      vj = (struct view_job *)job->job_data;
      g_message("Thread %d dumping view for `%s`.`%s`", td->thread_id,
                vj->database, vj->table);
      dump_view_data(thrconn, vj->database, vj->table, vj->filename,
                     vj->filename2);
      if (vj->database)
        g_free(vj->database);
      if (vj->table)
        g_free(vj->table);
      if (vj->filename)
        g_free(vj->filename);
      if (vj->filename2)
        g_free(vj->filename2);
      g_free(vj);
      g_free(job);
      break;
    case JOB_TRIGGERS:
      sj = (struct schema_job *)job->job_data;
      g_message("Thread %d dumping triggers for `%s`.`%s`", td->thread_id,
                sj->database, sj->table);
      dump_triggers_data(thrconn, sj->database, sj->table, sj->filename);
      if (sj->database)
        g_free(sj->database);
      if (sj->table)
        g_free(sj->table);
      if (sj->filename)
        g_free(sj->filename);
      g_free(sj);
      g_free(job);
      break;
    case JOB_SCHEMA_POST:
      sp = (struct schema_post_job *)job->job_data;
      g_message("Thread %d dumping SP and VIEWs for `%s`", td->thread_id,
                sp->database);
      dump_schema_post_data(thrconn, sp->database, sp->filename);
      if (sp->database)
        g_free(sp->database);
      if (sp->filename)
        g_free(sp->filename);
      g_free(sp);
      g_free(job);
      break;
#ifdef WITH_BINLOG
    case JOB_BINLOG:
      thrconn = reconnect_for_binlog(thrconn);
      g_message(
          "Thread %d connected using MySQL connection ID %lu (in binlog mode)",
          td->thread_id, mysql_thread_id(thrconn));
      bj = (struct binlog_job *)job->job_data;
      g_message("Thread %d dumping binary log file %s", td->thread_id,
                bj->filename);
      get_binlog_file(thrconn, bj->filename, binlog_directory,
                      bj->start_position, bj->stop_position, FALSE);
      if (bj->filename)
        g_free(bj->filename);
      g_free(bj);
      g_free(job);
      break;
#endif
    case JOB_SHUTDOWN:
      g_message("Thread %d shutting down", td->thread_id);
      if (thrconn)
        mysql_close(thrconn);
      g_free(job);
      mysql_thread_end();
      return NULL;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
  }
  if (thrconn)
    mysql_close(thrconn);
  mysql_thread_end();
  return NULL;
}

void *process_queue_less_locking(struct thread_data *td) {
  struct configuration *conf = td->conf;
  // mysql_init is not thread safe, especially in Connector/C
  g_mutex_lock(init_mutex);
  MYSQL *thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);

  configure_connection(thrconn, "mydumper");

  if (!mysql_real_connect(thrconn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Failed to connect to database: %s", mysql_error(thrconn));
    exit(EXIT_FAILURE);
  } else {
    g_message("Thread %d connected using MySQL connection ID %lu",
              td->thread_id, mysql_thread_id(thrconn));
  }

  if ((detected_server == SERVER_TYPE_MYSQL) &&
      mysql_query(thrconn, "SET SESSION wait_timeout = 2147483")) {
    g_warning("Failed to increase wait_timeout: %s", mysql_error(thrconn));
  }
  if (!skip_tz && mysql_query(thrconn, "/*!40103 SET TIME_ZONE='+00:00' */")) {
    g_critical("Failed to set time zone: %s", mysql_error(thrconn));
  }
  mysql_query(thrconn, "/*!40101 SET NAMES binary*/");

  g_async_queue_push(conf->ready_less_locking, GINT_TO_POINTER(1));

  struct job *job = NULL;
  struct table_job *tj = NULL;
  struct tables_job *mj = NULL;
  struct dump_database_job *ddj = NULL;
  struct create_database_job *cdj = NULL;
  struct schema_job *sj = NULL;
  struct view_job *vj = NULL;
  struct schema_post_job *sp = NULL;
#ifdef WITH_BINLOG
  struct binlog_job *bj = NULL;
#endif
  GList *glj;
  int first = 1;
  GString *query = g_string_new(NULL);
  GString *prev_table = g_string_new(NULL);
  GString *prev_database = g_string_new(NULL);

  for (;;) {
    GTimeVal tv;
    g_get_current_time(&tv);
    g_time_val_add(&tv, 1000 * 1000 * 1);
    job = (struct job *)g_async_queue_pop(conf->queue_less_locking);
    if (shutdown_triggered && (job->type != JOB_SHUTDOWN)) {
      continue;
    }

    switch (job->type) {
    case JOB_LOCK_DUMP_NON_INNODB:
      mj = (struct tables_job *)job->job_data;
      for (glj = mj->table_job_list; glj != NULL; glj = glj->next) {
        tj = (struct table_job *)glj->data;
        if (first) {
          g_string_printf(query, "LOCK TABLES `%s`.`%s` READ LOCAL",
                          tj->database, tj->table);
          first = 0;
        } else {
          if (g_ascii_strcasecmp(prev_database->str, tj->database) ||
              g_ascii_strcasecmp(prev_table->str, tj->table)) {
            g_string_append_printf(query, ", `%s`.`%s` READ LOCAL",
                                   tj->database, tj->table);
          }
        }
        g_string_printf(prev_table, "%s", tj->table);
        g_string_printf(prev_database, "%s", tj->database);
      }
      first = 1;
      if (mysql_query(thrconn, query->str)) {
        g_critical("Non Innodb lock tables fail: %s", mysql_error(thrconn));
        exit(EXIT_FAILURE);
      }
      if (g_atomic_int_dec_and_test(&non_innodb_table_counter) &&
          g_atomic_int_get(&non_innodb_done)) {
        g_async_queue_push(conf->unlock_tables, GINT_TO_POINTER(1));
      }
      for (glj = mj->table_job_list; glj != NULL; glj = glj->next) {
        tj = (struct table_job *)glj->data;
        if (tj->where)
          g_message("Thread %d dumping data for `%s`.`%s` where %s",
                    td->thread_id, tj->database, tj->table, tj->where);
        else
          g_message("Thread %d dumping data for `%s`.`%s`", td->thread_id,
                    tj->database, tj->table);
        dump_table_data_file(thrconn, tj->database, tj->table, tj->where,
                             tj->filename);
        if (tj->database)
          g_free(tj->database);
        if (tj->table)
          g_free(tj->table);
        if (tj->where)
          g_free(tj->where);
        if (tj->filename)
          g_free(tj->filename);
        g_free(tj);
      }
      mysql_query(thrconn, "UNLOCK TABLES /* Non Innodb */");
      g_list_free(mj->table_job_list);
      g_free(mj);
      g_free(job);
      break;
    case JOB_DUMP_DATABASE:
      ddj = (struct dump_database_job *)job->job_data;
      g_message("Thread %d dumping db information for `%s`", td->thread_id,
                ddj->database);
      dump_database_thread(thrconn, ddj->database);
      if (ddj->database)
        g_free(ddj->database);
      g_free(ddj);
      g_free(job);
      if (g_atomic_int_dec_and_test(&database_counter)) {
        g_async_queue_push(conf->ready_database_dump, GINT_TO_POINTER(1));
      }
      break;
    case JOB_CREATE_DATABASE:
      cdj = (struct create_database_job *)job->job_data;
      g_message("Thread %d dumping schema create for `%s`", td->thread_id,
                cdj->database);
      dump_create_database_data(thrconn, cdj->database, cdj->filename);
      if (cdj->database)
        g_free(cdj->database);
      if (cdj->filename)
        g_free(cdj->filename);
      g_free(cdj);
      g_free(job);
      break;
    case JOB_SCHEMA:
      sj = (struct schema_job *)job->job_data;
      g_message("Thread %d dumping schema for `%s`.`%s`", td->thread_id,
                sj->database, sj->table);
      dump_schema_data(thrconn, sj->database, sj->table, sj->filename);
      if (sj->database)
        g_free(sj->database);
      if (sj->table)
        g_free(sj->table);
      if (sj->filename)
        g_free(sj->filename);
      g_free(sj);
      g_free(job);
      break;
    case JOB_VIEW:
      vj = (struct view_job *)job->job_data;
      g_message("Thread %d dumping view for `%s`.`%s`", td->thread_id,
                sj->database, sj->table);
      dump_view_data(thrconn, vj->database, vj->table, vj->filename,
                     vj->filename2);
      if (vj->database)
        g_free(vj->database);
      if (vj->table)
        g_free(vj->table);
      if (vj->filename)
        g_free(vj->filename);
      if (vj->filename2)
        g_free(vj->filename2);
      g_free(vj);
      g_free(job);
      break;
    case JOB_TRIGGERS:
      sj = (struct schema_job *)job->job_data;
      g_message("Thread %d dumping triggers for `%s`.`%s`", td->thread_id,
                sj->database, sj->table);
      dump_triggers_data(thrconn, sj->database, sj->table, sj->filename);
      if (sj->database)
        g_free(sj->database);
      if (sj->table)
        g_free(sj->table);
      if (sj->filename)
        g_free(sj->filename);
      g_free(sj);
      g_free(job);
      break;
    case JOB_SCHEMA_POST:
      sp = (struct schema_post_job *)job->job_data;
      g_message("Thread %d dumping SP and VIEWs for `%s`", td->thread_id,
                sp->database);
      dump_schema_post_data(thrconn, sp->database, sp->filename);
      if (sp->database)
        g_free(sp->database);
      if (sp->filename)
        g_free(sp->filename);
      g_free(sp);
      g_free(job);
      break;
#ifdef WITH_BINLOG
    case JOB_BINLOG:
      thrconn = reconnect_for_binlog(thrconn);
      g_message(
          "Thread %d connected using MySQL connection ID %lu (in binlog mode)",
          td->thread_id, mysql_thread_id(thrconn));
      bj = (struct binlog_job *)job->job_data;
      g_message("Thread %d dumping binary log file %s", td->thread_id,
                bj->filename);
      get_binlog_file(thrconn, bj->filename, binlog_directory,
                      bj->start_position, bj->stop_position, FALSE);
      if (bj->filename)
        g_free(bj->filename);
      g_free(bj);
      g_free(job);
      break;
#endif
    case JOB_SHUTDOWN:
      g_message("Thread %d shutting down", td->thread_id);
      g_mutex_lock(ll_mutex);
      less_locking_threads--;
      g_cond_broadcast(ll_cond);
      g_mutex_unlock(ll_mutex);
      g_string_free(query, TRUE);
      g_string_free(prev_table, TRUE);
      g_string_free(prev_database, TRUE);
      if (thrconn)
        mysql_close(thrconn);
      g_free(job);
      mysql_thread_end();
      return NULL;
      break;
    default:
      g_critical("Something very bad happened!");
      exit(EXIT_FAILURE);
    }
  }

  if (thrconn)
    mysql_close(thrconn);
  mysql_thread_end();
  return NULL;
}
#ifdef WITH_BINLOG
MYSQL *reconnect_for_binlog(MYSQL *thrconn) {
  if (thrconn) {
    mysql_close(thrconn);
  }
  g_mutex_lock(init_mutex);
  thrconn = mysql_init(NULL);
  g_mutex_unlock(init_mutex);

  configure_connection(thrconn, "mydumper");

  int timeout = 1;
  mysql_options(thrconn, MYSQL_OPT_READ_TIMEOUT, (const char *)&timeout);

  if (!mysql_real_connect(thrconn, hostname, username, password, NULL, port,
                          socket_path, 0)) {
    g_critical("Failed to re-connect to database: %s", mysql_error(thrconn));
    exit(EXIT_FAILURE);
  }
  return thrconn;
}
#endif

int main(int argc, char *argv[]) {
  GError *error = NULL;
  GOptionContext *context;

  g_thread_init(NULL);

  init_mutex = g_mutex_new();
  innodb_tables_mutex = g_mutex_new();
  non_innodb_table_mutex = g_mutex_new();
  table_schemas_mutex = g_mutex_new();
  view_schemas_mutex = g_mutex_new();
  schema_post_mutex = g_mutex_new();
  ll_mutex = g_mutex_new();
  ll_cond = g_cond_new();

  context = g_option_context_new("multi-threaded MySQL dumping");
  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
  g_option_group_add_entries(main_group, common_entries);
  g_option_context_set_main_group(context, main_group);
  if (!g_option_context_parse(context, &argc, &argv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }
  g_option_context_free(context);

  // prompt for password if it's NULL
  if (sizeof(password) == 0 || (password == NULL && askPassword)) {
    password = passwordPrompt();
  }

  // printf("your password is %s and the size is %d
  // \n",password,sizeof(password));

  if (program_version) {
    g_print("mydumper %s, built against MySQL %s\n", VERSION,
            MYSQL_VERSION_STR);
    exit(EXIT_SUCCESS);
  }

  set_verbose(verbose);

  time_t t;
  time(&t);
  localtime_r(&t, &tval);

  // rows chunks have precedence over chunk_filesize
  if (rows_per_file > 0 && chunk_filesize > 0) {
    chunk_filesize = 0;
    g_warning("--chunk-filesize disabled by --rows option");
  }

  // until we have an unique option on lock types we need to ensure this
  if (no_locks || trx_consistency_only)
    less_locking = 0;

  /* savepoints workaround to avoid metadata locking issues
     doesnt work for chuncks */
  if (rows_per_file && use_savepoints) {
    use_savepoints = FALSE;
    g_warning("--use-savepoints disabled by --rows");
  }

  // clarify binlog coordinates with trx_consistency_only
  if (trx_consistency_only)
    g_warning("Using trx_consistency_only, binlog coordinates will not be "
              "accurate if you are writing to non transactional tables.");

  if (!output_directory)
    output_directory = g_strdup_printf(
        "%s-%04d%02d%02d-%02d%02d%02d", DIRECTORY, tval.tm_year + 1900,
        tval.tm_mon + 1, tval.tm_mday, tval.tm_hour, tval.tm_min, tval.tm_sec);

  create_backup_dir(output_directory);
  if (daemon_mode) {
    pid_t pid, sid;

    pid = fork();
    if (pid < 0)
      exit(EXIT_FAILURE);
    else if (pid > 0)
      exit(EXIT_SUCCESS);

    umask(0);
    sid = setsid();

    if (sid < 0)
      exit(EXIT_FAILURE);

    char *dump_directory = g_strdup_printf("%s/0", output_directory);
    create_backup_dir(dump_directory);
    g_free(dump_directory);
    dump_directory = g_strdup_printf("%s/1", output_directory);
    create_backup_dir(dump_directory);
    g_free(dump_directory);
#ifdef WITH_BINLOG
    daemon_binlog_directory =
        g_strdup_printf("%s/%s", output_directory, DAEMON_BINLOGS);
    create_backup_dir(daemon_binlog_directory);
#endif
  }
#ifdef WITH_BINLOG
  if (need_binlogs) {
    binlog_directory =
        g_strdup_printf("%s/%s", output_directory, BINLOG_DIRECTORY);
    create_backup_dir(binlog_directory);
  }
#endif
  /* Give ourselves an array of engines to ignore */
  if (ignore_engines)
    ignore = g_strsplit(ignore_engines, ",", 0);

  /* Give ourselves an array of tables to dump */
  if (tables_list)
    tables = g_strsplit(tables_list, ",", 0);

  /* Process list of tables to omit if specified */
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file);

  if (daemon_mode) {
    GError *terror;
#ifdef WITH_BINLOG
    GThread *bthread =
        g_thread_create(binlog_thread, GINT_TO_POINTER(1), FALSE, &terror);
    if (bthread == NULL) {
      g_critical("Could not create binlog thread: %s", terror->message);
      g_error_free(terror);
      exit(EXIT_FAILURE);
    }
#endif
    start_scheduled_dump = g_async_queue_new();
    GThread *ethread =
        g_thread_create(exec_thread, GINT_TO_POINTER(1), FALSE, &terror);
    if (ethread == NULL) {
      g_critical("Could not create exec thread: %s", terror->message);
      g_error_free(terror);
      exit(EXIT_FAILURE);
    }
    // Run initial snapshot
    run_snapshot(NULL);
#if GLIB_MINOR_VERSION < 14
    g_timeout_add(snapshot_interval * 60 * 1000, (GSourceFunc)run_snapshot,
                  NULL);
#else
    g_timeout_add_seconds(snapshot_interval * 60, (GSourceFunc)run_snapshot,
                          NULL);
#endif
    guint sigsource = g_unix_signal_add(SIGINT, sig_triggered, NULL);
    sigsource = g_unix_signal_add(SIGTERM, sig_triggered, NULL);
    m1 = g_main_loop_new(NULL, TRUE);
    g_main_loop_run(m1);
    g_source_remove(sigsource);
  } else {
    MYSQL *conn = create_main_connection();
    start_dump(conn);
  }

  // sleep(5);
  mysql_thread_end();
  mysql_library_end();
  g_free(output_directory);
  g_strfreev(ignore);
  g_strfreev(tables);

  if (logoutfile) {
    fclose(logoutfile);
  }

  exit(errors ? EXIT_FAILURE : EXIT_SUCCESS);
}

MYSQL *create_main_connection() {
  MYSQL *conn;
  conn = mysql_init(NULL);

  configure_connection(conn, "mydumper");

  if (!mysql_real_connect(conn, hostname, username, password, db, port,
                          socket_path, 0)) {
    g_critical("Error connecting to database: %s", mysql_error(conn));
    exit(EXIT_FAILURE);
  }

  detected_server = detect_server(conn);

  if ((detected_server == SERVER_TYPE_MYSQL) &&
      mysql_query(conn, "SET SESSION wait_timeout = 2147483")) {
    g_warning("Failed to increase wait_timeout: %s", mysql_error(conn));
  }
  if ((detected_server == SERVER_TYPE_MYSQL) &&
      mysql_query(conn, "SET SESSION net_write_timeout = 2147483")) {
    g_warning("Failed to increase net_write_timeout: %s", mysql_error(conn));
  }

  switch (detected_server) {
  case SERVER_TYPE_MYSQL:
    g_message("Connected to a MySQL server");
    break;
  case SERVER_TYPE_DRIZZLE:
    g_message("Connected to a Drizzle server");
    break;
  case SERVER_TYPE_TIDB:
    g_message("Connected to a TiDB server");
    break;
  default:
    g_critical("Cannot detect server type");
    exit(EXIT_FAILURE);
    break;
  }

  return conn;
}

void *exec_thread(void *data) {
  (void)data;

  while (1) {
    g_async_queue_pop(start_scheduled_dump);
    clear_dump_directory();
    MYSQL *conn = create_main_connection();
    start_dump(conn);
    // start_dump already closes mysql
    // mysql_close(conn);
    mysql_thread_end();

    // Don't switch the symlink on shutdown because the dump is probably
    // incomplete.
    if (!shutdown_triggered) {
      const char *dump_symlink_source = (dump_number == 0) ? "0" : "1";
      char *dump_symlink_dest =
          g_strdup_printf("%s/last_dump", output_directory);

      // We don't care if this fails
      g_unlink(dump_symlink_dest);

      if (symlink(dump_symlink_source, dump_symlink_dest) == -1) {
        g_critical("error setting last good dump symlink %s, %d",
                   dump_symlink_dest, errno);
      }
      g_free(dump_symlink_dest);

      dump_number = (dump_number == 1) ? 0 : 1;
    }
  }
  return NULL;
}
#ifdef WITH_BINLOG
void *binlog_thread(void *data) {
  (void)data;
  MYSQL_RES *master = NULL;
  MYSQL_ROW row;
  MYSQL *conn;
  conn = mysql_init(NULL);
  if (defaults_file != NULL) {
    mysql_options(conn, MYSQL_READ_DEFAULT_FILE, defaults_file);
  }
  mysql_options(conn, MYSQL_READ_DEFAULT_GROUP, "mydumper");

  if (!mysql_real_connect(conn, hostname, username, password, db, port,
                          socket_path, 0)) {
    g_critical("Error connecting to database: %s", mysql_error(conn));
    exit(EXIT_FAILURE);
  }

  mysql_query(conn, "SHOW MASTER STATUS");
  master = mysql_store_result(conn);
  if (master && (row = mysql_fetch_row(master))) {
    MYSQL *binlog_connection = NULL;
    binlog_connection = reconnect_for_binlog(binlog_connection);
    binlog_connect_id = mysql_thread_id(binlog_connection);
    guint64 start_position = g_ascii_strtoull(row[1], NULL, 10);
    gchar *filename = g_strdup(row[0]);
    mysql_free_result(master);
    mysql_close(conn);
    g_message(
        "Continuous binlog thread connected using MySQL connection ID %lu",
        mysql_thread_id(binlog_connection));
    get_binlog_file(binlog_connection, filename, daemon_binlog_directory,
                    start_position, 0, TRUE);
    g_free(filename);
    mysql_close(binlog_connection);
  } else {
    mysql_free_result(master);
    mysql_close(conn);
  }
  g_message("Continuous binlog thread shutdown");
  mysql_thread_end();
  return NULL;
}
#endif
void start_dump(MYSQL *conn) {
  struct configuration conf = {1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0};
  char *p;
  char *p2;
  char *p3;
  char *u;

  guint64 nits[num_threads];
  GList *nitl[num_threads];
  int tn = 0;
  guint64 min = 0;
  time_t t;
  struct db_table *dbt;
  struct schema_post *sp;
  guint n;
  FILE *nufile = NULL;
  guint have_backup_locks = 0;

  for (n = 0; n < num_threads; n++) {
    nits[n] = 0;
    nitl[n] = NULL;
  }

  if (daemon_mode)
    p = g_strdup_printf("%s/%d/metadata.partial", output_directory,
                        dump_number);
  else
    p = g_strdup_printf("%s/metadata.partial", output_directory);
  p2 = g_strndup(p, (unsigned)strlen(p) - 8);

  FILE *mdfile = g_fopen(p, "w");
  if (!mdfile) {
    g_critical("Couldn't write metadata file (%d)", errno);
    exit(EXIT_FAILURE);
  }

  if (updated_since > 0) {
    if (daemon_mode)
      u = g_strdup_printf("%s/%d/not_updated_tables", output_directory,
                          dump_number);
    else
      u = g_strdup_printf("%s/not_updated_tables", output_directory);
    nufile = g_fopen(u, "w");
    if (!nufile) {
      g_critical("Couldn't write not_updated_tables file (%d)", errno);
      exit(EXIT_FAILURE);
    }
    get_not_updated(conn, nufile);
  }

  /* We check SHOW PROCESSLIST, and if there're queries
     larger than preset value, we terminate the process.

     This avoids stalling whole server with flush */

  if (!no_locks) {
    if (mysql_query(conn, "SHOW PROCESSLIST")) {
      g_warning("Could not check PROCESSLIST, no long query guard enabled: %s",
                mysql_error(conn));
    } else {
      MYSQL_RES *res = mysql_store_result(conn);
      MYSQL_ROW row;

      /* Just in case PROCESSLIST output column order changes */
      MYSQL_FIELD *fields = mysql_fetch_fields(res);
      guint i;
      int tcol = -1, ccol = -1, icol = -1;
      for (i = 0; i < mysql_num_fields(res); i++) {
        if (!strcasecmp(fields[i].name, "Command"))
          ccol = i;
        else if (!strcasecmp(fields[i].name, "Time"))
          tcol = i;
        else if (!strcasecmp(fields[i].name, "Id"))
          icol = i;
      }
      if ((tcol < 0) || (ccol < 0) || (icol < 0)) {
        g_critical("Error obtaining information from processlist");
        exit(EXIT_FAILURE);
      }
      while ((row = mysql_fetch_row(res))) {
        if (row[ccol] && strcmp(row[ccol], "Query"))
          continue;
        if (row[tcol] && atoi(row[tcol]) > longquery) {
          if (killqueries) {
            if (mysql_query(conn,
                            p3 = g_strdup_printf("KILL %lu", atol(row[icol]))))
              g_warning("Could not KILL slow query: %s", mysql_error(conn));
            else
              g_warning("Killed a query that was running for %ss", row[tcol]);
            g_free(p3);
          } else {
            g_critical("There are queries in PROCESSLIST running longer than "
                       "%us, aborting dump,\n\t"
                       "use --long-query-guard to change the guard value, kill "
                       "queries (--kill-long-queries) or use \n\tdifferent "
                       "server for dump",
                       longquery);
            exit(EXIT_FAILURE);
          }
        }
      }
      mysql_free_result(res);
    }
  }

  if (!no_locks && (detected_server != SERVER_TYPE_TIDB)) {
    // Percona Server 8 removed LOCK BINLOG so backup locks is useless for
    // mydumper now and we need to fail back to FTWRL
    mysql_query(conn, "SELECT @@version_comment, @@version");
    MYSQL_RES *res2 = mysql_store_result(conn);
    MYSQL_ROW ver;
    while ((ver = mysql_fetch_row(res2))) {
      if (!g_str_has_prefix("Percona", ver[0]) &&
          !g_str_has_prefix("8.", ver[1])) {
        g_message("Disabling Percona Backup Locks for Percona Server 8");
        no_backup_locks = 1;
      }
    }
    mysql_free_result(res2);

    // Percona Backup Locks
    if (!no_backup_locks) {
      mysql_query(conn, "SELECT @@have_backup_locks");
      MYSQL_RES *rest = mysql_store_result(conn);
      if (rest != NULL && mysql_num_rows(rest)) {
        mysql_free_result(rest);
        g_message("Using Percona Backup Locks");
        have_backup_locks = 1;
      }
    }

    if (have_backup_locks) {
      if (mysql_query(conn, "LOCK TABLES FOR BACKUP")) {
        g_critical("Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will "
                   "not be consistent: %s",
                   mysql_error(conn));
        errors++;
      }

      if (mysql_query(conn, "LOCK BINLOG FOR BACKUP")) {
        g_critical("Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will "
                   "not be consistent: %s",
                   mysql_error(conn));
        errors++;
      }
    } else if (lock_all_tables) {
      // LOCK ALL TABLES
      GString *query = g_string_sized_new(16777216);
      gchar *dbtb = NULL;
      gchar **dt = NULL;
      GList *tables_lock = NULL;
      GList *iter = NULL;
      guint success = 0;
      guint retry = 0;
      guint lock = 1;
      int i = 0;

      if (db) {
        g_string_printf(
            query,
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE ='BASE TABLE' AND NOT "
            "(TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR "
            "TABLE_NAME = 'general_log'))",
            db);
      } else if (tables) {
        for (i = 0; tables[i] != NULL; i++) {
          dt = g_strsplit(tables[i], ".", 0);
          dbtb = g_strdup_printf("`%s`.`%s`", dt[0], dt[1]);
          tables_lock = g_list_prepend(tables_lock, dbtb);
        }
        tables_lock = g_list_reverse(tables_lock);
      } else {
        g_string_printf(
            query,
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_TYPE ='BASE TABLE' AND TABLE_SCHEMA NOT IN "
            "('information_schema', 'performance_schema', 'data_dictionary') "
            "AND NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR "
            "TABLE_NAME = 'general_log'))");
      }

      if (tables_lock == NULL) {
        if (mysql_query(conn, query->str)) {
          g_critical("Couldn't get table list for lock all tables: %s",
                     mysql_error(conn));
          errors++;
        } else {
          MYSQL_RES *res = mysql_store_result(conn);
          MYSQL_ROW row;

          while ((row = mysql_fetch_row(res))) {
            lock = 1;
            if (tables) {
              int table_found = 0;
              for (i = 0; tables[i] != NULL; i++)
                if (g_ascii_strcasecmp(tables[i], row[1]) == 0)
                  table_found = 1;
              if (!table_found)
                lock = 0;
            }
            if (lock && tables_skiplist_file && check_skiplist(row[0], row[1]))
              continue;
            if (lock && regexstring && !check_regex(row[0], row[1]))
              continue;

            if (lock) {
              dbtb = g_strdup_printf("`%s`.`%s`", row[0], row[1]);
              tables_lock = g_list_prepend(tables_lock, dbtb);
            }
          }
          tables_lock = g_list_reverse(tables_lock);
        }
      }

      // Try three times to get the lock, this is in case of tmp tables
      // disappearing
      while (!success && retry < 4) {
        n = 0;
        for (iter = tables_lock; iter != NULL; iter = iter->next) {
          if (n == 0) {
            g_string_printf(query, "LOCK TABLE %s READ", (char *)iter->data);
            n = 1;
          } else {
            g_string_append_printf(query, ", %s READ", (char *)iter->data);
          }
        }
        if (mysql_query(conn, query->str)) {
          gchar *failed_table = NULL;
          gchar **tmp_fail;

          tmp_fail = g_strsplit(mysql_error(conn), "'", 0);
          tmp_fail = g_strsplit(tmp_fail[1], ".", 0);
          failed_table = g_strdup_printf("`%s`.`%s`", tmp_fail[0], tmp_fail[1]);
          for (iter = tables_lock; iter != NULL; iter = iter->next) {
            if (strcmp(iter->data, failed_table) == 0) {
              tables_lock = g_list_remove(tables_lock, iter->data);
            }
          }
          g_free(tmp_fail);
          g_free(failed_table);
        } else {
          success = 1;
        }
        retry += 1;
      }
      if (!success) {
        g_critical("Lock all tables fail: %s", mysql_error(conn));
        exit(EXIT_FAILURE);
      }
      g_free(query->str);
      g_list_free(tables_lock);
    } else {
      if (mysql_query(conn, "FLUSH TABLES WITH READ LOCK")) {
        g_critical("Couldn't acquire global lock, snapshots will not be "
                   "consistent: %s",
                   mysql_error(conn));
        errors++;
      }
    }
  } else if (detected_server == SERVER_TYPE_TIDB) {
    g_message("Skipping locks because of TiDB");
    if (!tidb_snapshot) {

      // Generate a @@tidb_snapshot to use for the worker threads since
      // the tidb-snapshot argument was not specified when starting mydumper

      if (mysql_query(conn, "SHOW MASTER STATUS")) {
        g_critical("Couldn't generate @@tidb_snapshot: %s", mysql_error(conn));
        exit(EXIT_FAILURE);
      } else {

        MYSQL_RES *result = mysql_store_result(conn);
        MYSQL_ROW row = mysql_fetch_row(
            result); /* There should never be more than one row */
        tidb_snapshot = g_strdup(row[1]);
        mysql_free_result(result);
      }
    }

    // Need to set the @@tidb_snapshot for the master thread
    gchar *query =
        g_strdup_printf("SET SESSION tidb_snapshot = '%s'", tidb_snapshot);

    g_message("Set to tidb_snapshot '%s'", tidb_snapshot);

    if (mysql_query(conn, query)) {
      g_critical("Failed to set tidb_snapshot: %s", mysql_error(conn));
      exit(EXIT_FAILURE);
    }
    g_free(query);

  } else {
    g_warning("Executing in no-locks mode, snapshot will notbe consistent");
  }
  if (mysql_get_server_version(conn) < 40108) {
    mysql_query(
        conn,
        "CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB");
    need_dummy_read = 1;
  }

  // tokudb do not support consistent snapshot
  mysql_query(conn, "SELECT @@tokudb_version");
  MYSQL_RES *rest = mysql_store_result(conn);
  if (rest != NULL && mysql_num_rows(rest)) {
    mysql_free_result(rest);
    g_message("TokuDB detected, creating dummy table for CS");
    mysql_query(
        conn,
        "CREATE TABLE IF NOT EXISTS mysql.tokudbdummy (a INT) ENGINE=TokuDB");
    need_dummy_toku_read = 1;
  }

  // Do not start a transaction when lock all tables instead of FTWRL,
  // since it can implicitly release read locks we hold
  if (!lock_all_tables) {
    mysql_query(conn, "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */");
  }

  if (need_dummy_read) {
    mysql_query(conn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy");
    MYSQL_RES *res = mysql_store_result(conn);
    if (res)
      mysql_free_result(res);
  }
  if (need_dummy_toku_read) {
    mysql_query(conn,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy");
    MYSQL_RES *res = mysql_store_result(conn);
    if (res)
      mysql_free_result(res);
  }
  time(&t);
  localtime_r(&t, &tval);
  fprintf(mdfile, "Started dump at: %04d-%02d-%02d %02d:%02d:%02d\n",
          tval.tm_year + 1900, tval.tm_mon + 1, tval.tm_mday, tval.tm_hour,
          tval.tm_min, tval.tm_sec);

  g_message("Started dump at: %04d-%02d-%02d %02d:%02d:%02d\n",
            tval.tm_year + 1900, tval.tm_mon + 1, tval.tm_mday, tval.tm_hour,
            tval.tm_min, tval.tm_sec);

  if (detected_server == SERVER_TYPE_MYSQL) {
    mysql_query(conn, "/*!40101 SET NAMES binary*/");

    write_snapshot_info(conn, mdfile);
  }

  GThread **threads = g_new(GThread *, num_threads * (less_locking + 1));
  struct thread_data *td =
      g_new(struct thread_data, num_threads * (less_locking + 1));

  if (less_locking) {
    conf.queue_less_locking = g_async_queue_new();
    conf.ready_less_locking = g_async_queue_new();
    less_locking_threads = num_threads;
    for (n = num_threads; n < num_threads * 2; n++) {
      td[n].conf = &conf;
      td[n].thread_id = n + 1;
      threads[n] = g_thread_create((GThreadFunc)process_queue_less_locking,
                                   &td[n], TRUE, NULL);
      g_async_queue_pop(conf.ready_less_locking);
    }
    g_async_queue_unref(conf.ready_less_locking);
  }

  conf.queue = g_async_queue_new();
  conf.ready = g_async_queue_new();
  conf.unlock_tables = g_async_queue_new();
  conf.ready_database_dump = g_async_queue_new();

  for (n = 0; n < num_threads; n++) {
    td[n].conf = &conf;
    td[n].thread_id = n + 1;
    threads[n] =
        g_thread_create((GThreadFunc)process_queue, &td[n], TRUE, NULL);
    g_async_queue_pop(conf.ready);
  }

  g_async_queue_unref(conf.ready);

  if (trx_consistency_only) {
    g_message("Transactions started, unlocking tables");
    mysql_query(conn, "UNLOCK TABLES /* trx-only */");
    if (have_backup_locks)
      mysql_query(conn, "UNLOCK BINLOG");
  }

  if (db) {
    dump_database(db, &conf);
    if (!no_schemas)
      dump_create_database(db, &conf);
  } else if (tables) {
    get_tables(conn, &conf);
  } else {
    MYSQL_RES *databases;
    MYSQL_ROW row;
    if (mysql_query(conn, "SHOW DATABASES") ||
        !(databases = mysql_store_result(conn))) {
      g_critical("Unable to list databases: %s", mysql_error(conn));
      exit(EXIT_FAILURE);
    }

    while ((row = mysql_fetch_row(databases))) {
      if (!strcasecmp(row[0], "information_schema") ||
          !strcasecmp(row[0], "performance_schema") ||
          (!strcasecmp(row[0], "data_dictionary")))
        continue;
      dump_database(row[0], &conf);
      /* Checks PCRE expressions on 'database' string */
      if (!no_schemas && (regexstring == NULL || check_regex(row[0], NULL)))
        dump_create_database(row[0], &conf);
    }
    mysql_free_result(databases);
  }
  g_async_queue_pop(conf.ready_database_dump);
  g_async_queue_unref(conf.ready_database_dump);

  g_list_free(no_updated_tables);

  if (!non_innodb_table) {
    g_async_queue_push(conf.unlock_tables, GINT_TO_POINTER(1));
  }

  non_innodb_table = g_list_reverse(non_innodb_table);
  if (less_locking) {

    GList *iter;
    for (iter = non_innodb_table; iter != NULL; iter = iter->next) {
      dbt = (struct db_table *)iter->data;
      tn = 0;
      min = nits[0];
      for (n = 1; n < num_threads; n++) {
        if (nits[n] < min) {
          min = nits[n];
          tn = n;
        }
      }
      nitl[tn] = g_list_prepend(nitl[tn], dbt);
      nits[tn] += dbt->datalength;
    }
    nitl[tn] = g_list_reverse(nitl[tn]);

    for (n = 0; n < num_threads; n++) {
      if (nits[n] > 0) {
        g_atomic_int_inc(&non_innodb_table_counter);
        dump_tables(conn, nitl[n], &conf);
        g_list_free(nitl[n]);
      }
    }
    g_list_free(non_innodb_table);

    if (g_atomic_int_get(&non_innodb_table_counter))
      g_atomic_int_inc(&non_innodb_done);
    else
      g_async_queue_push(conf.unlock_tables, GINT_TO_POINTER(1));

    for (n = 0; n < num_threads; n++) {
      struct job *j = g_new0(struct job, 1);
      j->type = JOB_SHUTDOWN;
      g_async_queue_push(conf.queue_less_locking, j);
    }
  } else {
    GList *iter;
    for (iter = non_innodb_table; iter != NULL; iter = iter->next) {
      dbt = (struct db_table *)iter->data;
      dump_table(conn, dbt->database, dbt->table, &conf, FALSE);
      g_atomic_int_inc(&non_innodb_table_counter);
    }
    g_list_free(non_innodb_table);
    g_atomic_int_inc(&non_innodb_done);
  }

  innodb_tables = g_list_reverse(innodb_tables);
  GList *iter;
  for (iter = innodb_tables; iter != NULL; iter = iter->next) {
    dbt = (struct db_table *)iter->data;
    dump_table(conn, dbt->database, dbt->table, &conf, TRUE);
  }
  g_list_free(innodb_tables);

  table_schemas = g_list_reverse(table_schemas);
  for (iter = table_schemas; iter != NULL; iter = iter->next) {
    dbt = (struct db_table *)iter->data;
    dump_schema(conn, dbt->database, dbt->table, &conf);
    g_free(dbt->table);
    g_free(dbt->database);
    g_free(dbt);
  }
  g_list_free(table_schemas);

  view_schemas = g_list_reverse(view_schemas);
  for (iter = view_schemas; iter != NULL; iter = iter->next) {
    dbt = (struct db_table *)iter->data;
    dump_view(dbt->database, dbt->table, &conf);
    g_free(dbt->table);
    g_free(dbt->database);
    g_free(dbt);
  }
  g_list_free(view_schemas);

  schema_post = g_list_reverse(schema_post);
  for (iter = schema_post; iter != NULL; iter = iter->next) {
    sp = (struct schema_post *)iter->data;
    dump_schema_post(sp->database, &conf);
    g_free(sp->database);
    g_free(sp);
  }
  g_list_free(schema_post);

  if (!no_locks && !trx_consistency_only) {
    g_async_queue_pop(conf.unlock_tables);
    g_message("Non-InnoDB dump complete, unlocking tables");
    mysql_query(conn, "UNLOCK TABLES /* FTWRL */");
    if (have_backup_locks)
      mysql_query(conn, "UNLOCK BINLOG");
  }
#ifdef WITH_BINLOG
  if (need_binlogs) {
    get_binlogs(conn, &conf);
  }
#endif
  // close main connection
  mysql_close(conn);

  if (less_locking) {
    for (n = num_threads; n < num_threads * 2; n++) {
      g_thread_join(threads[n]);
    }
    g_async_queue_unref(conf.queue_less_locking);
  }

  for (n = 0; n < num_threads; n++) {
    struct job *j = g_new0(struct job, 1);
    j->type = JOB_SHUTDOWN;
    g_async_queue_push(conf.queue, j);
  }

  for (n = 0; n < num_threads; n++) {
    g_thread_join(threads[n]);
  }
  g_async_queue_unref(conf.queue);
  g_async_queue_unref(conf.unlock_tables);

  time(&t);
  localtime_r(&t, &tval);
  fprintf(mdfile, "Finished dump at: %04d-%02d-%02d %02d:%02d:%02d\n",
          tval.tm_year + 1900, tval.tm_mon + 1, tval.tm_mday, tval.tm_hour,
          tval.tm_min, tval.tm_sec);
  fclose(mdfile);
  if (updated_since > 0)
    fclose(nufile);
  g_rename(p, p2);
  g_free(p);
  g_free(p2);
  g_message("Finished dump at: %04d-%02d-%02d %02d:%02d:%02d\n",
            tval.tm_year + 1900, tval.tm_mon + 1, tval.tm_mday, tval.tm_hour,
            tval.tm_min, tval.tm_sec);

  g_free(td);
  g_free(threads);
}

void dump_create_database(char *database, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct create_database_job *cdj = g_new0(struct create_database_job, 1);
  j->job_data = (void *)cdj;
  cdj->database = g_strdup(database);
  j->conf = conf;
  j->type = JOB_CREATE_DATABASE;

  if (daemon_mode)
    cdj->filename =
        g_strdup_printf("%s/%d/%s-schema-create.sql%s", output_directory,
                        dump_number, database, (compress_output ? ".gz" : ""));
  else
    cdj->filename =
        g_strdup_printf("%s/%s-schema-create.sql%s", output_directory, database,
                        (compress_output ? ".gz" : ""));

  g_async_queue_push(conf->queue, j);
  return;
}

void dump_create_database_data(MYSQL *conn, char *database, char *filename) {
  void *outfile = NULL;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;

  if (!compress_output)
    outfile = g_fopen(filename, "w");
  else
    outfile = (void *)gzopen(filename, "w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  query = g_strdup_printf("SHOW CREATE DATABASE `%s`", database);
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

  if (!compress_output)
    fclose((FILE *)outfile);
  else
    gzclose((gzFile)outfile);

  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  return;
}

void get_not_updated(MYSQL *conn, FILE *file) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  gchar *query =
      g_strdup_printf("SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM "
                      "information_schema.TABLES WHERE TABLE_TYPE = 'BASE "
                      "TABLE' AND UPDATE_TIME < NOW() - INTERVAL %d DAY",
                      updated_since);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  while ((row = mysql_fetch_row(res))) {
    no_updated_tables = g_list_prepend(no_updated_tables, row[0]);
    fprintf(file, "%s\n", row[0]);
  }
  no_updated_tables = g_list_reverse(no_updated_tables);
  fflush(file);
}

gboolean detect_generated_fields(MYSQL *conn, char *database, char *table) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  gboolean result = FALSE;

  gchar *query = g_strdup_printf(
      "select COLUMN_NAME from information_schema.COLUMNS where "
      "TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra like '%%GENERATED%%'",
      database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  if ((row = mysql_fetch_row(res))) {
    result = TRUE;
  }
  mysql_free_result(res);

  return result;
}

GString *get_insertable_fields(MYSQL *conn, char *database, char *table) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
      g_strdup_printf("select COLUMN_NAME from information_schema.COLUMNS "
                      "where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra "
                      "not like '%%GENERATED%%'",
                      database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else {
      g_string_append(field_list, ",");
    }

    gchar *tb = g_strdup_printf("`%s`", row[0]);
    g_string_append(field_list, tb);
    g_free(tb);
  }
  mysql_free_result(res);

  return field_list;
}

/* Heuristic chunks building - based on estimates, produces list of ranges for
   datadumping WORK IN PROGRESS
*/
GList *get_chunks_for_table(MYSQL *conn, char *database, char *table,
                            struct configuration *conf) {

  GList *chunks = NULL;
  MYSQL_RES *indexes = NULL, *minmax = NULL, *total = NULL;
  MYSQL_ROW row;
  char *field = NULL;
  int showed_nulls = 0;

  /* first have to pick index, in future should be able to preset in
   * configuration too */
  gchar *query = g_strdup_printf("SHOW INDEX FROM `%s`.`%s`", database, table);
  mysql_query(conn, query);
  g_free(query);
  indexes = mysql_store_result(conn);

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
  /* Oh well, no chunks today - no suitable index */
  if (!field)
    goto cleanup;

  /* Get minimum/maximum */
  mysql_query(conn, query = g_strdup_printf(
                        "SELECT %s MIN(`%s`),MAX(`%s`) FROM `%s`.`%s`",
                        (detected_server == SERVER_TYPE_MYSQL)
                            ? "/*!40001 SQL_NO_CACHE */"
                            : "",
                        field, field, database, table));
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

  /* Got total number of rows, skip chunk logic if estimates are low */
  guint64 rows = estimate_count(conn, database, table, field, NULL, NULL);
  if (rows <= rows_per_file)
    goto cleanup;

  /* This is estimate, not to use as guarantee! Every chunk would have eventual
   * adjustments */
  guint64 estimated_chunks = rows / rows_per_file;
  guint64 estimated_step, nmin, nmax, cutoff;

  /* Support just bigger INTs for now, very dumb, no verify approach */
  switch (fields[0].type) {
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_INT24:
    /* static stepping */
    nmin = strtoul(min, NULL, 10);
    nmax = strtoul(max, NULL, 10);
    estimated_step = (nmax - nmin) / estimated_chunks + 1;
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

  default:
    goto cleanup;
  }

cleanup:
  if (indexes)
    mysql_free_result(indexes);
  if (minmax)
    mysql_free_result(minmax);
  if (total)
    mysql_free_result(total);
  return chunks;
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
      fromclause = g_strdup_printf(" `%s` >= \"%s\" ", field, escaped);
      g_free(escaped);
    }
    if (to) {
      escaped = g_new(char, strlen(to) * 2 + 1);
      mysql_real_escape_string(conn, escaped, from, strlen(from));
      toclause = g_strdup_printf(" `%s` <= \"%s\"", field, escaped);
      g_free(escaped);
    }
    query = g_strdup_printf("%s WHERE `%s` %s %s", querybase,
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

void create_backup_dir(char *new_directory) {
  if (g_mkdir(new_directory, 0700) == -1) {
    if (errno != EEXIST) {
      g_critical("Unable to create `%s': %s", new_directory, g_strerror(errno));
      exit(EXIT_FAILURE);
    }
  }
}

void dump_database(char *database, struct configuration *conf) {

  g_atomic_int_inc(&database_counter);

  struct job *j = g_new0(struct job, 1);
  struct dump_database_job *ddj = g_new0(struct dump_database_job, 1);
  j->job_data = (void *)ddj;
  ddj->database = g_strdup(database);
  j->conf = conf;
  j->type = JOB_DUMP_DATABASE;

  if (less_locking)
    g_async_queue_push(conf->queue_less_locking, j);
  else
    g_async_queue_push(conf->queue, j);
  return;
}

void dump_database_thread(MYSQL *conn, char *database) {

  char *query;
  mysql_select_db(conn, database);
  if (detected_server == SERVER_TYPE_MYSQL ||
      detected_server == SERVER_TYPE_TIDB)
    query = g_strdup("SHOW TABLE STATUS");
  else
    query =
        g_strdup_printf("SELECT TABLE_NAME, ENGINE, TABLE_TYPE as COMMENT FROM "
                        "DATA_DICTIONARY.TABLES WHERE TABLE_SCHEMA='%s'",
                        database);

  if (mysql_query(conn, (query))) {
    g_critical("Error: DB: %s - Could not execute query: %s", database,
               mysql_error(conn));
    errors++;
    g_free(query);
    return;
  }

  MYSQL_RES *result = mysql_store_result(conn);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);
  guint i;
  int ecol = -1, ccol = -1;
  for (i = 0; i < mysql_num_fields(result); i++) {
    if (!strcasecmp(fields[i].name, "Engine"))
      ecol = i;
    else if (!strcasecmp(fields[i].name, "Comment"))
      ccol = i;
  }

  if (!result) {
    g_critical("Could not list tables for %s: %s", database, mysql_error(conn));
    errors++;
    return;
  }

  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result))) {

    int dump = 1;
    int is_view = 0;

    /* We now do care about views!
            num_fields>1 kicks in only in case of 5.0 SHOW FULL TABLES or SHOW
       TABLE STATUS row[1] == NULL if it is a view in 5.0 'SHOW TABLE STATUS'
            row[1] == "VIEW" if it is a view in 5.0 'SHOW FULL TABLES'
    */
    if ((detected_server == SERVER_TYPE_MYSQL) &&
        (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
      is_view = 1;

    /* Check for broken tables, i.e. mrg with missing source tbl */
    if (!is_view && row[ecol] == NULL) {
      g_warning("Broken table detected, please review: %s.%s", database,
                row[0]);
      dump = 0;
    }

    /* Skip ignored engines, handy for avoiding Merge, Federated or Blackhole
     * :-) dumps */
    if (dump && ignore && !is_view) {
      for (i = 0; ignore[i] != NULL; i++) {
        if (g_ascii_strcasecmp(ignore[i], row[ecol]) == 0) {
          dump = 0;
          break;
        }
      }
    }

    /* Skip views */
    if (is_view && no_dump_views)
      dump = 0;

    if (!dump)
      continue;

    /* In case of table-list option is enabled, check if table is part of the
     * list */
    if (tables) {
      int table_found = 0;
      for (i = 0; tables[i] != NULL; i++)
        if (g_ascii_strcasecmp(tables[i], row[0]) == 0)
          table_found = 1;

      if (!table_found)
        dump = 0;
    }
    if (!dump)
      continue;

    /* Special tables */
    if (g_ascii_strcasecmp(database, "mysql") == 0 &&
        (g_ascii_strcasecmp(row[0], "general_log") == 0 ||
         g_ascii_strcasecmp(row[0], "slow_log") == 0 ||
         g_ascii_strcasecmp(row[0], "innodb_index_stats") == 0 ||
         g_ascii_strcasecmp(row[0], "innodb_table_stats") == 0)) {
      dump = 0;
      continue;
    }

    /* Checks skip list on 'database.table' string */
    if (tables_skiplist && check_skiplist(database, row[0]))
      continue;

    /* Checks PCRE expressions on 'database.table' string */
    if (regexstring && !check_regex(database, row[0]))
      continue;

    /* Check if the table was recently updated */
    if (no_updated_tables && !is_view) {
      GList *iter;
      for (iter = no_updated_tables; iter != NULL; iter = iter->next) {
        if (g_ascii_strcasecmp(
                iter->data, g_strdup_printf("%s.%s", database, row[0])) == 0) {
          g_message("NO UPDATED TABLE: %s.%s", database, row[0]);
          dump = 0;
        }
      }
    }

    if (!dump)
      continue;

    /* Green light! */
    struct db_table *dbt = g_new(struct db_table, 1);
    dbt->database = g_strdup(database);
    dbt->table = g_strdup(row[0]);
    if (!row[6])
      dbt->datalength = 0;
    else
      dbt->datalength = g_ascii_strtoull(row[6], NULL, 10);
    // if is a view we care only about schema
    if (!is_view) {
      // with trx_consistency_only we dump all as innodb_tables
      if (!no_data) {
        if (row[ecol] != NULL && g_ascii_strcasecmp("MRG_MYISAM", row[ecol])) {
          if (trx_consistency_only ||
              (row[ecol] != NULL && !g_ascii_strcasecmp("InnoDB", row[ecol]))) {
            g_mutex_lock(innodb_tables_mutex);
            innodb_tables = g_list_prepend(innodb_tables, dbt);
            g_mutex_unlock(innodb_tables_mutex);
          } else if (row[ecol] != NULL &&
                     !g_ascii_strcasecmp("TokuDB", row[ecol])) {
            g_mutex_lock(innodb_tables_mutex);
            innodb_tables = g_list_prepend(innodb_tables, dbt);
            g_mutex_unlock(innodb_tables_mutex);
          } else {
            g_mutex_lock(non_innodb_table_mutex);
            non_innodb_table = g_list_prepend(non_innodb_table, dbt);
            g_mutex_unlock(non_innodb_table_mutex);
          }
        }
      }
      if (!no_schemas) {
        g_mutex_lock(table_schemas_mutex);
        table_schemas = g_list_prepend(table_schemas, dbt);
        g_mutex_unlock(table_schemas_mutex);
      }
    } else {
      if (!no_schemas) {
        g_mutex_lock(view_schemas_mutex);
        view_schemas = g_list_prepend(view_schemas, dbt);
        g_mutex_unlock(view_schemas_mutex);
      }
    }
  }

  mysql_free_result(result);

  // Store Procedures and Events
  // As these are not attached to tables we need to define when we need to dump
  // or not Having regex filter make this hard because we dont now if a full
  // schema is filtered or not Also I cant decide this based on tables from a
  // schema being dumped So I will use only regex to dump or not SP and EVENTS I
  // only need one match to dump all

  int post_dump = 0;

  if (dump_routines) {
    // SP
    query = g_strdup_printf("SHOW PROCEDURE STATUS WHERE Db = '%s'", database);
    if (mysql_query(conn, (query))) {
      g_critical("Error: DB: %s - Could not execute query: %s", database,
                 mysql_error(conn));
      errors++;
      g_free(query);
      return;
    }
    result = mysql_store_result(conn);
    while ((row = mysql_fetch_row(result)) && !post_dump) {
      /* Checks skip list on 'database.sp' string */
      if (tables_skiplist && check_skiplist(database, row[1]))
        continue;

      /* Checks PCRE expressions on 'database.sp' string */
      if (regexstring && !check_regex(database, row[1]))
        continue;

      post_dump = 1;
    }

    if (!post_dump) {
      // FUNCTIONS
      query = g_strdup_printf("SHOW FUNCTION STATUS WHERE Db = '%s'", database);
      if (mysql_query(conn, (query))) {
        g_critical("Error: DB: %s - Could not execute query: %s", database,
                   mysql_error(conn));
        errors++;
        g_free(query);
        return;
      }
      result = mysql_store_result(conn);
      while ((row = mysql_fetch_row(result)) && !post_dump) {
        /* Checks skip list on 'database.sp' string */
        if (tables_skiplist_file && check_skiplist(database, row[1]))
          continue;
        /* Checks PCRE expressions on 'database.sp' string */
        if (regexstring && !check_regex(database, row[1]))
          continue;

        post_dump = 1;
      }
    }
    mysql_free_result(result);
  }

  if (dump_events && !post_dump) {
    // EVENTS
    query = g_strdup_printf("SHOW EVENTS FROM `%s`", database);
    if (mysql_query(conn, (query))) {
      g_critical("Error: DB: %s - Could not execute query: %s", database,
                 mysql_error(conn));
      errors++;
      g_free(query);
      return;
    }
    result = mysql_store_result(conn);
    while ((row = mysql_fetch_row(result)) && !post_dump) {
      /* Checks skip list on 'database.sp' string */
      if (tables_skiplist_file && check_skiplist(database, row[1]))
        continue;
      /* Checks PCRE expressions on 'database.sp' string */
      if (regexstring && !check_regex(database, row[1]))
        continue;

      post_dump = 1;
    }
    mysql_free_result(result);
  }

  if (post_dump) {
    struct schema_post *sp = g_new(struct schema_post, 1);
    sp->database = g_strdup(database);
    schema_post = g_list_prepend(schema_post, sp);
  }

  g_free(query);

  return;
}

void get_tables(MYSQL *conn, struct configuration *conf) {

  gchar **dt = NULL;
  char *query = NULL;
  guint i, x;

  for (x = 0; tables[x] != NULL; x++) {
    dt = g_strsplit(tables[x], ".", 0);
    query =
        g_strdup_printf("SHOW TABLE STATUS FROM %s LIKE '%s'", dt[0], dt[1]);

    if (mysql_query(conn, (query))) {
      g_critical("Error: DB: %s - Could not execute query: %s", dt[0],
                 mysql_error(conn));
      errors++;
      return;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_FIELD *fields = mysql_fetch_fields(result);
    guint ecol = -1;
    guint ccol = -1;
    for (i = 0; i < mysql_num_fields(result); i++) {
      if (!strcasecmp(fields[i].name, "Engine"))
        ecol = i;
      else if (!strcasecmp(fields[i].name, "Comment"))
        ccol = i;
    }

    if (!result) {
      g_warning("Could not list table for %s.%s: %s", dt[0], dt[1],
                mysql_error(conn));
      errors++;
      return;
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {

      int is_view = 0;

      if ((detected_server == SERVER_TYPE_MYSQL) &&
          (row[ccol] == NULL || !strcmp(row[ccol], "VIEW")))
        is_view = 1;

      /* Green light! */
      struct db_table *dbt = g_new(struct db_table, 1);
      dbt->database = g_strdup(dt[0]);
      dbt->table = g_strdup(dt[1]);
      if (!row[6])
        dbt->datalength = 0;
      else
        dbt->datalength = g_ascii_strtoull(row[6], NULL, 10);
      if (!is_view) {
        if (trx_consistency_only) {
          dump_table(conn, dbt->database, dbt->table, conf, TRUE);
        } else if (!g_ascii_strcasecmp("InnoDB", row[ecol])) {
          g_mutex_lock(innodb_tables_mutex);
          innodb_tables = g_list_prepend(innodb_tables, dbt);
          g_mutex_unlock(innodb_tables_mutex);
        } else if (!g_ascii_strcasecmp("TokuDB", row[ecol])) {
          g_mutex_lock(innodb_tables_mutex);
          innodb_tables = g_list_prepend(innodb_tables, dbt);
          g_mutex_unlock(innodb_tables_mutex);
        } else {
          g_mutex_lock(non_innodb_table_mutex);
          non_innodb_table = g_list_prepend(non_innodb_table, dbt);
          g_mutex_unlock(non_innodb_table_mutex);
        }
        if (!no_schemas) {
          g_mutex_lock(table_schemas_mutex);
          table_schemas = g_list_prepend(table_schemas, dbt);
          g_mutex_unlock(table_schemas_mutex);
        }
      } else {
        if (!no_schemas) {
          g_mutex_lock(view_schemas_mutex);
          view_schemas = g_list_prepend(view_schemas, dbt);
          g_mutex_unlock(view_schemas_mutex);
        }
      }
    }
  }
  g_free(query);
}

void set_charset(GString *statement, char *character_set,
                 char *collation_connection) {
  g_string_printf(statement,
                  "SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\n");
  g_string_append(statement,
                  "SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\n");
  g_string_append(statement,
                  "SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\n");

  g_string_append_printf(statement, "SET character_set_client = %s;\n",
                         character_set);
  g_string_append_printf(statement, "SET character_set_results = %s;\n",
                         character_set);
  g_string_append_printf(statement, "SET collation_connection = %s;\n",
                         collation_connection);
}

void restore_charset(GString *statement) {
  g_string_append(statement,
                  "SET character_set_client = @PREV_CHARACTER_SET_CLIENT;\n");
  g_string_append(statement,
                  "SET character_set_results = @PREV_CHARACTER_SET_RESULTS;\n");
  g_string_append(statement,
                  "SET collation_connection = @PREV_COLLATION_CONNECTION;\n");
}

void dump_schema_post_data(MYSQL *conn, char *database, char *filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row;
  MYSQL_ROW row2;
  gchar **splited_st = NULL;

  if (!compress_output)
    outfile = g_fopen(filename, "w");
  else
    outfile = (void *)gzopen(filename, "w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  if (dump_routines) {
    // get functions
    query = g_strdup_printf("SHOW FUNCTION STATUS WHERE Db = '%s'", database);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      if (success_on_1146 && mysql_errno(conn) == 1146) {
        g_warning("Error dumping functions from %s: %s", database,
                  mysql_error(conn));
      } else {
        g_critical("Error dumping functions from %s: %s", database,
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
        g_critical("Could not write stored procedure data for %s.%s", database,
                   row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
      query =
          g_strdup_printf("SHOW CREATE FUNCTION `%s`.`%s`", database, row[1]);
      mysql_query(conn, query);
      result2 = mysql_store_result(conn);
      row2 = mysql_fetch_row(result2);
      g_string_printf(statement, "%s", row2[2]);
      splited_st = g_strsplit(statement->str, ";\n", 0);
      g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
      g_string_append(statement, ";\n");
      restore_charset(statement);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write function data for %s.%s", database, row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
    }

    // get sp
    query = g_strdup_printf("SHOW PROCEDURE STATUS WHERE Db = '%s'", database);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      if (success_on_1146 && mysql_errno(conn) == 1146) {
        g_warning("Error dumping stored procedures from %s: %s", database,
                  mysql_error(conn));
      } else {
        g_critical("Error dumping stored procedures from %s: %s", database,
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
        g_critical("Could not write stored procedure data for %s.%s", database,
                   row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
      query =
          g_strdup_printf("SHOW CREATE PROCEDURE `%s`.`%s`", database, row[1]);
      mysql_query(conn, query);
      result2 = mysql_store_result(conn);
      row2 = mysql_fetch_row(result2);
      g_string_printf(statement, "%s", row2[2]);
      splited_st = g_strsplit(statement->str, ";\n", 0);
      g_string_printf(statement, "%s", g_strjoinv("; \n", splited_st));
      g_string_append(statement, ";\n");
      restore_charset(statement);
      if (!write_data((FILE *)outfile, statement)) {
        g_critical("Could not write stored procedure data for %s.%s", database,
                   row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
    }
  }

  // get events
  if (dump_events) {
    query = g_strdup_printf("SHOW EVENTS FROM `%s`", database);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      if (success_on_1146 && mysql_errno(conn) == 1146) {
        g_warning("Error dumping events from %s: %s", database,
                  mysql_error(conn));
      } else {
        g_critical("Error dumping events from %s: %s", database,
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
        g_critical("Could not write stored procedure data for %s.%s", database,
                   row[1]);
        errors++;
        return;
      }
      query = g_strdup_printf("SHOW CREATE EVENT `%s`.`%s`", database, row[1]);
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
        g_critical("Could not write event data for %s.%s", database, row[1]);
        errors++;
        return;
      }
      g_string_set_size(statement, 0);
    }
  }

  g_free(query);

  if (!compress_output)
    fclose((FILE *)outfile);
  else
    gzclose((gzFile)outfile);

  g_string_free(statement, TRUE);
  g_strfreev(splited_st);
  if (result)
    mysql_free_result(result);
  if (result2)
    mysql_free_result(result2);

  return;
}
void dump_triggers_data(MYSQL *conn, char *database, char *table,
                        char *filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_RES *result2 = NULL;
  MYSQL_ROW row;
  MYSQL_ROW row2;
  gchar **splited_st = NULL;

  if (!compress_output)
    outfile = g_fopen(filename, "w");
  else
    outfile = (void *)gzopen(filename, "w");

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

  if (!compress_output)
    fclose((FILE *)outfile);
  else
    gzclose((gzFile)outfile);

  g_string_free(statement, TRUE);
  g_strfreev(splited_st);
  if (result)
    mysql_free_result(result);
  if (result2)
    mysql_free_result(result2);

  return;
}
void dump_schema_data(MYSQL *conn, char *database, char *table,
                      char *filename) {
  void *outfile;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;

  if (!compress_output)
    outfile = g_fopen(filename, "w");
  else
    outfile = (void *)gzopen(filename, "w");

  if (!outfile) {
    g_critical("Error: DB: %s Could not create output file %s (%d)", database,
               filename, errno);
    errors++;
    return;
  }

  GString *statement = g_string_sized_new(statement_size);

  if (detected_server == SERVER_TYPE_MYSQL) {
    g_string_printf(statement, "/*!40101 SET NAMES binary*/;\n");
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

  if (!compress_output)
    fclose((FILE *)outfile);
  else
    gzclose((gzFile)outfile);

  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  return;
}

void dump_view_data(MYSQL *conn, char *database, char *table, char *filename,
                    char *filename2) {
  void *outfile, *outfile2;
  char *query = NULL;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  GString *statement = g_string_sized_new(statement_size);

  mysql_select_db(conn, database);

  if (!compress_output) {
    outfile = g_fopen(filename, "w");
    outfile2 = g_fopen(filename2, "w");
  } else {
    outfile = (void *)gzopen(filename, "w");
    outfile2 = (void *)gzopen(filename2, "w");
  }

  if (!outfile || !outfile2) {
    g_critical("Error: DB: %s Could not create output file (%d)", database,
               errno);
    errors++;
    return;
  }

  if (detected_server == SERVER_TYPE_MYSQL) {
    g_string_printf(statement, "/*!40101 SET NAMES binary*/;\n");
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

  // we create myisam tables as workaround
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
  g_string_append_printf(statement, "CREATE TABLE `%s`(\n", table);
  row = mysql_fetch_row(result);
  g_string_append_printf(statement, "`%s` int", row[0]);
  while ((row = mysql_fetch_row(result))) {
    g_string_append(statement, ",\n");
    g_string_append_printf(statement, "`%s` int", row[0]);
  }
  g_string_append(statement, "\n)ENGINE=MyISAM;\n");

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

  if (!compress_output) {
    fclose((FILE *)outfile);
    fclose((FILE *)outfile2);
  } else {
    gzclose((gzFile)outfile);
    gzclose((gzFile)outfile2);
  }

  g_string_free(statement, TRUE);
  if (result)
    mysql_free_result(result);

  return;
}

void dump_table_data_file(MYSQL *conn, char *database, char *table, char *where,
                          char *filename) {
  void *outfile = NULL;

  if (!compress_output)
    outfile = g_fopen(filename, "w");
  else
    outfile = (void *)gzopen(filename, "w");

  if (!outfile) {
    g_critical("Error: DB: %s TABLE: %s Could not create output file %s (%d)",
               database, table, filename, errno);
    errors++;
    return;
  }
  guint64 rows_count =
      dump_table_data(conn, (FILE *)outfile, database, table, where, filename);

  if (!rows_count)
    g_message("Empty table %s.%s", database, table);
}

void dump_schema(MYSQL *conn, char *database, char *table,
                 struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct schema_job *sj = g_new0(struct schema_job, 1);
  j->job_data = (void *)sj;
  sj->database = g_strdup(database);
  sj->table = g_strdup(table);
  j->conf = conf;
  j->type = JOB_SCHEMA;
  if (daemon_mode)
    sj->filename = g_strdup_printf("%s/%d/%s.%s-schema.sql%s", output_directory,
                                   dump_number, database, table,
                                   (compress_output ? ".gz" : ""));
  else
    sj->filename =
        g_strdup_printf("%s/%s.%s-schema.sql%s", output_directory, database,
                        table, (compress_output ? ".gz" : ""));
  g_async_queue_push(conf->queue, j);

  if (dump_triggers) {
    char *query = NULL;
    MYSQL_RES *result = NULL;

    query =
        g_strdup_printf("SHOW TRIGGERS FROM `%s` LIKE '%s'", database, table);
    if (mysql_query(conn, query) || !(result = mysql_store_result(conn))) {
      g_critical("Error Checking triggers for %s.%s. Err: %s", database, table,
                 mysql_error(conn));
      errors++;
    } else {
      if (mysql_num_rows(result)) {
        struct job *t = g_new0(struct job, 1);
        struct schema_job *st = g_new0(struct schema_job, 1);
        t->job_data = (void *)st;
        st->database = g_strdup(database);
        st->table = g_strdup(table);
        t->conf = conf;
        t->type = JOB_TRIGGERS;
        if (daemon_mode)
          st->filename = g_strdup_printf(
              "%s/%d/%s.%s-schema-triggers.sql%s", output_directory,
              dump_number, database, table, (compress_output ? ".gz" : ""));
        else
          st->filename = g_strdup_printf("%s/%s.%s-schema-triggers.sql%s",
                                         output_directory, database, table,
                                         (compress_output ? ".gz" : ""));
        g_async_queue_push(conf->queue, t);
      }
    }
    g_free(query);
    if (result) {
      mysql_free_result(result);
    }
  }
  return;
}

void dump_view(char *database, char *table, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct view_job *vj = g_new0(struct view_job, 1);
  j->job_data = (void *)vj;
  vj->database = g_strdup(database);
  vj->table = g_strdup(table);
  j->conf = conf;
  j->type = JOB_VIEW;
  if (daemon_mode) {
    vj->filename = g_strdup_printf("%s/%d/%s.%s-schema.sql%s", output_directory,
                                   dump_number, database, table,
                                   (compress_output ? ".gz" : ""));
    vj->filename2 = g_strdup_printf("%s/%d/%s.%s-schema-view.sql%s",
                                    output_directory, dump_number, database,
                                    table, (compress_output ? ".gz" : ""));
  } else {
    vj->filename =
        g_strdup_printf("%s/%s.%s-schema.sql%s", output_directory, database,
                        table, (compress_output ? ".gz" : ""));
    vj->filename2 =
        g_strdup_printf("%s/%s.%s-schema-view.sql%s", output_directory,
                        database, table, (compress_output ? ".gz" : ""));
  }
  g_async_queue_push(conf->queue, j);
  return;
}

void dump_schema_post(char *database, struct configuration *conf) {
  struct job *j = g_new0(struct job, 1);
  struct schema_post_job *sp = g_new0(struct schema_post_job, 1);
  j->job_data = (void *)sp;
  sp->database = g_strdup(database);
  j->conf = conf;
  j->type = JOB_SCHEMA_POST;
  if (daemon_mode) {
    sp->filename =
        g_strdup_printf("%s/%d/%s-schema-post.sql%s", output_directory,
                        dump_number, database, (compress_output ? ".gz" : ""));
  } else {
    sp->filename = g_strdup_printf("%s/%s-schema-post.sql%s", output_directory,
                                   database, (compress_output ? ".gz" : ""));
  }
  g_async_queue_push(conf->queue, j);
  return;
}

void dump_table(MYSQL *conn, char *database, char *table,
                struct configuration *conf, gboolean is_innodb) {

  GList *chunks = NULL;
  if (rows_per_file)
    chunks = get_chunks_for_table(conn, database, table, conf);

  if (chunks) {
    int nchunk = 0;
    GList *iter;
    for (iter = chunks; iter != NULL; iter = iter->next) {
      struct job *j = g_new0(struct job, 1);
      struct table_job *tj = g_new0(struct table_job, 1);
      j->job_data = (void *)tj;
      tj->database = g_strdup(database);
      tj->table = g_strdup(table);
      j->conf = conf;
      j->type = is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
      if (daemon_mode)
        tj->filename = g_strdup_printf(
            "%s/%d/%s.%s.%05d.sql%s", output_directory, dump_number, database,
            table, nchunk, (compress_output ? ".gz" : ""));
      else
        tj->filename =
            g_strdup_printf("%s/%s.%s.%05d.sql%s", output_directory, database,
                            table, nchunk, (compress_output ? ".gz" : ""));
      tj->where = (char *)iter->data;
      if (!is_innodb && nchunk)
        g_atomic_int_inc(&non_innodb_table_counter);
      g_async_queue_push(conf->queue, j);
      nchunk++;
    }
    g_list_free(chunks);
  } else {
    struct job *j = g_new0(struct job, 1);
    struct table_job *tj = g_new0(struct table_job, 1);
    j->job_data = (void *)tj;
    tj->database = g_strdup(database);
    tj->table = g_strdup(table);
    j->conf = conf;
    j->type = is_innodb ? JOB_DUMP : JOB_DUMP_NON_INNODB;
    if (daemon_mode)
      tj->filename = g_strdup_printf(
          "%s/%d/%s.%s%s.sql%s", output_directory, dump_number, database, table,
          (chunk_filesize ? ".00001" : ""), (compress_output ? ".gz" : ""));
    else
      tj->filename = g_strdup_printf(
          "%s/%s.%s%s.sql%s", output_directory, database, table,
          (chunk_filesize ? ".00001" : ""), (compress_output ? ".gz" : ""));
    g_async_queue_push(conf->queue, j);
    return;
  }
}

void dump_tables(MYSQL *conn, GList *noninnodb_tables_list,
                 struct configuration *conf) {
  struct db_table *dbt;
  GList *chunks = NULL;

  struct job *j = g_new0(struct job, 1);
  struct tables_job *tjs = g_new0(struct tables_job, 1);
  j->conf = conf;
  j->type = JOB_LOCK_DUMP_NON_INNODB;
  j->job_data = (void *)tjs;

  GList *iter;
  for (iter = noninnodb_tables_list; iter != NULL; iter = iter->next) {
    dbt = (struct db_table *)iter->data;

    if (rows_per_file)
      chunks = get_chunks_for_table(conn, dbt->database, dbt->table, conf);

    if (chunks) {
      int nchunk = 0;
      GList *citer;
      for (citer = chunks; citer != NULL; citer = citer->next) {
        struct table_job *tj = g_new0(struct table_job, 1);
        tj->database = g_strdup_printf("%s", dbt->database);
        tj->table = g_strdup_printf("%s", dbt->table);
        if (daemon_mode)
          tj->filename =
              g_strdup_printf("%s/%d/%s.%s.%05d.sql%s", output_directory,
                              dump_number, dbt->database, dbt->table, nchunk,
                              (compress_output ? ".gz" : ""));
        else
          tj->filename = g_strdup_printf(
              "%s/%s.%s.%05d.sql%s", output_directory, dbt->database,
              dbt->table, nchunk, (compress_output ? ".gz" : ""));
        tj->where = (char *)citer->data;
        tjs->table_job_list = g_list_prepend(tjs->table_job_list, tj);
        nchunk++;
      }
      g_list_free(chunks);
    } else {
      struct table_job *tj = g_new0(struct table_job, 1);
      tj->database = g_strdup_printf("%s", dbt->database);
      tj->table = g_strdup_printf("%s", dbt->table);
      if (daemon_mode)
        tj->filename = g_strdup_printf("%s/%d/%s.%s%s.sql%s", output_directory,
                                       dump_number, dbt->database, dbt->table,
                                       (chunk_filesize ? ".00001" : ""),
                                       (compress_output ? ".gz" : ""));
      else
        tj->filename = g_strdup_printf(
            "%s/%s.%s%s.sql%s", output_directory, dbt->database, dbt->table,
            (chunk_filesize ? ".00001" : ""), (compress_output ? ".gz" : ""));
      tj->where = NULL;
      tjs->table_job_list = g_list_prepend(tjs->table_job_list, tj);
    }
  }
  tjs->table_job_list = g_list_reverse(tjs->table_job_list);
  g_async_queue_push(conf->queue_less_locking, j);
}

/* Do actual data chunk reading/writing magic */
guint64 dump_table_data(MYSQL *conn, FILE *file, char *database, char *table,
                        char *where, char *filename) {
  guint i;
  guint fn = 1;
  guint st_in_file = 0;
  guint num_fields = 0;
  guint64 num_rows = 0;
  guint64 num_rows_st = 0;
  MYSQL_RES *result = NULL;
  char *query = NULL;
  gchar *fcfile = NULL;
  gchar *filename_prefix = NULL;
  /* Buffer for escaping field values */
  GString *escaped = g_string_sized_new(3000);

  fcfile = g_strdup(filename);

  if (chunk_filesize) {
    gchar **split_filename = g_strsplit(filename, ".00001.sql", 0);
    filename_prefix = g_strdup(split_filename[0]);
    g_strfreev(split_filename);
  }

  gboolean has_generated_fields =
      detect_generated_fields(conn, database, table);

  /* Ghm, not sure if this should be statement_size - but default isn't too big
   * for now */
  GString *statement = g_string_sized_new(statement_size);
  GString *statement_row = g_string_sized_new(0);

  GString *select_fields;

  if (has_generated_fields) {
    select_fields = get_insertable_fields(conn, database, table);
  } else {
    select_fields = g_string_new("*");
  }

  /* Poor man's database code */
  query = g_strdup_printf(
      "SELECT %s %s FROM `%s`.`%s` %s %s",
      (detected_server == SERVER_TYPE_MYSQL) ? "/*!40001 SQL_NO_CACHE */" : "",
      select_fields->str, database, table, where ? "WHERE" : "",
      where ? where : "");
  g_string_free(select_fields, TRUE);
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    // ERROR 1146
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping table (%s.%s) data: %s ", database, table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping table (%s.%s) data: %s ", database, table,
                 mysql_error(conn));
      errors++;
    }
    goto cleanup;
  }

  num_fields = mysql_num_fields(result);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);

  MYSQL_ROW row;

  g_string_set_size(statement, 0);

  /* Poor man's data dump code */
  while ((row = mysql_fetch_row(result))) {
    gulong *lengths = mysql_fetch_lengths(result);
    num_rows++;

    if (!statement->len) {
      if (!st_in_file) {
        if (detected_server == SERVER_TYPE_MYSQL) {
          g_string_printf(statement, "/*!40101 SET NAMES binary*/;\n");
          g_string_append(statement, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n");
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

        if (!write_data(file, statement)) {
          g_critical("Could not write out data for %s.%s", database, table);
          goto cleanup;
        }
      }
      if (complete_insert || has_generated_fields) {
        if (insert_ignore) {
          g_string_printf(statement, "INSERT IGNORE INTO `%s` (", table);
        } else {
          g_string_printf(statement, "INSERT INTO `%s` (", table);
        }
        for (i = 0; i < num_fields; ++i) {
          if (i > 0) {
            g_string_append_c(statement, ',');
          }
          g_string_append_printf(statement, "`%s`", fields[i].name);
        }
        g_string_append(statement, ") VALUES");
      } else {
        if (insert_ignore) {
          g_string_printf(statement, "INSERT IGNORE INTO `%s` VALUES", table);
        } else {
          g_string_printf(statement, "INSERT INTO `%s` VALUES", table);
        }
      }
      num_rows_st = 0;
    }

    if (statement_row->len) {
      g_string_append(statement, statement_row->str);
      g_string_set_size(statement_row, 0);
      num_rows_st++;
    }

    g_string_append(statement_row, "\n(");

    for (i = 0; i < num_fields; i++) {
      /* Don't escape safe formats, saves some time */
      if (!row[i]) {
        g_string_append(statement_row, "NULL");
      } else if (fields[i].flags & NUM_FLAG) {
        g_string_append(statement_row, row[i]);
      } else {
        /* We reuse buffers for string escaping, growing is expensive just at
         * the beginning */
        g_string_set_size(escaped, lengths[i] * 2 + 1);
        mysql_real_escape_string(conn, escaped->str, row[i], lengths[i]);
        if (fields[i].type == MYSQL_TYPE_JSON)
          g_string_append(statement_row, "CONVERT(");
        g_string_append_c(statement_row, '\"');
        g_string_append(statement_row, escaped->str);
        g_string_append_c(statement_row, '\"');
        if (fields[i].type == MYSQL_TYPE_JSON)
          g_string_append(statement_row, " USING UTF8MB4)");
      }
      if (i < num_fields - 1) {
        g_string_append_c(statement_row, ',');
      } else {
        g_string_append_c(statement_row, ')');
        /* INSERT statement is closed before over limit */
        if (statement->len + statement_row->len + 1 > statement_size) {
          if (num_rows_st == 0) {
            g_string_append(statement, statement_row->str);
            g_string_set_size(statement_row, 0);
            g_warning("Row bigger than statement_size for %s.%s", database,
                      table);
          }
          g_string_append(statement, ";\n");

          if (!write_data(file, statement)) {
            g_critical("Could not write out data for %s.%s", database, table);
            goto cleanup;
          } else {
            st_in_file++;
            if (chunk_filesize &&
                st_in_file * (guint)ceil((float)statement_size / 1024 / 1024) >
                    chunk_filesize) {
              fn++;
              g_free(fcfile);
              fcfile = g_strdup_printf("%s.%05d.sql%s", filename_prefix, fn,
                                       (compress_output ? ".gz" : ""));
              if (!compress_output) {
                fclose((FILE *)file);
                file = g_fopen(fcfile, "w");
              } else {
                gzclose((gzFile)file);
                file = (void *)gzopen(fcfile, "w");
              }
              st_in_file = 0;
            }
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
    }
  }
  if (mysql_errno(conn)) {
    g_critical("Could not read data from %s.%s: %s", database, table,
               mysql_error(conn));
    errors++;
  }

  if (statement_row->len > 0) {
    /* this last row has not been written out */
    if (statement->len > 0) {
      /* strange, should not happen */
      g_string_append(statement, statement_row->str);
    } else {
      if (complete_insert) {
        if (insert_ignore) {
          g_string_printf(statement, "INSERT IGNORE INTO `%s` (", table);
        } else {
          g_string_printf(statement, "INSERT INTO `%s` (", table);
        }
        for (i = 0; i < num_fields; ++i) {
          if (i > 0) {
            g_string_append_c(statement, ',');
          }
          g_string_append_printf(statement, "`%s`", fields[i].name);
        }
        g_string_append(statement, ") VALUES");
      } else {
        if (insert_ignore) {
          g_string_printf(statement, "INSERT IGNORE INTO `%s` VALUES", table);
        } else {
          g_string_printf(statement, "INSERT INTO `%s` VALUES", table);
        }
      }
      g_string_append(statement, statement_row->str);
    }
  }

  if (statement->len > 0) {
    g_string_append(statement, ";\n");
    if (!write_data(file, statement)) {
      g_critical(
          "Could not write out closing newline for %s.%s, now this is sad!",
          database, table);
      goto cleanup;
    }
    st_in_file++;
  }

cleanup:
  g_free(query);

  g_string_free(escaped, TRUE);
  g_string_free(statement, TRUE);
  g_string_free(statement_row, TRUE);

  if (result) {
    mysql_free_result(result);
  }

  if (file) {
    if (!compress_output) {
      fclose((FILE *)file);
    } else {
      gzclose((gzFile)file);
    }
  }

  if (!st_in_file && !build_empty_files) {
    // dropping the useless file
    if (remove(fcfile)) {
      g_warning("Failed to remove empty file : %s\n", fcfile);
    }
  } else if (chunk_filesize && fn == 1) {
    g_free(fcfile);
    fcfile = g_strdup_printf("%s.sql%s", filename_prefix,
                             (compress_output ? ".gz" : ""));
    g_rename(filename, fcfile);
  }

  g_free(filename_prefix);
  g_free(fcfile);

  return num_rows;
}

gboolean write_data(FILE *file, GString *data) {
  size_t written = 0;
  ssize_t r = 0;

  while (written < data->len) {
    if (!compress_output)
      r = write(fileno(file), data->str + written, data->len);
    else
      r = gzwrite((gzFile)file, data->str + written, data->len);

    if (r < 0) {
      g_critical("Couldn't write data to a file: %s", strerror(errno));
      errors++;
      return FALSE;
    }
    written += r;
  }

  return TRUE;
}

void write_log_file(const gchar *log_domain, GLogLevelFlags log_level,
                    const gchar *message, gpointer user_data) {
  (void)log_domain;
  (void)user_data;

  gchar date[20];
  time_t rawtime;
  struct tm timeinfo;

  time(&rawtime);
  localtime_r(&rawtime, &timeinfo);
  strftime(date, 20, "%Y-%m-%d %H:%M:%S", &timeinfo);

  GString *message_out = g_string_new(date);
  if (log_level & G_LOG_LEVEL_DEBUG) {
    g_string_append(message_out, " [DEBUG] - ");
  } else if ((log_level & G_LOG_LEVEL_INFO) ||
             (log_level & G_LOG_LEVEL_MESSAGE)) {
    g_string_append(message_out, " [INFO] - ");
  } else if (log_level & G_LOG_LEVEL_WARNING) {
    g_string_append(message_out, " [WARNING] - ");
  } else if ((log_level & G_LOG_LEVEL_ERROR) ||
             (log_level & G_LOG_LEVEL_CRITICAL)) {
    g_string_append(message_out, " [ERROR] - ");
  }

  g_string_append_printf(message_out, "%s\n", message);
  if (write(fileno(logoutfile), message_out->str, message_out->len) <= 0) {
    fprintf(stderr, "Cannot write to log file with error %d.  Exiting...",
            errno);
  }
  g_string_free(message_out, TRUE);
}
