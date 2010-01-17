#ifndef _binlog_h
#define _binlog_h
#include "mydumper.h"

void get_binlogs(MYSQL *conn, struct configuration *conf);
void get_binlog_file(MYSQL *conn, char *binlog_file, guint64 start_position);
unsigned int get_event(const char *buf, unsigned int len);


#endif
