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
#include "common.h"
#include "mydumper_start_dump.h"
#include "server_detect.h"
#include "mydumper_chunks.h"
#include "mydumper_database.h"
#include "mydumper_jobs.h"
#include "mydumper_global.h"
#include "regex.h"
#include "mydumper_integer_chunks.h"


union chunk_step *new_multicolumn_integer_step(gchar *prefix, gchar *field, gboolean is_unsigned, union type type, guint deep, guint64 number){
  g_message("New Multi Integer Step");
  union chunk_step * cs = g_new0(union chunk_step, 1);
  cs->multicolumn_integer_step.is_unsigned = is_unsigned;
  cs->multicolumn_integer_step.prefix = prefix;
  if (cs->multicolumn_integer_step.is_unsigned){
    cs->multicolumn_integer_step.type.unsign.min = type.unsign.min;
    cs->multicolumn_integer_step.type.unsign.cursor = cs->multicolumn_integer_step.type.unsign.min;
    cs->multicolumn_integer_step.type.unsign.max = type.unsign.max;
    cs->multicolumn_integer_step.estimated_remaining_steps=(cs->multicolumn_integer_step.type.unsign.max - cs->multicolumn_integer_step.type.unsign.min);
  }else{
    cs->multicolumn_integer_step.type.sign.min = type.sign.min;
    cs->multicolumn_integer_step.type.sign.cursor = cs->multicolumn_integer_step.type.sign.min;
    cs->multicolumn_integer_step.type.sign.max = type.sign.max;
    cs->multicolumn_integer_step.estimated_remaining_steps=(cs->multicolumn_integer_step.type.sign.max - cs->multicolumn_integer_step.type.sign.min);
  }
  cs->multicolumn_integer_step.deep = deep;
  cs->multicolumn_integer_step.number = number;
  cs->multicolumn_integer_step.field = g_strdup(field);
  cs->multicolumn_integer_step.mutex = g_mutex_new();
  cs->multicolumn_integer_step.status = UNASSIGNED;
  return cs;
}


gchar * update_multicolumn_integer_where(struct thread_data *td, union chunk_step * cs){
  (void)td;
  gchar *where = g_strdup_printf("%s AND %s" , cs->multicolumn_integer_step.prefix, cs->multicolumn_integer_step.chunk_functions.update_where(td,cs->multicolumn_integer_step.next_chunk_step));
  
  return where;
}
