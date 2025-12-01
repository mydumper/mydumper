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
#define CONFIG "config"
#include <stdio.h>
#include "myloader_restore_job.h"
struct fifo{
  int pid;
  gchar *filename;
  gchar *stdout_filename;
  GMutex *mutex;
  gboolean uses_decompressor;  // Track if this file uses a decompression slot
};

void initialize_process(struct configuration *c);

void process_tablespace_filename( char * filename) ;
void process_database_filename(char * filename);

gboolean process_table_filename(char * filename);
gboolean process_schema_post_filename(gchar *filename, enum restore_job_statement_type object);
gboolean process_data_filename(char * filename);
gboolean process_schema_view_filename(gchar *filename);
gboolean process_schema_sequence_filename(gchar *filename);

void process_metadata_global_filename(gchar *file, GOptionContext * local_context);
FILE * myl_open(char *filename, const char *type);
void myl_close(const char *filename, FILE *file, gboolean rm);
