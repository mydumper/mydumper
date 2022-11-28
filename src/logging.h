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

        Authors:        Domas Mituzas, Facebook ( domas at fb dot com )
                        Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
                        Andrew Hutchings, MariaDB Foundation (andrew at mariadb dot org)

*/

#ifndef LOGGING_H
#define LOGGING_H

// variables
extern gchar *logfile;
extern FILE *logoutfile;

// functions
void no_log(const gchar *log_domain, GLogLevelFlags log_level,
            const gchar *message, gpointer user_data);

void write_log_file(const gchar *log_domain, GLogLevelFlags log_level,
                    const gchar *message, gpointer user_data);

#endif
