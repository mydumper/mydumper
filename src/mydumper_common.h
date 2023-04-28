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

void initialize_common();
gchar *get_ref_table(gchar *k);
char * determine_filename (char * table);
char * escape_string(MYSQL *conn, char *str);
gchar * build_schema_table_filename(char *database, char *table, const char *suffix);
gchar * build_schema_filename(const char *database, const char *suffix);
gchar * build_meta_filename(char *database, char *table, const char *suffix);
void set_charset(GString *statement, char *character_set,
                 char *collation_connection);
void restore_charset(GString *statement);
void clear_dump_directory(gchar *directory);
void set_transaction_isolation_level_repeatable_read(MYSQL *conn);
gchar * build_tablespace_filename();
gchar * build_filename(char *database, char *table, guint part, guint sub_part, const gchar *extension);
gchar * build_data_filename(char *database, char *table, guint part, guint sub_part);
gchar * build_fifo_filename(char *database, char *table, guint part, guint sub_part, const gchar *extesion);
gchar * build_stdout_filename(char *database, char *table, guint part, guint sub_part, const gchar *extension, gchar *second_extension);
gchar * build_load_data_filename(char *database, char *table, guint part, guint sub_part);
void determine_ecol_ccol(MYSQL_RES *result, guint *ecol, guint *ccol, guint *collcol);
unsigned long m_real_escape_string(MYSQL *conn, char *to, const gchar *from, unsigned long length);
void m_replace_char_with_char(gchar neddle, gchar replace, gchar *from, unsigned long length);
void m_escape_char_with_char(gchar neddle, gchar replace, gchar *to, unsigned long length);
void free_common();
void initialize_sql_statement(GString *statement);
void set_tidb_snapshot(MYSQL *conn);
