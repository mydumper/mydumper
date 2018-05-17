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

    Authors: 	Taneli Leppä <taneli.leppae@rocket-internet.de>
*/

#ifndef _anonymizer_h
#define _anonymizer_h
#include "mydumper.h"
#include <mysql.h>

gboolean read_anonymizer_config(char *config_file);
GNode *get_table_anonymization(char *database, char *table);
gboolean should_truncate_table(GNode *table);
gboolean has_columns_to_anonymize(GNode *table);
void anonymize_table_columns(GNode *table, MYSQL_FIELD *fields, int num_fields, MYSQL_ROW row, gulong *lengths);
void free_anonymizer_config();


extern GNode *anonymizer_config;
extern gboolean anonymize;


#endif
