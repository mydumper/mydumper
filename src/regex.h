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
gboolean check_filename_regex(char *word);
gboolean eval_regex(char * a,char * b);
GOptionGroup * load_regex_entries(GOptionContext *context);
gboolean eval_partition_regex(char * word);
void initialize_regex(gchar * partition_regex);
void init_regex(pcre **r, const char *str);
gboolean eval_pcre_regex(pcre * p, char * word);
void free_regex();
