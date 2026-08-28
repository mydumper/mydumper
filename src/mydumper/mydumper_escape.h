/*
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef _src_mydumper_mydumper_escape_h
#define _src_mydumper_mydumper_escape_h

#include <glib.h>

/* Whether the connection character set is a single-byte superset of ASCII
 * (mbmaxlen == 1). For these the fused byte-wise escapers below produce
 * output identical to mysql_real_escape_string() and the LOAD DATA fixup
 * passes for any input bytes. */
gboolean charset_is_single_byte(const char *csname);

/* Whether the connection character set is in the utf8 family. Valid utf8
 * never contains an escape-relevant ASCII byte inside a multi-byte
 * character, so the fused escapers are byte-identical to
 * mysql_real_escape_string() for valid input; column data of non-binary
 * fields is validated by the server, binary fields must keep using
 * mysql_real_escape_string() because its handling of bytes that do not form
 * valid sequences differs. */
gboolean charset_is_utf8(const char *csname);

/* Escape src into out following the backslash conventions of
 * mysql_real_escape_string(). out must have room for length * 2 + 1 bytes.
 * Returns the escaped length; out is NUL terminated. */
gsize fused_sql_escape(const gchar *src, gulong length, gchar *out);

/* Escape src into out producing the exact byte stream of
 * mysql_real_escape_string() followed by the two LOAD DATA fixup passes
 * (backslash replaced by esc, and esc inserted before every term byte).
 * out must have room for length * 4 + 1 bytes. Returns the escaped length;
 * out is NUL terminated. */
gsize fused_load_data_escape(const gchar *src, gulong length, gchar *out, gchar esc, gchar term);

#endif
