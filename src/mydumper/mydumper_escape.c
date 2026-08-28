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
#include <string.h>
#include "mydumper_escape.h"

/* Second byte of the backslash escape sequence mysql_real_escape_string()
 * emits for each input byte, or 0 when the byte passes through verbatim. */
static const guchar sql_escape_second[256] = {
    [0]    = '0',
    ['\n'] = 'n',
    ['\r'] = 'r',
    [26]   = 'Z',
    ['\''] = '\'',
    ['"']  = '"',
    ['\\'] = '\\',
};

gboolean charset_is_single_byte(const char *csname)
{
  if (!csname)
    return FALSE;
  switch (csname[0])
  {
    case 'l':
      return strncmp(csname, "latin1", 6) == 0;
    case 'a':
      return strcmp(csname, "ascii") == 0;
    case 'b':
      return strcmp(csname, "binary") == 0;
  }
  return FALSE;
}

gboolean charset_is_utf8(const char *csname)
{
  return csname && strncmp(csname, "utf8", 4) == 0;
}

gsize fused_sql_escape(const gchar *src, gulong length, gchar *out)
{
  const guchar *p = (const guchar *)src;
  const guchar *end = p + length;
  gchar        *o = out;
  while (p < end)
  {
    guchar c = *p++;
    guchar sec = sql_escape_second[c];
    if (sec)
    {
      *o++ = '\\';
      *o++ = (gchar)sec;
    }
    else
      *o++ = (gchar)c;
  }
  *o = 0;
  return o - out;
}

gsize fused_load_data_escape(const gchar *src, gulong length, gchar *out, gchar esc, gchar term)
{
  const guchar *p = (const guchar *)src;
  const guchar *end = p + length;
  gchar        *o = out;
  while (p < end)
  {
    guchar c = *p++;
    guchar sec = sql_escape_second[c];
    if (sec)
    {
      /* the fixup pass rewrote every backslash byte to esc, including the
       * second byte of an escaped backslash */
      if ((gchar)sec == '\\')
        sec = (guchar)esc;
      /* the terminator pass inserts esc before every emitted term byte */
      if (esc == term)
        *o++ = esc;
      *o++ = esc;
      if ((gchar)sec == term)
        *o++ = esc;
      *o++ = (gchar)sec;
    }
    else
    {
      if ((gchar)c == term)
        *o++ = esc;
      *o++ = (gchar)c;
    }
  }
  *o = 0;
  return o - out;
}
