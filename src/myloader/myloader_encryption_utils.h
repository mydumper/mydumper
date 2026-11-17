#if !defined(myloader_myloader_encryption_utils)
#define myloader_myloader_encryption_utils

#include <glib.h>

/*
  mydumper writes the view dependency table as
    CREATE TABLE IF NOT EXISTS `v`(...) ENGINE=MEMORY ENCRYPTION='N';
  MariaDB does not know the ENCRYPTION table option (ERROR 1911), so it has to
  be removed when the target is MariaDB (issue #2367).

  --remove-encryption-option controls it:
    AUTO   (default) remove it only when the target server is MariaDB
    ALWAYS remove it whatever the target server is (opt in)
    NEVER  keep the statement untouched (opt out)
*/
enum remove_encryption_option_mode
{
  REMOVE_ENCRYPTION_OPTION_AUTO,
  REMOVE_ENCRYPTION_OPTION_ALWAYS,
  REMOVE_ENCRYPTION_OPTION_NEVER
};

/* Parses the --remove-encryption-option value. A NULL value (option given
   without argument) means ALWAYS. Returns FALSE on an unknown value. */
gboolean parse_remove_encryption_option_mode(const gchar *value, enum remove_encryption_option_mode *mode);
const gchar *remove_encryption_option_mode_to_str(enum remove_encryption_option_mode mode);

/* Decides whether the option has to be removed for the given mode/target. */
gboolean should_remove_encryption_option(enum remove_encryption_option_mode mode, gboolean target_is_mariadb);

/*
  Removes the unversioned trailing ENCRYPTION='N' option of a CREATE TABLE
  statement. data may contain previous statements (e.g. SET NAMES),
  only the end of the buffer is inspected. Returns TRUE if data was changed.
  Its signature matches the table_option_modifier of
  global_process_create_table_statement().
*/
gboolean remove_encryption_option_from_create_table(GString *data);

#endif
