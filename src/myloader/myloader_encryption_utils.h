#if !defined(myloader_myloader_encryption_utils)
#define myloader_myloader_encryption_utils

#include <glib.h>

/*
  The encryption options are not portable between MySQL and MariaDB
  (issue #2367):
  - MySQL/Percona write the table option as ENCRYPTION='Y'|'N' (mydumper also
    adds ENCRYPTION='N' to the MEMORY view dependency tables) and the database
    option as /\*!80016 DEFAULT ENCRYPTION='N' *\/.
  - MariaDB writes the table options as `ENCRYPTED`=YES|NO|DEFAULT and
    `ENCRYPTION_KEY_ID`=n, and has no database option.
  Each server rejects the syntax of the other one.

  --encryption-option controls it:
    CONVERT (default) rewrite the options with the syntax of the target server:
            ENCRYPTION='Y'|'N' <-> `ENCRYPTED`=YES|NO. Options without
            equivalent are removed: ENCRYPTION on non InnoDB tables (MariaDB
            only supports it on InnoDB), `ENCRYPTED`=DEFAULT,
            `ENCRYPTION_KEY_ID` and DEFAULT ENCRYPTION on MariaDB.
    REMOVE  remove all the encryption options, so the defaults of the target
            server are used
    KEEP    keep the statements untouched
*/
enum encryption_option_mode
{
  ENCRYPTION_OPTION_CONVERT,
  ENCRYPTION_OPTION_REMOVE,
  ENCRYPTION_OPTION_KEEP
};

/* Signature of the table_option_modifier of global_process_create_table_statement(). */
typedef gboolean (*table_option_modifier_func)(GString *data);

/* Parses the --encryption-option value. Returns FALSE on an unknown value. */
gboolean     parse_encryption_option_mode(const gchar *value, enum encryption_option_mode *mode);
const gchar *encryption_option_mode_to_str(enum encryption_option_mode mode);

/* Return the modifier to apply to the CREATE TABLE / CREATE DATABASE
   statements for the given mode/target, or NULL when they must be kept
   untouched. */
table_option_modifier_func get_table_encryption_modifier(enum encryption_option_mode mode, gboolean target_is_mariadb);
table_option_modifier_func get_database_encryption_modifier(enum encryption_option_mode mode, gboolean target_is_mariadb);

/*
  CREATE TABLE modifiers. data may contain previous statements (e.g. SET
  NAMES) and may come without the ';' terminator. Only the table options (the
  line that closes the column definitions) are changed, quoted strings like
  COMMENT='...' are skipped. They return TRUE if data was changed.
*/
gboolean remove_table_encryption_options(GString *data);
gboolean convert_table_encryption_options_to_mariadb(GString *data);
gboolean convert_table_encryption_options_to_mysql(GString *data);

/* CREATE DATABASE modifier: removes DEFAULT ENCRYPTION, versioned or not. */
gboolean remove_database_encryption_option(GString *data);

#endif
