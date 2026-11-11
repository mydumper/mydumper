#if !defined(myloader_myloader_encryption_utils)
#define myloader_myloader_encryption_utils

#include <glib.h>

/*
  mydumper writes the view dependency table as
    CREATE TABLE IF NOT EXISTS `v`(...) ENGINE=MEMORY ENCRYPTION='N';
  MariaDB does not know the ENCRYPTION table option (ERROR 1911), so it has to
  be removed when the target is MariaDB. Only that unversioned trailing option
  of a CREATE TABLE statement is removed. Returns TRUE if data was changed.
*/
gboolean remove_encryption_option_from_create_table(GString *data);

#endif
