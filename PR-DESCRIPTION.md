## Summary

Handle MariaDB native `UUID` columns as textual values when writing SQL dumps, so UUID literals are not emitted with the `_binary` introducer.

## Bug

MariaDB reports native `UUID` result metadata with `BINARY_FLAG`. `write_sql_column_into_string()` currently treats any non-numeric, non-hex-blob field with `BINARY_FLAG` as binary data and prefixes the literal with `_binary`.

That produces dump rows like:

```sql
INSERT INTO `uuid_test` VALUES (_binary '018f8c6d-...');
```

For MariaDB `UUID` columns, the value is already returned in textual UUID form. Adding `_binary` is unnecessary and can break reloads into native `UUID` columns.

## Fix

Add a guarded MariaDB Connector/C metadata helper:

- When Connector/C exposes `mariadb_field_attr()`, read `MARIADB_FIELD_ATTR_DATA_TYPE_NAME`.
- Treat fields whose extended data type name is `uuid` as non-binary for SQL dump literal formatting.
- Keep the existing `_binary` behavior for regular binary strings and blobs.
- Compile unchanged against MySQL clients or older MariaDB clients that do not expose the extended field attribute API.

## Test

New regression case `test/specific_39`:

- Creates a native `UUID` column on MariaDB 10.7+.
- Falls back to `CHAR(36)` on MySQL or older MariaDB so the general test suite can still run.
- Checks that SQL dumps for native UUID columns do not contain `_binary`.
