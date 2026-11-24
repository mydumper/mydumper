DROP DATABASE IF EXISTS specific_39;
CREATE DATABASE specific_39;

USE specific_39;

SET @version = SUBSTRING_INDEX(VERSION(), '-', 1);
SET @major = SUBSTRING_INDEX(@version, '.', 1) + 0;
SET @minor = SUBSTRING_INDEX(SUBSTRING_INDEX(@version, '.', 2), '.', -1) + 0;
SET @supports_uuid = VERSION() LIKE '%MariaDB%' AND (@major > 10 OR (@major = 10 AND @minor >= 7));
SET @create_table = IF(
  @supports_uuid,
  'CREATE TABLE uuid_test (id UUID NOT NULL PRIMARY KEY, val VARCHAR(32))',
  'CREATE TABLE uuid_test (id CHAR(36) NOT NULL PRIMARY KEY, val VARCHAR(32))'
);

PREPARE stmt FROM @create_table;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

INSERT INTO uuid_test VALUES
  ('00000000-0000-0000-0000-000000000001', 'row-1'),
  ('11111111-1111-4111-8111-111111111111', 'row-2'),
  ('abcdefab-cdef-4abc-8def-abcdefabcdef', 'row-3');
