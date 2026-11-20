DROP DATABASE IF EXISTS specific_39;
CREATE DATABASE specific_39;

USE specific_39;

SET @version = SUBSTRING_INDEX(VERSION(), '-', 1);
SET @major = SUBSTRING_INDEX(@version, '.', 1) + 0;
SET @minor = SUBSTRING_INDEX(SUBSTRING_INDEX(@version, '.', 2), '.', -1) + 0;
SET @maria = VERSION() LIKE '%MariaDB%';
SET @uuid_type = IF(@maria AND (@major > 10 OR (@major = 10 AND @minor >= 7)), 'UUID', 'CHAR(36)');
SET @inet4_type = IF(@maria AND (@major > 10 OR (@major = 10 AND @minor >= 10)), 'INET4', 'VARCHAR(15)');
SET @inet6_type = IF(@maria AND (@major > 10 OR (@major = 10 AND @minor >= 5)), 'INET6', 'VARCHAR(39)');
SET @create_table = CONCAT(
  'CREATE TABLE uuid_test (id ', @uuid_type, ' NOT NULL PRIMARY KEY, optional_id ', @uuid_type,
  ' NULL, ip4 ', @inet4_type, ', ip6 ', @inet6_type, ', raw_value VARBINARY(36), payload BLOB)'
);

PREPARE stmt FROM @create_table;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

INSERT INTO uuid_test VALUES
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
   '192.0.2.1', '2001:db8::1', 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 0x0041FF),
  ('cccccccc-cccc-4ccc-8ccc-cccccccccccc', NULL, NULL, '::1', 0x0000FF, 0xFF4100);
