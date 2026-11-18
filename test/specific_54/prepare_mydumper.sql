DROP DATABASE IF EXISTS specific_54;
CREATE DATABASE specific_54;
USE specific_54;
CREATE TABLE t (
  id INT NOT NULL PRIMARY KEY,
  a_latin1 TEXT CHARACTER SET latin1,
  a_utf8mb4 TEXT CHARACTER SET utf8mb4,
  a_blob BLOB
) ENGINE=InnoDB;
INSERT INTO t VALUES (1,'plain latin1 text','caf\xC3\xA9 text','blob one'),
(2,'second latin1 text','second utf8 text','blob two');
