DROP DATABASE IF EXISTS specific_55;
CREATE DATABASE specific_55;
USE specific_55;
-- myloader runs with no-schemas, so the table must exist before the helper
-- in pre_myloader.sh can lock one of its rows.
CREATE TABLE t (
  id INT NOT NULL PRIMARY KEY,
  pad VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- The helper transaction updates these rows first, so it is heavier than the
-- myloader transaction and InnoDB picks myloader as the deadlock victim.
DROP DATABASE IF EXISTS specific_55_helper;
CREATE DATABASE specific_55_helper;
CREATE TABLE specific_55_helper.weight (
  id INT NOT NULL PRIMARY KEY,
  v INT NOT NULL
) ENGINE=InnoDB;
-- No WITH RECURSIVE: it is not available on MySQL 5.7.
INSERT INTO specific_55_helper.weight
  SELECT a.n * 100 + b.n * 10 + c.n + 1, 0
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c;
