DROP DATABASE IF EXISTS specific_42_db1;
DROP DATABASE IF EXISTS specific_42_db2;
DROP DATABASE IF EXISTS specific_42_db3;
CREATE DATABASE specific_42_db1;
CREATE DATABASE specific_42_db2;
CREATE DATABASE specific_42_db3;

CREATE TABLE specific_42_db1.table1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(32));
CREATE TABLE specific_42_db2.table1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(32));
CREATE TABLE specific_42_db3.table1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(32));

INSERT INTO specific_42_db1.table1 VALUES (1, 'db1');
INSERT INTO specific_42_db2.table1 VALUES (1, 'db2'), (2, 'db2');
INSERT INTO specific_42_db3.table1 VALUES (1, 'db3'), (2, 'db3'), (3, 'db3');
