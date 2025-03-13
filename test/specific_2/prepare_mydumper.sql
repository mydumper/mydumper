

DROP DATABASE IF EXISTS specific_1;
CREATE DATABASE specific_1;

USE specific_1;

CREATE TABLE t_innodb (id int primary key auto_increment, val int ) engine=InnoDB;
CREATE TABLE t_myisam (id int primary key auto_increment, val int ) engine=MyISAM;

INSERT INTO t_innodb (val) values (1),(2),(3),(4),(5);
INSERT INTO t_myisam (val) values (1),(2),(3),(4),(5);
