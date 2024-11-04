-- Tables and views
CREATE table IF NOT EXISTS mydumper_aipk_uuid (id int primary key auto_increment, val varchar(36));
INSERT into mydumper_aipk_uuid (val) values (uuid());
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;

CREATE TABLE `char_hex_test` (  `id` binary(16) NOT NULL,  `val` int(11) DEFAULT NULL,  PRIMARY KEY (`id`),  KEY `val` (`val`));
INSERT INTO char_hex_test values (unhex(replace(uuid(),'-','')), rand()*1000);
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;

INSERT INTO `char_hex_test` VALUES("ÈCY	e1í´\0\'_÷M",539);

CREATE TABLE t (qty INT, price INT);
INSERT INTO t VALUES(3, 50);
CREATE VIEW v AS SELECT qty, price, qty*price AS value FROM t;
CREATE TABLE t_w_g (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `val` VARCHAR(30) NOT NULL,   `short_val` VARCHAR(10) GENERATED ALWAYS AS (left(val,10)) STORED NOT NULL,   PRIMARY KEY (`id`)  );
INSERT into t_w_g (val) values ("asdfljlkjgsadklfjewtjgasd");

CREATE TABLE `table_varchar_pk` (
  `id` char(5) NOT NULL COLLATE latin1_bin,
  `val` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO `table_varchar_pk` VALUES(binary "csdfg",3);
INSERT INTO `table_varchar_pk` VALUES("bsdfg",3);
INSERT INTO `table_varchar_pk` VALUES("asdfg",3);
INSERT INTO `table_varchar_pk` VALUES("Ã–asdf",3);
INSERT INTO `table_varchar_pk` VALUES(binary "Asdfg",3);
INSERT INTO `table_varchar_pk` VALUES("Btdfg",3);
INSERT INTO `table_varchar_pk` VALUES("Csdfg",3);

-- Tables with special characters
CREATE table IF NOT EXISTS `mydumper_aipk_uuid_%` (id int primary key auto_increment, val varchar(36));
INSERT into `mydumper_aipk_uuid_%` (val) values (uuid());
CREATE table IF NOT EXISTS `mydumper_aipk_uuid_*` (id int primary key auto_increment, val varchar(36));
INSERT into `mydumper_aipk_uuid_*` (val) values (uuid());
CREATE table IF NOT EXISTS `mydumper_aipk_uuid_?` (id int primary key auto_increment, val varchar(36));
INSERT into `mydumper_aipk_uuid_?` (val) values (uuid());
CREATE table IF NOT EXISTS `mydumper_aipk_uuid_.` (id int primary key auto_increment, val varchar(36));
INSERT into `mydumper_aipk_uuid_.` (val) values (uuid());
CREATE table IF NOT EXISTS `mydumper.aipk_uuid` (id int primary key auto_increment, val varchar(36));
INSERT into `mydumper.aipk_uuid` (val) values (uuid());
CREATE table IF NOT EXISTS `mydumper/aipk_uuid` (id int primary key auto_increment, val varchar(36));
INSERT into `mydumper/aipk_uuid` (val) values (uuid());
DROP TABLE IF EXISTS `perftest`;
CREATE TABLE `perftest` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `val` varchar(108) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `val` (`val`(2)),
  KEY `val_2` (`val`(4)),
  KEY `val_3` (`val`(8))
) ENGINE=InnoDB;

DROP TABLE IF EXISTS `perftest_2`;
CREATE TABLE `perftest_2` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `val` varchar(108) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `val` (`val`(2)),
  KEY `val_2` (`val`(4)),
  KEY `val_3` (`val`(8))
) ENGINE=InnoDB;

DROP TABLE IF EXISTS `pertest_child`;
CREATE TABLE `pertest_child` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `pertest_child_ibfk_1` FOREIGN KEY (`id`) REFERENCES `perftest` (`id`)
) ENGINE=InnoDB;


DROP TABLE IF EXISTS `table_view_1_a`;
CREATE TABLE `table_view_1_a` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `val` varchar(108) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `val_1` (`val`(8))
) ENGINE=InnoDB;

DROP TABLE IF EXISTS `table_view_1_b`;
CREATE VIEW `table_view_1_b` AS SELECT
 1 AS `id`,
 1 AS `val` FROM table_view_1_a LIMIT 1;

DROP TABLE IF EXISTS `table_view_2_b`;
CREATE TABLE `table_view_2_b` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `val` varchar(108) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `val_1` (`val`(8))
) ENGINE=InnoDB;

DROP TABLE IF EXISTS `table_view_2_a`;
CREATE VIEW `table_view_2_a` AS SELECT
 1 AS `id`,
 1 AS `val` FROM table_view_2_b LIMIT 1;

-- Store procedures and functions

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` TRIGGER perftest_trigger  AFTER    UPDATE ON perftest    FOR EACH ROW      INSERT INTO perftest_2 VALUES (old.id,old.val) ;;
DELIMITER ;

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` FUNCTION `count_perftest_higher_than`(
    val INT
) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE my_count INT;

    SELECT count(*) into my_count FROM perftest where id > val;
    RETURN (my_count);
END ;;
DELIMITER ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `get_all_perftest`()
BEGIN
    SELECT * FROM perftest;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `get_all_table_view_function`()
BEGIN
    SELECT * FROM table_view_function;
END ;;
DELIMITER ;

CREATE TABLE tb(id int) COMMENT='VIEW';



DROP TABLE IF EXISTS `table_view_function`;
CREATE VIEW `table_view_function` AS SELECT
 1 AS `id`,
 1 AS `val`,
 count_perftest_higher_than(2) FROM table_view_2_b LIMIT 1;
