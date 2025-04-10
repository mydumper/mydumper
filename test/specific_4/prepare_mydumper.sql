

DROP DATABASE IF EXISTS specific_4;
CREATE DATABASE specific_4;

USE specific_4;

CREATE TABLE `t_regex_partition` (
  `id` int NOT NULL AUTO_INCREMENT,
  `val` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (`id`)
(PARTITION p0 VALUES LESS THAN (6) ENGINE = InnoDB,
 PARTITION p1 VALUES LESS THAN (11) ENGINE = InnoDB,
 PARTITION p2 VALUES LESS THAN (16) ENGINE = InnoDB,
 PARTITION p3 VALUES LESS THAN (21) ENGINE = InnoDB) */
;

INSERT INTO t_regex_partition(val) values (1),(2),(3),(4),(5);
INSERT INTO t_regex_partition(val) values (1),(2),(3),(4),(5);
INSERT INTO t_regex_partition(val) values (1),(2),(3),(4),(5);
INSERT INTO t_regex_partition(val) values (1),(2),(3),(4),(5);
