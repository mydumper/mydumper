

DROP DATABASE IF EXISTS specific_20;
CREATE DATABASE specific_20;

USE specific_20;

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
