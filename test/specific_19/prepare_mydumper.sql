

DROP DATABASE IF EXISTS specific_19;
CREATE DATABASE specific_19;

USE specific_19;

CREATE TABLE `char_hex_test` (  `id` binary(16) NOT NULL,  `val` int(11) DEFAULT NULL,  PRIMARY KEY (`id`),  KEY `val` (`val`));
INSERT INTO char_hex_test values (unhex(replace(uuid(),'-','')), rand()*1000);
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
-- INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;
-- INSERT INTO char_hex_test SELECT unhex(replace(uuid(),'-','')), rand()*1000 from char_hex_test;

INSERT INTO `char_hex_test` VALUES("ÈCY e1^Qí<8d>´^H\0\'_÷M",539);
