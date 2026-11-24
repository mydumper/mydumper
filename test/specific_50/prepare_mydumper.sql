DROP DATABASE IF EXISTS specific_50;
CREATE DATABASE specific_50;
USE specific_50;

CREATE TABLE native_json (`doc` JSON);
CREATE TABLE quoted_json (`a``b` JSON);
CREATE TABLE plain_text (`doc` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);
CREATE TABLE checked_other (`doc` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, `other` JSON);
CREATE TABLE permissive_check (`doc` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  CHECK (JSON_VALID(`doc`) OR `doc` = 'not json'));
