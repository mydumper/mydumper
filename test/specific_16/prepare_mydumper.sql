

DROP DATABASE IF EXISTS specific_16;
CREATE DATABASE specific_16;

USE specific_16;

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

CREATE TABLE tb(id int) COMMENT='VIEW';
