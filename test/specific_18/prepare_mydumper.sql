

DROP DATABASE IF EXISTS specific_18;
CREATE DATABASE specific_18;

USE specific_18;

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
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
-- INSERT into mydumper_aipk_uuid (val) SELECT uuid() from mydumper_aipk_uuid;
