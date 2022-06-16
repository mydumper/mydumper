DROP DATABASE IF EXISTS myd_test;
CREATE DATABASE myd_test;
-- Tables and views
USE myd_test;
source mydumper_testing_db_template.sql ;

DROP DATABASE IF EXISTS myd_test_no_fk;
CREATE DATABASE myd_test_no_fk;
USE myd_test_no_fk;
source mydumper_testing_db_template.sql ;

ALTER TABLE myd_test_no_fk.pertest_child drop CONSTRAINT pertest_child_ibfk_1;
ALTER TABLE myd_test_no_fk.child drop CONSTRAINT child_ibfk_1;

-- Empty database
DROP DATABASE IF EXISTS empty_db;
CREATE DATABASE empty_db;

-- Tablespaces



-- Others
